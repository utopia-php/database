<?php

namespace Tests\Unit\Authorization;

use Closure;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Utopia\Database\PermissionType;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;

use function Swoole\Coroutine\run;

final class CoroutineStatusTest extends TestCase
{
    private Authorization $authorization;

    #[\Override]
    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine status');
        }

        $this->authorization = new Authorization();
    }

    public function testDisableInACoroutineIsSeenByTheCoroutinesItStarts(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->disable();
            $done = new Channel(1);

            Coroutine::create(function () use (&$seen, $done): void {
                $seen['child'] = $this->authorization->getStatus();
                Coroutine::create(function () use (&$seen, $done): void {
                    Coroutine::sleep(0.001);
                    $seen['grandchild'] = $this->authorization->getStatus();
                    $done->push(true);
                });
            });

            $done->pop();
        });

        $this->assertSame(['child' => false, 'grandchild' => false], $seen);
        $this->assertFalse($this->authorization->getStatus());
    }

    public function testDisableInACoroutineWithoutAScopeIsSeenBySiblings(): void
    {
        $seen = null;

        $this->inCoroutine(function () use (&$seen): void {
            $disabled = new Channel(1);

            Coroutine::create(function () use ($disabled): void {
                $this->authorization->disable();
                $disabled->push(true);
            });

            Coroutine::create(function () use (&$seen, $disabled): void {
                $disabled->pop();
                $seen = $this->authorization->getStatus();
            });
        });

        $this->assertFalse($seen);
    }

    public function testSkipInOneSiblingChangesNeitherTheParentNorAnotherSibling(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $entered = new Channel(1);
            $released = new Channel(1);

            Coroutine::create(function () use (&$seen, $entered, $released): void {
                $this->authorization->skip(function () use (&$seen, $entered, $released): void {
                    $seen['skipping'] = $this->authorization->getStatus();
                    $entered->push(true);
                    $released->pop();
                });
            });

            $entered->pop();
            $seen['parent'] = $this->authorization->getStatus();

            Coroutine::create(function () use (&$seen, $released): void {
                $seen['sibling'] = $this->authorization->getStatus();
                $released->push(true);
            });
        });

        $this->assertSame(['skipping' => false, 'parent' => true, 'sibling' => true], $seen);
        $this->assertTrue($this->authorization->getStatus());
    }

    public function testOverlappingSkipsInSiblingsRestoreTheirOwnStatus(): void
    {
        $this->inCoroutine(function (): void {
            $first = new Channel(1);
            $second = new Channel(1);

            Coroutine::create(function () use ($first, $second): void {
                $this->authorization->skip(function () use ($first, $second): void {
                    $first->push(true);
                    $second->pop();
                });
            });

            Coroutine::create(function () use ($first, $second): void {
                $first->pop();
                $this->authorization->skip(function () use ($second): void {
                    $second->push(true);
                    Coroutine::sleep(0.001);
                });
            });
        });

        $this->assertTrue($this->authorization->getStatus());
    }

    public function testSkipIsSeenByTheCoroutinesItStarts(): void
    {
        $seen = null;

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->skip(function () use (&$seen): void {
                $done = new Channel(1);
                Coroutine::create(function () use (&$seen, $done): void {
                    $seen = $this->authorization->getStatus();
                    $done->push(true);
                });
                $done->pop();
            });
        });

        $this->assertFalse($seen);
        $this->assertTrue($this->authorization->getStatus());
    }

    public function testStatusChangesInsideASkipLastUntilTheSkipEnds(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->disable();
            $this->authorization->skip(function () use (&$seen): void {
                $this->authorization->enable();
                $seen['enabled'] = $this->authorization->getStatus();
                $this->authorization->reset();
                $seen['reset'] = $this->authorization->getStatus();
            });
            $seen['after'] = $this->authorization->getStatus();
        });

        $this->assertSame(['enabled' => true, 'reset' => true, 'after' => false], $seen);
    }

    public function testResetInACoroutineStartedInsideASkipRestoresTheCheckForThatCoroutineOnly(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->skip(function () use (&$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use (&$seen, $done): void {
                    $this->authorization->reset();
                    $seen['child'] = $this->authorization->getStatus();
                    $seen['childValid'] = $this->authorization->isValid(new Input(PermissionType::Read, ['user:x']));
                    $done->push(true);
                });

                $done->pop();
                $seen['parent'] = $this->authorization->getStatus();
            });

            $seen['after'] = $this->authorization->getStatus();
        });

        $this->assertSame(['child' => true, 'childValid' => false, 'parent' => false, 'after' => true], $seen);
    }

    public function testDisableInACoroutineStartedInsideASkipEndsWithTheSkip(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->skip(function (): void {
                $done = new Channel(1);

                Coroutine::create(function () use ($done): void {
                    $this->authorization->reset();
                    $this->authorization->disable();
                    $done->push(true);
                });

                $done->pop();
            });

            $seen['after'] = $this->authorization->getStatus();

            Coroutine::create(function () use (&$seen): void {
                $seen['unrelated'] = $this->authorization->getStatus();
            });
        });

        $this->assertSame(['after' => true, 'unrelated' => true], $seen);
        $this->assertTrue($this->authorization->getStatus());
    }

    public function testEnableInACoroutineStartedInsideASkipOfADisabledCheckStaysInThatCoroutine(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->disable();
            $this->authorization->skip(function () use (&$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use (&$seen, $done): void {
                    $this->authorization->enable();
                    $seen['child'] = $this->authorization->getStatus();
                    $done->push(true);
                });

                $done->pop();
                $seen['parent'] = $this->authorization->getStatus();
            });

            $seen['after'] = $this->authorization->getStatus();
        });

        $this->assertSame(['child' => true, 'parent' => false, 'after' => false], $seen);
    }

    public function testACloneStartsFromTheCurrentStatusAndKeepsItsOwn(): void
    {
        $clone = $this->authorization->skip(fn (): Authorization => clone $this->authorization);

        $this->assertFalse($clone->getStatus());
        $this->assertTrue($this->authorization->getStatus());

        $clone->enable();
        $this->authorization->disable();

        $this->assertTrue($clone->getStatus());
        $this->assertFalse($this->authorization->getStatus());
    }

    private function inCoroutine(Closure $test): void
    {
        $hookFlags = Runtime::getHookFlags();

        try {
            run($test);
        } finally {
            Runtime::setHookFlags($hookFlags);
        }
    }
}
