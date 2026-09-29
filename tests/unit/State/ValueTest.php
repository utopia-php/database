<?php

namespace Tests\Unit\State;

use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Utopia\Database\State\Value;

use function Swoole\Coroutine\run;

final class ValueTest extends TestCase
{
    public function testAnOverrideOutsideACoroutineLastsForItsCallback(): void
    {
        $value = new Value('handle');

        $inside = $value->with('override', function () use ($value): array {
            $seen = [$value->get()];
            $value->set('changed');
            $seen[] = $value->get();

            return $seen;
        });

        $this->assertSame(['override', 'changed'], $inside);
        $this->assertSame('handle', $value->get());
    }

    public function testAWriteOutsideAnOverrideChangesTheHandleWideValue(): void
    {
        $value = new Value('handle');

        $value->set('changed');

        $this->assertSame('changed', $value->get());
    }

    public function testAnOverrideIsRemovedWhenItsCallbackThrows(): void
    {
        $value = new Value('handle');

        $thrown = null;
        try {
            $value->with('override', static fn (): never => throw new RuntimeException('failed'));
        } catch (RuntimeException $error) {
            $thrown = $error;
        }

        $this->assertInstanceOf(RuntimeException::class, $thrown);
        $this->assertSame('handle', $value->get());
    }

    public function testValuesKeepTheirOwnOverrides(): void
    {
        $first = new Value('first');
        $second = new Value('second');

        $seen = $first->with('override', static fn (): array => [$first->get(), $second->get()]);

        $this->assertSame(['override', 'second'], $seen);
    }

    public function testAnOverrideIsSeenByTheCoroutinesItStartsButNotBySiblingsOrTheParent(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $entered = new Channel(1);
            $released = new Channel(1);
            $childDone = new Channel(1);

            Coroutine::create(function () use ($value, &$seen, $entered, $released, $childDone): void {
                $value->with('override', function () use ($value, &$seen, $entered, $released, $childDone): void {
                    Coroutine::create(function () use ($value, &$seen, $childDone): void {
                        Coroutine::sleep(0.001);
                        $seen['child'] = $value->get();
                        $childDone->push(true);
                    });
                    $childDone->pop();
                    $entered->push(true);
                    $released->pop();
                });
            });

            $entered->pop();
            $seen['parent'] = $value->get();

            Coroutine::create(function () use ($value, &$seen, $released): void {
                $seen['sibling'] = $value->get();
                $released->push(true);
            });
        });

        $this->assertSame(['child' => 'override', 'parent' => 'handle', 'sibling' => 'handle'], $seen);
    }

    public function testAWriteInsideACoroutinesOverrideStaysInThatOverride(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $value->set('changed');
                $seen['inside'] = $value->get();
            });
            $seen['after'] = $value->get();
        });

        $this->assertSame(['inside' => 'changed', 'after' => 'handle'], $seen);
    }

    public function testAWriteInACoroutineWithoutAnOverrideChangesTheHandleWideValue(): void
    {
        $value = new Value('handle');

        $this->inCoroutine(function () use ($value): void {
            Coroutine::create(static fn () => $value->set('changed'));
        });

        $this->assertSame('changed', $value->get());
    }

    public function testAChildsOverrideLeavesItsParentUnchanged(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('parent', function () use ($value, &$seen): void {
                $done = new Channel(1);
                Coroutine::create(function () use ($value, &$seen, $done): void {
                    $value->with('child', function () use ($value, &$seen): void {
                        Coroutine::sleep(0.001);
                        $seen['child'] = $value->get();
                    });
                    $done->push(true);
                });
                $seen['parent'] = $value->get();
                $done->pop();
                $seen['parentAfter'] = $value->get();
            });
        });

        $this->assertSame(['parent' => 'parent', 'child' => 'child', 'parentAfter' => 'parent'], $seen);
    }

    public function testAnOverrideOpenedOutsideCoroutinesIsSeenInsideThem(): void
    {
        $value = new Value('handle');
        $seen = null;

        $value->with('override', function () use ($value, &$seen): void {
            $this->inCoroutine(function () use ($value, &$seen): void {
                $seen = $value->get();
            });
        });

        $this->assertSame('override', $seen);
        $this->assertSame('handle', $value->get());
    }

    private function inCoroutine(Closure $test): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine scopes');
        }

        $hookFlags = Runtime::getHookFlags();

        try {
            run($test);
        } finally {
            Runtime::setHookFlags($hookFlags);
        }
    }
}
