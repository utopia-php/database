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

        $this->assertSame('failed', $thrown->getMessage());
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

    public function testAWriteInACoroutineStartedInsideAnOverrideStaysInThatCoroutine(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $written = new Channel(1);
                $released = new Channel(1);

                Coroutine::create(function () use ($value, &$seen, $written, $released): void {
                    $value->set('child');
                    $seen['child'] = $value->get();
                    $written->push(true);
                    $released->pop();
                });

                $written->pop();
                $seen['parent'] = $value->get();

                $siblingDone = new Channel(1);
                Coroutine::create(function () use ($value, &$seen, $siblingDone): void {
                    $seen['sibling'] = $value->get();
                    $siblingDone->push(true);
                });
                $siblingDone->pop();
                $released->push(true);
            });

            Coroutine::create(function () use ($value, &$seen): void {
                $seen['unrelated'] = $value->get();
            });
        });

        $this->assertSame(
            ['child' => 'child', 'parent' => 'override', 'sibling' => 'override', 'unrelated' => 'handle'],
            $seen,
        );
        $this->assertSame('handle', $value->get());
    }

    public function testAWriteInACoroutineStartedInsideAnOverrideEndsWithThatOverride(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $written = new Channel(1);
            $closed = new Channel(1);
            $done = new Channel(1);

            $value->with('override', function () use ($value, &$seen, $written, $closed, $done): void {
                Coroutine::create(function () use ($value, &$seen, $written, $closed, $done): void {
                    $value->set('child');
                    $written->push(true);
                    $closed->pop();
                    $seen['afterScope'] = $value->get();
                    $done->push(true);
                });

                $written->pop();
            });

            $closed->push(true);
            $done->pop();
            $seen['parent'] = $value->get();
        });

        $this->assertSame(['afterScope' => 'handle', 'parent' => 'handle'], $seen);
    }

    public function testAWriteInACoroutineStartedInsideAnOverrideIsSeenByTheCoroutinesItStarts(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use ($value, &$seen, $done): void {
                    $value->set('child');
                    $grandchildDone = new Channel(1);

                    Coroutine::create(function () use ($value, &$seen, $grandchildDone): void {
                        $seen['grandchild'] = $value->get();
                        $value->set('grandchild');
                        $seen['grandchildAfterWrite'] = $value->get();
                        $grandchildDone->push(true);
                    });

                    $grandchildDone->pop();
                    $seen['child'] = $value->get();
                    $done->push(true);
                });

                $done->pop();
                $seen['parent'] = $value->get();
            });
        });

        $this->assertSame(
            ['grandchild' => 'child', 'grandchildAfterWrite' => 'grandchild', 'child' => 'child', 'parent' => 'override'],
            $seen,
        );
        $this->assertSame('handle', $value->get());
    }

    public function testAWriteInACoroutineUnderAnOverrideOpenedOutsideCoroutinesStaysInThatCoroutine(): void
    {
        $value = new Value('handle');
        $seen = [];

        $value->with('override', function () use ($value, &$seen): void {
            $this->inCoroutine(function () use ($value, &$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use ($value, &$seen, $done): void {
                    $value->set('child');
                    $seen['child'] = $value->get();
                    $done->push(true);
                });

                $done->pop();
                $seen['parent'] = $value->get();
            });

            $seen['outside'] = $value->get();
        });

        $this->assertSame(['child' => 'child', 'parent' => 'override', 'outside' => 'override'], $seen);
        $this->assertSame('handle', $value->get());
    }

    public function testANullWriteInACoroutineStartedInsideAnOverrideIsKept(): void
    {
        /** @var Value<string|null> $value */
        $value = new Value('handle');
        $seen = null;

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use ($value, &$seen, $done): void {
                    $value->set(null);
                    $seen = [$value->get()];
                    $done->push(true);
                });

                $done->pop();
            });
        });

        $this->assertSame([null], $seen);
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

    public function testAWriteInACoroutineWhoseStarterHasReturnedStaysInThatCoroutine(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $done = new Channel(1);

                $this->startDetached(function () use ($value, &$seen, $done): void {
                    $seen['before'] = $value->get();
                    $value->set('first');
                    $value->set('second');
                    $seen['after'] = $value->get();

                    $childDone = new Channel(1);
                    Coroutine::create(function () use ($value, &$seen, $childDone): void {
                        $seen['child'] = $value->get();
                        $childDone->push(true);
                    });
                    $childDone->pop();
                    $done->push(true);
                });

                $done->pop();
                $seen['owner'] = $value->get();
            });

            $unrelatedDone = new Channel(1);
            Coroutine::create(function () use ($value, &$seen, $unrelatedDone): void {
                $seen['unrelated'] = $value->get();
                $unrelatedDone->push(true);
            });
            $unrelatedDone->pop();
        });

        $this->assertSame([
            'before' => 'handle',
            'after' => 'second',
            'child' => 'second',
            'owner' => 'override',
            'unrelated' => 'handle',
        ], $seen);
        $this->assertSame('handle', $value->get());
    }

    public function testAWriteInACoroutineWhoseStarterHasReturnedEndsWithThatCoroutine(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $exited = new Channel(1);

                $this->startDetached(function () use ($value, $exited): void {
                    Coroutine::defer(static fn () => $exited->push(true));
                    $value->set('detached');
                });

                $exited->pop();

                $childDone = new Channel(1);
                Coroutine::create(function () use ($value, &$seen, $childDone): void {
                    $seen['child'] = $value->get();
                    $childDone->push(true);
                });
                $childDone->pop();
            });

            $seen['afterScope'] = $value->get();

            $written = new Channel(1);
            Coroutine::create(function () use ($value, $written): void {
                $value->set('later');
                $written->push(true);
            });
            $written->pop();
        });

        $this->assertSame(['child' => 'override', 'afterScope' => 'handle'], $seen);
        $this->assertSame('later', $value->get());
    }

    public function testAWriteInACoroutineWhoseStarterHasReturnedKeepsItsValueAcrossItsOwnOverride(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $value->with('override', function () use ($value, &$seen): void {
                $done = new Channel(1);

                $this->startDetached(function () use ($value, &$seen, $done): void {
                    $value->set('detached');
                    $seen['inner'] = $value->with('inner', static fn (): string => $value->get());
                    $seen['afterInner'] = $value->get();
                    $done->push(true);
                });

                $done->pop();
            });
        });

        $this->assertSame(['inner' => 'inner', 'afterInner' => 'detached'], $seen);
        $this->assertSame('handle', $value->get());
    }

    public function testADetachedWriteAfterEveryOverrideEndedChangesTheSharedValueWhileAnOrphanKeepsItsOwn(): void
    {
        $value = new Value('handle');
        $seen = [];

        $this->inCoroutine(function () use ($value, &$seen): void {
            $written = new Channel(1);
            $released = new Channel(1);
            $done = new Channel(1);

            $value->with('override', function () use ($value, &$seen, $written, $released, $done): void {
                $this->startDetached(function () use ($value, &$seen, $written, $released, $done): void {
                    $value->set('orphan');
                    $written->push(true);
                    $released->pop();
                    $seen['orphan'] = $value->get();
                    $done->push(true);
                });

                $written->pop();
            });

            $shared = new Channel(1);
            $this->startDetached(function () use ($value, $shared): void {
                $value->set('shared');
                $shared->push(true);
            });
            $shared->pop();
            $seen['owner'] = $value->get();

            $released->push(true);
            $done->pop();
        });

        $this->assertSame(['owner' => 'shared', 'orphan' => 'orphan'], $seen);
        $this->assertSame('shared', $value->get());
    }

    public function testAWriteInACoroutineWhoseStarterHasReturnedUnderAnOverrideOpenedOutsideCoroutinesStaysInThatCoroutine(): void
    {
        $value = new Value('handle');
        $seen = [];

        $value->with('override', function () use ($value, &$seen): void {
            $this->inCoroutine(function () use ($value, &$seen): void {
                $done = new Channel(1);

                $this->startDetached(function () use ($value, &$seen, $done): void {
                    $seen['before'] = $value->get();
                    $value->set('child');
                    $seen['after'] = $value->get();
                    $done->push(true);
                });

                $done->pop();
                $seen['parent'] = $value->get();
            });

            $seen['outside'] = $value->get();
        });

        $this->assertSame(
            ['before' => 'override', 'after' => 'child', 'parent' => 'override', 'outside' => 'override'],
            $seen,
        );
        $this->assertSame('handle', $value->get());
    }

    public function testAWriteInACoroutineWhoseStarterHasReturnedWithoutAnyOverrideChangesTheHandleWideValue(): void
    {
        $value = new Value('handle');

        $this->inCoroutine(function () use ($value): void {
            $this->startDetached(static fn () => $value->set('changed'));
        });

        $this->assertSame('changed', $value->get());
    }

    /**
     * Runs the work in a coroutine whose parent has already returned.
     */
    private function startDetached(Closure $work): void
    {
        Coroutine::create(static function () use ($work): void {
            Coroutine::create(static function () use ($work): void {
                Coroutine::sleep(0.01);
                $work();
            });
        });
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
