<?php

namespace Tests\Unit\Authorization;

use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Utopia\Database\PermissionType;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;

use function Swoole\Coroutine\run;

final class CoroutineRolesTest extends TestCase
{
    private const string ALICE = 'user:alice';

    private Authorization $authorization;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
    }

    public function testWithRolesReplacesTheRolesForItsCallback(): void
    {
        $seen = $this->authorization->withRoles([self::ALICE], fn (): array => [
            $this->authorization->getRoles(),
            $this->authorization->hasRole('any'),
            $this->authorization->isValid(new Input(PermissionType::Read, [self::ALICE])),
            $this->authorization->isValid(new Input(PermissionType::Read, ['any'])),
        ]);

        $this->assertSame([[self::ALICE], false, true, false], $seen);
        $this->assertSame(['any'], $this->authorization->getRoles());
    }

    public function testRoleChangesInsideWithRolesLastUntilItEnds(): void
    {
        $seen = $this->authorization->withRoles([self::ALICE], function (): array {
            $this->authorization->addRole('team:blue');
            $this->authorization->removeRole(self::ALICE);
            $changed = $this->authorization->getRoles();
            $this->authorization->cleanRoles();

            return [$changed, $this->authorization->getRoles()];
        });

        $this->assertSame([['team:blue'], []], $seen);
        $this->assertSame(['any'], $this->authorization->getRoles());
    }

    public function testWithRolesRestoresTheRolesWhenItsCallbackThrows(): void
    {
        $thrown = null;
        try {
            $this->authorization->withRoles([self::ALICE], static fn (): never => throw new RuntimeException('failed'));
        } catch (RuntimeException $error) {
            $thrown = $error->getMessage();
        }

        $this->assertSame('failed', $thrown);
        $this->assertSame(['any'], $this->authorization->getRoles());
    }

    public function testWithNoRolesDeniesEveryPermission(): void
    {
        $valid = $this->authorization->withRoles([], fn (): bool => $this->authorization->isValid(
            new Input(PermissionType::Read, ['any']),
        ));

        $this->assertFalse($valid);
    }

    public function testWithRolesInOneSiblingIsSeenByItsChildrenButNotByTheParentOrAnotherSibling(): void
    {
        $this->skipWithoutCoroutines();
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $entered = new Channel(1);
            $released = new Channel(1);
            $closed = new Channel(1);

            Coroutine::create(function () use (&$seen, $entered, $released, $closed): void {
                $this->authorization->withRoles([self::ALICE], function () use (&$seen, $entered, $released): void {
                    $childDone = new Channel(1);
                    Coroutine::create(function () use (&$seen, $childDone): void {
                        Coroutine::sleep(0.001);
                        $seen['child'] = $this->authorization->getRoles();
                        $childDone->push(true);
                    });
                    $childDone->pop();
                    $entered->push(true);
                    $released->pop();
                    $seen['insideAfterSibling'] = $this->authorization->getRoles();
                });
                $closed->push(true);
            });

            $entered->pop();
            $seen['parent'] = $this->authorization->getRoles();

            Coroutine::create(function () use (&$seen, $released): void {
                $this->authorization->addRole('team:blue');
                $seen['sibling'] = $this->authorization->getRoles();
                $released->push(true);
            });

            $closed->pop();
            $seen['after'] = $this->authorization->getRoles();
        });

        $this->assertSame([
            'child' => [self::ALICE],
            'parent' => ['any'],
            'sibling' => ['any', 'team:blue'],
            'insideAfterSibling' => [self::ALICE],
            'after' => ['any', 'team:blue'],
        ], $seen);
    }

    public function testOverlappingWithRolesInSiblingsLeaveTheSharedRoles(): void
    {
        $this->skipWithoutCoroutines();

        $this->inCoroutine(function (): void {
            $first = new Channel(1);
            $second = new Channel(1);

            Coroutine::create(function () use ($first, $second): void {
                $this->authorization->withRoles([self::ALICE], function () use ($first, $second): void {
                    $first->push(true);
                    $second->pop();
                });
            });

            Coroutine::create(function () use ($first, $second): void {
                $first->pop();
                $this->authorization->withRoles(['team:blue'], function () use ($second): void {
                    $second->push(true);
                    Coroutine::sleep(0.001);
                });
            });
        });

        $this->assertSame(['any'], $this->authorization->getRoles());
    }

    public function testRoleChangesInACoroutineStartedInsideWithRolesStayInThatCoroutine(): void
    {
        $this->skipWithoutCoroutines();
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->withRoles([self::ALICE], function () use (&$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use (&$seen, $done): void {
                    $this->authorization->addRole('team:admins');
                    $seen['childAfterAdd'] = $this->authorization->getRoles();
                    $this->authorization->removeRole(self::ALICE);
                    $seen['childAfterRemove'] = $this->authorization->getRoles();
                    $done->push(true);
                });

                $done->pop();
                $seen['parent'] = $this->authorization->getRoles();
            });

            $seen['after'] = $this->authorization->getRoles();

            Coroutine::create(function () use (&$seen): void {
                $seen['unrelated'] = $this->authorization->getRoles();
            });
        });

        $this->assertSame([
            'childAfterAdd' => [self::ALICE, 'team:admins'],
            'childAfterRemove' => ['team:admins'],
            'parent' => [self::ALICE],
            'after' => ['any'],
            'unrelated' => ['any'],
        ], $seen);
        $this->assertSame(['any'], $this->authorization->getRoles());
    }

    public function testCleanRolesInACoroutineStartedInsideWithRolesLeavesTheSharedRoles(): void
    {
        $this->skipWithoutCoroutines();
        $seen = null;

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->withRoles([self::ALICE], function () use (&$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use (&$seen, $done): void {
                    $this->authorization->cleanRoles();
                    $seen = $this->authorization->isValid(new Input(PermissionType::Read, [self::ALICE]));
                    $done->push(true);
                });

                $done->pop();
            });
        });

        $this->assertFalse($seen);
        $this->assertSame(['any'], $this->authorization->getRoles());
    }

    public function testACloneStartsFromTheCurrentRolesAndKeepsItsOwn(): void
    {
        $clone = $this->authorization->withRoles([self::ALICE], fn (): Authorization => clone $this->authorization);

        $this->assertSame([self::ALICE], $clone->getRoles());
        $this->assertSame(['any'], $this->authorization->getRoles());

        $clone->addRole('team:blue');
        $this->authorization->cleanRoles();

        $this->assertSame([self::ALICE, 'team:blue'], $clone->getRoles());
        $this->assertSame([], $this->authorization->getRoles());
    }

    private function skipWithoutCoroutines(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine-scoped roles');
        }
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
