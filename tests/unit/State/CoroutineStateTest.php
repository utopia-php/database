<?php

namespace Tests\Unit\State;

use ArrayObject;
use Closure;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Tests\Unit\Event\HookFixture;
use Tests\Unit\Event\NamedRecordingLifecycle;
use Tests\Unit\Event\RecordingLifecycle;
use Utopia\Database\Database;
use Utopia\Database\Event;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\State\Snapshot;
use Utopia\Database\Validator\Authorization;

use function Swoole\Coroutine\run;

final class CoroutineStateTest extends TestCase
{
    private Database $database;

    private Relationships $hook;

    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine state');
        }

        $this->database = HookFixture::memory();
        $this->hook = new Relationships();
        $this->database->addHook($this->hook);
    }

    public function testSkipRelationshipsInOneSiblingLeavesTheParentAndAnotherSiblingEnabled(): void
    {
        $seen = $this->whileASiblingIsInside(
            fn (Closure $inside): mixed => $this->database->skipRelationships($inside),
            fn (): bool => $this->hook->isEnabled(),
        );

        $this->assertSame(['inside' => false, 'parent' => true, 'sibling' => true, 'after' => true], $seen);
    }

    public function testSkipRelationshipsExistCheckInOneSiblingLeavesTheParentAndAnotherSiblingChecking(): void
    {
        $seen = $this->whileASiblingIsInside(
            fn (Closure $inside): mixed => $this->database->skipRelationshipsExistCheck($inside),
            fn (): bool => $this->hook->shouldCheckExist(),
        );

        $this->assertSame(['inside' => false, 'parent' => true, 'sibling' => true, 'after' => true], $seen);
    }

    public function testPopulationInOneSiblingLeavesAnotherSiblingFreeToPopulate(): void
    {
        $seen = $this->whileASiblingIsInside(
            fn (Closure $inside): mixed => $this->hook->withSnapshot($this->snapshot(population: true), $inside),
            fn (): bool => $this->hook->isInBatchPopulation(),
        );

        $this->assertSame(['inside' => true, 'parent' => false, 'sibling' => false, 'after' => false], $seen);
    }

    public function testSilentIsSeenByTheCoroutinesItStarts(): void
    {
        $recorder = new RecordingLifecycle();
        $this->database->addHook($recorder);

        $this->inCoroutine(function (): void {
            $this->database->silent(function (): void {
                $done = new Channel(1);
                Coroutine::create(function () use ($done): void {
                    $this->database->getCollection(HookFixture::COLLECTION);
                    $done->push(true);
                });
                $done->pop();
            });
        });

        $this->assertSame([], $recorder->getEvents());
    }

    public function testSilentInOneSiblingLeavesAnotherSiblingsEventsDelivered(): void
    {
        $recorder = new RecordingLifecycle();
        $this->database->addHook($recorder);

        $this->whileASiblingIsInside(
            fn (Closure $inside): mixed => $this->database->silent($inside),
            fn (): bool => $this->database->findCollection(HookFixture::COLLECTION) === null,
        );

        $this->assertSame([Event::CollectionRead, Event::CollectionRead, Event::CollectionRead], $recorder->getEvents());
    }

    public function testSnapshotCarriesTheCallersStateIntoACoroutineThatOutlivesIt(): void
    {
        $audits = new NamedRecordingLifecycle('audits');
        $this->database->addHook($audits);
        /** @var ArrayObject<string, bool> $seen */
        $seen = new ArrayObject();

        $this->inCoroutine(function () use ($seen): void {
            $released = new Channel(1);
            $done = new Channel(1);

            $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(
                fn () => $this->database->silent(function () use ($seen, $released, $done): void {
                    $snapshot = $this->database->snapshot();

                    Coroutine::create(function () use ($snapshot, $seen, $released, $done): void {
                        $released->pop();
                        $this->database->withSnapshot($snapshot, function () use ($seen): void {
                            $seen['authorization'] = $this->database->getAuthorization()->getStatus();
                            $seen['relationships'] = $this->hook->isEnabled();
                            $this->database->getCollection(HookFixture::COLLECTION);
                            $this->database->getAuthorization()->disable();
                            $this->hook->setEnabled(false);
                        });
                        $done->push(true);
                    });
                }, ['audits']),
            ));

            $released->push(true);
            $done->pop();
            $seen['callerAuthorization'] = $this->database->getAuthorization()->getStatus();
            $seen['callerRelationships'] = $this->hook->isEnabled();
        });

        $this->assertSame([
            'authorization' => false,
            'relationships' => false,
            'callerAuthorization' => true,
            'callerRelationships' => true,
        ], $seen->getArrayCopy());
        $this->assertSame([], $audits->getEvents());
    }

    public function testSnapshotAppliesToAnotherHandle(): void
    {
        $destination = HookFixture::memory();
        $destination->setAuthorization(new Authorization());
        $destinationHook = new Relationships();
        $destination->addHook($destinationHook);

        $snapshot = $this->database->getAuthorization()->skip(
            fn (): Snapshot => $this->database->skipRelationshipsExistCheck(fn (): Snapshot => $this->database->snapshot()),
        );

        $seen = $destination->withSnapshot($snapshot, fn (): array => [
            $destination->getAuthorization()->getStatus(),
            $destinationHook->shouldCheckExist(),
        ]);

        $this->assertSame([false, false], $seen);
        $this->assertTrue($destination->getAuthorization()->getStatus());
        $this->assertTrue($destinationHook->shouldCheckExist());
    }

    public function testSetTenantInACoroutineWhoseStarterHasReturnedStaysInThatCoroutine(): void
    {
        $this->database->setTenant(1);
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->database->withTenant(2, function () use (&$seen): void {
                $done = new Channel(1);

                Coroutine::create(function () use (&$seen, $done): void {
                    Coroutine::create(function () use (&$seen, $done): void {
                        Coroutine::sleep(0.01);
                        $seen['detachedBefore'] = $this->database->getTenant();
                        $this->database->setTenant(3);
                        $seen['detachedAfter'] = $this->database->getTenant();
                        $done->push(true);
                    });
                });

                $done->pop();
                $seen['owner'] = $this->database->getTenant();
            });

            $unrelatedDone = new Channel(1);
            Coroutine::create(function () use (&$seen, $unrelatedDone): void {
                $seen['unrelated'] = $this->database->getTenant();
                $unrelatedDone->push(true);
            });
            $unrelatedDone->pop();
        });

        $this->assertSame(['detachedBefore' => 1, 'detachedAfter' => 3, 'owner' => 2, 'unrelated' => 1], $seen);
        $this->assertSame(1, $this->database->getTenant());
    }

    /**
     * Opens a scope in one coroutine and reads the state from inside it, from the parent and from a sibling while
     * the scope is open, and from the parent after it closed.
     *
     * @param  Closure(Closure): mixed  $scope
     * @param  Closure(): bool  $read
     * @return array<string, bool>
     */
    private function whileASiblingIsInside(Closure $scope, Closure $read): array
    {
        $seen = [];

        $this->inCoroutine(function () use ($scope, $read, &$seen): void {
            $entered = new Channel(1);
            $released = new Channel(1);
            $closed = new Channel(1);

            Coroutine::create(function () use ($scope, $read, &$seen, $entered, $released, $closed): void {
                $scope(function () use ($read, &$seen, $entered, $released): void {
                    $seen['inside'] = $read();
                    $entered->push(true);
                    $released->pop();
                });
                $closed->push(true);
            });

            $entered->pop();
            $seen['parent'] = $read();

            Coroutine::create(function () use ($read, &$seen, $released): void {
                $seen['sibling'] = $read();
                $released->push(true);
            });

            $closed->pop();
            $seen['after'] = $read();
        });

        return $seen;
    }

    private function snapshot(bool $population): Snapshot
    {
        return new Snapshot(
            authorization: true,
            roles: ['any'],
            relationships: true,
            existCheck: true,
            population: $population,
            silenced: false,
            silencedListeners: [],
            tenant: null,
            filters: true,
            disabledFilters: [],
            validation: true,
            preserveDates: false,
            preserveSequence: false,
            skipDuplicates: false,
            requestTimestamp: null,
        );
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
