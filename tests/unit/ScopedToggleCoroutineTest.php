<?php

namespace Tests\Unit;

use Closure;
use DateTime;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\State\Snapshot;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

use function Swoole\Coroutine\run;

final class ScopedToggleCoroutineTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string SEEDED = 'seeded';

    private const string FILTER = 'shout';

    private const int TENANT = 1;

    private const int SCOPED_TENANT = 2;

    private const int OTHER_TENANT = 3;

    private const string PAST = '2000-01-01T00:00:00.000+00:00';

    private const int ROUNDS = 3;

    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine-scoped toggles');
        }
    }

    /**
     * @return array<string, array{Closure(Database, Closure): mixed, Closure(Database): mixed, mixed, mixed}>
     */
    public static function scopes(): array
    {
        return [
            'skipFilters' => [
                static fn (Database $database, Closure $callback): mixed => $database->skipFilters($callback),
                self::decodedTitle(...),
                'quiet',
                'QUIET',
            ],
            'skipFilters by name' => [
                static fn (Database $database, Closure $callback): mixed => $database->skipFilters($callback, [self::FILTER]),
                self::decodedTitle(...),
                'quiet',
                'QUIET',
            ],
            'skipValidation' => [
                static fn (Database $database, Closure $callback): mixed => $database->skipValidation($callback),
                static fn (Database $database): bool => $database->isValidationEnabled(),
                false,
                true,
            ],
            'withPreserveDates' => [
                static fn (Database $database, Closure $callback): mixed => $database->withPreserveDates($callback),
                static fn (Database $database): bool => $database->getPreserveDates(),
                true,
                false,
            ],
            'withPreserveSequence' => [
                static fn (Database $database, Closure $callback): mixed => $database->withPreserveSequence($callback),
                static fn (Database $database): bool => $database->getPreserveSequence(),
                true,
                false,
            ],
            'withTenant' => [
                static fn (Database $database, Closure $callback): mixed => $database->withTenant(self::SCOPED_TENANT, $callback),
                static fn (Database $database): int|string|null => $database->getTenant(),
                self::SCOPED_TENANT,
                self::TENANT,
            ],
            'withRequestTimestamp' => [
                static fn (Database $database, Closure $callback): mixed => $database->withRequestTimestamp(new DateTime(self::PAST), $callback),
                self::updateOutcome(...),
                'conflict',
                'updated',
            ],
            'skipDuplicates' => [
                static fn (Database $database, Closure $callback): mixed => $database->skipDuplicates($callback),
                self::duplicateOutcome(...),
                'skipped',
                'rejected',
            ],
            'adapter skipDuplicates' => [
                static fn (Database $database, Closure $callback): mixed => $database->getAdapter()->skipDuplicates($callback),
                self::duplicateOutcome(...),
                'skipped',
                'rejected',
            ],
        ];
    }

    /**
     * @param  Closure(Database, Closure): mixed  $scope
     * @param  Closure(Database): mixed  $probe
     */
    #[DataProvider('scopes')]
    public function testAScopeIsSeenByItsCoroutineAndItsChildrenButNotByASibling(Closure $scope, Closure $probe, mixed $inside, mixed $outside): void
    {
        $database = $this->database(new Memory());
        $expected = [
            'inside' => $inside,
            'child' => $inside,
            'parent' => $outside,
            'sibling' => $outside,
            'insideAfterSibling' => $inside,
            'after' => $outside,
        ];

        for ($round = 0; $round < self::ROUNDS; $round++) {
            $this->assertSame(
                $expected,
                $this->whileASiblingIsInside($database, $scope, $probe, siblingFirst: $round % 2 === 1),
                "round {$round}",
            );
        }
    }

    /**
     * @param  Closure(Database, Closure): mixed  $scope
     * @param  Closure(Database): mixed  $probe
     */
    #[DataProvider('scopes')]
    public function testOverlappingScopesInSiblingsLeaveTheHandleWideValue(Closure $scope, Closure $probe, mixed $inside, mixed $outside): void
    {
        $database = $this->database(new Memory());

        for ($round = 0; $round < self::ROUNDS; $round++) {
            $seen = [];

            $this->inCoroutine(function () use ($database, $scope, $probe, &$seen): void {
                $firstEntered = new Channel(1);
                $secondEntered = new Channel(1);
                $firstLeft = new Channel(1);
                $secondLeft = new Channel(1);

                Coroutine::create(function () use ($database, $scope, $probe, &$seen, $firstEntered, $secondEntered, $firstLeft): void {
                    $scope($database, function () use ($firstEntered, $secondEntered): void {
                        $firstEntered->push(true);
                        $secondEntered->pop();
                    });
                    $seen['firstAfter'] = self::guarded($probe, $database);
                    $firstLeft->push(true);
                });

                $firstEntered->pop();

                Coroutine::create(function () use ($database, $scope, $probe, &$seen, $secondEntered, $firstLeft, $secondLeft): void {
                    $scope($database, function () use ($database, $probe, &$seen, $secondEntered, $firstLeft): void {
                        $secondEntered->push(true);
                        $firstLeft->pop();
                        $seen['secondInsideAfterFirstLeft'] = self::guarded($probe, $database);
                    });
                    $secondLeft->push(true);
                });

                $secondLeft->pop();
                $seen['after'] = self::guarded($probe, $database);
            });

            $this->assertSame(
                ['firstAfter' => $outside, 'secondInsideAfterFirstLeft' => $inside, 'after' => $outside],
                $seen,
                "round {$round}",
            );
        }
    }

    /**
     * @param  Closure(Database, Closure): mixed  $scope
     * @param  Closure(Database): mixed  $probe
     */
    #[DataProvider('scopes')]
    public function testOutsideACoroutineAScopeLastsForItsCallback(Closure $scope, Closure $probe, mixed $inside, mixed $outside): void
    {
        $database = $this->database(new Memory());

        $seen = $scope($database, fn (): array => [
            'inside' => self::guarded($probe, $database),
            'nested' => $scope($database, fn (): mixed => self::guarded($probe, $database)),
            'afterNested' => self::guarded($probe, $database),
        ]);
        $this->assertIsArray($seen);
        $seen['after'] = self::guarded($probe, $database);

        $thrown = null;
        try {
            $scope($database, static fn (): never => throw new RuntimeException('failed'));
        } catch (RuntimeException $error) {
            $thrown = $error;
        }
        $seen['afterFailure'] = self::guarded($probe, $database);

        $this->assertInstanceOf(RuntimeException::class, $thrown);
        $this->assertSame(
            ['inside' => $inside, 'nested' => $inside, 'afterNested' => $inside, 'after' => $outside, 'afterFailure' => $outside],
            $seen,
        );
    }

    public function testASetterInsideAScopeChangesTheScopeOnly(): void
    {
        $database = $this->database(new Memory());

        $seen = $database->skipValidation(function () use ($database): array {
            $database->enableValidation();
            $database->setPreserveDates(true);

            return [$database->isValidationEnabled(), $database->withPreserveDates(fn (): bool => $database->getPreserveDates())];
        });

        $this->assertSame([true, true], $seen);
        $this->assertTrue($database->isValidationEnabled());
        $this->assertTrue($database->getPreserveDates(), 'setPreserveDates() outside a withPreserveDates() scope changes the handle-wide value');

        $database->withTenant(self::SCOPED_TENANT, fn (): Database => $database->setTenant(self::OTHER_TENANT));

        $this->assertSame(self::TENANT, $database->getTenant(), 'setTenant() inside withTenant() changes that scope only');
    }

    public function testASiblingWritesUnderItsOwnTenantWhileAnotherIsInsideWithTenant(): void
    {
        $database = $this->sharedTablesDatabase();
        $failures = [];

        $this->inCoroutine(function () use ($database, &$failures): void {
            $entered = new Channel(1);
            $written = new Channel(1);
            $closed = new Channel(1);

            Coroutine::create(function () use ($database, &$failures, $entered, $written, $closed): void {
                $database->withTenant(self::SCOPED_TENANT, function () use ($database, &$failures, $entered, $written): void {
                    $entered->push(true);
                    $written->pop();
                    $failures['scoped'] = $this->failureOf(fn (): Document => $database->createDocument(self::COLLECTION, $this->note('scoped')));
                });
                $closed->push(true);
            });

            $entered->pop();
            Coroutine::create(function () use ($database, &$failures, $written): void {
                $failures['sibling'] = $this->failureOf(fn (): Document => $database->createDocument(self::COLLECTION, $this->note('sibling')));
                $failures['own'] = $this->failureOf(fn (): Document => $database->withTenant(
                    self::OTHER_TENANT,
                    fn (): Document => $database->createDocument(self::COLLECTION, $this->note('own')),
                ));
                $written->push(true);
            });
            $closed->pop();
        });

        $this->assertSame(['sibling' => null, 'own' => null, 'scoped' => null], $failures);
        $this->assertSame(self::TENANT, $database->getTenant());
        $this->assertSame(
            [self::TENANT => ['sibling'], self::SCOPED_TENANT => ['scoped'], self::OTHER_TENANT => ['own']],
            [
                self::TENANT => $this->idsUnder($database, self::TENANT),
                self::SCOPED_TENANT => $this->idsUnder($database, self::SCOPED_TENANT),
                self::OTHER_TENANT => $this->idsUnder($database, self::OTHER_TENANT),
            ],
        );
    }

    public function testASearchAfterAScopedTenantsSearchUsesItsOwnTenantsFulltextIndex(): void
    {
        $database = $this->sharedTablesDatabase(fulltext: true);
        $database->createDocument(self::COLLECTION, $this->note('apple'));

        $search = static fn (): array => \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->find(self::COLLECTION, [Query::search('title', 'apple*')]),
        );

        $this->assertSame([], $database->withTenant(self::SCOPED_TENANT, $search));
        $this->assertSame(['apple'], $search());
    }

    public function testASiblingsDatesAreNotPreservedWhileAnotherIsInsideWithPreserveDates(): void
    {
        $database = $this->database(new Memory());

        $this->inCoroutine(function () use ($database): void {
            $entered = new Channel(1);
            $written = new Channel(1);
            $closed = new Channel(1);

            Coroutine::create(function () use ($database, $entered, $written, $closed): void {
                $database->withPreserveDates(function () use ($database, $entered, $written): void {
                    $entered->push(true);
                    $written->pop();
                    $this->failureOf(fn (): Document => $database->createDocument(self::COLLECTION, $this->dated('preserved')));
                });
                $closed->push(true);
            });

            $entered->pop();
            Coroutine::create(function () use ($database, $written): void {
                $this->failureOf(fn (): Document => $database->createDocument(self::COLLECTION, $this->dated('stamped')));
                $written->push(true);
            });
            $closed->pop();
        });

        $this->assertSame(self::PAST, $database->getDocument(self::COLLECTION, 'preserved')->getCreatedAt());
        $this->assertNotSame(self::PAST, $database->getDocument(self::COLLECTION, 'stamped')->getCreatedAt());
    }

    public function testASnapshotCarriesTheScopedTogglesIntoACoroutineThatOutlivesTheScope(): void
    {
        $database = $this->database(new Memory());
        $seen = [];

        $this->inCoroutine(function () use ($database, &$seen): void {
            $released = new Channel(1);
            $done = new Channel(1);

            $this->insideEveryScope($database, function () use ($database, &$seen, $released, $done): void {
                $snapshot = $database->snapshot();

                Coroutine::create(function () use ($database, $snapshot, &$seen, $released, $done): void {
                    $released->pop();
                    $seen['carried'] = $database->withSnapshot($snapshot, function () use ($database): array {
                        $state = $this->state($database);
                        $database->enableValidation();
                        $database->setTenant(9);

                        return $state;
                    });
                    $seen['afterSnapshot'] = $this->state($database);
                    $done->push(true);
                });
            });

            $released->push(true);
            $done->pop();
            $seen['caller'] = $this->state($database);
        });

        $this->assertSame($this->scopedState(), $seen['carried']);
        $this->assertSame($this->handleState(), $seen['afterSnapshot']);
        $this->assertSame($this->handleState(), $seen['caller']);
    }

    public function testASnapshotAppliesTheScopedTogglesToAnotherHandle(): void
    {
        $source = $this->database(new Memory());
        $destination = $this->database(new Memory());

        $snapshot = $this->insideEveryScope($source, static fn (): Snapshot => $source->snapshot());
        $carried = $destination->withSnapshot($snapshot, fn (): array => $this->state($destination));

        $this->assertSame($this->scopedState(), $carried);
        $this->assertSame($this->handleState(), $this->state($destination));
        $this->assertSame($this->handleState(), $this->state($source));
    }

    public function testASnapshotTakenOutsideAnyScopeCarriesTheHandleWideValues(): void
    {
        $source = $this->database(new Memory());
        $source->disableValidation()->setPreserveDates(true);
        $destination = $this->database(new Memory());

        $carried = $destination->withSnapshot($source->snapshot(), fn (): array => $this->state($destination));

        $this->assertSame(
            ['validation' => false, 'preserveDates' => true] + $this->handleState(),
            $carried,
        );
        $this->assertSame($this->handleState(), $this->state($destination));
    }

    public function testAdapterSkipDuplicatesThroughAPoolReachesTheBorrowedAdapterOnlyForItsCoroutine(): void
    {
        $database = $this->database($this->pool(new Memory()));

        $this->assertSame(
            ['inside' => 'skipped', 'child' => 'skipped', 'parent' => 'rejected', 'sibling' => 'rejected', 'insideAfterSibling' => 'skipped', 'after' => 'rejected'],
            $this->whileASiblingIsInside(
                $database,
                static fn (Database $database, Closure $callback): mixed => $database->getAdapter()->skipDuplicates($callback),
                self::duplicateOutcome(...),
                siblingFirst: false,
            ),
        );
    }

    /**
     * Opens a scope in one coroutine and probes it from inside, from a coroutine started inside, from the parent and
     * from a sibling while the scope is open, from inside again after the sibling ran, and from the parent after it
     * closed.
     *
     * @param  Closure(Database, Closure): mixed  $scope
     * @param  Closure(Database): mixed  $probe
     * @return array<string, mixed>
     */
    private function whileASiblingIsInside(Database $database, Closure $scope, Closure $probe, bool $siblingFirst): array
    {
        $seen = [];

        $this->inCoroutine(function () use ($database, $scope, $probe, $siblingFirst, &$seen): void {
            $entered = new Channel(1);
            $released = new Channel(1);
            $closed = new Channel(1);

            Coroutine::create(function () use ($database, $scope, $probe, $siblingFirst, &$seen, $entered, $released, $closed): void {
                $scope($database, function () use ($database, $probe, $siblingFirst, &$seen, $entered, $released): void {
                    $seen['inside'] = self::guarded($probe, $database);
                    $childDone = new Channel(1);
                    Coroutine::create(function () use ($database, $probe, $siblingFirst, &$seen, $childDone, $entered, $released): void {
                        if ($siblingFirst) {
                            $entered->push(true);
                            $released->pop();
                        }
                        Coroutine::sleep(0.001);
                        $seen['child'] = self::guarded($probe, $database);
                        $childDone->push(true);
                    });
                    if (! $siblingFirst) {
                        $childDone->pop();
                        $entered->push(true);
                        $released->pop();
                    } else {
                        $childDone->pop();
                    }
                    $seen['insideAfterSibling'] = self::guarded($probe, $database);
                });
                $closed->push(true);
            });

            $entered->pop();
            $seen['parent'] = self::guarded($probe, $database);

            Coroutine::create(function () use ($database, $probe, &$seen, $released): void {
                Coroutine::sleep(0.001);
                $seen['sibling'] = self::guarded($probe, $database);
                $released->push(true);
            });

            $closed->pop();
            $seen['after'] = self::guarded($probe, $database);
        });

        return [
            'inside' => $seen['inside'] ?? null,
            'child' => $seen['child'] ?? null,
            'parent' => $seen['parent'] ?? null,
            'sibling' => $seen['sibling'] ?? null,
            'insideAfterSibling' => $seen['insideAfterSibling'] ?? null,
            'after' => $seen['after'] ?? null,
        ];
    }

    /**
     * @template T
     *
     * @param  Closure(): T  $callback
     * @return T
     */
    private function insideEveryScope(Database $database, Closure $callback): mixed
    {
        return $database->skipValidation(
            fn (): mixed => $database->withPreserveDates(
                fn (): mixed => $database->withPreserveSequence(
                    fn (): mixed => $database->withTenant(
                        self::SCOPED_TENANT,
                        fn (): mixed => $database->withRequestTimestamp(
                            new DateTime(self::PAST),
                            fn (): mixed => $database->skipDuplicates(
                                fn (): mixed => $database->skipFilters($callback, [self::FILTER]),
                            ),
                        ),
                    ),
                ),
            ),
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function state(Database $database): array
    {
        return [
            'validation' => $database->isValidationEnabled(),
            'preserveDates' => $database->getPreserveDates(),
            'preserveSequence' => $database->getPreserveSequence(),
            'tenant' => $database->getTenant(),
            'title' => self::guarded(self::decodedTitle(...), $database),
            'update' => self::guarded(self::updateOutcome(...), $database),
            'duplicate' => self::guarded(self::duplicateOutcome(...), $database),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function scopedState(): array
    {
        return [
            'validation' => false,
            'preserveDates' => true,
            'preserveSequence' => true,
            'tenant' => self::SCOPED_TENANT,
            'title' => 'quiet',
            'update' => 'conflict',
            'duplicate' => 'skipped',
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function handleState(): array
    {
        return [
            'validation' => true,
            'preserveDates' => false,
            'preserveSequence' => false,
            'tenant' => self::TENANT,
            'title' => 'QUIET',
            'update' => 'updated',
            'duplicate' => 'rejected',
        ];
    }

    private static function decodedTitle(Database $database): mixed
    {
        $collection = new Document(['attributes' => [[
            '$id' => 'title',
            'type' => 'string',
            'filters' => [self::FILTER],
        ]]]);

        return $database->decode($collection, new Document(['title' => 'quiet']))->getAttribute('title');
    }

    private static function updateOutcome(Database $database): string
    {
        try {
            $database->withTenant(
                self::TENANT,
                static fn (): Document => $database->updateDocument(self::COLLECTION, self::SEEDED, new Document(['views' => 2])),
            );

            return 'updated';
        } catch (ConflictException) {
            return 'conflict';
        }
    }

    private static function duplicateOutcome(Database $database): string
    {
        try {
            $database->withTenant(
                self::TENANT,
                static fn (): int => $database->createDocuments(self::COLLECTION, [new Document([
                    '$id' => self::SEEDED,
                    'title' => 'duplicate',
                    'views' => 1,
                ])]),
            );

            return 'skipped';
        } catch (DuplicateException) {
            return 'rejected';
        }
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()), self::filters());
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('toggles')
            ->setNamespace('toggles_'.\uniqid())
            ->setTenant(self::TENANT);
        $database->create();
        $database->createCollection($this->collection());
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => self::SEEDED,
            'title' => 'quiet',
            'views' => 1,
        ]));

        return $database;
    }

    private function sharedTablesDatabase(bool $fulltext = false): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()), self::filters());
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('toggles')
            ->setNamespace('toggles_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(self::TENANT);
        $database->create();
        foreach ([self::TENANT, self::SCOPED_TENANT, self::OTHER_TENANT] as $tenant) {
            $database->withTenant($tenant, fn (): Document => $database->createCollection($this->collection($fulltext)));
        }

        return $database;
    }

    /**
     * @return array<string, array{encode: callable, decode: callable}>
     */
    private static function filters(): array
    {
        return [
            self::FILTER => [
                'encode' => static fn (mixed $value): mixed => \is_string($value) ? \strtolower($value) : $value,
                'decode' => static fn (mixed $value): mixed => \is_string($value) ? \strtoupper($value) : $value,
            ],
        ];
    }

    private function collection(bool $fulltext = false): Collection
    {
        return Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64, filters: [self::FILTER]),
                Attribute::integer(key: 'views'),
            ],
            indexes: $fulltext ? [Index::fulltext(key: 'title_search', attributes: ['title'])] : [],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        );
    }

    /**
     * @return list<string>
     */
    private function idsUnder(Database $database, int $tenant): array
    {
        $ids = \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->withTenant($tenant, static fn (): array => $database->find(self::COLLECTION, [Query::limit(10)])),
        );
        \sort($ids);

        return $ids;
    }

    /**
     * @param  Closure(Database): mixed  $probe
     */
    private static function guarded(Closure $probe, Database $database): mixed
    {
        try {
            return $probe($database);
        } catch (Throwable $error) {
            return 'failed: '.$error->getMessage();
        }
    }

    private function failureOf(Closure $write): ?string
    {
        try {
            $write();

            return null;
        } catch (Throwable $error) {
            return $error->getMessage();
        }
    }

    private function note(string $id): Document
    {
        return new Document(['$id' => $id, 'title' => $id, 'views' => 1]);
    }

    private function dated(string $id): Document
    {
        return new Document([
            '$id' => $id,
            'title' => $id,
            'views' => 1,
            '$createdAt' => self::PAST,
            '$updatedAt' => self::PAST,
        ]);
    }

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return new Pool($connections);
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
