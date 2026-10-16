<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Documents\Interleaving\AuditingPool;
use Tests\Unit\Documents\Interleaving\Memory;
use Tests\Unit\Documents\Interleaving\Miscounting;
use Tests\Unit\Documents\Interleaving\Refresh;
use Tests\Unit\Documents\Interleaving\SQLite;
use Tests\Unit\Documents\Interleaving\Standalone;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event\Document\BatchDeleted;
use Utopia\Database\Event\Domain;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * deleteDocuments() deletes only the documents that still match its queries when they are deleted: a document
 * another write changes after the documents were found, so that it no longer matches, is kept, and the count,
 * $onNext, cascades and permission rows cover only the documents deleted.
 */
final class DeleteDocumentsRaceTest extends TestCase
{
    private const string COLLECTION = 'presences';

    private const string CHILDREN = 'sessions';

    private const string EXPIRED = '2000-01-01T00:00:00.000+00:00';

    private const string REFRESHED = '2100-01-01T00:00:00.000+00:00';

    private const string NOW = '2050-01-01T00:00:00.000+00:00';

    private const string OWNER = 'user:owner';

    /**
     * @return iterable<string, array{Closure(): (Memory|SQLite)}>
     */
    public static function adapters(): iterable
    {
        yield 'memory' => [static fn (): Memory => new Memory()];
        yield 'sqlite' => [static fn (): SQLite => SQLite::inMemory()];
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testADocumentChangedAfterTheFindIsKept(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::REFRESHED]);
        $engine->afterFind = $this->refreshOnce($database, 'b');

        $deleted = [];
        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()], onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });

        $this->assertSame(2, $count, 'The refreshed document is not counted');
        $this->assertSame(['a', 'c'], $deleted, 'The refreshed document is not handed to $onNext');
        $this->assertSame(['b', 'd'], $this->stored($database));
        $this->assertSame(self::REFRESHED, $this->expiry($database, 'b'));
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testADocumentChangedAfterALaterBatchIsFoundIsKept(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::EXPIRED, 'e' => self::EXPIRED, 'f' => self::REFRESHED]);
        $engine->afterFind = $this->refreshOnce($database, 'd');

        $deleted = [];
        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()], batchSize: 2, onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });

        $this->assertSame(4, $count);
        $this->assertSame(['a', 'b', 'c', 'e'], $deleted);
        $this->assertSame(['d', 'f'], $this->stored($database));
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheLimitCountsTheDocumentsFoundSoAKeptDocumentIsNotReplaced(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::EXPIRED, 'e' => self::EXPIRED]);
        $engine->afterFind = $this->refreshOnce($database, 'b');

        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired(), Query::limit(4)], batchSize: 2);

        $this->assertSame(3, $count);
        $this->assertSame(['b', 'e'], $this->stored($database), 'Only the first four documents found are deleted, less the one refreshed');
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testACascadeSkipsTheChildrenOfADocumentChangedAfterTheFind(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $database->createCollection(Collection::create(self::CHILDREN, permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship(self::COLLECTION, Relationship::oneToMany(self::CHILDREN, key: 'sessions', twoWay: true, twoWayKey: 'presence', onDelete: RelationshipDeleteAction::Cascade));
        $database->createDocument(self::CHILDREN, new Document([Document::ID => 'a-session']));
        $database->createDocument(self::CHILDREN, new Document([Document::ID => 'b-session']));
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED], ['a' => ['a-session'], 'b' => ['b-session']]);
        $engine->afterFind = $this->refreshOnce($database, 'b');

        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()]);

        $this->assertSame(1, $count);
        $this->assertSame(['b'], $this->stored($database));
        $this->assertSame(['b-session'], $this->stored($database, self::CHILDREN), 'The refreshed document keeps its children');
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testADocumentChangedRightBeforeTheDeleteRollsTheBatchBack(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED]);
        $engine->beforeDelete = function (Document $collection) use ($database): void {
            if ($collection->getId() === self::COLLECTION) {
                $database->updateDocument(self::COLLECTION, 'b', new Document(['expiresAt' => self::REFRESHED]));
            }
        };

        try {
            $database->deleteDocuments(self::COLLECTION, [$this->expired()]);
            $this->fail('A document that stops matching between the check and the delete must fail the batch');
        } catch (ContentionException) {
        }

        $this->assertSame(3, $engine->deletes, 'The batch is tried again before it fails');
        $this->assertSame(['a', 'b', 'c'], $this->stored($database), 'Nothing of the failed batch is stored');
    }

    public function testADocumentChangedRightBeforeTheDeleteIsKeptWhenTheRetryReadsItAgain(): void
    {
        $engine = new Memory();
        [$database] = $this->database($engine);
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED]);
        $engine->beforeDelete = function (Document $collection) use ($database, $engine): void {
            if ($collection->getId() === self::COLLECTION && $engine->deletes === 1) {
                $engine->detached(fn (): Document => $database->updateDocument(self::COLLECTION, 'b', new Document(['expiresAt' => self::REFRESHED])));
            }
        };

        $deleted = [];
        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()], onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });

        $this->assertSame(2, $engine->deletes, 'The batch is rolled back once and deleted on the retry');
        $this->assertSame(2, $count, 'Only the retry that committed is counted');
        $this->assertSame(['a', 'c'], $deleted, 'Each deleted document is handed to $onNext once');
        $this->assertSame(['b'], $this->stored($database));
        $this->assertSame(self::REFRESHED, $this->expiry($database, 'b'));
    }

    public function testAContentionInALaterBatchReportsTheBatchesAlreadyDeleted(): void
    {
        [$database, $engine] = $this->database(new Memory());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::EXPIRED]);
        $engine->beforeDelete = function (Document $collection) use ($database, $engine): void {
            if ($collection->getId() === self::COLLECTION && $engine->deletes > 1) {
                $database->updateDocument(self::COLLECTION, 'd', new Document(['expiresAt' => self::REFRESHED]));
            }
        };
        $events = $this->batchEvents($database);

        $deleted = [];
        try {
            $database->deleteDocuments(self::COLLECTION, [$this->expired()], batchSize: 2, onNext: function (Document $document) use (&$deleted): void {
                $deleted[] = $document->getId();
            });
            $this->fail('The second batch must fail with a contention');
        } catch (ContentionException) {
        }

        $this->assertSame(['a', 'b'], $deleted);
        $this->assertSame(['c', 'd'], $this->stored($database), 'The first batch stays deleted');
        $this->assertSame([2], $events->counts, 'The deleted batch is reported');
    }

    public function testWithoutATransactionADocumentChangedRightBeforeTheDeleteIsKeptAndTheRestReported(): void
    {
        [$database, $engine] = $this->database(new Standalone());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::EXPIRED, 'e' => self::REFRESHED]);
        $engine->beforeDelete = function (Document $collection) use ($database, $engine): void {
            if ($collection->getId() === self::COLLECTION && $engine->deletes === 1) {
                $database->updateDocument(self::COLLECTION, 'b', new Document(['expiresAt' => self::REFRESHED]));
            }
        };
        $events = $this->batchEvents($database);

        $deleted = [];
        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()], batchSize: 2, onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });

        $this->assertSame(3, $count, 'The document kept is not counted, and the next batch is still deleted');
        $this->assertSame(['a', 'c', 'd'], $deleted, 'Every deleted document is handed to $onNext');
        $this->assertSame([3], $events->counts);
        $this->assertSame(['b', 'e'], $this->stored($database));
        $this->assertSame(['b', 'e'], \array_map(static fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)), 'The kept document keeps its permission rows');
    }

    /**
     * @return iterable<string, array{Closure(): (Memory|SQLite), list<string>}>
     */
    public static function floatFinds(): iterable
    {
        yield 'memory compares exactly' => [static fn (): Memory => new Memory(), ['large']];
        yield 'sqlite compares exactly' => [static fn (): SQLite => SQLite::inMemory(), ['large']];
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     * @param  list<string>  $expected
     */
    #[DataProvider('floatFinds')]
    public function testAFloatFilterDeletesTheDocumentsTheFindMatched(Closure $adapter, array $expected): void
    {
        [$database] = $this->database($adapter());
        $database->createCollection(Collection::create(
            id: 'scores',
            attributes: [Attribute::float(key: 'score')],
            permissions: $this->permissions(),
            documentSecurity: false,
        ));
        foreach (['tiny' => 1.0E-21, 'large' => 1.5, 'negative' => -1.0] as $id => $score) {
            $database->createDocument('scores', new Document([Document::ID => $id, 'score' => $score]));
        }
        $filter = Query::greaterThan('score', 1.0E-20);

        $found = \array_map(static fn (Document $document): string => $document->getId(), $database->find('scores', [$filter]));
        \sort($found);
        $this->assertSame($expected, $found);

        $deleted = [];
        $count = $database->deleteDocuments('scores', [$filter], onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });
        \sort($deleted);

        $this->assertSame(\count($found), $count, 'The delete binds a float as the find does');
        $this->assertSame($found, $deleted);
        $remaining = \array_map(static fn (Document $document): string => $document->getId(), $database->find('scores'));
        $this->assertSame([], \array_values(\array_intersect($found, $remaining)));
        $this->assertContains('negative', $remaining);
    }

    public function testAPoolSubclassThatWrapsDeleteDocumentsRunsForAFilteredBatch(): void
    {
        $engine = new Memory();
        $pool = new AuditingPool(new UtopiaPool(new Stack(), 'race', 2, static fn (): Memory => $engine, timeout: 0.0));
        [$database] = $this->database($pool);
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::REFRESHED]);
        $engine->afterFind = $this->refreshOnce($database, 'b');

        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()]);

        $this->assertSame(1, $pool->deletes, 'The pool\'s override of deleteDocuments() runs');
        $this->assertSame(2, $count);
        $this->assertSame(['b', 'd'], $this->stored($database), 'The pooled adapter is handed the filters');
    }

    /**
     * @return iterable<string, array{Closure(int, array<string>): int}>
     */
    public static function miscounts(): iterable
    {
        yield 'none' => [static fn (int $deleted, array $permissionIds): int => 0];
        yield 'permission rows too' => [static fn (int $deleted, array $permissionIds): int => $deleted + \count($permissionIds)];
    }

    /**
     * @param  Closure(int, array<string>): int  $report
     */
    #[DataProvider('miscounts')]
    public function testACountTheAdapterGetsWrongDoesNotFailADeleteWithoutARace(Closure $report): void
    {
        [$database] = $this->database(new Miscounting($report));
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::REFRESHED]);

        $deleted = [];
        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()], batchSize: 2, onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });

        $this->assertSame(3, $count);
        $this->assertSame(['a', 'b', 'c'], $deleted);
        $this->assertSame(['d'], $this->stored($database));
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testABatchFilteredOnlyOnTheSequenceIsNotReadAgain(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::REFRESHED, 'c' => self::EXPIRED]);
        $sequences = $database->getAuthorization()->skip(fn (): array => \array_map(
            static fn (Document $document): string => (string) $document->getSequence(),
            $database->find(self::COLLECTION, [Query::equal(Document::ID, ['a', 'c'])]),
        ));
        $finds = $engine->finds;

        $this->assertSame(2, $database->deleteDocuments(self::COLLECTION, [Query::equal(Document::SEQUENCE, $sequences)], batchSize: 2));
        $this->assertSame(2, $engine->finds - $finds, 'Each batch is found once and not read again');
        $this->assertSame(['b'], $this->stored($database));
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheAdapterDeletesOnlyTheRowsThatStillMatchAndKeepsTheirPermissions(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::REFRESHED]);
        $found = $database->find(self::COLLECTION, [$this->expired()]);
        $database->updateDocument(self::COLLECTION, 'b', new Document(['expiresAt' => self::REFRESHED]));

        $collection = $database->getCollection(self::COLLECTION);
        $sequences = \array_values(\array_filter(\array_map(static fn (Document $document): ?string => $document->getSequence(), $found)));
        $permissionIds = \array_map(static fn (Document $document): string => $document->getId(), $found);

        $count = $engine->deleteDocumentsMatching($collection, $sequences, $permissionIds, $database->convertQueries($collection, [$this->expired()]));

        $this->assertSame(2, $count);
        $this->assertSame(['b', 'd'], $this->stored($database));
        $this->assertSame(['b', 'd'], \array_map(static fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)), 'The kept row keeps its permission rows');
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testADeleteWithNoRaceDeletesEveryMatchingDocumentAndItsPermissions(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::EXPIRED, 'c' => self::EXPIRED, 'd' => self::REFRESHED]);

        $deleted = [];
        $count = $database->deleteDocuments(self::COLLECTION, [$this->expired()], batchSize: 2, onNext: function (Document $document) use (&$deleted): void {
            $deleted[] = $document->getId();
        });

        $this->assertSame(3, $count);
        $this->assertSame(['a', 'b', 'c'], $deleted);
        $this->assertSame(2, $engine->deletes);
        $this->assertSame(['d'], $this->stored($database));

        $database->getAuthorization()->skip(fn (): Document => $database->createDocument(self::COLLECTION, new Document([
            Document::ID => 'a',
            Document::PERMISSIONS => [],
            'expiresAt' => self::EXPIRED,
        ])));
        $this->assertSame(['d'], \array_map(static fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)), 'A deleted document leaves no permission rows behind');
    }

    /**
     * @param  Closure(): (Memory|SQLite)  $adapter
     */
    #[DataProvider('adapters')]
    public function testADeleteWithoutFiltersDeletesEveryDocument(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter());
        $this->seed($database, ['a' => self::EXPIRED, 'b' => self::REFRESHED, 'c' => self::EXPIRED]);
        $finds = $engine->finds;

        $this->assertSame(3, $database->deleteDocuments(self::COLLECTION, batchSize: 2));
        $this->assertSame(2, $engine->finds - $finds, 'Each batch is found once and not read again');
        $this->assertSame([], $this->stored($database));
        $this->assertSame(2, $engine->deletes);
    }

    /**
     * @template T of Adapter
     *
     * @param  T  $adapter
     * @return array{Database, T}
     */
    private function database(Adapter $adapter): array
    {
        $authorization = new Authorization();
        $authorization->addRole(self::OWNER);

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('race')
            ->setNamespace('race_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());
        $database->addHook(new Permissions());

        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::datetime(key: 'expiresAt')],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        return [$database, $adapter];
    }

    /**
     * @return object{counts: list<int>}
     */
    private function batchEvents(Database $database): object
    {
        $events = new class () implements Lifecycle {
            /** @var list<int> */
            public array $counts = [];

            #[\Override]
            public function handle(Domain $event): void
            {
                if ($event instanceof BatchDeleted) {
                    $this->counts[] = $event->count;
                }
            }
        };
        $database->addHook($events);

        return $events;
    }

    /**
     * @return list<string>
     */
    private function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    /**
     * @param  array<string, string>  $expiries
     * @param  array<string, list<string>>  $sessions
     */
    private function seed(Database $database, array $expiries, array $sessions = []): void
    {
        foreach ($expiries as $id => $expiresAt) {
            $attributes = [
                Document::ID => $id,
                Document::PERMISSIONS => [
                    Permission::read(Role::user('owner')),
                    Permission::update(Role::user('owner')),
                    Permission::delete(Role::user('owner')),
                ],
                'expiresAt' => $expiresAt,
            ];
            if (isset($sessions[$id])) {
                $attributes['sessions'] = $sessions[$id];
            }
            $database->createDocument(self::COLLECTION, new Document($attributes));
        }
    }

    /**
     * @return Closure(Document, array<Document>): void
     */
    private function refreshOnce(Database $database, string $id): Closure
    {
        return (new Refresh($database, self::COLLECTION, $id, ['expiresAt' => self::REFRESHED]))(...);
    }

    private function expired(): Query
    {
        return Query::lessThan('expiresAt', self::NOW);
    }

    /**
     * @return list<string>
     */
    private function stored(Database $database, string $collection = self::COLLECTION): array
    {
        $ids = $database->getAuthorization()->skip(fn (): array => $database->skipRelationships(fn (): array => \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->find($collection),
        )));
        \sort($ids);

        return $ids;
    }

    private function expiry(Database $database, string $id): mixed
    {
        return $database->getAuthorization()->skip(fn (): Document => $database->getDocument(self::COLLECTION, $id))->getAttribute('expiresAt');
    }
}
