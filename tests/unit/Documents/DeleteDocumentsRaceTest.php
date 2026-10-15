<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Documents\Interleaving\Memory;
use Tests\Unit\Documents\Interleaving\Refresh;
use Tests\Unit\Documents\Interleaving\SQLite;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

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
        $this->assertSame(self::EXPIRED, $this->expiry($database, 'b'));
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

        $this->assertSame(3, $database->deleteDocuments(self::COLLECTION, batchSize: 2));
        $this->assertSame([], $this->stored($database));
        $this->assertSame(2, $engine->deletes);
    }

    /**
     * @return array{Database, Memory|SQLite}
     */
    private function database(Memory|SQLite $adapter): array
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
