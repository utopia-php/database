<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Tests\Unit\HashAwareMemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Mirror;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class DatabaseUpdateTest extends TestCase
{
    private const string SOURCE = 'rename_source';

    private const string TARGET = 'rename_target';

    private const string NAMESPACE = 'rename';

    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private Authorization $authorization;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
    }

    public function testRenameKeepsCollectionsDocumentsIndexesRelationshipsAndPermissions(): void
    {
        $database = $this->database(new Memory());
        $this->populate($database, self::SOURCE);

        $this->assertTrue($database->update(self::SOURCE, self::TARGET));

        $this->assertSame(self::TARGET, $database->getDatabase(), 'The renamed current database stays current');
        $this->assertFalse($database->exists(self::SOURCE));
        $this->assertTrue($database->exists(self::TARGET));
        $this->assertSame(
            [self::AUTHORS, self::BOOKS],
            $this->sorted(\array_map(
                static fn (Collection $collection): string => $collection->getId(),
                $this->authorization->skip(fn (): array => $database->listCollections()),
            )),
        );
        $this->assertContains('by_title', \array_map(static fn (Index $index): string => $index->key, $database->getCollection(self::BOOKS)->indexes()));

        $author = $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'));
        $this->assertSame('Tolkien', $author->getAttribute('name'));
        $this->assertSame(
            ['hobbit', 'silmarillion'],
            $this->relatedBookIds($author),
        );

        $this->authorization->addRole(Role::user('reader')->toString());
        $visible = $database->find(self::BOOKS, [Query::orderAsc('title')]);
        $this->assertSame(['hobbit'], \array_map(static fn (Document $book): string => $book->getId(), $visible), 'Only the book readable by the reader is found');

        $this->expectException(DuplicateException::class);
        $this->authorization->skip(fn (): Document => $database->createDocument(self::BOOKS, new Document([
            '$id' => 'duplicate',
            'title' => 'The Hobbit',
            '$permissions' => [],
        ])));
    }

    public function testRenamingToAnExistingDatabaseThrowsDuplicateAndMovesNothing(): void
    {
        $database = $this->database(new Memory());
        $this->populate($database, self::SOURCE);
        $database->setDatabase(self::TARGET)->create();

        try {
            $database->update(self::SOURCE, self::TARGET);
            $this->fail('Renaming onto an existing database must throw');
        } catch (DuplicateException) {
        }

        $database->setDatabase(self::SOURCE);
        $this->assertSame('Tolkien', $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'))->getAttribute('name'));
        $database->setDatabase(self::TARGET);
        $this->assertNull($database->findCollection(self::AUTHORS));
    }

    public function testRenamingAMissingDatabaseThrowsNotFound(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(NotFoundException::class);
        $database->update('missing', self::TARGET);
    }

    public function testSharedTablesRefuseTheRename(): void
    {
        $database = $this->database(new Memory());
        $this->populate($database, self::SOURCE);
        $database->setSharedTables(true)->setTenant(1);

        try {
            $database->update(self::SOURCE, self::TARGET);
            $this->fail('A rename under shared tables must be refused');
        } catch (DatabaseException $error) {
            $this->assertNotInstanceOf(DuplicateException::class, $error);
            $this->assertNotInstanceOf(NotFoundException::class, $error);
        }

        $database->setSharedTables(false)->setTenant(null);
        $this->assertTrue($database->exists(self::SOURCE));
        $this->assertFalse($database->exists(self::TARGET));
    }

    public function testMetadataCachedUnderTheOldNameIsNotServedAfterTheRename(): void
    {
        $database = $this->database(new Memory());
        $this->populate($database, self::SOURCE);
        $this->assertNotNull($database->findCollection(self::AUTHORS));
        $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'));

        $database->update(self::SOURCE, self::TARGET);
        $database->setDatabase(self::SOURCE)->create();

        $this->assertNull($database->findCollection(self::AUTHORS), 'A recreated database must not see the renamed one through the cache');
    }

    public function testMetadataCachedUnderTheNewNameIsNotServedAfterTheRename(): void
    {
        $database = $this->database(new Memory());
        $this->populate($database, self::SOURCE);
        $database->setDatabase(self::TARGET);
        $this->assertNull($database->findCollection(self::AUTHORS));
        $database->setDatabase(self::SOURCE);

        $database->update(self::SOURCE, self::TARGET);

        $this->assertNotNull($database->findCollection(self::AUTHORS), 'A miss cached under the new name must not hide the renamed collection');
        $this->assertSame('Tolkien', $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'))->getAttribute('name'));
    }

    public function testRenamingAnotherDatabaseKeepsTheCurrentOne(): void
    {
        $database = $this->database(new Memory());
        $this->populate($database, self::SOURCE);
        $database->setDatabase('current')->create();

        $database->update(self::SOURCE, self::TARGET);

        $this->assertSame('current', $database->getDatabase());
        $database->setDatabase(self::TARGET);
        $this->assertSame('Tolkien', $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'))->getAttribute('name'));
    }

    public function testRollingBackRestoresTheRenamedDatabase(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter);
        $this->populate($database, self::SOURCE);

        $adapter->startTransaction();
        $adapter->update(self::SOURCE, self::TARGET);
        $adapter->rollbackTransaction();

        $this->assertTrue($adapter->exists(self::SOURCE));
        $this->assertFalse($adapter->exists(self::TARGET));
        $this->assertSame('Tolkien', $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'))->getAttribute('name'));
    }

    public function testSQLiteRenameKeepsEveryCollectionReachable(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $this->populate($database, self::SOURCE);

        $this->assertTrue($database->update(self::SOURCE, self::TARGET));

        $this->assertSame(self::TARGET, $database->getDatabase());
        $author = $this->authorization->skip(fn (): Document => $database->getDocument(self::AUTHORS, 'tolkien'));
        $this->assertSame('Tolkien', $author->getAttribute('name'));
        $this->assertSame(['hobbit', 'silmarillion'], $this->relatedBookIds($author));
    }

    public function testMirrorRenamesTheSourceAndTheDestination(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror($source, $destination);
        $mirror->setAuthorization($this->authorization)->setNamespace(self::NAMESPACE)->setDatabase(self::SOURCE)->create();
        $this->authorization->skip(fn (): Collection => $mirror->createCollection(Collection::create(id: self::AUTHORS, attributes: [Attribute::string(key: 'name', size: 64)])));

        $this->assertTrue($mirror->update(self::SOURCE, self::TARGET));

        foreach (['source' => $source, 'destination' => $destination] as $side => $replica) {
            $this->assertSame(self::TARGET, $replica->getDatabase(), "The {$side} follows the rename");
            $this->assertFalse($replica->exists(self::SOURCE), "The {$side} no longer holds the old name");
            $this->assertNotNull($replica->findCollection(self::AUTHORS), "The {$side} keeps its collections");
        }
    }

    public function testReadWritePoolRenamesOnThePrimary(): void
    {
        $primary = new Memory();
        $replica = new Memory();
        $primary->create(self::SOURCE);
        $replica->create(self::SOURCE);
        $pool = new ReadWritePool($this->connections($primary), $this->connections($replica));
        $pool->setAuthorization($this->authorization);

        $this->assertTrue($pool->update(self::SOURCE, self::TARGET));

        $this->assertTrue($primary->exists(self::TARGET));
        $this->assertFalse($primary->exists(self::SOURCE));
        $this->assertTrue($replica->exists(self::SOURCE), 'A replica follows the primary through replication, never through the pool');
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($adapter));

        return $connections;
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new HashAwareMemoryCache()));
        $database->setAuthorization($this->authorization)->setNamespace(self::NAMESPACE);
        $database->addHook(new Relationships());

        return $database;
    }

    private function populate(Database $database, string $name): void
    {
        $database->setDatabase($name)->create();

        $this->authorization->skip(function () use ($database): void {
            $database->createCollection(Collection::create(
                id: self::AUTHORS,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::read(Role::any())],
                documentSecurity: false,
            ));
            $database->createCollection(Collection::create(
                id: self::BOOKS,
                attributes: [Attribute::string(key: 'title', size: 64)],
                indexes: [Index::unique(key: 'by_title', attributes: ['title'])],
                permissions: [],
                documentSecurity: true,
            ));
            $database->createRelationship(self::AUTHORS, Relationship::oneToMany(
                relatedCollection: self::BOOKS,
                key: 'books',
                twoWay: true,
                twoWayKey: 'author',
            ));

            $database->createDocument(self::AUTHORS, new Document(['$id' => 'tolkien', 'name' => 'Tolkien']));
            $database->createDocument(self::BOOKS, new Document([
                '$id' => 'hobbit',
                'title' => 'The Hobbit',
                'author' => 'tolkien',
                '$permissions' => [Permission::read(Role::user('reader'))],
            ]));
            $database->createDocument(self::BOOKS, new Document([
                '$id' => 'silmarillion',
                'title' => 'The Silmarillion',
                'author' => 'tolkien',
                '$permissions' => [],
            ]));
        });
    }

    /**
     * @return list<string>
     */
    private function relatedBookIds(Document $author): array
    {
        $books = $author->getAttribute('books', []);
        $ids = [];
        foreach (\is_array($books) ? $books : [] as $book) {
            $ids[] = $book instanceof Document ? $book->getId() : (\is_string($book) ? $book : '');
        }
        \sort($ids);

        return $ids;
    }

    /**
     * @param  list<string>  $values
     * @return list<string>
     */
    private function sorted(array $values): array
    {
        \sort($values);

        return $values;
    }
}
