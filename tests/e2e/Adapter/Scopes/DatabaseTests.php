<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;

/**
 * Database::update() renames a database. Adapters without separate databases (Capability::Schemas) and SQLite, which
 * keeps no database name, have nothing to move and are covered by unit tests; Mongo binds its client to one database
 * and is covered in MongoDBTest.
 */
trait DatabaseTests
{
    private const string RENAME_AUTHORS = 'renameAuthors';

    private const string RENAME_BOOKS = 'renameBooks';

    public function testUpdateMovesEveryCollectionUnderTheNewName(): void
    {
        $database = $this->getDatabase();
        if (! $this->renamesDatabases($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $original = $database->getDatabase();
        [$source, $target] = $this->renameNames();

        try {
            $this->createRenameFixture($database, $source);
            $relationships = $database->getAdapter()->hasFeature(Feature\Relationships::class);

            $this->assertTrue($database->update($source, $target));

            $this->assertSame($target, $database->getDatabase(), 'The renamed current database stays current');
            $this->assertFalse($database->exists($source));
            $this->assertTrue($database->exists($target));

            $books = $database->getCollection(self::RENAME_BOOKS);
            $this->assertContains('byTitle', \array_map(static fn (Index $index): string => $index->key, $books->indexes()));

            $authorization = $database->getAuthorization();
            $author = $authorization->skip(fn (): Document => $database->getDocument(self::RENAME_AUTHORS, 'tolkien'));
            $this->assertSame('Tolkien', $author->getAttribute('name'));
            if ($relationships) {
                $this->assertSame(['hobbit', 'silmarillion'], $this->relatedRenameBookIds($author));
            }

            $hidden = $this->visibleRenameBookIds($database);
            $this->assertSame([], $hidden, 'Documents without a read permission for the caller stay hidden');
            $authorization->addRole(Role::user('reader')->toString());
            try {
                $visible = $this->visibleRenameBookIds($database);
            } finally {
                $authorization->removeRole(Role::user('reader')->toString());
            }
            $this->assertSame(['hobbit'], $visible, 'Document permissions move with the documents');

            try {
                $authorization->skip(fn (): Document => $database->createDocument(self::RENAME_BOOKS, new Document([
                    '$id' => 'copy',
                    'title' => 'The Hobbit',
                    '$permissions' => [],
                ])));
                $this->fail('The unique index must move with its table');
            } catch (UniqueException) {
            }
        } finally {
            $this->dropRenameDatabases($database, $original, $source, $target);
        }
    }

    public function testUpdateToAnExistingDatabaseThrowsDuplicate(): void
    {
        $database = $this->getDatabase();
        if (! $this->renamesDatabases($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $original = $database->getDatabase();
        [$source, $target] = $this->renameNames();

        try {
            $this->createRenameFixture($database, $source);
            $database->setDatabase($target)->create();

            try {
                $database->update($source, $target);
                $this->fail('Renaming onto an existing database must throw');
            } catch (DuplicateException $error) {
                $this->assertNotInstanceOf(UniqueException::class, $error);
            }

            $this->assertNull($database->findCollection(self::RENAME_AUTHORS), 'Nothing moved into the existing database');
            $database->setDatabase($source);
            $this->assertSame('Tolkien', $database->getAuthorization()->skip(fn (): Document => $database->getDocument(self::RENAME_AUTHORS, 'tolkien'))->getAttribute('name'));
        } finally {
            $this->dropRenameDatabases($database, $original, $source, $target);
        }
    }

    public function testUpdateOfAMissingDatabaseThrowsNotFound(): void
    {
        $database = $this->getDatabase();
        if (! $this->renamesDatabases($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$source, $target] = $this->renameNames();

        $this->expectException(NotFoundException::class);
        $database->update($source, $target);
    }

    public function testUpdateLeavesNoStaleMetadata(): void
    {
        $database = $this->getDatabase();
        if (! $this->renamesDatabases($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $original = $database->getDatabase();
        [$source, $target] = $this->renameNames();

        try {
            $this->createRenameFixture($database, $source);
            $this->assertNotNull($database->findCollection(self::RENAME_AUTHORS));
            $database->getAuthorization()->skip(fn (): Document => $database->getDocument(self::RENAME_AUTHORS, 'tolkien'));
            $database->setDatabase($target);
            $this->assertNull($this->findCollectionIn($database, self::RENAME_AUTHORS));

            $database->update($source, $target);

            $this->assertNotNull($database->findCollection(self::RENAME_AUTHORS), 'A miss cached under the new name must not hide the moved collection');
            $database->setDatabase($source)->create();
            $this->assertNull($database->findCollection(self::RENAME_AUTHORS), 'A definition cached under the old name must not outlive the rename');
        } finally {
            $this->dropRenameDatabases($database, $original, $source, $target);
        }
    }

    public function testUpdateIsRefusedUnderSharedTables(): void
    {
        $database = $this->getDatabase();
        if (! $database->hasSharedTables()) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $current = $database->getDatabase();

        try {
            $database->update($current, $current.'Renamed');
            $this->fail('A rename under shared tables must be refused');
        } catch (DatabaseException $error) {
            $this->assertNotInstanceOf(DuplicateException::class, $error);
            $this->assertNotInstanceOf(NotFoundException::class, $error);
        }

        $this->assertSame($current, $database->getDatabase());
        $this->assertFalse($database->exists($current.'Renamed'));
    }

    /**
     * A database that does not exist holds no collection: SQL engines report its missing table instead of a miss.
     */
    private function findCollectionIn(Database $database, string $collection): ?Collection
    {
        try {
            return $database->findCollection($collection);
        } catch (NotFoundException) {
            return null;
        }
    }

    /**
     * @return list<string>
     */
    private function relatedRenameBookIds(Document $author): array
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
     * @return list<string>
     */
    private function visibleRenameBookIds(Database $database): array
    {
        $ids = [];
        foreach ($database->find(self::RENAME_BOOKS) as $book) {
            $ids[] = $book->getId();
        }

        return $ids;
    }

    /**
     * SQLite keeps no database name, so its rename moves nothing and its exists() is always false: it is skipped by
     * name, not only through the Schemas capability it does not declare, and covered by unit tests.
     */
    private function renamesDatabases(Database $database): bool
    {
        return ! $database->hasSharedTables()
            && $database->getAdapter()->supports(Capability::Schemas)
            && ! $this->engineIs(SQLite::class);
    }

    /**
     * @return array{string, string}
     */
    private function renameNames(): array
    {
        $suffix = \substr(\uniqid(), -6);

        return [$this->testDatabase.'_from'.$suffix, $this->testDatabase.'_to'.$suffix];
    }

    private function createRenameFixture(Database $database, string $name): void
    {
        $database->setDatabase($name)->create();
        $relationships = $database->getAdapter()->hasFeature(Feature\Relationships::class);

        $database->getAuthorization()->skip(function () use ($database, $relationships): void {
            $database->createCollection(Collection::create(
                id: self::RENAME_AUTHORS,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::read(Role::any())],
                documentSecurity: false,
            ));
            $database->createCollection(Collection::create(
                id: self::RENAME_BOOKS,
                attributes: [Attribute::string(key: 'title', size: 64)],
                indexes: [Index::unique(key: 'byTitle', attributes: ['title'])],
                permissions: [],
                documentSecurity: true,
            ));
            if ($relationships) {
                $database->createRelationship(self::RENAME_AUTHORS, Relationship::oneToMany(
                    relatedCollection: self::RENAME_BOOKS,
                    key: 'books',
                    twoWay: true,
                    twoWayKey: 'author',
                ));
            }

            $database->createDocument(self::RENAME_AUTHORS, new Document(['$id' => 'tolkien', 'name' => 'Tolkien']));
            foreach (['hobbit' => 'The Hobbit', 'silmarillion' => 'The Silmarillion'] as $id => $title) {
                $database->createDocument(self::RENAME_BOOKS, new Document([
                    '$id' => $id,
                    'title' => $title,
                    ...($relationships ? ['author' => 'tolkien'] : []),
                    '$permissions' => $id === 'hobbit' ? [Permission::read(Role::user('reader'))] : [],
                ]));
            }
        });
    }

    private function dropRenameDatabases(Database $database, string $original, string ...$names): void
    {
        foreach ($names as $name) {
            if ($database->exists($name)) {
                $database->delete($name);
            }
        }

        $database->setDatabase($original);
    }
}
