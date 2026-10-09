<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

final class SQLiteSchemaIndexesTest extends TestCase
{
    private const string COLLECTION = 'articles';

    /**
     * @return iterable<string, array{bool}>
     */
    public static function tables(): iterable
    {
        yield 'plain tables' => [false];
        yield 'shared tables' => [true];
    }

    #[DataProvider('tables')]
    public function testFulltextIndexesAreListedUnderTheirIds(bool $shared): void
    {
        $database = $this->database($shared);
        $database->createIndex(self::COLLECTION, Index::fulltext(key: 'title_search', attributes: ['title']));
        $database->createIndex(self::COLLECTION, Index::fulltext(key: 'body_search', attributes: ['body']));

        $this->assertSame([
            'body_search' => [IndexType::Fulltext, ['body']],
            'title_search' => [IndexType::Fulltext, ['title']],
        ], $this->fulltextIndexes($database));

        $database->renameIndex(self::COLLECTION, 'title_search', 'title_lookup');

        $this->assertSame([
            'body_search' => [IndexType::Fulltext, ['body']],
            'title_lookup' => [IndexType::Fulltext, ['title']],
        ], $this->fulltextIndexes($database));
    }

    #[DataProvider('tables')]
    public function testDeletingOneOfTwoFulltextIndexesKeepsTheOther(bool $shared): void
    {
        $database = $this->database($shared);
        $database->createIndex(self::COLLECTION, Index::fulltext(key: 'title_search', attributes: ['title']));
        $database->createIndex(self::COLLECTION, Index::fulltext(key: 'body_search', attributes: ['body']));

        $database->deleteIndex(self::COLLECTION, 'title_search');

        $this->assertSame(['body_search' => [IndexType::Fulltext, ['body']]], $this->fulltextIndexes($database));
        $this->assertSame(['fox'], $this->search($database, 'body', 'lazy'));

        try {
            $this->search($database, 'title', 'quick');
            $this->fail('A search on an attribute whose fulltext index was deleted must be refused');
        } catch (QueryException $error) {
            $this->assertSame('Searching by attribute "title" requires a fulltext index.', $error->getMessage());
        }

        $database->createIndex(self::COLLECTION, Index::fulltext(key: 'title_search', attributes: ['title']));
        $this->assertSame(['fox'], $this->search($database, 'title', 'quick'));
    }

    #[DataProvider('tables')]
    public function testIndexesAreListedUnderTheirIds(bool $shared): void
    {
        $database = $this->database($shared);
        $database->createIndex(self::COLLECTION, Index::key(key: 'by_title', attributes: ['title']));
        $database->createIndex(self::COLLECTION, Index::unique(key: 'by_body', attributes: ['body']));
        $tenant = $shared ? ['_tenant'] : [];

        $indexes = $this->indexes($database);

        $this->assertSame([IndexType::Key, [...$tenant, 'title']], $indexes['by_title'] ?? null);
        $this->assertSame([IndexType::Unique, [...$tenant, 'body']], $indexes['by_body'] ?? null);
        $this->assertArrayHasKey('_index1', $indexes);
        $namespace = $database->getNamespace();
        $this->assertNotSame('', $namespace);
        foreach (\array_keys($indexes) as $id) {
            $this->assertStringStartsNotWith($namespace, $id);
        }
    }

    public function testSharedTablesListEveryTenantsIndexOnceAndPreferTheirOwn(): void
    {
        $database = $this->database(true);
        $database->withTenant(2, function () use ($database): void {
            $database->createCollection(Collection::create(
                id: self::COLLECTION,
                attributes: [
                    Attribute::string(key: 'title', size: 64),
                    Attribute::string(key: 'body', size: 64),
                ],
            ));
        });
        $database->createIndex(self::COLLECTION, Index::key(key: 'by_title', attributes: ['title']));
        $database->getAdapter()->createIndex(self::COLLECTION, Index::key(key: 'lookup', attributes: ['title']));
        $database->withTenant(2, fn (): bool => $database->getAdapter()->createIndex(self::COLLECTION, Index::unique(key: 'lookup', attributes: ['body'])));

        $first = $this->indexes($database);
        $second = $database->withTenant(2, fn (): array => $this->indexes($database));

        $this->assertSame(\array_keys($first), \array_keys($second));
        $this->assertSame([IndexType::Key, ['_tenant', 'title']], $first['by_title'] ?? null);
        $this->assertSame([IndexType::Key, ['_tenant', 'title']], $second['by_title'] ?? null);
        $this->assertSame([IndexType::Key, ['_tenant', 'title']], $first['lookup'] ?? null);
        $this->assertSame([IndexType::Unique, ['_tenant', 'body']], $second['lookup'] ?? null);
    }

    public function testAnOrphanIndexIsListedForReconciliation(): void
    {
        $database = $this->database(false);
        $adapter = $database->getAdapter();
        $adapter->createIndex(self::COLLECTION, Index::key(key: 'lookup', attributes: ['title']));

        $this->assertSame([IndexType::Key, ['title']], $this->indexes($database)['lookup'] ?? null);

        $this->assertTrue($adapter->deleteIndex(self::COLLECTION, 'lookup'));
        $this->assertArrayNotHasKey('lookup', $this->indexes($database));

        $this->assertTrue($adapter->createIndex(self::COLLECTION, Index::unique(key: 'lookup', attributes: ['body'])));
        $this->assertSame([IndexType::Unique, ['body']], $this->indexes($database)['lookup'] ?? null);
    }

    #[DataProvider('tables')]
    public function testRenamingAnIndexTheSchemaNoLongerHasRebuildsItUnderTheNewName(bool $shared): void
    {
        $database = $this->database($shared);
        $database->createIndex(self::COLLECTION, Index::key(key: 'by_title', attributes: ['title']));
        $database->getAdapter()->deleteIndex(self::COLLECTION, 'by_title');
        $this->assertArrayNotHasKey('by_title', $this->indexes($database));

        $database->renameIndex(self::COLLECTION, 'by_title', 'by_heading');

        $indexes = $this->indexes($database);
        $this->assertArrayNotHasKey('by_title', $indexes);
        $this->assertSame([IndexType::Key, [...($shared ? ['_tenant'] : []), 'title']], $indexes['by_heading'] ?? null, 'the metadata names an index the schema has');
        $this->assertSame(['by_heading'], \array_map(
            static fn (Index $index): string => $index->key,
            $database->getCollection(self::COLLECTION)->indexes(),
        ));
    }

    /**
     * @return array<string, array{IndexType, list<string>}>
     */
    private function indexes(Database $database): array
    {
        $indexes = [];
        foreach ($database->getSchemaIndexes(self::COLLECTION) as $index) {
            $this->assertArrayNotHasKey($index->name, $indexes, 'Each index is listed once');
            $indexes[$index->name] = [$index->type, $index->columns];
        }
        \ksort($indexes);

        return $indexes;
    }

    /**
     * @return array<string>
     */
    private function search(Database $database, string $attribute, string $term): array
    {
        return \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->find(self::COLLECTION, [Query::search($attribute, $term)]),
        );
    }

    /**
     * @return array<string, array{IndexType, list<string>}>
     */
    private function fulltextIndexes(Database $database): array
    {
        $indexes = [];
        foreach ($database->getSchemaIndexes(self::COLLECTION) as $index) {
            if ($index->type === IndexType::Fulltext) {
                $indexes[$index->name] = [$index->type, $index->columns];
            }
        }
        \ksort($indexes);

        return $indexes;
    }

    private function database(bool $shared): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('schema_indexes')
            ->setNamespace('schema_indexes');

        if ($shared) {
            $database->setSharedTables(true)->setTenant(1);
        }

        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64),
                Attribute::string(key: 'body', size: 64),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument(self::COLLECTION, new Document([
            Document::ID => 'fox',
            'title' => 'quick brown fox',
            'body' => 'lazy dog',
        ]));

        return $database;
    }
}
