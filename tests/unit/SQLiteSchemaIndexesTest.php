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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

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
        $database->createIndex(self::COLLECTION, Index::fullText(key: 'title_search', attributes: ['title']));
        $database->createIndex(self::COLLECTION, Index::fullText(key: 'body_search', attributes: ['body']));

        $this->assertSame([
            'body_search' => ['FULLTEXT', ['body']],
            'title_search' => ['FULLTEXT', ['title']],
        ], $this->fulltextIndexes($database));

        $database->renameIndex(self::COLLECTION, 'title_search', 'title_lookup');

        $this->assertSame([
            'body_search' => ['FULLTEXT', ['body']],
            'title_lookup' => ['FULLTEXT', ['title']],
        ], $this->fulltextIndexes($database));
    }

    #[DataProvider('tables')]
    public function testDeletingOneOfTwoFulltextIndexesKeepsTheOther(bool $shared): void
    {
        $database = $this->database($shared);
        $database->createIndex(self::COLLECTION, Index::fullText(key: 'title_search', attributes: ['title']));
        $database->createIndex(self::COLLECTION, Index::fullText(key: 'body_search', attributes: ['body']));

        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'title_search'));

        $this->assertSame(['body_search' => ['FULLTEXT', ['body']]], $this->fulltextIndexes($database));
        $this->assertSame(['fox'], $this->search($database, 'body', 'lazy'));

        try {
            $this->search($database, 'title', 'quick');
            $this->fail('A search on an attribute whose fulltext index was deleted must be refused');
        } catch (QueryException $error) {
            $this->assertSame('Searching by attribute "title" requires a fulltext index.', $error->getMessage());
        }

        $this->assertTrue($database->createIndex(self::COLLECTION, Index::fullText(key: 'title_search', attributes: ['title'])));
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

        $this->assertSame([1, [...$tenant, 'title']], $indexes['by_title'] ?? null);
        $this->assertSame([0, [...$tenant, 'body']], $indexes['by_body'] ?? null);
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
            $database->createCollection(new Collection(
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
        $this->assertSame([1, ['_tenant', 'title']], $first['by_title'] ?? null);
        $this->assertSame([1, ['_tenant', 'title']], $second['by_title'] ?? null);
        $this->assertSame([1, ['_tenant', 'title']], $first['lookup'] ?? null);
        $this->assertSame([0, ['_tenant', 'body']], $second['lookup'] ?? null);
    }

    public function testAnOrphanIndexIsListedForReconciliation(): void
    {
        $database = $this->database(false);
        $adapter = $database->getAdapter();
        $adapter->createIndex(self::COLLECTION, Index::key(key: 'lookup', attributes: ['title']));

        $this->assertSame([1, ['title']], $this->indexes($database)['lookup'] ?? null);

        $this->assertTrue($adapter->deleteIndex(self::COLLECTION, 'lookup'));
        $this->assertArrayNotHasKey('lookup', $this->indexes($database));

        $this->assertTrue($adapter->createIndex(self::COLLECTION, Index::unique(key: 'lookup', attributes: ['body'])));
        $this->assertSame([0, ['body']], $this->indexes($database)['lookup'] ?? null);
    }

    #[DataProvider('tables')]
    public function testRenamingAnIndexTheSchemaNoLongerHasRebuildsItUnderTheNewName(bool $shared): void
    {
        $database = $this->database($shared);
        $database->createIndex(self::COLLECTION, Index::key(key: 'by_title', attributes: ['title']));
        $database->getAdapter()->deleteIndex(self::COLLECTION, 'by_title');
        $this->assertArrayNotHasKey('by_title', $this->indexes($database));

        $this->assertTrue($database->renameIndex(self::COLLECTION, 'by_title', 'by_heading'));

        $indexes = $this->indexes($database);
        $this->assertArrayNotHasKey('by_title', $indexes);
        $this->assertSame([1, [...($shared ? ['_tenant'] : []), 'title']], $indexes['by_heading'] ?? null, 'the metadata names an index the schema has');
        $this->assertSame(['by_heading'], \array_values(\array_map(
            static fn (Index $index): string => $index->key,
            $database->getCollection(self::COLLECTION)->indexes,
        )));
    }

    /**
     * @return array<string, array{int, list<string>}>
     */
    private function indexes(Database $database): array
    {
        $indexes = [];
        foreach ($database->getSchemaIndexes(self::COLLECTION) as $index) {
            $nonUnique = $index->getAttribute('nonUnique');
            $columns = $index->getAttribute('columns');
            $this->assertIsInt($nonUnique);
            $this->assertIsArray($columns);
            $this->assertArrayNotHasKey($index->getId(), $indexes, 'Each index is listed once');
            /** @var list<string> $columns */
            $indexes[$index->getId()] = [$nonUnique, $columns];
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
     * @return array<string, array{string, list<string>}>
     */
    private function fulltextIndexes(Database $database): array
    {
        $indexes = [];
        foreach ($database->getSchemaIndexes(self::COLLECTION) as $index) {
            $type = $index->getAttribute('indexType');
            $columns = $index->getAttribute('columns');
            $this->assertIsString($type);
            $this->assertIsArray($columns);
            if ($type !== 'FULLTEXT') {
                continue;
            }
            /** @var list<string> $columns */
            $indexes[$index->getId()] = [$type, $columns];
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
        $database->createCollection(new Collection(
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
