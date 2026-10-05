<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

final class SchemaMagicReadsTest extends TestCase
{
    use MagicAccessAssertions;

    private const string BOOKS = 'books';

    private const string AUTHORS = 'authors';

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, RelationType, bool}>
     */
    public static function relationships(): iterable
    {
        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach (RelationType::cases() as $type) {
                yield $adapterName.' '.$type->value.' one way' => [$adapter, $type, false];
                yield $adapterName.' '.$type->value.' two way' => [$adapter, $type, true];
            }
        }
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, RelationType, RelationSide}>
     */
    public static function relationshipSides(): iterable
    {
        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach (RelationType::cases() as $type) {
                foreach (RelationSide::cases() as $side) {
                    yield $adapterName.' '.$type->value.' from the '.$side->value.' side' => [$adapter, $type, $side];
                }
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateCollection(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $collection = CountingCollection::of(new Collection(
            id: 'shelves',
            attributes: [
                CountingAttribute::of(Attribute::string(key: 'label', size: 64, required: true), $recorder),
                CountingAttribute::of(Attribute::string(key: 'codes', size: 32, array: true), $recorder),
                CountingAttribute::of(Attribute::integer(key: 'capacity', default: 10), $recorder),
                CountingAttribute::of(Attribute::datetime(key: 'installedAt'), $recorder),
                CountingAttribute::of(Attribute::relationship(key: 'room', options: [
                    'relatedCollection' => self::AUTHORS,
                    'relationType' => RelationType::ManyToOne->value,
                    'twoWay' => false,
                    'twoWayKey' => 'shelves',
                    'onDelete' => ForeignKeyAction::Restrict->value,
                    'side' => RelationSide::Parent->value,
                ]), $recorder),
            ],
            indexes: [
                CountingIndex::of(Index::key(key: 'by_label', attributes: ['label', 'capacity'], lengths: [32], orders: [Order::Asc, Order::Desc]), $recorder),
                CountingIndex::of(Index::key(key: 'by_codes', attributes: ['codes'], lengths: [32]), $recorder),
                CountingIndex::of(Index::unique(key: 'unique_label', attributes: ['label'], lengths: [64]), $recorder),
            ],
            permissions: $this->permissions(),
        ), $recorder);

        $this->record($recorder, static fn (): Collection => $database->createCollection($collection));

        $this->assertSame(['label', 'codes', 'capacity', 'installedAt', 'room'], $this->attributeKeys($database, 'shelves'));
        $this->assertSame(['by_label', 'by_codes', 'unique_label'], $this->indexKeys($database, 'shelves'));
        $this->assertNoMagicAccess($recorder, 'createCollection()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteCollection(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $database->createRelationship(Relationship::manyToOne(collection: self::BOOKS, relatedCollection: self::AUTHORS, twoWay: true, key: 'author', twoWayKey: 'books'));

        $this->record($recorder, static fn (): bool => $database->deleteCollection(self::BOOKS));

        $this->assertTrue($database->getCollection(self::BOOKS)->isEmpty());
        $this->assertNoMagicAccess($recorder, 'deleteCollection()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateAttribute(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $attribute = CountingAttribute::of(Attribute::datetime(key: 'publishedAt'), $recorder);

        $this->record($recorder, static fn (): bool => $database->createAttribute(self::BOOKS, $attribute));

        $this->assertContains('publishedAt', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'createAttribute()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateAttributes(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $attributes = [
            CountingAttribute::of(Attribute::string(key: 'subtitle', size: 128, default: 'none'), $recorder),
            CountingAttribute::of(Attribute::datetime(key: 'printedAt'), $recorder),
            CountingAttribute::of(Attribute::float(key: 'rating', default: 2.5), $recorder),
            CountingAttribute::of(Attribute::boolean(key: 'lent', default: false), $recorder),
        ];

        $this->record($recorder, static fn (): bool => $database->createAttributes(self::BOOKS, $attributes));

        $keys = $this->attributeKeys($database, self::BOOKS);
        foreach (['subtitle', 'printedAt', 'rating', 'lent'] as $key) {
            $this->assertContains($key, $keys);
        }
        $this->assertNoMagicAccess($recorder, 'createAttributes()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUpdateAttribute(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);

        $this->record($recorder, static fn (): mixed => $database->updateAttribute(self::BOOKS, 'title', size: 128, required: false, default: 'untitled', newKey: 'heading'));

        $keys = $this->attributeKeys($database, self::BOOKS);
        $this->assertContains('heading', $keys);
        $this->assertNotContains('title', $keys);
        $this->assertNoMagicAccess($recorder, 'updateAttribute()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenameAttribute(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);

        $this->record($recorder, static fn (): bool => $database->renameAttribute(self::BOOKS, 'pages', 'length'));

        $keys = $this->attributeKeys($database, self::BOOKS);
        $this->assertContains('length', $keys);
        $this->assertNotContains('pages', $keys);
        $this->assertNoMagicAccess($recorder, 'renameAttribute()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteAttribute(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);

        $this->record($recorder, static fn (): bool => $database->deleteAttribute(self::BOOKS, 'tags'));

        $this->assertNotContains('tags', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'deleteAttribute()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateIndex(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $index = CountingIndex::of(Index::key(key: 'by_title_and_pages', attributes: ['title', 'pages'], lengths: [64], orders: [Order::Asc, Order::Desc]), $recorder);

        $this->record($recorder, static fn (): bool => $database->createIndex(self::BOOKS, $index));

        $this->assertContains('by_title_and_pages', $this->indexKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'createIndex()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateArrayIndex(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $index = CountingIndex::of(new Index(key: 'by_tags', type: IndexType::Key, attributes: ['tags'], lengths: [32]), $recorder);

        $this->record($recorder, static fn (): bool => $database->createIndex(self::BOOKS, $index));

        $this->assertContains('by_tags', $this->indexKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'createIndex() on an array attribute');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenameIndex(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);

        $this->record($recorder, static fn (): bool => $database->renameIndex(self::BOOKS, 'by_title', 'by_heading'));

        $this->assertSame(['by_heading'], $this->indexKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'renameIndex()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteIndex(Closure $adapter): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);

        $this->record($recorder, static fn (): bool => $database->deleteIndex(self::BOOKS, 'by_title'));

        $this->assertSame([], $this->indexKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'deleteIndex()');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('relationships')]
    public function testCreateRelationship(Closure $adapter, RelationType $type, bool $twoWay): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $relationship = CountingRelationship::of(new Relationship(
            collection: self::BOOKS,
            relatedCollection: self::AUTHORS,
            type: $type,
            twoWay: $twoWay,
            key: 'author',
            twoWayKey: 'books',
            onDelete: ForeignKeyAction::SetNull,
        ), $recorder);

        $this->record($recorder, static fn (): bool => $database->createRelationship($relationship));

        $this->assertContains('author', $this->attributeKeys($database, self::BOOKS));
        $this->assertContains('books', $this->attributeKeys($database, self::AUTHORS));
        $this->assertNoMagicAccess($recorder, 'createRelationship() '.$type->value);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('relationships')]
    public function testUpdateRelationship(Closure $adapter, RelationType $type, bool $twoWay): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $database->createRelationship(new Relationship(collection: self::BOOKS, relatedCollection: self::AUTHORS, type: $type, twoWay: $twoWay, key: 'author', twoWayKey: 'books'));

        $twoWayKey = $twoWay ? 'works' : 'books';

        $this->record($recorder, static fn (): bool => $database->updateRelationship(self::BOOKS, 'author', newKey: 'writer', newTwoWayKey: $twoWay ? $twoWayKey : null, onDelete: ForeignKeyAction::Cascade));

        $this->assertContains('writer', $this->attributeKeys($database, self::BOOKS));
        $this->assertContains($twoWayKey, $this->attributeKeys($database, self::AUTHORS));
        $this->assertNoMagicAccess($recorder, 'updateRelationship() '.$type->value);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('relationshipSides')]
    public function testDeleteRelationship(Closure $adapter, RelationType $type, RelationSide $side): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($adapter, $recorder);
        $database->createRelationship(new Relationship(collection: self::BOOKS, relatedCollection: self::AUTHORS, type: $type, twoWay: true, key: 'author', twoWayKey: 'books'));
        [$collection, $key] = $side === RelationSide::Parent ? [self::BOOKS, 'author'] : [self::AUTHORS, 'books'];

        $this->record($recorder, static fn (): bool => $database->deleteRelationship($collection, $key));

        $this->assertNotContains('author', $this->attributeKeys($database, self::BOOKS));
        $this->assertNotContains('books', $this->attributeKeys($database, self::AUTHORS));
        $this->assertNoMagicAccess($recorder, 'deleteRelationship() '.$type->value.' from the '.$side->value.' side');
    }

    /**
     * @param  Closure(): mixed  $operation
     */
    private function record(MagicAccessRecorder $recorder, Closure $operation): void
    {
        $recorder->start();
        try {
            $operation();
        } finally {
            $recorder->stop();
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function database(Closure $adapter, MagicAccessRecorder $recorder): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new CountingDatabase($adapter(), new Cache(new None()), $recorder);
        $database
            ->setAuthorization($authorization)
            ->setDatabase('schema_magic_reads')
            ->setNamespace('schema_magic_reads_'.\uniqid())
            ->enableValidation();
        $database->create();
        $database->addHook(new Relationships($database));

        $database->createCollection(new Collection(
            id: self::BOOKS,
            attributes: [
                Attribute::string(key: 'title', size: 64, required: true),
                Attribute::integer(key: 'pages'),
                Attribute::string(key: 'tags', size: 32, array: true),
            ],
            indexes: [Index::key(key: 'by_title', attributes: ['title'], lengths: [32])],
            permissions: $this->permissions(),
        ));
        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: $this->permissions(),
        ));

        return $database;
    }

    /**
     * @return list<string>
     */
    private function permissions(): array
    {
        return [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->getKey(),
            \array_values($database->getCollection($collection)->getDeclaredAttributes()),
        );
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Index $index): string => $index->getKey(),
            \array_values($database->getCollection($collection)->getIndexes()),
        );
    }
}
