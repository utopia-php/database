<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Mongo\Client;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

final class MongoSchemaMagicReadsTest extends TestCase
{
    use MagicAccessAssertions;

    private const string BOOKS = 'books';

    private const string AUTHORS = 'authors';

    /**
     * @return iterable<string, array{IndexType}>
     */
    public static function indexes(): iterable
    {
        foreach ([IndexType::Key, IndexType::Unique, IndexType::Fulltext, IndexType::Ttl] as $type) {
            yield $type->value => [$type];
        }
    }

    /**
     * @return iterable<string, array{RelationType}>
     */
    public static function relationships(): iterable
    {
        foreach (RelationType::cases() as $type) {
            yield $type->value => [$type];
        }
    }

    /**
     * @return iterable<string, array{RelationType, RelationSide}>
     */
    public static function relationshipSides(): iterable
    {
        foreach (RelationType::cases() as $type) {
            foreach (RelationSide::cases() as $side) {
                yield $type->value.' '.$side->value.' side' => [$type, $side];
            }
        }
    }

    public function testCollectionLimits(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $collection = CountingCollection::of(new Collection(
            id: self::BOOKS,
            attributes: $this->counting($recorder, [
                Attribute::string(key: 'title', size: 64),
                Attribute::string(key: 'tags', size: 32, array: true),
                Attribute::integer(key: 'pages', size: 8),
            ]),
            indexes: [CountingIndex::of(Index::key(key: 'by_title', attributes: ['title']), $recorder)],
        ), $recorder);

        $recorder->start();
        $adapter->getAttributeWidth($collection);
        $attributes = $adapter->getCountOfAttributes($collection);
        $indexes = $adapter->getCountOfIndexes($collection);
        $recorder->stop();

        $this->assertGreaterThan(0, $attributes);
        $this->assertGreaterThan(0, $indexes);
        $this->assertNoMagicAccess($recorder, 'MongoDB getAttributeWidth(), getCountOfAttributes() and getCountOfIndexes()');
    }

    public function testCreateCollection(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attributes = $this->counting($recorder, [
            Attribute::string(key: 'label', size: 64, required: true),
            Attribute::integer(key: 'capacity', size: 8),
            Attribute::datetime(key: 'expiresAt'),
        ]);
        $indexes = [
            CountingIndex::of(Index::key(key: 'by_label', attributes: ['label', 'capacity'], orders: [Order::Asc, Order::Desc]), $recorder),
            CountingIndex::of(Index::unique(key: 'unique_label', attributes: ['label']), $recorder),
            CountingIndex::of(Index::fullText(key: 'search_label', attributes: ['label']), $recorder),
            CountingIndex::of(Index::ttl(key: 'expiry', attributes: ['expiresAt'], ttl: 3600), $recorder),
        ];

        $recorder->start();
        $created = $adapter->createCollection('shelves', $attributes, $indexes);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'MongoDB createCollection()');
    }

    public function testCreateAttribute(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attribute = CountingAttribute::of(Attribute::string(key: 'subtitle', size: 128), $recorder);

        $recorder->start();
        $created = $adapter->createAttribute(self::BOOKS, $attribute) && $adapter->createAttributes(self::BOOKS, [$attribute]);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'MongoDB createAttribute() and createAttributes()');
    }

    public function testUpdateAttribute(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attribute = CountingAttribute::of(Attribute::integer(key: 'pages', size: 8), $recorder);

        $recorder->start();
        $updated = $adapter->updateAttribute(self::BOOKS, $attribute, 'length');
        $recorder->stop();

        $this->assertTrue($updated);
        $this->assertNoMagicAccess($recorder, 'MongoDB updateAttribute()');
    }

    #[DataProvider('indexes')]
    public function testCreateIndex(IndexType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $index = CountingIndex::of(match ($type) {
            IndexType::Ttl => Index::ttl(key: 'expiry', attributes: ['printedAt'], ttl: 3600),
            IndexType::Fulltext => Index::fullText(key: 'search_title', attributes: ['title']),
            default => new Index(key: 'by_title_and_pages', type: $type, attributes: ['title', 'pages'], orders: [Order::Asc, Order::Desc]),
        }, $recorder);

        $recorder->start();
        $created = $adapter->createIndex(self::BOOKS, $index, [
            'title' => ColumnType::String->value,
            'pages' => ColumnType::Integer->value,
            'printedAt' => ColumnType::Datetime->value,
        ]);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'MongoDB createIndex() '.$type->value);
    }

    #[DataProvider('relationships')]
    public function testCreateRelationship(RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $created = $adapter->createRelationship($relationship);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'MongoDB createRelationship() '.$type->value);
    }

    #[DataProvider('relationships')]
    public function testUpdateRelationship(RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $updated = $adapter->updateRelationship($relationship, 'writer', 'works');
        $recorder->stop();

        $this->assertTrue($updated);
        $this->assertNoMagicAccess($recorder, 'MongoDB updateRelationship() '.$type->value);
    }

    #[DataProvider('relationshipSides')]
    public function testDeleteRelationship(RelationType $type, RelationSide $side): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $relationship = CountingRelationship::of($this->relationship($type, $side), $recorder);

        $recorder->start();
        $deleted = $adapter->deleteRelationship($relationship);
        $recorder->stop();

        $this->assertTrue($deleted);
        $this->assertNoMagicAccess($recorder, 'MongoDB deleteRelationship() '.$type->value.' from the '.$side->value.' side');
    }

    private function relationship(RelationType $type, RelationSide $side): Relationship
    {
        return $side === RelationSide::Parent
            ? new Relationship(collection: self::BOOKS, relatedCollection: self::AUTHORS, type: $type, twoWay: true, key: 'author', twoWayKey: 'books', onDelete: ForeignKeyAction::SetNull, side: $side)
            : new Relationship(collection: self::AUTHORS, relatedCollection: self::BOOKS, type: $type, twoWay: true, key: 'books', twoWayKey: 'author', onDelete: ForeignKeyAction::SetNull, side: $side);
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<Attribute>
     */
    private function counting(MagicAccessRecorder $recorder, array $attributes): array
    {
        return \array_map(static fn (Attribute $attribute): Attribute => CountingAttribute::of($attribute, $recorder), $attributes);
    }

    private function adapter(MagicAccessRecorder $recorder): Mongo
    {
        $client = new class () extends Client {
            /**
             * @var list<string>
             */
            private array $indexNames = [];

            public function __construct()
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createCollection(string $name, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function dropCollection(string $name, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                foreach ($indexes as $index) {
                    if (\is_array($index) && \is_string($index['name'] ?? null)) {
                        $this->indexNames[] = $index['name'];
                    }
                }

                return true;
            }

            /**
             * @param  array<mixed>  $where
             * @param  array<mixed>  $updates
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): int
            {
                return 0;
            }

            /**
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass
            {
                $batch = \array_map(static fn (string $name): stdClass => (object) ['name' => $name], $this->indexNames);

                return (object) ['cursor' => (object) ['firstBatch' => $batch, 'id' => 0]];
            }
        };

        $adapter = new class ($client) extends Mongo {
            use CountingAdapterHooks;

            /**
             * @param  array<mixed>  $queries
             */
            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                if ($collection->getId() !== Database::METADATA) {
                    return new Document();
                }

                return match ($id) {
                    'books' => new Document(['$id' => 'books', '$sequence' => '1']),
                    'authors' => new Document(['$id' => 'authors', '$sequence' => '2']),
                    default => new Document(),
                };
            }
        };
        $adapter->setNamespace('engine');
        $adapter->countHookReads($recorder);

        return $adapter;
    }
}
