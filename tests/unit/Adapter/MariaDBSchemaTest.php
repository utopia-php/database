<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

final class MariaDBSchemaTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<MariaDB>}>
     */
    public static function engines(): iterable
    {
        yield 'MariaDB' => [MariaDB::class];
        yield 'MySQL' => [MySQL::class];
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testCreateCollectionAddsColumnsOnlyForRelationshipSidesThatStoreAKey(string $class): void
    {
        $adapter = $this->adapter($class);

        $adapter->createCollection('books', [
            Attribute::string('title', size: 64),
            $this->relationship('tags', RelationType::ManyToMany, twoWay: true, side: RelationSide::Parent),
            $this->relationship('cover', RelationType::OneToOne, twoWay: false, side: RelationSide::Child),
            $this->relationship('chapters', RelationType::OneToMany, twoWay: true, side: RelationSide::Parent),
            $this->relationship('shelf', RelationType::ManyToOne, twoWay: true, side: RelationSide::Child),
            $this->relationship('isbn', RelationType::OneToOne, twoWay: false, side: RelationSide::Parent),
            $this->relationship('summary', RelationType::OneToOne, twoWay: true, side: RelationSide::Child),
            $this->relationship('series', RelationType::OneToMany, twoWay: true, side: RelationSide::Child),
            $this->relationship('publisher', RelationType::ManyToOne, twoWay: true, side: RelationSide::Parent),
        ]);

        $create = $this->statements[0] ?? '';
        $this->assertStringStartsWith('CREATE TABLE', $create);

        foreach (['title', 'isbn', 'summary', 'series', 'publisher'] as $stored) {
            $this->assertStringContainsString('`' . $stored . '` ', $create, $stored . ' stores a column');
        }

        foreach (['tags', 'cover', 'chapters', 'shelf'] as $skipped) {
            $this->assertStringNotContainsString('`' . $skipped . '`', $create, $skipped . ' stores nothing on this side');
        }
    }

    public function testCreateCollectionRefusesASpatialIndexWithOrdersWhereTheEngineCannotOrderIt(): void
    {
        $adapter = $this->adapter(MySQL::class);

        try {
            $adapter->createCollection('places', [Attribute::point('location', required: true)], [
                new Index('location_index', IndexType::Spatial, ['location'], orders: [Order::Desc]),
            ]);
            $this->fail('A spatial index with orders must be refused where the engine cannot order it');
        } catch (DatabaseException $error) {
            $this->assertSame('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.', $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    public function testCreateCollectionKeepsASpatialIndexOrderWhereTheEngineSupportsIt(): void
    {
        $adapter = $this->adapter(MariaDB::class);

        $adapter->createCollection('places', [Attribute::point('location', required: true)], [
            new Index('location_index', IndexType::Spatial, ['location'], orders: [Order::Desc]),
        ]);

        $this->assertStringContainsString('SPATIAL INDEX `location_index` (`location` DESC)', $this->statements[0] ?? '');
    }

    /**
     * @return iterable<string, array{class-string<MariaDB>, IndexType}>
     */
    public static function unknownIndexTypes(): iterable
    {
        foreach (self::engines() as $engine => [$class]) {
            foreach ([IndexType::Ttl, IndexType::Index, IndexType::Object, IndexType::Trigram, IndexType::HnswCosine] as $type) {
                yield $engine . ' ' . $type->value => [$class, $type];
            }
        }
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('unknownIndexTypes')]
    public function testCreateIndexRefusesATypeTheEngineDoesNotCreate(string $class, IndexType $type): void
    {
        $adapter = $this->adapterWithCollection($class);

        try {
            $adapter->createIndex('events', new Index('happened_index', $type, ['happened']));
            $this->fail('An index type the engine does not create must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame(
                'Unknown index type: ' . $type->value . '. Must be one of key, unique, fulltext, spatial',
                $error->getMessage(),
            );
        }

        $this->assertSame([], $this->statements);
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testCreateIndexCreatesAKeyIndex(string $class): void
    {
        $adapter = $this->adapterWithCollection($class);

        $this->assertTrue($adapter->createIndex('events', new Index('happened_index', IndexType::Key, ['happened'])));
        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('`happened_index`', $this->statements[0]);
    }

    private function relationship(string $key, RelationType $type, bool $twoWay, RelationSide $side): Attribute
    {
        return Attribute::relationship(key: $key, options: [
            'relatedCollection' => 'related_' . $key,
            'relationType' => $type->value,
            'twoWay' => $twoWay,
            'twoWayKey' => 'back_' . $key,
            'side' => $side->value,
        ]);
    }

    /**
     * @param class-string<MariaDB> $class
     */
    private function adapter(string $class): MariaDB
    {
        $adapter = new $class($this->connection());
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }

    /**
     * @param class-string<MariaDB> $class
     */
    private function adapterWithCollection(string $class): MariaDB
    {
        $collection = new Document([
            '$id' => 'events',
            'attributes' => \json_encode([['$id' => 'happened', 'type' => 'datetime', 'array' => false]]),
        ]);

        $adapter = $class === MySQL::class
            ? new class ($this->connection(), $collection) extends MySQL {
                public function __construct(object $pdo, private readonly Document $collection)
                {
                    parent::__construct($pdo);
                }

                public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
                {
                    return $collection->getId() === Database::METADATA && $id === $this->collection->getId() ? $this->collection : new Document();
                }
            }
            : new class ($this->connection(), $collection) extends MariaDB {
                public function __construct(object $pdo, private readonly Document $collection)
                {
                    parent::__construct($pdo);
                }

                public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
                {
                    return $collection->getId() === Database::METADATA && $id === $this->collection->getId() ? $this->collection : new Document();
                }
            };
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }

    private function connection(): PDO
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });

        return $pdo;
    }
}
