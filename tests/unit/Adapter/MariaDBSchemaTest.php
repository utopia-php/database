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
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Index;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

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
            $this->relationship('tags', RelationshipType::ManyToMany, twoWay: true, side: RelationshipSide::Parent),
            $this->relationship('cover', RelationshipType::OneToOne, twoWay: false, side: RelationshipSide::Child),
            $this->relationship('chapters', RelationshipType::OneToMany, twoWay: true, side: RelationshipSide::Parent),
            $this->relationship('shelf', RelationshipType::ManyToOne, twoWay: true, side: RelationshipSide::Child),
            $this->relationship('isbn', RelationshipType::OneToOne, twoWay: false, side: RelationshipSide::Parent),
            $this->relationship('summary', RelationshipType::OneToOne, twoWay: true, side: RelationshipSide::Child),
            $this->relationship('series', RelationshipType::OneToMany, twoWay: true, side: RelationshipSide::Child),
            $this->relationship('publisher', RelationshipType::ManyToOne, twoWay: true, side: RelationshipSide::Parent),
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
                Index::spatial('location_index', 'location', order: OrderDirection::Desc),
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
            Index::spatial('location_index', 'location', order: OrderDirection::Desc),
        ]);

        $this->assertStringContainsString('SPATIAL INDEX `location_index` (`location` DESC)', $this->statements[0] ?? '');
    }

    /**
     * @return iterable<string, array{class-string<MariaDB>, IndexType}>
     */
    public static function unknownIndexTypes(): iterable
    {
        foreach (self::engines() as $engine => [$class]) {
            foreach ([IndexType::Ttl, IndexType::Object, IndexType::Trigram, IndexType::HnswCosine] as $type) {
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
            $adapter->createIndex('events', Index::fromArray(['key' => 'happened_index', 'type' => $type, 'attributes' => ['happened'], 'ttl' => 3600]));
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

        $this->assertTrue($adapter->createIndex('events', Index::key('happened_index', ['happened'])));
        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('`happened_index`', $this->statements[0]);
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testCreateIndexOnACollectionWithoutADefinitionIsNotFound(string $class): void
    {
        $adapter = $this->adapterWithCollection($class);

        try {
            $adapter->createIndex('missing', Index::key('happened_index', ['happened']));
            $this->fail('An index on a collection without a definition must not be created');
        } catch (NotFoundException $error) {
            $this->assertSame('Collection not found', $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    private function relationship(string $key, RelationshipType $type, bool $twoWay, RelationshipSide $side): Attribute
    {
        return Attribute::fromArray(['key' => $key, 'type' => ColumnType::Relationship, 'options' => [
            'relatedCollection' => 'related_' . $key,
            'relationType' => $type->value,
            'twoWay' => $twoWay,
            'twoWayKey' => 'back_' . $key,
            'side' => $side->value,
        ]]);
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

        if ($class === MySQL::class) {
            $adapter = new class ($this->connection(), $collection) extends MySQL {
                public function __construct(object $pdo, private readonly Document $collection)
                {
                    parent::__construct($pdo);
                }

                public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
                {
                    return $collection->getId() === Database::METADATA && $id === $this->collection->getId() ? $this->collection : new Document();
                }
            };
        } else {
            $adapter = new class ($this->connection(), $collection) extends MariaDB {
                public function __construct(object $pdo, private readonly Document $collection)
                {
                    parent::__construct($pdo);
                }

                public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
                {
                    return $collection->getId() === Database::METADATA && $id === $this->collection->getId() ? $this->collection : new Document();
                }
            };
        }

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
