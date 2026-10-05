<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\CountingAttribute;
use Tests\Unit\CountingCollection;
use Tests\Unit\CountingIndex;
use Tests\Unit\CountingRelationship;
use Tests\Unit\MagicAccessAssertions;
use Tests\Unit\MagicAccessRecorder;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

final class SQLSchemaMagicReadsTest extends TestCase
{
    use MagicAccessAssertions;

    private const string MARIADB = 'MariaDB';

    private const string MYSQL = 'MySQL';

    private const string POSTGRES = 'Postgres';

    private const string SQLITE = 'SQLite';

    private const string BOOKS = 'books';

    private const string AUTHORS = 'authors';

    private const string JUNCTION = '_1_2';

    /**
     * @return iterable<string, array{string}>
     */
    public static function engines(): iterable
    {
        foreach ([self::MARIADB, self::MYSQL, self::POSTGRES, self::SQLITE] as $engine) {
            yield $engine => [$engine];
        }
    }

    /**
     * @return iterable<string, array{string, IndexType}>
     */
    public static function indexes(): iterable
    {
        foreach (self::engines() as $engine => [$name]) {
            foreach ([IndexType::Key, IndexType::Unique] as $type) {
                yield $engine.' '.$type->value => [$name, $type];
            }
        }
    }

    /**
     * @return iterable<string, array{string, RelationType}>
     */
    public static function relationships(): iterable
    {
        foreach (self::engines() as $engine => [$name]) {
            foreach (RelationType::cases() as $type) {
                yield $engine.' '.$type->value => [$name, $type];
            }
        }
    }

    /**
     * @return iterable<string, array{string, RelationType, RelationSide}>
     */
    public static function relationshipSides(): iterable
    {
        foreach (self::engines() as $engine => [$name]) {
            foreach (RelationType::cases() as $type) {
                foreach (RelationSide::cases() as $side) {
                    yield $engine.' '.$type->value.' '.$side->value.' side' => [$name, $type, $side];
                }
            }
        }
    }

    #[DataProvider('engines')]
    public function testCollectionLimits(string $engine): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $collection = CountingCollection::of(new Collection(
            id: self::BOOKS,
            attributes: $this->counting($recorder, [
                Attribute::string(key: 'title', size: 64),
                Attribute::string(key: 'tags', size: 32, array: true),
                Attribute::integer(key: 'pages', size: 8),
                Attribute::float(key: 'rating'),
                Attribute::datetime(key: 'printedAt'),
                Attribute::point(key: 'origin'),
                $this->relationshipAttribute('author', RelationType::ManyToOne, RelationSide::Parent),
            ]),
            indexes: [CountingIndex::of(Index::key(key: 'by_title', attributes: ['title'], lengths: [32]), $recorder)],
        ), $recorder);

        $recorder->start();
        $width = $adapter->getAttributeWidth($collection);
        $attributes = $adapter->getCountOfAttributes($collection);
        $indexes = $adapter->getCountOfIndexes($collection);
        $recorder->stop();

        $this->assertGreaterThan(0, $width);
        $this->assertGreaterThan(0, $attributes);
        $this->assertGreaterThan(0, $indexes);
        $this->assertNoMagicAccess($recorder, $engine.' getAttributeWidth(), getCountOfAttributes() and getCountOfIndexes()');
    }

    #[DataProvider('engines')]
    public function testCreateCollection(string $engine): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $attributes = $this->counting($recorder, [
            Attribute::string(key: 'label', size: 64, required: true),
            Attribute::string(key: 'codes', size: 32, array: true),
            Attribute::integer(key: 'capacity', size: 8, signed: false),
            Attribute::datetime(key: 'installedAt'),
        ]);
        foreach (RelationType::cases() as $type) {
            foreach (RelationSide::cases() as $side) {
                $attributes[] = CountingAttribute::of($this->relationshipAttribute($type->value.'_'.$side->value, $type, $side), $recorder);
            }
        }
        $indexes = [
            CountingIndex::of(Index::key(key: 'by_label', attributes: ['label', 'capacity'], lengths: [32], orders: [Order::Asc, Order::Desc]), $recorder),
            CountingIndex::of(Index::unique(key: 'unique_label', attributes: ['label'], lengths: [64]), $recorder),
        ];

        $recorder->start();
        $created = $adapter->createCollection('shelves', $attributes, $indexes);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, $engine.' createCollection()');
    }

    #[DataProvider('engines')]
    public function testCreateAttribute(string $engine): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $attribute = CountingAttribute::of(Attribute::string(key: 'subtitle', size: 128, default: 'none'), $recorder);

        $recorder->start();
        $created = $adapter->createAttribute(self::BOOKS, $attribute);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, $engine.' createAttribute()');
    }

    #[DataProvider('engines')]
    public function testCreateAttributes(string $engine): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $attributes = $this->counting($recorder, [
            Attribute::string(key: 'subtitle', size: 128),
            Attribute::string(key: 'labels', size: 32, array: true),
            Attribute::integer(key: 'copies', size: 8, signed: false),
            Attribute::double(key: 'price'),
            Attribute::boolean(key: 'lent'),
        ]);

        $recorder->start();
        $created = $adapter->createAttributes(self::BOOKS, $attributes);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, $engine.' createAttributes()');
    }

    #[DataProvider('engines')]
    public function testUpdateAttribute(string $engine): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $attribute = CountingAttribute::of(Attribute::integer(key: 'pages', size: 8, signed: false), $recorder);

        $recorder->start();
        $updated = $adapter->updateAttribute(self::BOOKS, $attribute, 'length');
        $recorder->stop();

        $this->assertTrue($updated);
        $this->assertNoMagicAccess($recorder, $engine.' updateAttribute()');
    }

    #[DataProvider('indexes')]
    public function testCreateIndex(string $engine, IndexType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $index = CountingIndex::of(new Index(key: 'by_title_and_pages', type: $type, attributes: ['title', 'pages'], lengths: [32], orders: [Order::Asc, Order::Desc]), $recorder);

        $recorder->start();
        $created = $adapter->createIndex(self::BOOKS, $index, ['title' => ColumnType::String->value, 'pages' => ColumnType::Integer->value]);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, $engine.' createIndex() '.$type->value);
    }

    #[DataProvider('relationships')]
    public function testCreateRelationship(string $engine, RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $created = $adapter->createRelationship($relationship);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, $engine.' createRelationship() '.$type->value);
    }

    #[DataProvider('relationships')]
    public function testUpdateRelationship(string $engine, RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $this->relate($adapter, $type);
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $updated = $adapter->updateRelationship($relationship, 'writer', 'works');
        $recorder->stop();

        $this->assertTrue($updated);
        $this->assertNoMagicAccess($recorder, $engine.' updateRelationship() '.$type->value);
    }

    #[DataProvider('relationshipSides')]
    public function testDeleteRelationship(string $engine, RelationType $type, RelationSide $side): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($engine, $recorder);
        $this->relate($adapter, $type);
        $relationship = CountingRelationship::of($this->relationship($type, $side), $recorder);

        $recorder->start();
        $deleted = $adapter->deleteRelationship($relationship);
        $recorder->stop();

        $this->assertTrue($deleted);
        $this->assertNoMagicAccess($recorder, $engine.' deleteRelationship() '.$type->value.' from the '.$side->value.' side');
    }

    private function relationship(RelationType $type, RelationSide $side): Relationship
    {
        return $side === RelationSide::Parent
            ? new Relationship(collection: self::BOOKS, relatedCollection: self::AUTHORS, type: $type, twoWay: true, key: 'author', twoWayKey: 'books', onDelete: ForeignKeyAction::SetNull, side: $side)
            : new Relationship(collection: self::AUTHORS, relatedCollection: self::BOOKS, type: $type, twoWay: true, key: 'books', twoWayKey: 'author', onDelete: ForeignKeyAction::SetNull, side: $side);
    }

    private function relationshipAttribute(string $key, RelationType $type, RelationSide $side): Attribute
    {
        return Attribute::relationship(key: $key, options: [
            'relatedCollection' => self::AUTHORS,
            'relationType' => $type->value,
            'twoWay' => true,
            'twoWayKey' => 'back_'.$key,
            'onDelete' => ForeignKeyAction::Restrict->value,
            'side' => $side->value,
        ]);
    }

    private function relate(SQL $adapter, RelationType $type): void
    {
        if ($type === RelationType::ManyToMany) {
            $adapter->createCollection(self::JUNCTION, [
                Attribute::string(key: 'author', size: 255),
                Attribute::string(key: 'books', size: 255),
            ]);

            return;
        }

        $adapter->createRelationship($this->relationship($type, RelationSide::Parent));
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<Attribute>
     */
    private function counting(MagicAccessRecorder $recorder, array $attributes): array
    {
        return \array_map(static fn (Attribute $attribute): Attribute => CountingAttribute::of($attribute, $recorder), $attributes);
    }

    private function adapter(string $engine, MagicAccessRecorder $recorder): SQL
    {
        $adapter = match ($engine) {
            self::MARIADB => new class ($this->connection()) extends MariaDB {
                use SQLSchemaMagicReadsAdapter;
            },
            self::MYSQL => new class ($this->connection()) extends MySQL {
                use SQLSchemaMagicReadsAdapter;
            },
            self::POSTGRES => new class ($this->connection()) extends Postgres {
                use SQLSchemaMagicReadsAdapter;
            },
            default => new class (new PDO('sqlite::memory:')) extends SQLite {
                use SQLSchemaMagicReadsAdapter;
            },
        };
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->define(new Document([
            '$id' => self::BOOKS,
            '$sequence' => '1',
            'attributes' => \json_encode([
                ['$id' => 'title', 'type' => ColumnType::String->value, 'size' => 64, 'array' => false],
                ['$id' => 'pages', 'type' => ColumnType::Integer->value, 'size' => 4, 'array' => false],
            ]),
        ]));
        $adapter->define(new Document([
            '$id' => self::AUTHORS,
            '$sequence' => '2',
            'attributes' => \json_encode([['$id' => 'name', 'type' => ColumnType::String->value, 'size' => 64, 'array' => false]]),
        ]));

        if ($engine === self::SQLITE) {
            $adapter->createCollection(self::BOOKS, [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'pages')]);
            $adapter->createCollection(self::AUTHORS, [Attribute::string(key: 'name', size: 64)]);
        }

        $adapter->countHookReads($recorder);

        return $adapter;
    }

    private function connection(): PDO
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturn($statement);

        return $pdo;
    }
}
