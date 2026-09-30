<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class PostgresSpatialCacheTest extends TestCase
{
    /**
     * @var list<string>
     */
    private array $statements = [];

    /**
     * @var list<mixed>
     */
    private array $bindings = [];

    public function testSpatialCacheRescansWhenAttributeSetChanges(): void
    {
        $adapter = $this->adapter();

        $before = new Document([
            '$id' => 'places',
            'attributes' => [new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value])],
        ]);
        $this->assertStringNotContainsString('ST_GeomFromText', $this->insert($adapter, $before, ['name' => 'x', 'loc' => [0.0, 0.0]]));

        $after = new Document([
            '$id' => 'places',
            'attributes' => [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value]),
                new Document(['$id' => 'loc', 'key' => 'loc', 'type' => ColumnType::Point->value]),
            ],
        ]);
        $this->assertStringContainsString('VALUES (?, ?, ST_GeomFromText(?, 4326), ?', $this->insert($adapter, $after, ['name' => 'x', 'loc' => [0.0, 0.0]]));
        $this->assertSame(['x', 'POINT(0 0)'], \array_slice($this->bindings, 1, 2));
    }

    public function testSpatialAttributesFromTypedObjectsAndEnums(): void
    {
        $collection = new Document([
            '$id' => 'mixed',
            'attributes' => [
                Attribute::point(key: 'loc'),
                new Document(['$id' => 'route', 'key' => 'route', 'type' => ColumnType::Linestring]),
                ['$id' => 'area', 'key' => 'area', 'type' => ColumnType::Polygon->value],
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value]),
            ],
        ]);

        $statement = $this->insert($this->adapter(), $collection, [
            'loc' => [0.0, 0.0],
            'route' => [[0.0, 0.0], [1.0, 1.0]],
            'area' => 'POLYGON((0 0, 1 0, 1 1, 0 0))',
            'name' => 'x',
        ]);

        $this->assertStringContainsString('VALUES (?, ST_GeomFromText(?, 4326), ST_GeomFromText(?, 4326), ST_GeomFromText(?, 4326), ?,', $statement);
    }

    public function testSpatialWriteValueEncoding(): void
    {
        $collection = new Document([
            '$id' => 'shapes',
            'attributes' => [
                Attribute::point(key: 'origin'),
                Attribute::linestring(key: 'path'),
                Attribute::point(key: 'wellKnown'),
            ],
        ]);

        $this->insert($this->adapter(), $collection, [
            'origin' => [0.0, 0.0],
            'path' => [[0.0, 0.0], [1.0, 1.0]],
            'wellKnown' => 'POINT(0 0)',
        ]);

        $this->assertSame(['POINT(0 0)', 'LINESTRING(0 0, 1 1)', 'POINT(0 0)'], \array_slice($this->bindings, 1, 3));
    }

    public function testAttributeWidthAcceptsDocumentAndTypedAttributes(): void
    {
        $adapter = new Postgres($this->createStub(\PDO::class));
        $collection = new Document([
            '$id' => 'export',
            'attributes' => [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'size' => 255, 'array' => false]),
                new Document(['$id' => 'body', 'key' => 'body', 'type' => ColumnType::MediumText->value, 'size' => 0, 'array' => false]),
                new Document(['$id' => 'notes', 'key' => 'notes', 'type' => ColumnType::LongText->value, 'size' => 0, 'array' => false]),
                new Document(['$id' => 'count', 'key' => 'count', 'type' => ColumnType::BigInteger, 'size' => 0, 'array' => false]),
                Attribute::point(key: 'loc'),
            ],
        ]);

        $this->assertGreaterThan(0, $adapter->getAttributeWidth($collection));
    }

    private function adapter(): Postgres
    {
        $statement = self::createStub(\PDOStatement::class);
        $statement->method('bindValue')->willReturnCallback(function (int|string $position, mixed $value): bool {
            $this->bindings[] = $value;

            return true;
        });
        $statement->method('execute')->willReturn(true);
        $pdo = self::createStub(\PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): \PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });
        $pdo->method('lastInsertId')->willReturn('1');

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function insert(Postgres $adapter, Document $collection, array $attributes): string
    {
        $this->statements = [];
        $this->bindings = [];

        $adapter->createDocument($collection, new Document(['$id' => 'document', '$permissions' => [], ...$attributes]));

        $statement = $this->statements[0] ?? null;
        $this->assertIsString($statement);

        return $statement;
    }
}
