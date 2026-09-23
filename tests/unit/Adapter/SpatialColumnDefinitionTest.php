<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Attribute;

final class SpatialColumnDefinitionTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return array<string, array{0: class-string<SQL>, 1: string, 2: list<string>}>
     */
    public static function definitions(): array
    {
        return [
            'MariaDB has no column SRID' => [
                MariaDB::class,
                '`database`.`namespace_places`',
                ['`location` POINT NOT NULL', '`route` LINESTRING NULL', '`area` POLYGON NOT NULL'],
            ],
            'MySQL declares the column SRID' => [
                MySQL::class,
                '`database`.`namespace_places`',
                ['`location` POINT SRID 4326 NOT NULL', '`route` LINESTRING SRID 4326 NULL', '`area` POLYGON SRID 4326 NOT NULL'],
            ],
            'PostgreSQL keeps spatial columns nullable' => [
                Postgres::class,
                '"database"."namespace_places"',
                ['"location" GEOMETRY(POINT, 4326) NULL', '"route" GEOMETRY(LINESTRING, 4326) NULL', '"area" GEOMETRY(POLYGON, 4326) NULL'],
            ],
        ];
    }

    /**
     * @param  class-string<SQL>  $adapterClass
     * @param  list<string>  $columns
     */
    #[DataProvider('definitions')]
    public function testBatchAndSingleCreationEmitTheSameSpatialColumns(string $adapterClass, string $table, array $columns): void
    {
        $attributes = [
            Attribute::point(key: 'location', required: true),
            Attribute::linestring(key: 'route'),
            Attribute::polygon(key: 'area', required: true),
        ];
        $adapter = $this->createAdapter($adapterClass);

        foreach ($attributes as $attribute) {
            $adapter->createAttribute('places', $attribute);
        }
        $adapter->createAttributes('places', $attributes);

        $expected = [];
        foreach ($columns as $column) {
            $expected[] = 'ALTER TABLE '.$table.' ADD COLUMN '.$column;
        }
        $expected[] = 'ALTER TABLE '.$table.' ADD COLUMN '.\implode(', ADD COLUMN ', $columns);

        $this->assertSame($expected, $this->statements);
    }

    /**
     * @param  class-string<SQL>  $adapterClass
     */
    private function createAdapter(string $adapterClass): SQL
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });

        $adapter = new $adapterClass($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
