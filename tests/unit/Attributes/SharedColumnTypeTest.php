<?php

namespace Tests\Unit\Attributes;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Mismatch as MismatchException;
use Utopia\Database\Validator\Authorization;

final class SharedColumnTypeTest extends TestCase
{
    private const string CATALOG = 'SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a';

    private const string MESSAGE = 'Attribute exists in the shared table with another type';

    /**
     * Columns another tenant created, as PostgreSQL's format_type() spells them.
     *
     * @var array<string, string>
     */
    private const array COLUMNS = [
        'age' => 'integer',
        'name' => 'character varying(64)',
        'body' => 'text',
        'price' => 'double precision',
        'at' => 'timestamp(3) without time zone',
        'tags' => 'jsonb',
        'shape' => 'geometry(Point,4326)',
        'embedding' => 'vector(3)',
    ];

    /** @var list<string> */
    private array $statements = [];

    /**
     * @return array<string, array{Attribute}>
     */
    public static function sameTypes(): array
    {
        return [
            'integer' => [Attribute::integer(key: 'age')],
            'varchar' => [Attribute::string(key: 'name', size: 64)],
            'text' => [Attribute::text(key: 'body')],
            'sizeless string' => [Attribute::string(key: 'body', size: 0)],
            'double' => [Attribute::float(key: 'price')],
            'datetime' => [Attribute::datetime(key: 'at')],
            'array' => [Attribute::string(key: 'tags', size: 32, array: true)],
            'point' => [Attribute::point(key: 'shape', required: true)],
            'vector' => [Attribute::vector(key: 'embedding', size: 3)],
        ];
    }

    /**
     * @return array<string, array{Attribute}>
     */
    public static function otherTypes(): array
    {
        return [
            'string over integer' => [Attribute::string(key: 'age', size: 64)],
            'bigint over integer' => [Attribute::integer(key: 'age', size: 8)],
            'longer varchar' => [Attribute::string(key: 'name', size: 128)],
            'integer over text' => [Attribute::integer(key: 'body')],
            'linestring over point' => [Attribute::linestring(key: 'shape')],
            'wider vector' => [Attribute::vector(key: 'embedding', size: 4)],
        ];
    }

    #[DataProvider('otherTypes')]
    public function testPostgresRefusesAnotherTenantsColumnOfAnotherType(Attribute $attribute): void
    {
        $adapter = $this->createPostgres(sharedTables: true);

        try {
            $adapter->createAttribute('items', $attribute);
            $this->fail('A column another tenant created with another type must be refused');
        } catch (DuplicateException $e) {
            $this->assertInstanceOf(MismatchException::class, $e);
            $this->assertSame(self::MESSAGE, $e->getMessage());
        }

        $this->assertCount(1, $this->statements);
        $this->assertStringStartsWith(self::CATALOG, $this->statements[0]);
    }

    #[DataProvider('sameTypes')]
    public function testPostgresLeavesAnotherTenantsColumnOfTheSameTypeToTheEngine(Attribute $attribute): void
    {
        $adapter = $this->createPostgres(sharedTables: true);

        $this->assertTrue($adapter->createAttribute('items', $attribute));

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith(self::CATALOG, $this->statements[0]);
        $this->assertStringStartsWith('ALTER TABLE "database"."namespace_items" ADD COLUMN "'.$attribute->key.'"', $this->statements[1]);
    }

    public function testPostgresRefusesABatchWithAnotherTenantsColumnOfAnotherType(): void
    {
        $adapter = $this->createPostgres(sharedTables: true);

        try {
            $adapter->createAttributes('items', [Attribute::string(key: 'label', size: 16), Attribute::string(key: 'age', size: 64)]);
            $this->fail('A batch holding a column another tenant created with another type must be refused');
        } catch (MismatchException $e) {
            $this->assertSame(self::MESSAGE, $e->getMessage());
        }

        $this->assertCount(1, $this->statements);
        $this->assertStringStartsWith(self::CATALOG, $this->statements[0]);
    }

    public function testPostgresReadsNoCatalogOutsideSharedTables(): void
    {
        $adapter = $this->createPostgres(sharedTables: false);

        $this->assertTrue($adapter->createAttribute('items', Attribute::string(key: 'age', size: 64)));
        $this->assertTrue($adapter->createAttributes('items', [Attribute::string(key: 'label', size: 16)]));

        $this->assertSame([
            'ALTER TABLE "database"."namespace_items" ADD COLUMN "age" VARCHAR(64) NULL',
            'ALTER TABLE "database"."namespace_items" ADD COLUMN "label" VARCHAR(16) NULL',
        ], $this->statements);
    }

    public function testDatabaseSurfacesTheRefusalOfASingleAttribute(): void
    {
        $database = $this->createRefusingDatabase();

        try {
            $database->createAttribute('items', Attribute::string(key: 'age', size: 64));
            $this->fail('The adapter refusal must reach the caller');
        } catch (MismatchException $e) {
            $this->assertSame(self::MESSAGE, $e->getMessage());
        }

        $this->assertSame([], $database->getCollection('items')->getAttribute('attributes', []));
    }

    public function testDatabaseSurfacesTheRefusalOfABatch(): void
    {
        $database = $this->createRefusingDatabase();

        try {
            $database->createAttributes('items', [Attribute::string(key: 'label', size: 16), Attribute::string(key: 'age', size: 64)]);
            $this->fail('The adapter refusal must reach the caller');
        } catch (MismatchException $e) {
            $this->assertSame(self::MESSAGE, $e->getMessage());
        }

        $this->assertSame([], $database->getCollection('items')->getAttribute('attributes', []));
    }

    private function createPostgres(bool $sharedTables): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn(\str_starts_with($query, self::CATALOG) ? self::COLUMNS : []);

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant($sharedTables ? 2 : null);

        return $adapter;
    }

    private function createRefusingDatabase(): Database
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public function createAttribute(string $collection, Attribute $attribute): bool
            {
                throw new MismatchException('Attribute exists in the shared table with another type');
            }

            /**
             * @param  array<Attribute>  $attributes
             */
            public function createAttributes(string $collection, array $attributes): bool
            {
                throw new MismatchException('Attribute exists in the shared table with another type');
            }
        };

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('shared_column_type')
            ->setNamespace('shared_column_type');
        $database->create();
        $database->createCollection(new Collection(id: 'items'));

        return $database;
    }
}
