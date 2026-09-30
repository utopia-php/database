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
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Mismatch as MismatchException;
use Utopia\Database\Index;
use Utopia\Database\Storage;

/**
 * A later tenant's createCollection() under shared tables finds the table another tenant
 * created: it adds only the columns and indexes its own definition declares that the table
 * lacks, and reports the table as existing.
 */
final class SharedTableCompletionTest extends TestCase
{
    private const string POSTGRES_CATALOG = 'FROM pg_attribute a WHERE a.attrelid = to_regclass(?)';

    private const string MARIADB_CATALOG = 'FROM INFORMATION_SCHEMA.COLUMNS';

    /** @var list<string> */
    private array $statements = [];

    /**
     * @return array<string, array{class-string<MariaDB>}>
     */
    public static function mariaDBAdapters(): array
    {
        return [
            'MariaDB' => [MariaDB::class],
            'MySQL' => [MySQL::class],
        ];
    }

    public function testPostgresAddsTheMissingColumnAndIndex(): void
    {
        $adapter = $this->createPostgres(['_id' => 'bigint', '_uid' => 'character varying(255)', '_tenant' => 'integer', 'age' => 'integer']);

        $this->assertExistingTableReported($adapter);

        $this->assertSame([], $this->statementsLike('CREATE TABLE'));
        $this->assertSame(['ALTER TABLE "database"."namespace_users" ADD COLUMN "title" VARCHAR(64) NULL'], $this->statementsLike('ALTER TABLE'));
        $this->assertCount(1, $this->statementsLike('CREATE UNIQUE INDEX'));
        $this->assertStringContainsString('("_tenant", "title")', $this->statementsLike('CREATE UNIQUE INDEX')[0]);
    }

    public function testPostgresRefusesAColumnOfAnotherTypeBeforeAnyDDL(): void
    {
        $adapter = $this->createPostgres(['_id' => 'bigint', '_tenant' => 'integer', 'age' => 'character varying(32)']);

        try {
            $adapter->createCollection('users', $this->attributes(), $this->indexes());
            $this->fail('A column another tenant stores with another type must be refused');
        } catch (MismatchException $error) {
            $this->assertSame('Attribute exists in the shared table with another type', $error->getMessage());
        }

        $this->assertSame([], \array_values(\array_filter($this->statements, fn (string $statement): bool => ! \str_contains($statement, self::POSTGRES_CATALOG))));
    }

    public function testPostgresCreatesATableNoTenantHasYet(): void
    {
        $adapter = $this->createPostgres([]);

        $this->assertTrue($adapter->createCollection('users', $this->attributes(), $this->indexes()));

        $this->assertStringContainsString(self::POSTGRES_CATALOG, $this->statements[0]);
        $this->assertCount(1, $this->statementsLike('CREATE TABLE "database"."namespace_users"'));
    }

    public function testPostgresReadsNoCatalogOutsideSharedTables(): void
    {
        $adapter = $this->createPostgres([]);
        $adapter->setSharedTables(false);

        $adapter->createCollection('users', $this->attributes(), $this->indexes());

        $this->assertSame([], \array_values(\array_filter($this->statements, fn (string $statement): bool => \str_contains($statement, self::POSTGRES_CATALOG))));
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBAddsTheMissingColumnAndIndexWithoutMetadata(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id' => 'bigint(20) unsigned', '_tenant' => 'int(11) unsigned', 'age' => 'int(11)']);

        $this->assertExistingTableReported($adapter);

        $this->assertSame([], $this->statementsLike('CREATE TABLE'));
        $this->assertSame([], \array_values(\array_filter($this->statements, fn (string $statement): bool => \str_contains($statement, '_metadata'))), 'The collection\'s metadata does not exist yet');
        $this->assertCount(1, $this->statementsLike('ALTER TABLE'));
        $this->assertStringContainsString('ADD COLUMN `title` VARCHAR(64)', $this->statementsLike('ALTER TABLE')[0]);
        $this->assertCount(1, $this->statementsLike('CREATE UNIQUE INDEX `byTitle`'));
        $this->assertStringContainsString('(`_tenant`, `title`)', $this->statementsLike('CREATE UNIQUE INDEX')[0]);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBRefusesAColumnOfAnotherTypeBeforeAnyDDL(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id' => 'bigint(20) unsigned', 'age' => 'varchar(32)']);

        try {
            $adapter->createCollection('users', $this->attributes(), $this->indexes());
            $this->fail('A column another tenant stores with another type must be refused');
        } catch (MismatchException) {
            $this->addToAssertionCount(1);
        }

        $this->assertSame([], \array_values(\array_filter($this->statements, fn (string $statement): bool => ! \str_contains($statement, self::MARIADB_CATALOG))));
    }

    private function assertExistingTableReported(SQL $adapter): void
    {
        try {
            $adapter->createCollection('users', $this->attributes(), $this->indexes());
            $this->fail('A table another tenant created must be reported as existing');
        } catch (MismatchException $error) {
            $this->fail('A column of the same type must be reused: '.$error->getMessage());
        } catch (DuplicateException $error) {
            $this->assertSame('Collection already exists', $error->getMessage());
        }
    }

    /**
     * @return list<Attribute>
     */
    private function attributes(): array
    {
        return [Attribute::integer(key: 'age'), Attribute::string(key: 'title', size: 64)];
    }

    /**
     * @return list<Index>
     */
    private function indexes(): array
    {
        return [Index::unique(key: 'byTitle', attributes: ['title'])];
    }

    /**
     * @return list<string>
     */
    private function statementsLike(string $prefix): array
    {
        return \array_values(\array_filter($this->statements, static fn (string $statement): bool => \str_starts_with($statement, $prefix)));
    }

    /**
     * @param  array<string, string>  $columns  Column name to catalog type
     */
    private function createPostgres(array $columns): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($columns): PDOStatement {
            $this->statements[] = \trim($query);
            $catalog = \str_contains($query, self::POSTGRES_CATALOG);

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturnCallback(
                static fn (int $mode = PDO::FETCH_DEFAULT): array => ! $catalog ? [] : ($mode === PDO::FETCH_KEY_PAIR ? $columns : \array_keys($columns)),
            );

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        return $adapter;
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     * @param  array<string, string>  $columns  Column name to COLUMN_TYPE
     */
    private function createMariaDB(string $adapterClass, array $columns): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($columns): PDOStatement {
            $this->statements[] = \trim($query);

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn(\str_contains($query, self::MARIADB_CATALOG)
                ? \array_map(
                    static fn (string $column, string $type): array => [Storage::SEQUENCE => $column, 'columnType' => $type],
                    \array_keys($columns),
                    $columns,
                )
                : []);

            return $statement;
        });

        $adapter = new $adapterClass($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        return $adapter;
    }
}
