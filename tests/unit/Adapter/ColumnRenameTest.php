<?php

namespace Tests\Unit\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Storage;

final class ColumnRenameTest extends TestCase
{
    private const string POSTGRES_CATALOG = 'SELECT a.attname FROM pg_attribute a WHERE a.attrelid = to_regclass(?)';

    private const string MARIADB_CATALOG = 'FROM INFORMATION_SCHEMA.COLUMNS';

    private const string POSTGRES_RENAME = 'ALTER TABLE "database"."namespace_users" RENAME COLUMN "age" TO "years"';

    private const string POSTGRES_RETYPE = 'ALTER TABLE "database"."namespace_users" ALTER COLUMN "years" TYPE INTEGER';

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

    public function testPostgresRenameAdoptsAColumnAnotherTenantAlreadyRenamed(): void
    {
        $adapter = $this->createPostgres(['_id', 'years']);

        $this->assertTrue($adapter->renameAttribute('users', 'age', 'years'));

        $this->assertCount(1, $this->statements);
        $this->assertStringStartsWith(self::POSTGRES_CATALOG, $this->statements[0]);
    }

    public function testPostgresRenameRunsWhileTheOldColumnExists(): void
    {
        $adapter = $this->createPostgres(['_id', 'age']);

        $this->assertTrue($adapter->renameAttribute('users', 'age', 'years'));

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith(self::POSTGRES_CATALOG, $this->statements[0]);
        $this->assertSame(self::POSTGRES_RENAME, $this->statements[1]);
    }

    public function testPostgresRenameRefusesATargetThatExistsBesideTheOldColumn(): void
    {
        $adapter = $this->createPostgres(['_id', 'age', 'years'], $this->postgresError('42701', 'column "years" of relation "namespace_users" already exists'));

        try {
            $adapter->renameAttribute('users', 'age', 'years');
            $this->fail('A rename onto a column that exists beside the old one must be refused');
        } catch (DuplicateException $e) {
            $this->assertSame('Attribute already exists', $e->getMessage());
        }

        $this->assertSame(self::POSTGRES_RENAME, $this->statements[1]);
    }

    public function testPostgresRenameOfAMissingColumnIsNotFound(): void
    {
        $adapter = $this->createPostgres(['_id'], $this->postgresError('42703', 'column "age" does not exist'));

        try {
            $adapter->renameAttribute('users', 'age', 'years');
            $this->fail('A rename of a column that is gone without a renamed one must be reported');
        } catch (NotFoundException $e) {
            $this->assertSame('Attribute not found', $e->getMessage());
        }

        $this->assertSame(self::POSTGRES_RENAME, $this->statements[1]);
    }

    public function testPostgresUpdateAttributeAdoptsAColumnAnotherTenantAlreadyRenamed(): void
    {
        $adapter = $this->createPostgres(['_id', 'years']);

        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true), 'years'));

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith(self::POSTGRES_CATALOG, $this->statements[0]);
        $this->assertStringStartsWith(self::POSTGRES_RETYPE, $this->statements[1]);
    }

    public function testPostgresUpdateAttributeRenamesWhileTheOldColumnExists(): void
    {
        $adapter = $this->createPostgres(['_id', 'age']);

        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true), 'years'));

        $this->assertCount(3, $this->statements);
        $this->assertStringStartsWith(self::POSTGRES_CATALOG, $this->statements[0]);
        $this->assertStringStartsWith('ALTER TABLE "database"."namespace_users" ALTER COLUMN "age" TYPE INTEGER', $this->statements[1], 'The type changes before the rename, so a refused change leaves the column under its old key');
        $this->assertSame(self::POSTGRES_RENAME, $this->statements[2]);
    }

    public function testPostgresUpdateAttributeRefusesATargetThatExistsBesideTheOldColumn(): void
    {
        $adapter = $this->createPostgres(['_id', 'age', 'years'], $this->postgresError('42701', 'column "years" of relation "namespace_users" already exists'));

        try {
            $adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true), 'years');
            $this->fail('A target beside the old column must be refused');
        } catch (DuplicateException $error) {
            $this->assertSame('Attribute already exists', $error->getMessage());
        }

        $this->assertCount(1, $this->statements, 'The refusal must come before any DDL');
    }

    public function testPostgresReadsNoCatalogWithoutARename(): void
    {
        $adapter = $this->createPostgres(['_id', 'age']);

        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true)));
        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true), 'age'));

        $this->assertSame([], \array_filter($this->statements, fn (string $statement): bool => \str_starts_with($statement, self::POSTGRES_CATALOG)));
    }

    public function testPostgresRenameIndexRenamesTheSharedIndexAndTheTenantsOwnCopy(): void
    {
        $adapter = $this->createPostgres([]);

        $this->assertTrue($adapter->renameIndex('users', 'byAge', 'byYears'));

        $this->assertSame([
            'ALTER INDEX IF EXISTS "database"."namespace__users_byAge" RENAME TO "namespace__users_byYears"; '
            .'ALTER INDEX IF EXISTS "database"."namespace_2_users_byAge" RENAME TO "namespace_2_users_byYears"',
        ], $this->statements);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBRenameAdoptsAColumnAnotherTenantAlreadyRenamed(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id', 'years']);

        $this->assertTrue($adapter->renameAttribute('users', 'age', 'years'));

        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString(self::MARIADB_CATALOG, $this->statements[0]);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBRenameRunsWhileTheOldColumnExists(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id', 'age']);

        $this->assertTrue($adapter->renameAttribute('users', 'age', 'years'));

        $this->assertCount(2, $this->statements);
        $this->assertSame('ALTER TABLE `database`.`namespace_users` RENAME COLUMN `age` TO `years`', $this->statements[1]);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBUpdateAttributeAdoptsAColumnAnotherTenantAlreadyRenamed(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id', 'years']);

        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true), 'years'));

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith('ALTER TABLE `database`.`namespace_users` MODIFY `years` INT', $this->statements[1]);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBUpdateAttributeChangesTheColumnWhileTheOldOneExists(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id', 'age']);

        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true), 'years'));

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith('ALTER TABLE `database`.`namespace_users` CHANGE COLUMN `age` `years` INT', $this->statements[1]);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('mariaDBAdapters')]
    public function testMariaDBReadsNoCatalogWithoutARename(string $adapterClass): void
    {
        $adapter = $this->createMariaDB($adapterClass, ['_id', 'age']);

        $this->assertTrue($adapter->updateAttribute('users', Attribute::integer(key: 'age', required: true)));

        $this->assertCount(1, $this->statements);
        $this->assertStringStartsWith('ALTER TABLE `database`.`namespace_users` MODIFY `age` INT', $this->statements[0]);
    }

    /**
     * @param  list<string>  $columns
     */
    private function createPostgres(array $columns, ?PDOException $ddlError = null): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($columns, $ddlError): PDOStatement {
            $this->statements[] = $query;
            $catalog = \str_starts_with($query, self::POSTGRES_CATALOG);

            $statement = $this->createStub(PDOStatement::class);
            if ($ddlError !== null && ! $catalog) {
                $statement->method('execute')->willThrowException($ddlError);
            } else {
                $statement->method('execute')->willReturn(true);
            }
            $statement->method('fetchAll')->willReturn($catalog ? $columns : []);

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
     * @param  list<string>  $columns
     */
    private function createMariaDB(string $adapterClass, array $columns): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($columns): PDOStatement {
            $this->statements[] = \trim($query);

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn(\str_contains($query, self::MARIADB_CATALOG)
                ? \array_map(static fn (string $column): array => [Storage::SEQUENCE => $column], $columns)
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

    private function postgresError(string $state, string $message): PDOException
    {
        $error = new PDOException("SQLSTATE[{$state}]: 7 ERROR:  {$message}");
        (new ReflectionProperty(Exception::class, 'code'))->setValue($error, $state);
        $error->errorInfo = [$state, 7, $message];

        return $error;
    }
}
