<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;

final class PostgresRenameIndexTest extends TestCase
{
    private const string RENAME = 'ALTER INDEX IF EXISTS "database"."namespace_2_users_byAge" RENAME TO "namespace_2_users_byYears"';

    /** @var list<string> */
    private array $indexes = [];

    /** @var list<string> */
    private array $tenants = [];

    /** @var list<string> */
    private array $statements = [];

    public function testAnIndexTheSchemaHasIsRenamed(): void
    {
        $this->indexes = ['namespace__users_byAge'];

        $this->assertTrue($this->adapter(shared: false)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame(['namespace__users_byYears'], $this->indexes);
    }

    public function testAnIndexTheSchemaDoesNotHaveIsNotRenamed(): void
    {
        $this->assertFalse($this->adapter(shared: false)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame([], $this->indexes);
    }

    public function testAnIndexTheSchemaAlreadyRenamedIsReportedRenamed(): void
    {
        $this->indexes = ['namespace__users_byYears'];

        $this->assertTrue($this->adapter(shared: false)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame(['namespace__users_byYears'], $this->indexes);
    }

    public function testSharedTablesRenameTheTenantsOwnIndex(): void
    {
        $this->indexes = ['namespace_1_users_byAge', 'namespace_2_users_byAge'];
        $this->tenants = ['1', '2'];

        $this->assertTrue($this->adapter(shared: true)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame(['namespace_1_users_byAge', 'namespace_2_users_byYears'], $this->indexes);
        $this->assertSame(self::RENAME, $this->statements[0]);
    }

    public function testSharedTablesLeaveAnIndexAnotherTenantCreatedToItsOwner(): void
    {
        $this->indexes = ['namespace_1_users_byAge'];
        $this->tenants = ['1', '2'];

        $this->assertTrue($this->adapter(shared: true)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame(['namespace_1_users_byAge'], $this->indexes);
        $this->assertSame(self::RENAME, $this->statements[0]);
    }

    public function testSharedTablesCompleteARenameAnotherTenantAlreadyMade(): void
    {
        $this->indexes = ['namespace_1_users_byYears'];
        $this->tenants = ['1', '2'];

        $this->assertTrue($this->adapter(shared: true)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame(['namespace_1_users_byYears'], $this->indexes);
    }

    public function testSharedTablesReportARenameNoTenantsIndexBacks(): void
    {
        $this->indexes = ['namespace_1_users_byName'];
        $this->tenants = ['1', '2'];

        $this->assertFalse($this->adapter(shared: true)->renameIndex('users', 'byAge', 'byYears'));
        $this->assertSame(['namespace_1_users_byName'], $this->indexes);
    }

    private function adapter(bool $shared): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $bound = [];

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value) use (&$bound): bool {
                $bound[] = $value;

                return true;
            });
            $statement->method('execute')->willReturnCallback(function () use ($query): bool {
                if (\preg_match('/^ALTER INDEX IF EXISTS "database"\."([^"]+)" RENAME TO "([^"]+)"$/', $query, $names) === 1) {
                    $this->indexes = \array_values(\array_map(static fn (string $index): string => $index === $names[1] ? $names[2] : $index, $this->indexes));
                }

                return true;
            });
            $statement->method('fetchAll')->willReturnCallback(function () use ($query, &$bound): array {
                if (\str_contains($query, 'pg_class')) {
                    $schema = \array_shift($bound);

                    return $schema === 'database' ? \array_values(\array_intersect($bound, $this->indexes)) : [];
                }

                return \str_contains($query, '_metadata') ? $this->tenants : [];
            });

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        if ($shared) {
            $adapter->setSharedTables(true);
            $adapter->setTenant(2);
        }

        return $adapter;
    }
}
