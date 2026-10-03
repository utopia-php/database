<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Collection;

final class LockedDocumentReadTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /** @var list<array<int|string, mixed>> */
    private array $bindings = [];

    /**
     * @return iterable<string, array{Closure(PDO): SQL, string}>
     */
    public static function adapters(): iterable
    {
        yield 'mariadb' => [static fn (PDO $pdo): SQL => new MariaDB($pdo), 'SELECT * FROM `database`.`namespace_books` AS `table_main` WHERE `_uid` = :_uid'];
        yield 'postgres' => [static fn (PDO $pdo): SQL => new Postgres($pdo), 'SELECT * FROM "database"."namespace_books" AS "table_main" WHERE "_uid" = :_uid'];
    }

    /**
     * @param  Closure(PDO): SQL  $make
     */
    #[DataProvider('adapters')]
    public function testALockedReadSelectsTheDocumentForUpdate(Closure $make, string $select): void
    {
        $document = $this->adapter($make)->getDocument(new Collection(id: 'books'), 'dune', forUpdate: true);

        $this->assertSame([$select.' FOR UPDATE'], $this->statements);
        $this->assertSame([[':_uid', 'dune']], $this->bindings);
        $this->assertSame('dune', $document->getId());
        $this->assertSame('7', $document->getSequence());
        $this->assertSame('Dune', $document->getAttribute('title'));
    }

    /**
     * @param  Closure(PDO): SQL  $make
     */
    #[DataProvider('adapters')]
    public function testAnUnlockedReadSelectsTheDocumentWithoutALock(Closure $make, string $select): void
    {
        $document = $this->adapter($make)->getDocument(new Collection(id: 'books'), 'dune');

        $this->assertSame([$select], $this->statements);
        $this->assertSame('Dune', $document->getAttribute('title'));
    }

    /**
     * @return iterable<string, array{Closure(PDO): SQL, string, int|null, string}>
     */
    public static function sharedReads(): iterable
    {
        $mariadb = static fn (PDO $pdo): SQL => new MariaDB($pdo);
        $postgres = static fn (PDO $pdo): SQL => new Postgres($pdo);

        yield 'mariadb' => [$mariadb, 'books', 3, 'SELECT * FROM `database`.`namespace_books` AS `table_main` WHERE `_uid` = :_uid AND `table_main`._tenant IN (:_tenant)'];
        yield 'mariadb metadata' => [$mariadb, '_metadata', 3, 'SELECT * FROM `database`.`namespace__metadata` AS `table_main` WHERE `_uid` = :_uid AND (`table_main`._tenant IN (:_tenant) OR `table_main`._tenant IS NULL)'];
        yield 'mariadb without a tenant' => [$mariadb, 'books', null, 'SELECT * FROM `database`.`namespace_books` AS `table_main` WHERE `_uid` = :_uid AND `table_main`._tenant IN (:_tenant)'];
        yield 'postgres' => [$postgres, 'books', 3, 'SELECT * FROM "database"."namespace_books" AS "table_main" WHERE "_uid" = :_uid AND "table_main"._tenant IN (:_tenant)'];
        yield 'postgres metadata' => [$postgres, '_metadata', 3, 'SELECT * FROM "database"."namespace__metadata" AS "table_main" WHERE "_uid" = :_uid AND ("table_main"._tenant IN (:_tenant) OR "table_main"._tenant IS NULL)'];
    }

    /**
     * @param  Closure(PDO): SQL  $make
     */
    #[DataProvider('sharedReads')]
    public function testASharedTableReadKeepsTheTenantFilter(Closure $make, string $collection, ?int $tenant, string $select): void
    {
        $adapter = $this->adapter($make);
        $adapter->setSharedTables(true);
        $adapter->setTenant($tenant);

        $adapter->getDocument(new Collection(id: $collection), 'dune', forUpdate: true);
        $adapter->getDocument(new Collection(id: $collection), 'dune');

        $this->assertSame([$select.' FOR UPDATE', $select], $this->statements);
        $this->assertSame([[':_uid', 'dune'], [':_tenant', $tenant], [':_uid', 'dune'], [':_tenant', $tenant]], $this->bindings);
    }

    /**
     * @param  Closure(PDO): SQL  $make
     */
    private function adapter(Closure $make): SQL
    {
        $row = ['_id' => '7', '_uid' => 'dune', '_createdAt' => null, '_updatedAt' => null, '_permissions' => '[]', 'title' => 'Dune'];

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($row): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bindings[] = [$parameter, $value];

                return true;
            });
            $statement->method('fetch')->willReturn($row);
            $statement->method('fetchAll')->willReturn([$row]);

            return $statement;
        });

        $adapter = $make($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
