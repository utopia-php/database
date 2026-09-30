<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Attribute;
use Utopia\Database\Exception as DatabaseException;

final class AddTableColumnTest extends TestCase
{
    /**
     * @var list<string>
     */
    private array $statements = [];

    public function testVectorOnMySQLTableThrowsDatabaseException(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector columns are only supported on PostgreSQL');

        $this->adapter(MariaDB::class)->createAttribute('movies', Attribute::vector(key: 'embedding', size: 3));
    }

    public function testVectorOnPostgreSQLTableSetsTypeAndDimensions(): void
    {
        $this->assertTrue($this->adapter(Postgres::class)->createAttribute('movies', Attribute::vector(key: 'embedding', size: 4)));

        $this->assertSame(['ALTER TABLE "database"."namespace_movies" ADD COLUMN "embedding" VECTOR(4) NULL'], $this->statements);
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    private function adapter(string $adapter): SQL
    {
        $statement = self::createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $pdo = self::createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });

        $sql = new $adapter($pdo);
        $sql->setDatabase('database');
        $sql->setNamespace('namespace');

        return $sql;
    }
}
