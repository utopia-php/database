<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Exception as DatabaseException;

final class PostgresCollectionSizeTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function sizes(): iterable
    {
        yield 'on disk' => ['getSizeOfCollectionOnDisk', 'pg_total_relation_size'];
        yield 'raw data' => ['getSizeOfCollection', 'pg_relation_size'];
    }

    #[DataProvider('sizes')]
    public function testTheSizeAddsTheCollectionAndItsPermissionsTable(string $method, string $function): void
    {
        $adapter = $this->adapter(['8192', '4096']);

        $this->assertSame(12288, $this->readSize($adapter, $method));
        $this->assertCount(2, $this->statements);
        foreach ($this->statements as $statement) {
            $this->assertStringContainsString($function . '(', $statement);
        }
    }

    #[DataProvider('sizes')]
    public function testAFailedSizeReadIsADatabaseError(string $method, string $function): void
    {
        $adapter = $this->adapter(['8192', '4096'], new PDOException('SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "database.namespace_books" does not exist'));

        try {
            $this->readSize($adapter, $method);
            $this->fail('A size read that fails must reach the caller');
        } catch (DatabaseException $error) {
            $this->assertSame(
                'Failed to get collection size: SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "database.namespace_books" does not exist',
                $error->getMessage(),
            );
        }
    }

    private function readSize(Postgres $adapter, string $method): int
    {
        return $method === 'getSizeOfCollectionOnDisk'
            ? $adapter->getSizeOfCollectionOnDisk('books')
            : $adapter->getSizeOfCollection('books');
    }

    /**
     * @param list<string> $sizes
     */
    private function adapter(array $sizes, ?PDOException $failure = null): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sizes, $failure): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('fetchColumn')->willReturn(\array_shift($sizes));
            if ($failure === null) {
                $statement->method('execute')->willReturn(true);
            } else {
                $statement->method('execute')->willThrowException($failure);
            }

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
