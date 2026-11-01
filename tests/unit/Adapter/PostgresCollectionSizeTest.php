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

    public function testTheEstimateReadsTableStatistics(): void
    {
        $adapter = $this->adapter(['3000000']);

        $this->assertSame(3000000, $adapter->estimateCount('books'));
        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('n_live_tup', $this->statements[0]);
        $this->assertStringContainsString('reltuples', $this->statements[0]);
    }

    /**
     * @return iterable<string, array{string|false}>
     */
    public static function missingEstimates(): iterable
    {
        yield 'never analyzed' => ['-1'];
        yield 'missing table' => [false];
    }

    #[DataProvider('missingEstimates')]
    public function testNoStatisticsIsNoEstimate(string|false $estimate): void
    {
        $adapter = $this->adapter([$estimate]);

        $this->assertNull($adapter->estimateCount('books'));
    }

    public function testSharedTablesHaveNoEstimate(): void
    {
        $adapter = $this->adapter(['3000000']);
        $adapter->setSharedTables(true);

        $this->assertNull($adapter->estimateCount('books'));
        $this->assertSame([], $this->statements);
    }

    public function testAFailedEstimateIsADatabaseError(): void
    {
        $adapter = $this->adapter(['3000000'], new PDOException('SQLSTATE[08006]: connection failure'));

        try {
            $adapter->estimateCount('books');
            $this->fail('An estimate that fails must reach the caller');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to estimate collection count: SQLSTATE[08006]: connection failure', $error->getMessage());
            $this->assertInstanceOf(PDOException::class, $error->getPrevious());
        }
    }

    private function readSize(Postgres $adapter, string $method): int
    {
        return $method === 'getSizeOfCollectionOnDisk'
            ? $adapter->getSizeOfCollectionOnDisk('books')
            : $adapter->getSizeOfCollection('books');
    }

    /**
     * @param list<string|false> $sizes
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
