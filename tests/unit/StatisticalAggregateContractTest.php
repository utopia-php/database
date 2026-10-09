<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * stddev() and variance() mean the POPULATION statistic on every adapter.
 *
 * Bare SQL `STDDEV` / `VARIANCE` are population on MySQL and MariaDB and sample
 * on PostgreSQL, so the same query used to answer different numbers per engine.
 * Every adapter now emits the explicit population form.
 */
final class StatisticalAggregateContractTest extends TestCase
{
    /**
     * @return iterable<string, array{class-string<SQL>}>
     */
    public static function adapters(): iterable
    {
        yield 'mysql' => [MySQL::class];
        yield 'mariadb' => [MariaDB::class];
        yield 'postgres' => [Postgres::class];
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    #[DataProvider('adapters')]
    public function testStddevCompilesToThePopulationForm(string $adapter): void
    {
        $sql = $this->captureFindSql($adapter, [Query::stddev('price', 'result')]);

        $this->assertMatchesRegularExpression('/\bSTDDEV_POP\s*\(/i', $sql, $sql);
        $this->assertDoesNotMatchRegularExpression('/\bSTDDEV\s*\(/i', $sql, $sql);
        $this->assertDoesNotMatchRegularExpression('/\bSTDDEV_SAMP\s*\(/i', $sql, $sql);
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    #[DataProvider('adapters')]
    public function testVarianceCompilesToThePopulationForm(string $adapter): void
    {
        $sql = $this->captureFindSql($adapter, [Query::variance('price', 'result')]);

        $this->assertMatchesRegularExpression('/\bVAR_POP\s*\(/i', $sql, $sql);
        $this->assertDoesNotMatchRegularExpression('/\bVARIANCE\s*\(/i', $sql, $sql);
        $this->assertDoesNotMatchRegularExpression('/\bVAR_SAMP\s*\(/i', $sql, $sql);
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheExplicitSampleFormsAreLeftAlone(string $adapter): void
    {
        $this->assertMatchesRegularExpression(
            '/\bSTDDEV_SAMP\s*\(/i',
            $this->captureFindSql($adapter, [Query::stddevSamp('price', 'result')]),
        );
        $this->assertMatchesRegularExpression(
            '/\bVAR_SAMP\s*\(/i',
            $this->captureFindSql($adapter, [Query::varSamp('price', 'result')]),
        );
    }

    /**
     * @param  class-string<SQL>  $adapter
     * @param  array<Query>  $queries
     */
    private function captureFindSql(string $adapter, array $queries): string
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->getMockBuilder(PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturnCallback(function (string $query) use (&$sql, $statement): PDOStatement {
                $sql = $query;

                return $statement;
            });

        $instance = new $adapter($pdo);
        $instance->setDatabase('database');
        $instance->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $instance->setAuthorization($authorization);

        $instance->find(new Document(['$id' => 'collection']), $queries, limit: 25);

        $this->assertNotSame('', $sql);

        return $sql;
    }
}
