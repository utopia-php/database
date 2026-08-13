<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Operator;
use Utopia\Database\PDO;

final class SQLOperatorBindingTest extends TestCase
{
    /**
     * @return array<string, array{class-string<SQL>}>
     */
    public static function adapterClasses(): array
    {
        return [
            'mysql' => [MySQL::class],
            'postgres' => [Postgres::class],
            'sqlite' => [SQLite::class],
        ];
    }

    /**
     * @param class-string<SQL> $adapterClass
     */
    #[DataProvider('adapterClasses')]
    public function testBindsOperatorsToDatabaseStatement(string $adapterClass): void
    {
        $connection = new PDO('sqlite::memory:', null, null);
        $statement = $connection->prepare('SELECT :op_0');
        $adapter = new $adapterClass($connection);
        $bindIndex = 0;

        $method = new ReflectionMethod($adapter, 'bindOperatorParams');
        $method->invokeArgs($adapter, [$statement, Operator::increment(2), &$bindIndex]);

        $this->assertSame(1, $bindIndex);
        $this->assertTrue($statement->execute());
        $this->assertSame(2, $statement->fetchColumn());
    }
}
