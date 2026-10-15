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
use Utopia\Database\Exception as DatabaseException;

final class SQLVarcharAttributeTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<SQL>, bool, int, string}>
     */
    public static function invalidSizes(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class, 'Postgres' => Postgres::class] as $engine => $class) {
            foreach (['one attribute' => false, 'several attributes' => true] as $shape => $several) {
                yield $engine . ' ' . $shape . ' zero' => [$class, $several, 0, 'VARCHAR size 0 is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
                yield $engine . ' ' . $shape . ' negative' => [$class, $several, -1, 'VARCHAR size -1 is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
                yield $engine . ' ' . $shape . ' above the maximum' => [$class, $several, 16382, 'VARCHAR size 16382 exceeds maximum varchar length 16381. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
            }
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('invalidSizes')]
    public function testAVarcharAttributeOutsideItsSizesIsRefusedAsACollectionColumnIs(string $class, bool $several, int $size, string $message): void
    {
        $adapter = $this->adapter($class);

        try {
            if ($several) {
                $adapter->createAttributes('codes', [Attribute::string('name', size: 16), Attribute::varchar('code', size: $size)]);
            } else {
                $adapter->createAttribute('codes', Attribute::varchar('code', size: $size));
            }
            $this->fail('A varchar column outside its sizes must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        try {
            $adapter->createCollection('codes', [Attribute::varchar('code', size: $size)]);
            $this->fail('A varchar column outside its sizes must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    /**
     * @return iterable<string, array{class-string<SQL>, string}>
     */
    public static function engines(): iterable
    {
        yield 'MariaDB' => [MariaDB::class, '`code` VARCHAR(16381)'];
        yield 'MySQL' => [MySQL::class, '`code` VARCHAR(16381)'];
        yield 'Postgres' => [Postgres::class, '"code" VARCHAR(16381)'];
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('engines')]
    public function testAVarcharAttributeWithinItsSizesIsAdded(string $class, string $column): void
    {
        $this->assertTrue($this->adapter($class)->createAttribute('codes', Attribute::varchar('code', size: 16381)));

        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString($column, $this->statements[0]);
    }

    /**
     * @param class-string<SQL> $class
     */
    private function adapter(string $class): SQL
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);

            return $statement;
        });

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
