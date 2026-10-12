<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Attribute;
use Utopia\Database\Exception as DatabaseException;

final class MariaDBVarcharColumnTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<MariaDB>, int}>
     */
    public static function validSizes(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class] as $engine => $class) {
            yield $engine . ' smallest' => [$class, 1];
            yield $engine . ' typical' => [$class, 64];
            yield $engine . ' largest' => [$class, 16381];
        }
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('validSizes')]
    public function testAVarcharAttributeIsAVarcharColumnOfItsSize(string $class, int $size): void
    {
        $adapter = $this->adapter($class);

        $this->assertSame('VARCHAR(' . $size . ')', $adapter->getColumnType(Attribute::varchar('code', size: $size)));

        $adapter->createCollection('codes', [Attribute::varchar('code', size: $size)]);
        $this->assertStringContainsString('`code` VARCHAR(' . $size . ')', $this->statements[0] ?? '');
    }

    /**
     * @return iterable<string, array{class-string<MariaDB>, int, string}>
     */
    public static function invalidSizes(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class] as $engine => $class) {
            yield $engine . ' zero' => [$class, 0, 'VARCHAR size 0 is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
            yield $engine . ' negative' => [$class, -5, 'VARCHAR size -5 is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
            yield $engine . ' above the maximum' => [$class, 16382, 'VARCHAR size 16382 exceeds maximum varchar length 16381. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
        }
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('invalidSizes')]
    public function testAVarcharCollectionColumnOutsideItsSizesIsRefused(string $class, int $size, string $message): void
    {
        try {
            $this->adapter($class)->createCollection('codes', [Attribute::varchar('code', size: $size)]);
            $this->fail('A varchar column outside its sizes must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    /**
     * @param class-string<MariaDB> $class
     */
    private function adapter(string $class): MariaDB
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
