<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Exception\NotFound as NotFoundException;

final class MariaDBDeleteCollectionTest extends TestCase
{
    private const string MAIN = 'DROP TABLE `database`.`namespace_places`';

    private const string PERMISSIONS = 'DROP TABLE IF EXISTS `database`.`namespace_places_perms`';

    /** @var list<string> */
    private array $statements = [];

    /**
     * @return array<string, array{class-string<MariaDB>}>
     */
    public static function adapters(): array
    {
        return [
            'MariaDB' => [MariaDB::class],
            'MySQL' => [MySQL::class],
        ];
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('adapters')]
    public function testDropsBothTablesInOneStatement(string $adapterClass): void
    {
        $adapter = $this->createAdapter($adapterClass, mainTableExists: true);

        $this->assertTrue($adapter->deleteCollection('places'));
        $this->assertSame([self::MAIN.'; '.self::PERMISSIONS], $this->statements);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('adapters')]
    public function testDropsThePermissionsTableWhenTheMainTableIsGone(string $adapterClass): void
    {
        $adapter = $this->createAdapter($adapterClass, mainTableExists: false);

        try {
            $adapter->deleteCollection('places');
            $this->fail('A collection whose table is gone must be reported as not found');
        } catch (NotFoundException $e) {
            $this->assertSame('Collection not found', $e->getMessage());
        }

        $this->assertSame([self::MAIN.'; '.self::PERMISSIONS, self::PERMISSIONS], $this->statements);
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    private function createAdapter(string $adapterClass, bool $mainTableExists): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($mainTableExists): PDOStatement {
            $this->statements[] = $query;

            $statement = $this->createStub(PDOStatement::class);
            if (! $mainTableExists && \str_starts_with($query, self::MAIN.';')) {
                $statement->method('execute')->willThrowException($this->unknownTable());
            } else {
                $statement->method('execute')->willReturn(true);
            }

            return $statement;
        });

        $adapter = new $adapterClass($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }

    private function unknownTable(): PDOException
    {
        return new class () extends PDOException {
            public function __construct()
            {
                parent::__construct("SQLSTATE[42S02]: Base table or view not found: 1051 Unknown table 'database.namespace_places'");
                $this->code = '42S02';
                $this->errorInfo = ['42S02', 1051, "Unknown table 'database.namespace_places'"];
            }
        };
    }
}
