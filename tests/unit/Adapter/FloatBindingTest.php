<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class FloatBindingTest extends TestCase
{
    /**
     * @var list<mixed>
     */
    private array $bindings = [];

    public function testAWriteBindsAFloatAsPhpWritesIt(): void
    {
        $adapter = $this->adapter();
        $collection = Collection::create(id: 'numbers', attributes: [Attribute::double(key: 'f')]);

        $adapter->createDocument($collection, new Document(['$id' => 'tiny', '$permissions' => [], 'f' => 1e-300]));

        $this->assertContains(1e-300, $this->bindings);
        $this->assertNotContains('0.00000000000000000', $this->bindings);
    }

    public function testAFindBindsAFloatInFixedPoint(): void
    {
        $adapter = $this->adapter();
        $collection = Collection::create(id: 'numbers', attributes: [Attribute::double(key: 'f')]);

        $adapter->find($collection, [Query::lessThan('f', 0.5)]);

        $this->assertContains('0.50000000000000000', $this->bindings);
    }

    private function adapter(): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bindings[] = $value;

                return true;
            });
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('fetch')->willReturn(false);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('closeCursor')->willReturn(true);

            return $statement;
        });
        $pdo->method('lastInsertId')->willReturn('1');

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        return $adapter;
    }
}
