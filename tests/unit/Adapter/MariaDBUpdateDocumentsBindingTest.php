<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Document;

final class MariaDBUpdateDocumentsBindingTest extends TestCase
{
    /** @var list<mixed> */
    private array $bound = [];

    public function testABulkUpdateBindsBooleansAsIntegersAndArraysAsJson(): void
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bound[] = $value;

                return true;
            });

            return $statement;
        });

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $this->assertSame(1, $adapter->updateDocuments(
            new Document(['$id' => 'items', 'attributes' => []]),
            new Document(['active' => true, 'archived' => false, 'tags' => ['a', 'b']]),
            [new Document(['$id' => 'first', '$sequence' => '1'])],
        ));

        $this->assertContains(1, $this->bound);
        $this->assertContains(0, $this->bound);
        $this->assertContains('["a","b"]', $this->bound);
        $this->assertNotContains(true, $this->bound);
        $this->assertNotContains(false, $this->bound);
    }
}
