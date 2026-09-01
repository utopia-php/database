<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

final class PostgresIdentifierTest extends TestCase
{
    public function testCreateAndLookupShareHashedNameForMongoShapedCollection(): void
    {
        $namespace = '_507f1f77bcf86cd799439011';
        $collection = 'database_507f1f77bcf86cd799439012_collection_507f1f77bcf86cd799439013';
        $unhashed = $namespace.'_'.$collection;
        $this->assertGreaterThan(Postgres::MAX_IDENTIFIER_NAME, \strlen($unhashed));

        $adapter = new Postgres($this->createStub(PDO::class));
        $adapter->setDatabase('appwrite');
        $adapter->setNamespace($namespace);

        $physical = $this->invoke($adapter, 'getPhysicalTableName', $collection);
        $quoted = $this->invoke($adapter, 'getSQLTable', $collection);
        $raw = $this->invoke($adapter, 'getSQLTableRaw', $collection);

        $this->assertNotSame($unhashed, $physical);
        $this->assertLessThanOrEqual(Postgres::MAX_IDENTIFIER_NAME, \strlen($physical));
        $this->assertSame('"appwrite"."'.$physical.'"', $quoted);
        $this->assertSame('appwrite.'.$physical, $raw);
    }

    public function testCreateCollectionSqlUsesHashedTableName(): void
    {
        $namespace = '_507f1f77bcf86cd799439011';
        $collection = 'database_507f1f77bcf86cd799439012_collection_507f1f77bcf86cd799439013';

        $statement = $this->getMockBuilder(PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->exactly(2))
            ->method('execute')
            ->willReturn(true);

        $queries = [];
        $pdo = $this->getMockBuilder(PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('prepare')
            ->willReturnCallback(function (string $sql) use (&$queries, $statement): PDOStatement {
                $queries[] = $sql;

                return $statement;
            });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('appwrite');
        $adapter->setNamespace($namespace);
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $physical = $this->invoke($adapter, 'getPhysicalTableName', $collection);
        $permissions = $this->invoke($adapter, 'getPhysicalTableName', Storage::permissionsTable($collection));

        $this->assertTrue($adapter->createCollection($collection));
        $this->assertNotSame($namespace.'_'.$collection, $physical);
        $this->assertStringContainsString('"'.$physical.'"', $queries[0]);
        $this->assertStringNotContainsString($collection, $queries[0]);
        $this->assertStringContainsString('"'.$permissions.'"', $queries[1]);
    }

    public function testExistsBindsHashedTableName(): void
    {
        $namespace = '_507f1f77bcf86cd799439011';
        $collection = 'database_507f1f77bcf86cd799439012_collection_507f1f77bcf86cd799439013';
        $bound = [];

        $statement = $this->getMockBuilder(PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->exactly(2))
            ->method('bindValue')
            ->willReturnCallback(function (int $position, mixed $value) use (&$bound): bool {
                $bound[$position] = $value;

                return true;
            });
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([['table_name' => 'hashed']]);
        $statement->expects($this->once())->method('closeCursor')->willReturn(true);

        $pdo = $this->getMockBuilder(PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($statement);

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('appwrite');
        $adapter->setNamespace($namespace);

        $physical = $this->invoke($adapter, 'getPhysicalTableName', $collection);

        $this->assertTrue($adapter->exists('appwrite', $collection));
        $this->assertSame('appwrite', $bound[1]);
        $this->assertSame($physical, $bound[2]);
        $this->assertNotSame($namespace.'_'.$collection, $bound[2]);
    }

    private function invoke(Postgres $adapter, string $method, string ...$arguments): string
    {
        $reflection = new ReflectionMethod($adapter, $method);
        $result = $reflection->invoke($adapter, ...$arguments);
        $this->assertIsString($result);

        return $result;
    }
}
