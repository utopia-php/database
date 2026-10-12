<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

final class PostgresIdentifierTest extends TestCase
{
    private const string NAMESPACE = '_507f1f77bcf86cd799439011';

    private const string COLLECTION = 'database_507f1f77bcf86cd799439012_collection_507f1f77bcf86cd799439013';

    public function testCreateAndLookupShareHashedNameForMongoShapedCollection(): void
    {
        $unhashed = self::NAMESPACE.'_'.self::COLLECTION;
        $this->assertGreaterThan(Postgres::MAX_IDENTIFIER_NAME, \strlen($unhashed));

        $physical = $this->existsBinding(self::COLLECTION);
        $queries = $this->createCollectionQueries(self::COLLECTION);

        $this->assertNotSame($unhashed, $physical);
        $this->assertLessThanOrEqual(Postgres::MAX_IDENTIFIER_NAME, \strlen($physical));
        $this->assertStringContainsString('"appwrite"."'.$physical.'"', $queries[0]);
    }

    public function testCreateCollectionSqlUsesHashedTableName(): void
    {
        $physical = $this->existsBinding(self::COLLECTION);
        $permissions = $this->existsBinding(Storage::permissionsTable(self::COLLECTION));

        $queries = $this->createCollectionQueries(self::COLLECTION);

        $this->assertNotSame(self::NAMESPACE.'_'.self::COLLECTION, $physical);
        $this->assertStringContainsString('"'.$physical.'"', $queries[0]);
        $this->assertStringNotContainsString(self::COLLECTION, $queries[0]);
        $this->assertStringContainsString('"'.$permissions.'"', $queries[1]);
    }

    public function testExistsBindsHashedTableName(): void
    {
        $queries = $this->createCollectionQueries(self::COLLECTION);

        $physical = $this->existsBinding(self::COLLECTION);

        $this->assertNotSame(self::NAMESPACE.'_'.self::COLLECTION, $physical);
        $this->assertStringContainsString('"appwrite"."'.$physical.'"', $queries[0]);
    }

    /**
     * @return list<string>
     */
    private function createCollectionQueries(string $collection): array
    {
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
        $adapter->setNamespace(self::NAMESPACE);
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $this->assertTrue($adapter->createCollection($collection));
        $this->assertCount(2, $queries);

        return $queries;
    }

    private function existsBinding(string $collection): string
    {
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
        $adapter->setNamespace(self::NAMESPACE);

        $this->assertTrue($adapter->collectionExists('appwrite', $collection));
        $this->assertSame('appwrite', $bound[1] ?? null);
        $physical = $bound[2] ?? null;
        $this->assertIsString($physical);

        return $physical;
    }
}
