<?php

namespace Tests\Unit\Adapter;

use ArrayObject;
use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Role;

/**
 * The statements ignoreDuplicates() sends on the engines the host cannot run.
 */
final class IgnoreDuplicatesStatementTest extends TestCase
{
    private const string STORED = 'stored';

    private const string FRESH = 'fresh';

    public function testMariaDBLearnsTheInsertedRowsFromReturning(): void
    {
        /** @var ArrayObject<int, string> $statements */
        $statements = new ArrayObject();
        $adapter = new MariaDB($this->pdo($statements, [[[[self::FRESH]], 1]]));

        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::STORED, self::FRESH]));
        $this->assertCount(1, $statements);
        $this->assertStringStartsWith('INSERT IGNORE INTO', self::sent($statements, 0));
        $this->assertStringEndsWith(' RETURNING `_uid`', self::sent($statements, 0));
    }

    public function testARepeatedIdIsSentOnlyOnce(): void
    {
        /** @var ArrayObject<int, string> $statements */
        $statements = new ArrayObject();
        $adapter = new MariaDB($this->pdo($statements, [[[[self::FRESH]], 1]]));

        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::FRESH, self::FRESH]));
        $this->assertStringNotContainsString('), (', self::sent($statements, 0), 'Only the first copy of an id is inserted');
    }

    public function testMySQLInsertsOnlyTheIdsItFoundUnstoredWithoutLocking(): void
    {
        /** @var ArrayObject<int, string> $statements */
        $statements = new ArrayObject();
        $adapter = new MySQL($this->pdo($statements, [[[[self::STORED]], 0], [[], 1]]));

        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::STORED, self::FRESH]));
        $this->assertCount(2, $statements);
        $this->assertStringStartsWith('SELECT `_uid` FROM', self::sent($statements, 0));
        $this->assertStringNotContainsString('FOR UPDATE', self::sent($statements, 0));
        $this->assertStringStartsWith('INSERT IGNORE INTO', self::sent($statements, 1));
        $this->assertStringNotContainsString('), (', self::sent($statements, 1), 'The stored id is left out of the insert');
        $this->assertStringNotContainsString('RETURNING', self::sent($statements, 1));
    }

    public function testMySQLDoesNotReportADocumentTheInsertSkipped(): void
    {
        /** @var ArrayObject<int, string> $statements */
        $statements = new ArrayObject();
        $adapter = new MySQL($this->pdo($statements, [[[], 0], [[], 0], [[], 0]]));

        $this->assertSame([], $this->createDocuments($adapter, [self::FRESH]));
        $this->assertCount(3, $statements);
        $this->assertStringStartsWith('SELECT `_uid`, `_permissions` FROM', self::sent($statements, 2));
    }

    public function testMySQLReportsARowReadBackOnlyWhenItCarriesTheDocumentsPermissions(): void
    {
        $granted = \json_encode([Permission::read(Role::any())], JSON_THROW_ON_ERROR);
        $foreign = \json_encode([Permission::read(Role::user('alice'))], JSON_THROW_ON_ERROR);

        $adapter = new MySQL($this->pdo(new ArrayObject(), [[[], 0], [[], 0], [[[self::FRESH, $foreign]], 0]]));
        $this->assertSame([], $this->createDocuments($adapter, [self::FRESH]), 'A row another writer stored with other permissions is not ours');

        $adapter = new MySQL($this->pdo(new ArrayObject(), [[[], 0], [[], 0], [[[self::FRESH, $granted]], 0]]));
        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::FRESH]));
    }

    public function testPostgresSkipsOnlyAStoredIdSoAnotherUniqueCollisionFails(): void
    {
        /** @var ArrayObject<int, string> $statements */
        $statements = new ArrayObject();
        $adapter = new Postgres($this->pdo($statements, [[[[self::FRESH]], 1]]));

        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::STORED, self::FRESH]));
        $this->assertCount(1, $statements);
        $this->assertStringStartsWith('INSERT INTO', self::sent($statements, 0));
        $this->assertStringEndsWith(' ON CONFLICT ("_uid") DO NOTHING RETURNING "_uid"', self::sent($statements, 0));
    }

    public function testPostgresNamesTheTenantInTheConflictTargetUnderSharedTables(): void
    {
        /** @var ArrayObject<int, string> $statements */
        $statements = new ArrayObject();
        $adapter = new Postgres($this->pdo($statements, [[[[self::FRESH, 7]], 1]]));
        $adapter->setSharedTables(true);
        $adapter->setTenant(7);

        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::STORED, self::FRESH], tenant: 7));
        $this->assertStringEndsWith(' ON CONFLICT ("_uid", "_tenant") DO NOTHING RETURNING "_uid", "_tenant"', self::sent($statements, 0));
    }

    public function testADocumentWithoutATenantIsMatchedUnderTheAdaptersTenant(): void
    {
        $adapter = new Postgres($this->pdo(new ArrayObject(), [[[[self::FRESH, 7]], 1]]));
        $adapter->setSharedTables(true);
        $adapter->setTenant(7);

        $this->assertSame([self::FRESH], $this->createDocuments($adapter, [self::FRESH]));
    }

    /**
     * @param  list<string>  $ids
     * @return list<string>
     */
    private function createDocuments(SQL $adapter, array $ids, ?int $tenant = null): array
    {
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $documents = [];
        foreach ($ids as $id) {
            $document = new Document(['$id' => $id, '$permissions' => [Permission::read(Role::any())], 'title' => $id]);
            if ($tenant !== null) {
                $document->setAttribute('$tenant', $tenant);
            }
            $documents[] = $document;
        }

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => 'notes', 'attributes' => []]), $documents));

        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $created));
    }

    /**
     * @param  ArrayObject<int, string>  $statements
     */
    private static function sent(ArrayObject $statements, int $index): string
    {
        $sent = $statements->getArrayCopy();
        self::assertArrayHasKey($index, $sent);

        return $sent[$index];
    }

    /**
     * Each prepared statement is recorded and answers with the next rows and written-row count.
     *
     * @param  ArrayObject<int, string>  $statements
     * @param  list<array{list<list<mixed>>, int}>  $results
     */
    private function pdo(ArrayObject $statements, array $results): PDO
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statements, &$results): PDOStatement {
            [$rows, $written] = \array_shift($results) ?? [[], 0];
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn($rows);
            $statement->method('rowCount')->willReturn($written);
            $statements->append(\trim($query));

            return $statement;
        });

        return $pdo;
    }
}
