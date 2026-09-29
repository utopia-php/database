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

/**
 * The statements skipDuplicates() sends on the engines the host cannot run. Engines with
 * RETURNING learn the inserted rows from the insert itself; MySQL locks the batch's stored ids
 * first and inserts only the new ones.
 */
final class SkipDuplicatesStatementTest extends TestCase
{
    public function testMariaDBReturnsTheKeysOfTheInsertedRows(): void
    {
        $statements = new ArrayObject();
        $adapter = new MariaDB($this->pdo($statements, rows: [['fresh']], written: 1));

        $created = $this->createDocuments($adapter);

        $this->assertSame(['fresh'], $created);
        $this->assertCount(1, $statements);
        $this->assertStringStartsWith('INSERT IGNORE INTO', $statements[0]);
        $this->assertStringEndsWith(' RETURNING `_uid`', $statements[0]);
    }

    public function testMySQLInsertsOnlyTheIdsItFoundUnstoredUnderLock(): void
    {
        $statements = new ArrayObject();
        $adapter = new MySQL($this->pdo($statements, rows: [['stored']], written: 1));
        $adapter->startTransaction();

        $created = $this->createDocuments($adapter);

        $this->assertSame(['fresh'], $created);
        $this->assertCount(2, $statements);
        $this->assertStringStartsWith('SELECT `_uid` FROM', $statements[0]);
        $this->assertStringEndsWith(' FOR UPDATE', $statements[0]);
        $this->assertStringStartsWith('INSERT IGNORE INTO', $statements[1]);
        $this->assertStringNotContainsString('), (', $statements[1], 'The stored id is left out of the insert');
        $this->assertStringNotContainsString('RETURNING', $statements[1]);
    }

    public function testMySQLReadsTheIdsBackWhenTheInsertWroteFewerRows(): void
    {
        $statements = new ArrayObject();
        $adapter = new MySQL($this->pdo($statements, rows: [['stored']], written: 0));
        $adapter->startTransaction();

        $created = $this->createDocuments($adapter);

        $this->assertSame([], $created, 'A document the insert skipped for another unique value is not reported');
        $this->assertCount(3, $statements);
        $this->assertStringStartsWith('SELECT `_uid` FROM', $statements[2]);
        $this->assertStringEndsNotWith(' FOR UPDATE', $statements[2]);
    }

    public function testPostgresReturnsTheKeysOfTheInsertedRows(): void
    {
        $statements = new ArrayObject();
        $adapter = new Postgres($this->pdo($statements, rows: [['fresh', 7]], written: 1));
        $adapter->setSharedTables(true);
        $adapter->setTenant(7);

        $created = $this->createDocuments($adapter, tenant: 7);

        $this->assertSame(['fresh'], $created);
        $this->assertCount(1, $statements);
        $this->assertStringEndsWith(' RETURNING "_uid", "_tenant"', $statements[0]);
    }

    /**
     * @return list<string>
     */
    private function createDocuments(SQL $adapter, ?int $tenant = null): array
    {
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $documents = [];
        foreach (['stored', 'fresh'] as $id) {
            $document = new Document(['$id' => $id, '$permissions' => [], 'title' => $id]);
            if ($tenant !== null) {
                $document->setAttribute('$tenant', $tenant);
            }
            $documents[] = $document;
        }

        $created = $adapter->skipDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => 'notes', 'attributes' => []]), $documents));

        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $created));
    }

    /**
     * Every prepared statement past the transaction reset is recorded; each returns the given
     * rows and reports the given number of written rows.
     *
     * @param  ArrayObject<int, string>  $statements
     * @param  list<list<mixed>>  $rows
     */
    private function pdo(ArrayObject $statements, array $rows, int $written): PDO
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statements, $rows, $written): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn($rows);
            $statement->method('rowCount')->willReturn($written);

            $query = \trim($query);
            if ($query !== 'ROLLBACK') {
                $statements->append($query);
            }

            return $statement;
        });

        return $pdo;
    }
}
