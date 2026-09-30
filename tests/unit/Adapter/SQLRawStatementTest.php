<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\PDO as DatabasePDO;

final class SQLRawStatementTest extends TestCase
{
    public function testTheHostnameIsEmptyWhenTheConnectionNamesNoHost(): void
    {
        $adapter = new SQLite(new DatabasePDO('sqlite::memory:', null, null));

        $this->assertSame('', $adapter->getHostname());
    }

    public function testTheHostnameIsTheOneTheConnectionNames(): void
    {
        $adapter = new Postgres(new class ('pgsql:host=db.internal;dbname=app') extends DatabasePDO {
            public function __construct(string $dsn)
            {
                $this->dsn = $dsn;
            }
        });

        $this->assertSame('db.internal', $adapter->getHostname());
    }

    public function testARawReadOnAMissingTableIsNotFound(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $adapter->rawQuery('SELECT * FROM missing_table');
    }

    public function testARawWriteOnAMissingTableIsNotFound(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $adapter->rawMutation('UPDATE missing_table SET value = ?', [1]);
    }

    public function testRawStatementsReadAndWriteAnExistingTable(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->exec('CREATE TABLE present (value INTEGER)');
        $adapter = new SQLite($pdo);

        $this->assertSame(2, $adapter->rawMutation('INSERT INTO present (value) VALUES (?), (?)', [1, 2]));
        $rows = $adapter->rawQuery('SELECT value FROM present WHERE value > ? ORDER BY value', [1]);

        $this->assertCount(1, $rows);
        $this->assertSame(2, $rows[0]->getAttribute('value'));
    }

    /**
     * @return iterable<string, array{SQL}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [new MariaDB(new \stdClass())];
        yield 'MySQL' => [new MySQL(new \stdClass())];
        yield 'Postgres' => [new Postgres(new \stdClass())];
        yield 'SQLite' => [new SQLite(new PDO('sqlite::memory:'))];
    }

    #[DataProvider('adapters')]
    public function testAnUnknownColumnTypeSpellingIsRefused(SQL $adapter): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Unknown column type: not-a-type');

        $adapter->getColumnType('not-a-type', 0);
    }

    #[DataProvider('adapters')]
    public function testAKnownColumnTypeSpellingIsMapped(SQL $adapter): void
    {
        $this->assertNotSame('', $adapter->getColumnType('integer', 0));
    }
}
