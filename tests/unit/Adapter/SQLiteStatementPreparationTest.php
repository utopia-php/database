<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Hook\Transform;

final class SQLiteStatementPreparationTest extends TestCase
{
    private string $file = '';

    protected function tearDown(): void
    {
        if ($this->file !== '' && \is_file($this->file)) {
            \unlink($this->file);
        }
    }

    public function testATransformErrorReachesTheCallerUnchanged(): void
    {
        $refusal = new DatabaseException('refused');
        $adapter = $this->adapter(new PDO('sqlite::memory:'));
        $adapter->addTransform('refuse', $this->transform(static function (Event $event, string $query) use ($refusal): string {
            if ($event === Event::CollectionCreate) {
                throw $refusal;
            }

            return $query;
        }));

        try {
            $adapter->createCollection('notes', [Attribute::string('body', size: 64)]);
            $this->fail('The transform refusal must reach the caller');
        } catch (DatabaseException $error) {
            $this->assertSame($refusal, $error);
        }

        $this->assertFalse($adapter->exists('main', 'notes'));
    }

    public function testAStatementTheDriverCannotPrepareIsAnAdapterError(): void
    {
        $adapter = $this->adapter(new PDO('sqlite::memory:', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT]));
        $adapter->addTransform('break', $this->transform(static fn (Event $event, string $query): string => 'NOT SQL'));

        try {
            $adapter->createCollection('notes', [Attribute::string('body', size: 64)]);
            $this->fail('A statement the driver cannot prepare must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to prepare SQLite statement', $error->getMessage());
            $previous = $error->getPrevious();
            $this->assertInstanceOf(DatabaseException::class, $previous);
            $this->assertSame('Failed to prepare SQL statement', $previous->getMessage());
        }
    }

    public function testAStrayDriverTransactionIsRolledBackBeforeTheNextOne(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->exec('CREATE TABLE stray (value INTEGER)');
        $adapter = $this->adapter($pdo);

        $pdo->beginTransaction();
        $pdo->exec('INSERT INTO stray VALUES (1)');

        $this->assertTrue($adapter->startTransaction());
        $pdo->exec('INSERT INTO stray VALUES (2)');
        $this->assertTrue($adapter->commitTransaction());

        $this->assertFalse($pdo->inTransaction());
        $this->assertSame([2], $this->values($pdo));
    }

    public function testABusyDatabaseFailsToStartATransaction(): void
    {
        $this->file = \tempnam(\sys_get_temp_dir(), 'sqlite-busy-') ?: '';
        $this->assertNotSame('', $this->file);

        $holder = new PDO('sqlite:' . $this->file);
        $holder->exec('BEGIN IMMEDIATE');

        $adapter = $this->adapter(new PDO('sqlite:' . $this->file, options: [PDO::ATTR_TIMEOUT => 0]));

        try {
            $adapter->startTransaction();
            $this->fail('A transaction must not start while another connection holds the writer lock');
        } catch (TransactionException $error) {
            $this->assertStringStartsWith('Failed to start transaction: ', $error->getMessage());
            $this->assertStringContainsString('database is locked', $error->getMessage());
        } finally {
            $holder->exec('ROLLBACK');
        }

        $this->assertFalse($adapter->inTransaction());
        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->commitTransaction());
    }

    private function adapter(PDO $pdo): SQLite
    {
        $adapter = new SQLite($pdo);
        $adapter->setDatabase('main');
        $adapter->setNamespace('preparation');

        return $adapter;
    }

    /**
     * @param callable(Event, string): string $callback
     */
    private function transform(callable $callback): Transform
    {
        return new class ($callback) implements Transform {
            /**
             * @var callable(Event, string): string
             */
            private $callback;

            /**
             * @param callable(Event, string): string $callback
             */
            public function __construct(callable $callback)
            {
                $this->callback = $callback;
            }

            public function transform(Event $event, string $query): string
            {
                return ($this->callback)($event, $query);
            }
        };
    }

    /**
     * @return list<int>
     */
    private function values(PDO $pdo): array
    {
        $statement = $pdo->query('SELECT value FROM stray ORDER BY value');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        return \array_map(intval(...), $statement->fetchAll(PDO::FETCH_COLUMN));
    }
}
