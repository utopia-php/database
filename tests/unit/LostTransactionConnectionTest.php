<?php

namespace Tests\Unit;

use Closure;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\EngineError;
use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\PDO;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * The server ends the session while a transaction is open: its uncommitted work is gone,
 * and the connection the adapter keeps must run the caller's next statement.
 */
final class LostTransactionConnectionTest extends TestCase
{
    /**
     * @return array<string, array{Closure(PDO): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'MariaDB' => [static fn (PDO $pdo): Adapter => new MariaDB($pdo)],
            'Postgres' => [static fn (PDO $pdo): Adapter => new Postgres($pdo)],
            'SQLite' => [static fn (PDO $pdo): Adapter => new SQLite($pdo)],
        ];
    }

    /**
     * @return array<string, array{Closure(PDO): Adapter, Closure(PDO, int): void}>
     */
    public static function nestedLosses(): array
    {
        $prepared = static function (PDO $pdo, int $value): void {
            $pdo->prepare("INSERT INTO items VALUES ({$value})")->execute();
        };
        $executed = static function (PDO $pdo, int $value): void {
            $pdo->exec("INSERT INTO items VALUES ({$value})");
        };

        $losses = [];
        foreach (self::adapters() as $name => [$create]) {
            $losses["{$name}, prepared statement"] = [$create, $prepared];
            $losses["{$name}, executed statement"] = [$create, $executed];
        }

        return $losses;
    }

    /**
     * @param  Closure(PDO): Adapter  $create
     * @param  Closure(PDO, int): void  $write
     */
    #[DataProvider('nestedLosses')]
    public function testAConnectionLostInANestedTransactionLeavesTheConnectionUsable(Closure $create, Closure $write): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $adapter = $create($pdo);
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $pdo, $endSession, $write, &$attempts): void {
            $adapter->withTransaction(function () use ($adapter, $pdo, $endSession, $write, &$attempts): void {
                $attempts++;
                $write($pdo, 1);

                $adapter->withTransaction(function () use ($pdo, $endSession, $write): void {
                    $endSession();
                    $write($pdo, 2);
                });
            });
        });

        $this->assertInstanceOf(TransactionException::class, $error, 'A transaction lost under a nested call fails the outermost call');
        $this->assertSame(1, $attempts, 'The work of a lost transaction must not run again');
        $this->assertSame([], $this->values($path), 'Nothing written in the lost transaction may be stored');
        $this->assertFalse($adapter->inTransaction());
        $this->assertConnectionUsable($pdo, $path);
    }

    /**
     * The outer callback catches the nested failure and carries on: its statements must not
     * run in autocommit while it still runs, and the outer call must not report the lost
     * work as committed.
     *
     * @param  Closure(PDO): Adapter  $create
     */
    #[DataProvider('adapters')]
    public function testAnOuterCallbackThatCarriesOnAfterALostNestedTransactionFails(Closure $create): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $adapter = $create($pdo);
        $nested = null;
        $carriedOn = null;

        $error = $this->capture(function () use ($adapter, $pdo, $endSession, &$nested, &$carriedOn): void {
            $adapter->withTransaction(function () use ($adapter, $pdo, $endSession, &$nested, &$carriedOn): void {
                $pdo->exec('INSERT INTO items VALUES (1)');

                $nested = $this->capture(function () use ($adapter, $pdo, $endSession): void {
                    $adapter->withTransaction(function () use ($pdo, $endSession): void {
                        $endSession();
                        $pdo->prepare('INSERT INTO items VALUES (2)')->execute();
                    });
                });

                $carriedOn = $this->capture(fn (): int|false => $pdo->exec('INSERT INTO items VALUES (5)'));
            });
        });

        $this->assertInstanceOf(TransactionException::class, $nested);
        $this->assertInstanceOf(PDOException::class, $carriedOn, 'A statement after the lost transaction must not run while the outer call runs');
        $this->assertInstanceOf(TransactionException::class, $error, 'The outer call must not return as if its lost work committed');
        $this->assertSame([], $this->values($path), 'Nothing written in or after the lost transaction may be stored');
        $this->assertFalse($adapter->inTransaction());
        $this->assertConnectionUsable($pdo, $path);
    }

    /**
     * @param  Closure(PDO): Adapter  $create
     */
    #[DataProvider('adapters')]
    public function testATransactionThatLosesTheConnectionOnEveryAttemptLeavesTheConnectionUsable(Closure $create): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $adapter = $create($pdo);
        $attempts = 0;

        $error = $this->capture(function () use ($adapter, $pdo, $endSession, &$attempts): void {
            $adapter->withTransaction(function () use ($pdo, $endSession, &$attempts): void {
                $attempts++;
                $pdo->prepare('INSERT INTO items VALUES (1)')->execute();
                $endSession();
                $pdo->prepare('INSERT INTO items VALUES (2)')->execute();
            });
        });

        $this->assertInstanceOf(Throwable::class, $error, 'Every attempt lost its connection');
        $this->assertSame(3, $attempts, 'A top-level transaction that lost its connection runs again up to its retries');
        $this->assertSame([], $this->values($path), 'Nothing written in the lost transactions may be stored');
        $this->assertFalse($adapter->inTransaction());
        $this->assertConnectionUsable($pdo, $path);
    }

    public function testAPooledConnectionLostInANestedTransactionServesTheNextCheckout(): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $connection = new MariaDB($pdo);
        $pool = new Pool(new UtopiaPool(new Stack(), 'lost-transaction', 1, static fn (): MariaDB => $connection, timeout: 0.0));
        $pool->setAuthorization(new Authorization());

        $error = $this->capture(fn (): mixed => $pool->withTransaction(function () use ($pool, $pdo, $endSession): void {
            $pdo->exec('INSERT INTO items VALUES (1)');

            $pool->withTransaction(function () use ($pdo, $endSession): void {
                $endSession();
                $pdo->prepare('INSERT INTO items VALUES (2)')->execute();
            });
        }));

        $this->assertInstanceOf(TransactionException::class, $error);
        $this->assertFalse($pool->inTransaction());
        $this->assertTrue($pool->ping(), 'The next checkout of the connection must run its statement');
        $this->assertSame([], $this->values($path), 'Nothing written in the lost transaction may be stored');
    }

    private function assertConnectionUsable(PDO $pdo, string $path): void
    {
        $pdo->prepare('INSERT INTO items VALUES (3)')->execute();
        $pdo->exec('INSERT INTO items VALUES (4)');

        $statement = $pdo->query('SELECT COUNT(*) FROM items');
        $this->assertInstanceOf(\PDOStatement::class, $statement);
        $this->assertSame(2, $statement->fetchColumn(), 'A read on the connection must see its own writes');
        $this->assertSame([3, 4], $this->values($path), 'Writes after the lost transaction must be stored');
        $this->assertFalse($pdo->inTransaction(), 'The connection must not keep the lost transaction');
    }

    /**
     * @param  callable(): mixed  $callback
     */
    private function capture(callable $callback): ?Throwable
    {
        try {
            $callback();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }

    private function createDatabaseFile(): string
    {
        $path = \tempnam(\sys_get_temp_dir(), 'lost-transaction-');
        $this->assertIsString($path);
        \register_shutdown_function(static fn (): bool => @\unlink($path));

        $database = new \PDO("sqlite:{$path}");
        $database->exec('CREATE TABLE items (value INTEGER)');

        return $path;
    }

    /**
     * A connection whose session the server can end: the server rolls the session's
     * transaction back, and every call on the old handle then fails as a dropped MySQL
     * connection does while the handle still reports its transaction. A reconnect opens
     * a new session on the same database.
     *
     * @return array{PDO, Closure(): void}
     */
    private function createLosableConnection(string $path): array
    {
        $pdo = new class ("sqlite:{$path}", null, null) extends PDO {
            public function endSession(): void
            {
                if ($this->pdo->inTransaction()) {
                    $this->pdo->rollBack();
                }

                $this->pdo = new class () extends \PDO {
                    public function __construct()
                    {
                    }

                    public function inTransaction(): bool
                    {
                        return true;
                    }

                    public function beginTransaction(): bool
                    {
                        throw self::lost();
                    }

                    public function commit(): bool
                    {
                        throw self::lost();
                    }

                    public function rollBack(): bool
                    {
                        throw self::lost();
                    }

                    public function exec(string $statement): int|false
                    {
                        throw self::lost();
                    }

                    /**
                     * @param  array<mixed>  $options
                     */
                    public function prepare(string $query, array $options = []): \PDOStatement|false
                    {
                        throw self::lost();
                    }

                    public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): \PDOStatement|false
                    {
                        throw self::lost();
                    }

                    private static function lost(): PDOException
                    {
                        return EngineError::create('HY000', 2006, 'SQLSTATE[HY000]: General error: 2006 MySQL server has gone away');
                    }
                };
            }
        };

        return [$pdo, $pdo->endSession(...)];
    }

    /**
     * @return array<int>
     */
    private function values(string $path): array
    {
        $statement = (new \PDO("sqlite:{$path}"))->query('SELECT value FROM items ORDER BY value');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        return \array_map(static function (mixed $value): int {
            self::assertIsNumeric($value);

            return (int) $value;
        }, $statement->fetchAll(\PDO::FETCH_COLUMN));
    }
}
