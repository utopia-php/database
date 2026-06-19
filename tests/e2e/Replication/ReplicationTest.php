<?php

namespace Tests\E2E\Replication;

use PDO;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Scheduler;
use Swoole\Runtime;
use Utopia\Database\Replication\Change;
use Utopia\Database\Replication\Replication;

/**
 * Integration tests for the binlog Replication reader against a live MySQL 8.
 *
 * Opt-in: skipped unless REPLICATION_TEST_HOST is set. The test self-configures
 * the server (GTID + FULL row metadata), so the connecting user must have
 * privileges to SET GLOBAL (e.g. root).
 *
 *   REPLICATION_TEST_HOST=127.0.0.1 REPLICATION_TEST_PORT=3306 \
 *   REPLICATION_TEST_USER=root REPLICATION_TEST_PASS=password \
 *   vendor/bin/phpunit tests/e2e/Replication
 */
class ReplicationTest extends TestCase
{
    private const string SCHEMA = 'replication_test';
    private const string TABLE = 'console15x_projects';
    private const int SERVER_ID = 223344;

    private string $host;
    private int $port;
    private string $user;
    private string $pass;
    private PDO $pdo;

    protected function setUp(): void
    {
        $host = \getenv('REPLICATION_TEST_HOST');
        if ($host === false) {
            $this->markTestSkipped('Set REPLICATION_TEST_HOST to run replication integration tests.');
        }

        $this->host = $host;
        $this->port = (int) (\getenv('REPLICATION_TEST_PORT') ?: '3306');
        $this->user = \getenv('REPLICATION_TEST_USER') ?: 'root';
        $this->pass = \getenv('REPLICATION_TEST_PASS') ?: 'password';

        $this->pdo = new PDO(
            "mysql:host={$this->host};port={$this->port}",
            $this->user,
            $this->pass,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->configureServer();
        $this->resetSchema();

        Runtime::enableCoroutine(SWOOLE_HOOK_ALL);
    }

    protected function tearDown(): void
    {
        Runtime::enableCoroutine(false);
    }

    public function testBasicCrudIsStreamed(): void
    {
        $changes = $this->capture(3, function (PDO $pdo) {
            $pdo->exec("INSERT INTO " . self::TABLE . " (_uid, name) VALUES ('proj_a', 'First')");
            $pdo->exec("UPDATE " . self::TABLE . " SET name = 'Second' WHERE _uid = 'proj_a'");
            $pdo->exec("DELETE FROM " . self::TABLE . " WHERE _uid = 'proj_a'");
        });

        $this->assertCount(3, $changes);
        $this->assertSame([Change::INSERT, Change::UPDATE, Change::DELETE], \array_map(fn ($c) => $c->action, $changes));
        $this->assertSame('proj_a', $changes[0]->rows[0]['_uid']);
        $this->assertSame('Second', $changes[1]->rows[0]['name']); // UPDATE after-image
        $this->assertSame(self::SCHEMA, $changes[0]->database);
        $this->assertNotSame('', $changes[2]->gtid);                // checkpoint advanced
    }

    public function testLargeRowSpanningMultiplePackets(): void
    {
        // > 16 MiB forces the binlog event across multiple 0xFFFFFF protocol frames.
        $size = 18 * 1024 * 1024;
        $blob = \str_repeat('x', $size);

        $changes = $this->capture(1, function (PDO $pdo) use ($blob) {
            $stmt = $pdo->prepare("INSERT INTO " . self::TABLE . " (_uid, name, data) VALUES ('big', 'Big', :data)");
            $stmt->execute([':data' => $blob]);
        });

        $this->assertCount(1, $changes);
        $this->assertSame('big', $changes[0]->rows[0]['_uid']);
        $this->assertSame($size, \strlen($changes[0]->rows[0]['data']));
    }

    public function testCachingSha2FullAuthentication(): void
    {
        // A freshly (re)created user is not in the server's auth cache, so the
        // reader must complete the caching_sha2_password full-auth (RSA) path.
        $this->pdo->exec("DROP USER IF EXISTS 'repl_full'@'%'");
        $this->pdo->exec("CREATE USER 'repl_full'@'%' IDENTIFIED WITH caching_sha2_password BY 'Repl!Full#123'");
        $this->pdo->exec("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_full'@'%'");
        $this->pdo->exec('FLUSH PRIVILEGES');

        $changes = $this->capture(1, function (PDO $pdo) {
            $pdo->exec("INSERT INTO " . self::TABLE . " (_uid, name) VALUES ('proj_fa', 'FullAuth')");
        }, user: 'repl_full', pass: 'Repl!Full#123');

        $this->assertCount(1, $changes);
        $this->assertSame('proj_fa', $changes[0]->rows[0]['_uid']);
    }

    public function testTlsConnection(): void
    {
        $changes = $this->capture(1, function (PDO $pdo) {
            $pdo->exec("INSERT INTO " . self::TABLE . " (_uid, name) VALUES ('proj_tls', 'Secure')");
        }, ssl: true);

        $this->assertCount(1, $changes);
        $this->assertSame('proj_tls', $changes[0]->rows[0]['_uid']);
    }

    /**
     * Start the reader, run $writer against the table, and collect $expected changes.
     *
     * @return array<int, Change>
     */
    private function capture(int $expected, callable $writer, string $user = '', string $pass = '', bool $ssl = false): array
    {
        $collected = [];
        $error = null;
        $dsn = "mysql:host={$this->host};port={$this->port};dbname=" . self::SCHEMA;
        $writerUser = $this->user;
        $writerPass = $this->pass;
        $readerUser = $user !== '' ? $user : $this->user;
        $readerPass = $user !== '' ? $pass : $this->pass;

        $scheduler = new Scheduler();
        $scheduler->add(function () use (&$collected, &$error, $writer, $expected, $dsn, $writerUser, $writerPass, $readerUser, $readerPass, $ssl) {
            try {
                $replication = new Replication($this->host, $this->port, $readerUser, $readerPass, self::SERVER_ID, $ssl);
                $replication->setSchema(self::SCHEMA)->start(null);

                Coroutine::create(function () use ($writer, $dsn, $writerUser, $writerPass) {
                    Coroutine::sleep(0.5);
                    $writer(new PDO($dsn, $writerUser, $writerPass, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]));
                });

                $count = 0;
                foreach ($replication->getChanges() as $change) {
                    $collected[] = $change;
                    if (++$count >= $expected) {
                        break;
                    }
                }
                $replication->stop();
            } catch (\Throwable $e) {
                $error = $e;
            }
        });
        $scheduler->start();

        if ($error !== null) {
            throw $error;
        }

        return $collected;
    }

    private function configureServer(): void
    {
        $this->pdo->exec('SET GLOBAL binlog_row_metadata = FULL');

        $statement = $this->pdo->query('SELECT @@global.gtid_mode');
        $mode = $statement === false ? null : $statement->fetchColumn();
        if ($mode !== 'ON') {
            $this->pdo->exec('SET GLOBAL enforce_gtid_consistency = ON');
            $this->pdo->exec('SET GLOBAL gtid_mode = OFF_PERMISSIVE');
            $this->pdo->exec('SET GLOBAL gtid_mode = ON_PERMISSIVE');
            $this->pdo->exec('SET GLOBAL gtid_mode = ON');
        }
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('CREATE DATABASE IF NOT EXISTS ' . self::SCHEMA);
        $this->pdo->exec('USE ' . self::SCHEMA);
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->pdo->exec(
            'CREATE TABLE ' . self::TABLE . ' (
                _id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                _uid VARCHAR(255) NOT NULL,
                name VARCHAR(255) NULL,
                _createdAt DATETIME(3) NULL,
                _permissions JSON NULL,
                data LONGBLOB NULL,
                PRIMARY KEY (_id),
                UNIQUE KEY (_uid)
            )'
        );
    }
}
