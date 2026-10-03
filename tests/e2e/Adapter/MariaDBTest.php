<?php

namespace Tests\E2E\Adapter;

use PDO as PhpPDO;
use Redis;
use Swoole\Coroutine;
use Swoole\Runtime;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PDO;

use function Swoole\Coroutine\run;

class MariaDBTest extends Base
{
    protected static ?Database $database = null;

    protected static ?PDO $pdo = null;

    protected static string $namespace;

    public function getDatabase(bool $fresh = false): Database
    {
        if (! is_null(self::$database) && ! $fresh) {
            return self::$database;
        }

        $dbHost = 'mariadb';
        $dbPort = '3306';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(0);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $database = new Database(new MariaDB($pdo), $cache);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($this->testDatabase)
            ->setNamespace(static::$namespace = 'myapp_'.uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;

        return self::$database = $database;
    }

    /**
     * Two transactions lock the same missing row range FOR UPDATE, then each inserts its own row through a nested
     * transaction, as cloud's concurrent billing aggregations do. InnoDB rolls the deadlock's loser back whole,
     * savepoint included. Nothing of the loser's attempt is stored, so it runs again and both rows commit.
     */
    public function testTransactionsDeadlockedOnALockedGapBothCommit(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('Concurrent transactions need Swoole coroutines.');
        }

        $database = $this->getDatabase();
        $collection = 'deadlockedGap';
        $database->createCollection(new Collection(
            id: $collection,
            attributes: [Attribute::integer(key: 'count', required: false)],
            permissions: [Permission::read(Role::any()), Permission::create(Role::any())],
        ));

        $hooks = Runtime::getHookFlags();
        $options = Coroutine::getOptions()['hook_flags'] ?? SWOOLE_HOOK_ALL;
        /** @var int $arrived */
        $arrived = 0;
        /** @var array<string, int> $attempts */
        $attempts = [];
        /** @var list<string> $conflicts */
        $conflicts = [];
        /** @var array<string, string> $failures */
        $failures = [];

        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

        try {
            run(function () use ($collection, &$arrived, &$attempts, &$conflicts, &$failures): void {
                foreach (['starter', 'pro'] as $id) {
                    Coroutine::create(function () use ($collection, $id, &$arrived, &$attempts, &$conflicts, &$failures): void {
                        try {
                            $connection = $this->connect();
                            $attempts[$id] = 0;
                            $connection->withTransaction(function () use ($connection, $collection, $id, &$arrived, &$attempts, &$conflicts): void {
                                $attempts[$id]++;
                                $connection->getDocument($collection, $id, forUpdate: true);

                                if ($attempts[$id] === 1) {
                                    $arrived++;
                                    for ($waited = 0; $arrived < 2 && $waited < 500; $waited++) {
                                        Coroutine::sleep(0.01);
                                    }
                                }

                                try {
                                    $connection->createDocument($collection, new Document(['$id' => $id, 'count' => 1]));
                                } catch (ContentionException $error) {
                                    $conflicts[] = $error->getMessage();

                                    throw $error;
                                }
                            });
                        } catch (Throwable $error) {
                            $failures[$id] = $error::class.': '.$error->getMessage();
                        }
                    });
                }
            });
        } finally {
            Coroutine::set(['hook_flags' => $options]);
            Runtime::setHookFlags($hooks);
        }

        try {
            $this->assertSame([], $failures, 'Both transactions must commit');
            $this->assertSame(['Deadlock detected'], $conflicts, 'Exactly one transaction must lose the deadlock');
            $counts = \array_values($attempts);
            \sort($counts);
            $this->assertSame([1, 2], $counts, 'Only the loser runs again');
            $this->assertFalse($database->getDocument($collection, 'starter')->isEmpty());
            $this->assertFalse($database->getDocument($collection, 'pro')->isEmpty());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    private function connect(): Database
    {
        $pdo = new PDO(
            'mysql:host=mariadb;port=3306;charset=utf8mb4',
            'root',
            'password',
            [PhpPDO::ATTR_PERSISTENT => false] + MariaDB::getPDOAttributes(),
        );

        $main = $this->getDatabase();
        $database = new Database(new MariaDB($pdo), new Cache(new NoCache()));
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($main->getDatabase())
            ->setNamespace($main->getNamespace());

        if (! $database->getAdapter()->hasPermissionHook()) {
            $database->addHook(new Permissions());
        }

        return $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '`'.$this->getDatabase()->getDatabase().'`.`'.$this->getDatabase()->getNamespace().'_'.$collection.'`';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = '`'.$this->getDatabase()->getDatabase().'`.`'.$this->getDatabase()->getNamespace().'_'.$collection.'`';
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }
}
