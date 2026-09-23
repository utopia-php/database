<?php

namespace Tests\E2E\Adapter\SharedTables;

use Redis;
use Tests\E2E\Adapter\Base;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\PDO;

class SQLiteTest extends Base
{
    public static ?Database $database = null;

    public static ?PDO $pdo = null;

    protected static string $namespace;

    // Remove once all methods are implemented
    /**
     * Return name of adapter
     */
    public static function getAdapterName(): string
    {
        return 'sqlite';
    }

    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $db = __DIR__.'/database_'.static::getTestToken().'.sql';

        if (file_exists($db)) {
            unlink($db);
        }

        $dsn = $db;
        // $dsn = 'memory'; // Overwrite for fast tests
        $pdo = new PDO('sqlite:'.$dsn, null, null, SQLite::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis');
        $redis->select(10);

        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $adapter = new SQLite($pdo);
        $adapter->setEmulateMySQL(true);

        $database = new Database($adapter, $cache);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($this->testDatabase)
            ->setSharedTables(true)
            ->setTenant(999)
            ->setNamespace(static::$namespace = 'st_'.static::getTestToken().'_'.uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;

        return self::$database = $database;
    }

    public function testIndexNamesUseTheFilteredTenant(): void
    {
        $database = $this->getDatabase();
        $collection = 'tenantIndexNames';

        $database->withTenant('acme.1', function () use ($database, $collection): void {
            $database->createCollection(new Collection(id: $collection, attributes: [
                Attribute::string(key: 'email', size: 64, required: true),
            ], permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ], documentSecurity: false));

            $index = Index::unique(key: 'email', attributes: ['email']);
            $database->createIndex($collection, $index);

            $this->assertTrue($database->getAdapter()->createIndex($collection, $index), 'Creating an existing index must be a no-op');
            $this->assertSame([$database->getNamespace().'_acme1_'.$collection.'_email'], $this->emailIndexes($database, $collection));

            $this->assertTrue($database->deleteIndex($collection, 'email'));
            $this->assertSame([], $this->emailIndexes($database, $collection), 'The index deleteIndex() reported as dropped must be gone');

            $database->createDocument($collection, new Document(['email' => 'user@example.com']));
            $database->createDocument($collection, new Document(['email' => 'user@example.com']));

            $this->assertSame(2, $database->count($collection), 'A deleted unique index must stop rejecting duplicates');
        });
    }

    /**
     * @return list<string>
     */
    private function emailIndexes(Database $database, string $collection): array
    {
        $names = [];
        foreach ($database->getSchemaIndexes($collection) as $index) {
            if (\str_ends_with($index->getId(), '_email')) {
                $names[] = $index->getId();
            }
        }

        return $names;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '`'.$this->getDatabase()->getNamespace().'_'.$collection.'`';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $index = '`'.$this->getDatabase()->getNamespace().'_'.$this->getDatabase()->getTenant()."_{$collection}_{$index}`";
        $sql = "DROP INDEX {$index}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }
}
