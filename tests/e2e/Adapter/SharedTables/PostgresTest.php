<?php

namespace Tests\E2E\Adapter\SharedTables;

use Redis;
use Tests\E2E\Adapter\Base;
use Tests\E2E\Adapter\Scopes\PostgresIdCollationTests;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Index;
use Utopia\Database\PDO;
use Utopia\Database\Permission;
use Utopia\Database\Role;

class PostgresTest extends Base
{
    use PostgresIdCollationTests;

    public static ?Database $database = null;

    public static ?PDO $pdo = null;

    #[\Override]
    protected static string $namespace;

    /**
     * Return name of adapter
     */
    public static function getAdapterName(): string
    {
        return 'postgres';
    }

    /**
     * @reture Adapter
     */
    #[\Override]
    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'postgres';
        $dbPort = '5432';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};", $dbUser, $dbPass, self::PDO_ATTRIBUTES);
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(9);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $database = new Database(new Postgres($pdo), $cache);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($this->testDatabase)
            ->setSharedTables(true)
            ->setTenant(999)
            ->setNamespace(static::$namespace = 'st_'.static::getTestToken());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;

        return self::$database = $database;
    }

    #[\Override]
    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '"'.$this->getDatabase()->getDatabase().'"."'.$this->getDatabase()->getNamespace().'_'.$collection.'"';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN \"{$column}\"";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    #[\Override]
    protected function deleteIndex(string $collection, string $index): bool
    {
        $key = '"'.$this->getDatabase()->getNamespace().'_'.$this->getDatabase()->getTenant()."_{$collection}_{$index}\"";

        $sql = 'DROP INDEX "'.$this->getDatabase()->getDatabase()."\".{$key}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    /**
     * Table names are quoted, so a collection whose id differs only by case is another table: the creator of the
     * index a tenant without its own copy renames must be found by the exact id.
     */
    public function testRenameIndexFindsTheCreatorOfTheExactCollectionId(): void
    {
        $database = $this->getDatabase();
        $tenant = $database->getTenant();

        $create = static fn (string $id) => $database->createCollection(Collection::create(id: $id, attributes: [
            Attribute::string(key: 'title', size: 64, required: false),
        ], indexes: [
            Index::key(key: 'idx', attributes: ['title']),
        ], permissions: [Permission::read(Role::any())]));

        try {
            $database->setTenant(998);
            $create('RenameCreator');
            $database->setTenant(1000);
            $create('renameCreator');
            $database->setTenant(999);
            $create('renameCreator');

            $database->renameIndex('renameCreator', 'idx', 'idx2');

            $this->assertSame(['idx2'], \array_map(
                static fn (Index $index): string => $index->key,
                $database->getCollection('renameCreator')->indexes(),
            ));
        } finally {
            foreach ([999 => 'renameCreator', 1000 => 'renameCreator', 998 => 'RenameCreator'] as $owner => $id) {
                $database->setTenant($owner);
                $database->deleteCollection($id);
            }
            $database->setTenant($tenant);
        }
    }
}
