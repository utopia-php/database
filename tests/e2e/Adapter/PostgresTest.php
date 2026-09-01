<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;

class PostgresTest extends Base
{
    public static ?Database $database = null;

    protected static ?PDO $pdo = null;

    protected static string $namespace;

    /**
     * @reture Adapter
     */
    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'postgres';
        $dbPort = '5432';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};", $dbUser, $dbPass, Postgres::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(2);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $database = new Database(new Postgres($pdo), $cache);
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

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '"'.$this->getDatabase()->getDatabase().'"."'.$this->getDatabase()->getNamespace().'_'.$collection.'"';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN \"{$column}\"";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $key = '"'.$this->getDatabase()->getNamespace().'_'.$this->getDatabase()->getTenant()."_{$collection}_{$index}\"";

        $sql = 'DROP INDEX "'.$this->getDatabase()->getDatabase()."\".{$key}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    public function testCreateCollectionWithMongoSequenceShapedId(): void
    {
        $database = $this->getDatabase();
        $collection = 'database_507f1f77bcf86cd799439012_collection_507f1f77bcf86cd799439013';

        $this->assertGreaterThan(
            Postgres::MAX_IDENTIFIER_NAME,
            \strlen($database->getNamespace().'_'.$collection)
        );

        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'name', size: 128, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        $document = $database->createDocument($collection, new Document([
            '$id' => 'vector-doc',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'embeddings',
        ]));

        $this->assertSame('vector-doc', $document->getId());
        $this->assertSame('embeddings', $database->getDocument($collection, 'vector-doc')->getAttribute('name'));
        $this->assertTrue($database->exists($database->getDatabase(), $collection));
        $this->assertTrue($database->deleteCollection($collection));
    }
}
