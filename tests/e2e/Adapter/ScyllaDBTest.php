<?php

namespace Tests\E2E\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\ScyllaDB;
use Utopia\Database\Database;

class ScyllaDBTest extends Base
{
    protected static ?Database $database = null;
    protected static ?PDO $pdo = null;
    protected static string $namespace;

    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "scylladb";
    }

    /**
     * @return Database
     */
    public static function getDatabase(bool $fresh = false): Database
    {
        if (!is_null(self::$database) && !$fresh) {
            return self::$database;
        }

        $dbHost = 'scylladb';
        $dbPort = '9042';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("scylla:host={$dbHost};port={$dbPort}", $dbUser, $dbPass, ScyllaDB::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new ScyllaDB($pdo), $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;
        return self::$database = $database;
    }

    protected static function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = self::getDatabase()->getNamespace() . '_' . $collection;
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN {$column}";

        self::$pdo->exec($sql);

        return true;
    }

    protected static function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = self::getDatabase()->getNamespace() . '_' . $collection;
        $sql = "DROP INDEX IF EXISTS {$index} ON {$sqlTable}";

        self::$pdo->exec($sql);

        return true;
    }

    public function testCreateCollection(): void
    {
        $this->assertEquals(true, static::getDatabase()->createCollection('testCreateCollection'));
    }

    public function testCreateAttribute(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('testCreateAttribute'));

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testCreateAttribute',
                'string_attr',
                Database::VAR_STRING,
                255
            )
        );

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testCreateAttribute',
                'integer_attr',
                Database::VAR_INTEGER,
                0
            )
        );

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testCreateAttribute',
                'float_attr',
                Database::VAR_FLOAT,
                0
            )
        );

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testCreateAttribute',
                'boolean_attr',
                Database::VAR_BOOLEAN,
                0
            )
        );

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testCreateAttribute',
                'datetime_attr',
                Database::VAR_DATETIME,
                0
            )
        );
    }

    public function testCreateIndex(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('testCreateIndex'));

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testCreateIndex',
                'string_attr',
                Database::VAR_STRING,
                255
            )
        );

        $this->assertEquals(
            true,
            $database->createIndex(
                'testCreateIndex',
                'string_index',
                Database::INDEX_KEY,
                ['string_attr']
            )
        );

        $this->assertEquals(
            true,
            $database->createIndex(
                'testCreateIndex',
                'unique_index',
                Database::INDEX_UNIQUE,
                ['string_attr']
            )
        );
    }

    public function testDeleteCollection(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('testDeleteCollection'));
        $this->assertEquals(true, $database->deleteCollection('testDeleteCollection'));
    }

    public function testDeleteAttribute(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('testDeleteAttribute'));

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testDeleteAttribute',
                'string_attr',
                Database::VAR_STRING,
                255
            )
        );

        $this->assertEquals(true, $database->deleteAttribute('testDeleteAttribute', 'string_attr'));
    }

    public function testDeleteIndex(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('testDeleteIndex'));

        $this->assertEquals(
            true,
            $database->createAttribute(
                'testDeleteIndex',
                'string_attr',
                Database::VAR_STRING,
                255
            )
        );

        $this->assertEquals(
            true,
            $database->createIndex(
                'testDeleteIndex',
                'string_index',
                Database::INDEX_KEY,
                ['string_attr']
            )
        );

        $this->assertEquals(true, $database->deleteIndex('testDeleteIndex', 'string_index'));
    }
} 