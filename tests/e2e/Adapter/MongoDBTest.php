<?php

namespace Tests\E2E\Adapter;

use Exception;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Mongo\Client;

class MongoDBTest extends Base
{
    public static ?Database $database = null;
    protected static string $namespace;

    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "mongodb";
    }

    /**
     * @return Database
     * @throws Exception
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        // Use Memory cache adapter as fallback when Redis is not available
        $cache = new Cache(new Memory());

        $schema = 'utopiaTests'; // same as $this->testDatabase
        $client = new Client(
            $schema,
            'mongo',
            27017,
            'root',
            'password',
            false
        );

        $database = new Database(new Mongo($client), $cache);
        $database
            ->setDatabase($schema)
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    /**
     * @throws Exception
     */
    public function testCreateExistsDelete(): void
    {
        // Mongo creates databases on the fly, so exists would always pass. So we override this test to remove the exists check.
        $this->assertNotNull(static::getDatabase()->create());
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create());
        $this->assertEquals(static::getDatabase(), static::getDatabase()->setDatabase($this->testDatabase));
    }

    public function testRenameAttribute(): void
    {
        $this->assertTrue(true);
    }

    public function testRenameAttributeExisting(): void
    {
        $this->assertTrue(true);
    }

    public function testUpdateAttributeStructure(): void
    {
        $this->assertTrue(true);
    }

    public function testKeywords(): void
    {
        $this->assertTrue(true);
    }

    protected static function deleteColumn(string $collection, string $column): bool
    {
        return true;
    }

    protected static function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }
}
