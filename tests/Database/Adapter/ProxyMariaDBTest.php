<?php

namespace Utopia\Tests\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Database\Database;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\ProxyMariaDB;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Tests\Base;

class ProxyMariaDBTest extends TestCase
{
    protected string $testDatabase = 'utopiaTests';

    public static ?Database $database = null;

    // TODO@kodumbeats hacky way to identify adapters for tests
    // Remove once all methods are implemented
    /**
     * Return name of adapter
     *
     * @return string
     */
    public static function getAdapterName(): string
    {
        return "proxy-mariadb";
    }

    /**
     * @return Database
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $database = new Database(new ProxyMariaDB('http://proxy/v1', 'test-secret', 'default'), new Cache(new None()));
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_' . uniqid());

        if ($database->exists('utopiaTests')) {
            $database->delete('utopiaTests');
        }

        $database->create();

        return self::$database = $database;
    }

    public function testCreateExistsDelete(): void
    {
        $schemaSupport = $this->getDatabase()->getAdapter()->getSupportForSchemas();
        if (!$schemaSupport) {
            $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
            $this->assertEquals(true, static::getDatabase()->create());
            return;
        }

        if (!static::getDatabase()->exists($this->testDatabase)) {
            $this->assertEquals(true, static::getDatabase()->create());
        }
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->create());
    }

      /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors'));

        static::getDatabase()->createCollection('actors2');
        static::getDatabase()->createCollection('actors3');
        static::getDatabase()->createCollection('actors4');

        sleep(3);

        \var_dump(static::getDatabase()->listCollections());
        \var_dump(static::getDatabase()->listCollections());
        \var_dump(static::getDatabase()->listCollections());
        \var_dump(static::getDatabase()->listCollections());
        $this->assertCount(2, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors2'));
        $this->assertCount(3, static::getDatabase()->listCollections());
        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase, 'actors2'));
        $collection = static::getDatabase()->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->updateDocument($collection->getCollection(), $collection->getId(), $collection));
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(2, static::getDatabase()->listCollections());

        $this->assertEquals(false, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase, 'actors'));
    }
}
