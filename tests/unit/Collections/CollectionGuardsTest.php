<?php

namespace Tests\Unit\Collections;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class CollectionGuardsTest extends TestCase
{
    private const string COLLECTION = 'shared';

    public function testDeletingTheMetadataCollectionDropsItsTable(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter);

        $this->assertTrue($adapter->exists('guards', Database::METADATA));
        $this->assertTrue($database->deleteCollection(Database::METADATA));
        $this->assertFalse($adapter->exists('guards', Database::METADATA));
    }

    public function testDeletingTheMetadataCollectionDropsItsTableOnSQLite(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->database($adapter);

        $this->assertTrue($database->deleteCollection(Database::METADATA));
        $this->assertFalse($adapter->exists('guards', Database::METADATA));
    }

    private function database(Adapter $adapter): Database
    {
        return $this->prepare(new Database($adapter, new Cache(new None())));
    }

    private function prepare(Database $database): Database
    {
        $database->setDatabase('guards')->setNamespace('guards_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
