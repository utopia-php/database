<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class CreateCollectionRaceTest extends TestCase
{
    public function testCreateCollectionDoesNotDropUncommittedPeerTable(): void
    {
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, new Cache(new CacheMemory()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('create_race_' . uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();

        $collection = 'preCommitCreate';
        $name = new Document([
            '$id' => ID::custom('name'),
            'type' => Database::VAR_STRING,
            'size' => 128,
            'required' => false,
        ]);

        $adapter->createCollection($collection, [$name], []);

        $schema = new Document([
            '$id' => $collection,
            '$collection' => Database::METADATA,
            'name' => $collection,
            'attributes' => [$name],
            'indexes' => [],
            'documentSecurity' => true,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);

        $adapter->createDocument($schema, new Document([
            '$id' => ID::custom('written'),
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'peer',
        ]));

        $created = $database->createCollection($collection, [$name], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $this->assertSame($collection, $created->getId());
        $this->assertSame(
            'peer',
            $database->getDocument($collection, 'written')->getAttribute('name'),
            'Physical collection was dropped while metadata was still uncommitted'
        );
    }
}
