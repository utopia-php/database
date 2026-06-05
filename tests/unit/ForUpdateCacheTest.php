<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class ForUpdateCacheTest extends TestCase
{
    public function testForUpdateBypassesCachedDocument(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('for_update_' . uniqid());

        $database->create();
        $database->createCollection('projects');
        $database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'stale',
        ]));

        $cached = $database->getDocument('projects', 'project');
        $this->assertSame('stale', $cached->getAttribute('name'));

        $collection = $database->getCollection('projects');
        $document = $adapter->getDocument($collection, 'project');
        $document->setAttribute('name', 'fresh');
        $adapter->updateDocument($collection, 'project', $document, true);

        $cached = $database->getDocument('projects', 'project');
        $this->assertSame('stale', $cached->getAttribute('name'));

        $fresh = $database->getDocument('projects', 'project', forUpdate: true);
        $this->assertSame('fresh', $fresh->getAttribute('name'));
    }
}
