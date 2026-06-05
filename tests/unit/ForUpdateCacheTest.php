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

        // A locking read must not repopulate the cache, otherwise subsequent
        // non-locking reads would see transactionally-scoped data.
        $afterForUpdate = $database->getDocument('projects', 'project');
        $this->assertSame('stale', $afterForUpdate->getAttribute('name'));
    }

    public function testNoopUpdateIgnoresStaleCachedUpdatedAt(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('stale_updated_at_' . uniqid());

        $database->create();
        $database->createCollection('projects', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);
        $database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'same',
        ]));

        $cached = $database->getDocument('projects', 'project');
        $this->assertSame('same', $cached->getAttribute('name'));

        $collection = $database->getCollection('projects');
        $stored = $adapter->getDocument($collection, 'project');
        $stored->setAttribute('$updatedAt', '2030-01-01T00:00:00.000+00:00');
        $adapter->updateDocument($collection, 'project', $stored, true);

        $stale = $database->getDocument('projects', 'project');
        $this->assertNotSame('2030-01-01T00:00:00.000+00:00', $stale->getUpdatedAt());

        $database->setPreserveDates(true);
        $updated = $database->updateDocument('projects', 'project', $stale);
        $this->assertSame('2030-01-01T00:00:00.000+00:00', $updated->getUpdatedAt());
    }
}
