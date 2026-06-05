<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
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

        // The no-op must not mutate stored attribute values.
        $afterUpdate = $adapter->getDocument($collection, 'project');
        $this->assertSame('same', $afterUpdate->getAttribute('name'));
        $this->assertSame('2030-01-01T00:00:00.000+00:00', $afterUpdate->getUpdatedAt());
    }

    public function testBareUpdatedAtInputStillRequiresUpdatePermission(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('bare_updated_at_' . uniqid('', true));

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

        // A read-only caller submitting *only* $updatedAt is an explicit timestamp
        // write — the stale-cache tolerance must not apply, and UPDATE perm is required.
        $database->setPreserveDates(true);
        $this->expectException(AuthorizationException::class);
        $database->updateDocument('projects', 'project', new Document([
            '$updatedAt' => '2030-01-01T00:00:00.000+00:00',
        ]));
    }

    public function testStaleCacheResubmitWithRealChangeStillRequiresUpdatePermission(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('stale_real_change_' . uniqid('', true));

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
            'name' => 'original',
        ]));

        $stale = $database->getDocument('projects', 'project');
        $stale->setAttribute('name', 'mutated');

        // The tolerance branch must reject any input with a real attribute diff,
        // even if the caller also has a stale $updatedAt.
        $this->expectException(AuthorizationException::class);
        $database->updateDocument('projects', 'project', $stale);
    }

    public function testNumericallyEqualFloatDoesNotTriggerSpuriousUpdate(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('float_noop_' . uniqid());

        $database->create();
        $database->createCollection('measurements', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);
        $database->createAttribute('measurements', 'value', Database::VAR_FLOAT, 0, false);
        $database->createDocument('measurements', new Document([
            '$id' => 'm1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'value' => 5.0,
        ]));

        // Simulate cache returning the float as an int (JSON round-trips drop trailing zeros).
        $stale = $database->getDocument('measurements', 'm1');
        $stale->setAttribute('value', 5);

        // Read-only caller resubmits the doc; equal-as-float should be treated as a no-op
        // instead of failing the update permission check.
        $updated = $database->updateDocument('measurements', 'm1', $stale);
        $this->assertEquals(5.0, $updated->getAttribute('value'));

        // Storage must still hold the original float; no spurious write should have occurred.
        $collection = $database->getCollection('measurements');
        $stored = $adapter->getDocument($collection, 'm1');
        $this->assertEquals(5.0, $stored->getAttribute('value'));
    }

    public function testExplicitNullUpdatedAtStillUpdatesTimestamp(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('null_updated_at_' . uniqid());

        $database->create();
        $database->createCollection('projects', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]);
        $database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $created = $database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'same',
        ]));

        \usleep(2000);

        $database->setPreserveDates(true);
        $updated = $database->updateDocument('projects', 'project', new Document([
            '$updatedAt' => null,
        ]));

        $this->assertNotSame($created->getUpdatedAt(), $updated->getUpdatedAt());
    }

    public function testNonBareNullUpdatedAtIsSilentNoopForReadOnlyCaller(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('null_noop_readonly_' . uniqid('', true));

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

        // Caller has READ but not UPDATE. Resubmits the full document with
        // $updatedAt nulled and no actual attribute diff → must be a silent
        // no-op (no AuthorizationException, no storage write, no audit event).
        $stale = $database->getDocument('projects', 'project');
        $stale->setAttribute('$updatedAt', null);

        $result = $database->updateDocument('projects', 'project', $stale);

        $this->assertSame('same', $result->getAttribute('name'));

        $collection = $database->getCollection('projects');
        $stored = $adapter->getDocument($collection, 'project');
        $this->assertSame('same', $stored->getAttribute('name'));
    }

    public function testUnparseableUpdatedAtRejectsReadOnlyCaller(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('unparseable_updated_at_' . uniqid('', true));

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

        $stale = $database->getDocument('projects', 'project');
        $stale->setAttribute('$updatedAt', 'not-a-date');

        // An unparseable $updatedAt is not a recognizable stale-cache value, so the
        // tolerance branch must NOT apply; a caller without UPDATE perm must be
        // rejected, not silently treated as a no-op.
        $database->setPreserveDates(true);
        $this->expectException(AuthorizationException::class);
        $database->updateDocument('projects', 'project', $stale);
    }

    public function testFloatNoopDoesNotAdvanceUpdatedAt(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('float_noop_storage_' . uniqid('', true));

        $database->create();
        $database->createCollection('measurements', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]);
        $database->createAttribute('measurements', 'value', Database::VAR_FLOAT, 0, false);
        $created = $database->createDocument('measurements', new Document([
            '$id' => 'm1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'value' => 5.0,
        ]));

        \usleep(2000);

        // 5 vs 5.0 is a float-drift no-op. Without the no-op detection, the
        // adapter would still be called and $updatedAt would advance to now().
        $stale = $database->getDocument('measurements', 'm1');
        $stale->setAttribute('value', 5);

        $updated = $database->updateDocument('measurements', 'm1', $stale);

        // No write happened → $updatedAt must be byte-for-byte identical, proving
        // the adapter->updateDocument call was skipped (otherwise it would have
        // advanced to DateTime::now()).
        $this->assertSame($created->getUpdatedAt(), $updated->getUpdatedAt());

        // Storage should also hold the original value untouched.
        $reread = $database->getDocument('measurements', 'm1', forUpdate: true);
        $this->assertSame($created->getUpdatedAt(), $reread->getUpdatedAt());
        $this->assertEquals(5.0, $reread->getAttribute('value'));
    }
}
