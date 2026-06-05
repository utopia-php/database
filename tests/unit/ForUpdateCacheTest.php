<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
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
        // Numerically equal is the contract this branch promises (5 == 5.0); the
        // post-write storage type is intentionally adapter-defined and is
        // re-coerced by casting() on subsequent reads.
        $this->assertEquals(5.0, $updated->getAttribute('value'));

        // Storage holds a numerically equal value (intentional pre-existing
        // drift on adapters without castingBefore); subsequent reads through
        // the Database layer re-coerce via casting().
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

        // DateTime::now() formats with millisecond precision (Y-m-d H:i:s.v),
        // so the sleep must be well above 1ms to avoid same-bucket flakes on
        // loaded CI runners.
        \usleep(50_000);

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

        // Even a 50ms sleep here would still pass the assertSame below if the
        // no-op detection works, since no write should happen. The sleep proves
        // the assertion is meaningful: if the adapter were called, $updatedAt
        // would advance by ≥1ms and the assertion would fail.
        \usleep(50_000);

        // 5 vs 5.0 is a float-drift no-op. Without the no-op detection, the
        // adapter would still be called and $updatedAt would advance to now().
        $stale = $database->getDocument('measurements', 'm1');
        $stale->setAttribute('value', 5);

        $updated = $database->updateDocument('measurements', 'm1', $stale);

        // shouldUpdate stayed false because of the float-noop detection, so
        // $updatedAt was not bumped — proves the diff loop treated 5 vs 5.0 as
        // equal even though strict !== would say otherwise.
        $this->assertSame($created->getUpdatedAt(), $updated->getUpdatedAt());
        $this->assertEquals(5.0, $updated->getAttribute('value'));

        // Subsequent reads through the Database layer re-coerce via casting()
        // back to float, even if the adapter stores a numerically equal int.
        $reread = $database->getDocument('measurements', 'm1', forUpdate: true);
        $this->assertSame($created->getUpdatedAt(), $reread->getUpdatedAt());
        $this->assertIsFloat($reread->getAttribute('value'));
        $this->assertSame(5.0, $reread->getAttribute('value'));
    }

    public function testMetaOnlyInputStillRequiresUpdatePermission(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('meta_only_updated_at_' . \uniqid('', true));

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

        // Caller submits only system meta keys ($id + $updatedAt) — no real attribute
        // keys. This is an explicit timestamp write, not a stale-cache resubmit, so it
        // must require UPDATE perm regardless of how many meta keys are echoed back.
        // Without the meta-aware "bare" check, a strict array_keys === ['$updatedAt']
        // comparison would fail open for this shape.
        $database->setPreserveDates(true);
        $this->expectException(AuthorizationException::class);
        $database->updateDocument('projects', 'project', new Document([
            '$id' => 'project',
            '$updatedAt' => '2030-01-01T00:00:00.000+00:00',
        ]));
    }

    public function testRelativeTimestampStringStillRequiresUpdatePermission(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('relative_updated_at_' . \uniqid('', true));

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
        // \DateTime("now") and \DateTime("yesterday") both parse without throwing,
        // but no real cached timestamp would carry these values. The tolerance branch
        // must reject relative/symbolic time expressions via a strict ISO-shape check;
        // otherwise an attacker who knows neither the real $updatedAt nor any prior
        // doc state can engage tolerance just by submitting "now".
        $stale->setAttribute('$updatedAt', 'now');

        $database->setPreserveDates(true);
        $this->expectException(AuthorizationException::class);
        $database->updateDocument('projects', 'project', $stale);
    }

    public function testStaleCacheToleranceDoesNotEmitUpdateEvent(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('tolerance_event_' . \uniqid('', true));

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

        // Warm the cache with the original $updatedAt before poking the adapter,
        // otherwise the next getDocument() reads straight from storage and there's
        // no stale value for tolerance to engage on.
        $database->getDocument('projects', 'project');

        // Force a stale cached $updatedAt so the tolerance branch will engage.
        $collection = $database->getCollection('projects');
        $stored = $adapter->getDocument($collection, 'project');
        $stored->setAttribute('$updatedAt', '2030-01-01T00:00:00.000+00:00');
        $adapter->updateDocument($collection, 'project', $stored, true);

        $stale = $database->getDocument('projects', 'project');

        // Subscribe before triggering the no-op resubmit.
        $eventHits = 0;
        $database->on(Database::EVENT_DOCUMENT_UPDATE, 'tolerance-probe', function () use (&$eventHits) {
            $eventHits++;
        });

        // Caller has READ but not UPDATE. Tolerance triggers a silent no-op.
        // The event must NOT fire: it would let a read-only caller forge audit
        // entries and probe document existence via downstream listeners.
        $result = $database->updateDocument('projects', 'project', $stale);

        $this->assertSame('same', $result->getAttribute('name'));
        $this->assertSame(0, $eventHits);
    }

    public function testLegitimateNoopStillEmitsUpdateEvent(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('legit_noop_event_' . \uniqid('', true));

        $database->create();
        $database->createCollection('projects', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
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
        $stale->setAttribute('$updatedAt', null);

        $eventHits = 0;
        $database->on(Database::EVENT_DOCUMENT_UPDATE, 'legit-noop', function () use (&$eventHits) {
            $eventHits++;
        });

        // Caller HAS UPDATE perm; the no-op (null-$updatedAt) path is a legitimate
        // invocation, so the event must still fire for audit / change-stream consumers.
        $database->updateDocument('projects', 'project', $stale);
        $this->assertSame(1, $eventHits);
    }

    public function testJunkKeyDoesNotUnlockStaleCacheTolerance(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('junk_key_' . \uniqid('', true));

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

        // A read-only caller submitting only a stale $updatedAt + a junk null-valued
        // key (not in the schema, not an internal meta key) must NOT engage the
        // tolerance branch — junk keys can't count as "real attribute keys" or a
        // caller could trivially unlock the tolerance path by appending any unknown
        // null key.
        $database->setPreserveDates(true);
        $this->expectException(AuthorizationException::class);
        $database->updateDocument('projects', 'project', new Document([
            '$updatedAt' => '2030-01-01T00:00:00.000+00:00',
            'garbageKey' => null,
        ]));
    }

    public function testNoopStillEnforcesRequestTimestampConflict(): void
    {
        $cache = new Cache(new CacheMemory());
        $adapter = new DatabaseMemory();
        $database = new Database($adapter, $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('noop_conflict_' . \uniqid('', true));

        $database->create();
        $database->createCollection('projects', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]);
        $database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'same',
        ]));

        // No-op resubmit (null $updatedAt + UPDATE perm + no real diff) under a
        // request-timestamp older than storage's $updatedAt must still throw
        // ConflictException — the short-circuit return must not silently
        // succeed when the optimistic-concurrency contract is violated.
        $stale = $database->getDocument('projects', 'project');
        $stale->setAttribute('$updatedAt', null);

        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));

        $this->expectException(ConflictException::class);
        $database->withRequestTimestamp($oneHourAgo, function () use ($database, $stale) {
            return $database->updateDocument('projects', 'project', $stale);
        });
    }
}
