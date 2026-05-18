<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

/**
 * Integration tests for Database::withExplain() against real backends.
 *
 * Verifies the full path: write-path hook fires inside Adapter::find(),
 * adapter runs vendor-native EXPLAIN, the parsed plan flows back through
 * the buffer, and the sanitizer strips internal storage references.
 *
 * Mongo skips itself — explain capture is stubbed there pending real
 * cursor->explain() integration.
 */
trait ExplainTests
{
    public function testWithExplainCapturesPlanForFind(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof SQL) {
            $this->markTestSkipped('Explain capture is only wired in the SQL adapter today.');
        }

        $database->getAuthorization()->addRole(Role::any()->toString());

        $collection = 'explain_test_' . \uniqid();

        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);
        $database->createAttribute($collection, 'title', Database::VAR_STRING, 128, true);
        $database->createAttribute($collection, 'status', Database::VAR_STRING, 32, true);

        $database->createDocument($collection, new Document([
            '$id' => ID::unique(),
            '$permissions' => [Permission::read(Role::any())],
            'title'  => 'first',
            'status' => 'published',
        ]));
        $database->createDocument($collection, new Document([
            '$id' => ID::unique(),
            '$permissions' => [Permission::read(Role::any())],
            'title'  => 'second',
            'status' => 'draft',
        ]));

        // The actual capture: wrap a real find() in withExplain.
        $plan = $database->withExplain(fn () => $database->find($collection, [
            Query::equal('status', ['published']),
            Query::limit(10),
        ]));

        $this->assertInstanceOf(Document::class, $plan);

        $entries = $plan->getAttribute('queries');
        $this->assertIsArray($entries);
        $this->assertNotEmpty($entries, 'find() should produce at least one captured plan entry');

        $entry = $entries[0];
        $this->assertSame('find', $entry['purpose']);
        $this->assertArrayHasKey('context', $entry);
        $this->assertArrayHasKey('collection', $entry['context']);
        $this->assertSame($collection, $entry['context']['collection']);

        $this->assertArrayHasKey('plan', $entry);
        $this->assertSame('sql', $entry['plan']['engine']);
        $this->assertArrayHasKey('tree', $entry['plan']);
        $this->assertNotNull($entry['plan']['tree'], 'EXPLAIN FORMAT=JSON tree must be parsed');

        // The extracted top-level fields should appear even if some are null
        // (e.g. when there's no usable index).
        $this->assertArrayHasKey('rowsScanned', $entry['plan']);
        $this->assertArrayHasKey('indexUsed', $entry['plan']);
        $this->assertArrayHasKey('estimatedCost', $entry['plan']);

        // Sanitizer must have hidden the internal permission table reference
        // (the EXISTS subquery against the _perms shadow table).
        $rawTree = \json_encode($entry['plan']['tree']);
        $this->assertStringNotContainsString('_perms', $rawTree, 'permission companion table must be redacted');

        // Cleanup.
        $database->deleteCollection($collection);
    }

    public function testWithExplainBufferIsClearedAfterScope(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof SQL) {
            $this->markTestSkipped('Explain capture is only wired in the SQL adapter today.');
        }

        // Outside the scope: no capture, no overhead.
        $this->assertFalse($database->getAdapter()->isExplainCapturing());

        $database->withExplain(function () use ($database) {
            $this->assertTrue($database->getAdapter()->isExplainCapturing());
            // Don't even need to run a query — just verify the flag toggles.
        });

        // After: cleanly reset.
        $this->assertFalse($database->getAdapter()->isExplainCapturing());
    }

    public function testWithExplainExceptionDoesNotLeaveBufferOpen(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof SQL) {
            $this->markTestSkipped('Explain capture is only wired in the SQL adapter today.');
        }

        try {
            $database->withExplain(function () {
                throw new \RuntimeException('boom');
            });
            $this->fail('Expected RuntimeException to propagate');
        } catch (\RuntimeException) {
            // expected
        }

        $this->assertFalse(
            $database->getAdapter()->isExplainCapturing(),
            'capture buffer must be cleared even when callback throws',
        );
    }
}
