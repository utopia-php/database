<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

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

        $plan = $database->withExplain(fn () => $database->find($collection, [
            Query::equal('status', ['published']),
            Query::limit(10),
        ]));

        $this->assertInstanceOf(Document::class, $plan);

        $entries = $plan->getAttribute('queries');
        $this->assertIsArray($entries);
        $this->assertNotEmpty($entries);

        $entry = $entries[0];
        $this->assertSame('find', $entry['purpose']);
        $this->assertArrayHasKey('context', $entry);
        $this->assertArrayHasKey('collection', $entry['context']);
        $this->assertSame($collection, $entry['context']['collection']);

        $this->assertArrayHasKey('plan', $entry);
        // Engine label is precise per adapter (mysql/mariadb/postgres/sqlite).
        $this->assertContains($entry['plan']['engine'], ['mysql', 'mariadb', 'postgres', 'sqlite']);
        $this->assertArrayHasKey('tree', $entry['plan']);
        $this->assertNotNull($entry['plan']['tree']);

        $this->assertArrayHasKey('rowsScanned', $entry['plan']);
        $this->assertArrayHasKey('indexUsed', $entry['plan']);
        $this->assertArrayHasKey('estimatedCost', $entry['plan']);

        // Actual execution stats from the real find() that ran under explain.
        $this->assertArrayHasKey('rowsReturned', $entry['plan']);
        $this->assertArrayHasKey('executionTime', $entry['plan']);
        // One document matches status=published, and find() rows are measured directly.
        $this->assertSame(1, $entry['plan']['rowsReturned']);
        $this->assertIsFloat($entry['plan']['executionTime']);

        $rawTree = \json_encode($entry['plan']['tree']);
        $this->assertStringNotContainsString('_perms', $rawTree);

        $database->deleteCollection($collection);
    }

    public function testWithExplainBufferIsClearedAfterScope(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof SQL) {
            $this->markTestSkipped('Explain capture is only wired in the SQL adapter today.');
        }

        $this->assertFalse($database->getAdapter()->isExplainCapturing());

        $database->withExplain(function () use ($database) {
            $this->assertTrue($database->getAdapter()->isExplainCapturing());
        });

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
        }

        $this->assertFalse($database->getAdapter()->isExplainCapturing());
    }

    public function testWithExplainCapturesCountAndSum(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof SQL) {
            $this->markTestSkipped('Explain capture is only wired in the SQL adapter today.');
        }

        $database->getAuthorization()->addRole(Role::any()->toString());

        $collection = 'explain_count_sum_' . \uniqid();
        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);
        $database->createAttribute($collection, 'score', Database::VAR_INTEGER, 0, true);

        for ($i = 0; $i < 3; $i++) {
            $database->createDocument($collection, new Document([
                '$id' => ID::unique(),
                '$permissions' => [Permission::read(Role::any())],
                'score' => $i + 1,
            ]));
        }

        // Regression guard: count() used to crash inside withExplain because
        // the context array called ->getId() on a string.
        $plan = $database->withExplain(function () use ($database, $collection): void {
            $database->count($collection);
            $database->sum($collection, 'score');
        });

        $entries = $plan->getAttribute('queries');
        $this->assertIsArray($entries);
        $this->assertGreaterThanOrEqual(2, \count($entries));

        $purposes = \array_column($entries, 'purpose');
        $this->assertContains('count', $purposes);
        $this->assertContains('sum', $purposes);

        foreach ($entries as $entry) {
            $this->assertSame($collection, $entry['context']['collection']);
            if ($entry['purpose'] === 'sum') {
                $this->assertSame('score', $entry['context']['attribute']);
            }
        }

        $database->deleteCollection($collection);
    }

    public function testWithExplainWorksInsideTransactionOnPooledAdapter(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof Pool) {
            $this->markTestSkipped('Transaction-pinned adapter only matters when running through the Pool adapter.');
        }

        $database->getAuthorization()->addRole(Role::any()->toString());

        $collection = 'explain_txn_' . \uniqid();
        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]);
        $database->createAttribute($collection, 'title', Database::VAR_STRING, 32, true);
        $database->createDocument($collection, new Document([
            '$id' => ID::unique(),
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'one',
        ]));

        // Pin a connection by running inside a transaction, then capture
        // explain on the *same* pinned connection. Regression guard: this used
        // to silently return [] because the pinned-adapter early return
        // bypassed the start/stop/drain logic.
        $plan = $database->withExplain(function () use ($database, $collection) {
            return $database->withTransaction(fn () => $database->find($collection, [
                Query::limit(5),
            ]));
        });

        $entries = $plan->getAttribute('queries');
        $this->assertIsArray($entries);
        $this->assertNotEmpty($entries, 'pinned-adapter explain capture must reach the pool buffer');
        $this->assertSame('find', $entries[0]['purpose']);

        $database->deleteCollection($collection);
    }

    public function testNestedWithExplainThrows(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter() instanceof SQL) {
            $this->markTestSkipped('Explain capture is only wired in the SQL adapter today.');
        }

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessageMatches('/cannot be nested/i');

        $database->withExplain(function () use ($database): void {
            $database->withExplain(fn () => null);
        });

        // Even though the inner call threw, the outer scope's finally still
        // runs and clears the buffer — verified separately to keep this test
        // focused on the nesting guard itself.
    }
}
