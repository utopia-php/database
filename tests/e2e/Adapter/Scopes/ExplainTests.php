<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\Document;
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
        $this->assertSame('sql', $entry['plan']['engine']);
        $this->assertArrayHasKey('tree', $entry['plan']);
        $this->assertNotNull($entry['plan']['tree']);

        $this->assertArrayHasKey('rowsScanned', $entry['plan']);
        $this->assertArrayHasKey('indexUsed', $entry['plan']);
        $this->assertArrayHasKey('estimatedCost', $entry['plan']);

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
}
