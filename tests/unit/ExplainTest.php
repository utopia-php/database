<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;

/**
 * Tests for the in-process query-plan capture scope (Database::withExplain)
 * and the Adapter-level sanitizer that strips internal storage details.
 *
 * Covers behavior — not specific vendor plan parsing (that lives in
 * SQL::explainSQL / Mongo::explainSQL and is exercised by the e2e adapter
 * tests against real backends).
 */
class ExplainTest extends TestCase
{
    public function testWithExplainTogglesCaptureAndWrapsResult(): void
    {
        $adapter = $this->createMock(Adapter::class);

        // start must be called once before the callback, stop once after.
        $adapter->expects($this->once())->method('startExplainCapture');
        $adapter
            ->expects($this->once())
            ->method('stopExplainCapture')
            ->willReturn([
                [
                    'purpose' => 'find',
                    'context' => ['collection' => 'movies'],
                    'plan'    => [
                        'engine'        => 'sql',
                        'rowsScanned'   => 25,
                        'indexUsed'     => '_idx_status',
                        'estimatedCost' => 4.5,
                        'tree'          => null,
                    ],
                ],
            ]);

        $db = new Database($adapter, new Cache(new None()));

        $callbackRan = false;
        $result = $db->withExplain(function () use (&$callbackRan) {
            $callbackRan = true;
        });

        $this->assertTrue($callbackRan, 'callback must run inside the capture scope');
        $this->assertInstanceOf(Document::class, $result);

        $entries = $result->getAttribute('queries');
        $this->assertIsArray($entries);
        $this->assertCount(1, $entries);
        $this->assertSame('find', $entries[0]['purpose']);
        $this->assertSame('movies', $entries[0]['context']['collection']);
        $this->assertSame(25, $entries[0]['plan']['rowsScanned']);
    }

    public function testStopExplainCaptureIsCalledEvenWhenCallbackThrows(): void
    {
        $adapter = $this->createMock(Adapter::class);

        // The point: stop must run via finally even on exception.
        $adapter->expects($this->once())->method('startExplainCapture');
        $adapter->expects($this->once())->method('stopExplainCapture')->willReturn([]);

        $db = new Database($adapter, new Cache(new None()));

        $this->expectException(\RuntimeException::class);
        $db->withExplain(function () {
            throw new \RuntimeException('boom');
        });
    }

    public function testSanitizePlanHidesInternalTablesAndRenamesColumns(): void
    {
        // The sanitizer is a protected method on Adapter; use a mock + reflection
        // so we don't have to satisfy the (huge) abstract surface manually.
        $adapter = $this->createMock(Adapter::class);
        $sanitize = (new \ReflectionMethod(Adapter::class, 'sanitizePlan'))
            ->getClosure($adapter);

        $sanitized = $sanitize([
            'table_name'   => 'project_1_collection_5_perms',
            'meta_table'   => 'appwrite__metadata',
            'projection'   => ['_uid', '_createdAt', '_updatedAt', '_permissions', 'title'],
            'tenant_col'   => '_tenant',
            'nested'       => [
                'inner_table' => 'project_1__metadata',
                'cols'        => ['_uid', '_id'],
            ],
        ]);

        // Internal companion tables redacted to opaque markers.
        $this->assertSame('<permissionCheck>', $sanitized['table_name']);
        $this->assertSame('<metadata>', $sanitized['meta_table']);
        $this->assertSame('<metadata>', $sanitized['nested']['inner_table']);

        // Internal column names translated to user-facing form.
        $this->assertSame(['$id', '$createdAt', '$updatedAt', '$permissions', 'title'], $sanitized['projection']);
        $this->assertSame('$tenant', $sanitized['tenant_col']);

        // _id intentionally NOT renamed — it's MySQL's auto-increment column reused
        // throughout EXPLAIN output; translating it would muddle the plan.
        $this->assertSame(['$id', '_id'], $sanitized['nested']['cols']);
    }
}
