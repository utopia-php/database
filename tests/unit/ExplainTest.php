<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\Document;

class ExplainTest extends TestCase
{
    public function testWithExplainTogglesCaptureAndWrapsResult(): void
    {
        $adapter = $this->createMock(Adapter::class);

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

        // The callback result must propagate; the plan comes via the out-param.
        $plan = null;
        $result = $db->withExplain(fn () => 'callback-result', $plan);

        $this->assertSame('callback-result', $result);
        $this->assertInstanceOf(Document::class, $plan);

        $entries = $plan->getAttribute('queries');
        $this->assertIsArray($entries);
        $this->assertCount(1, $entries);
        $this->assertSame('find', $entries[0]['purpose']);
        $this->assertSame('movies', $entries[0]['context']['collection']);
        $this->assertSame(25, $entries[0]['plan']['rowsScanned']);
    }

    public function testStopExplainCaptureIsCalledEvenWhenCallbackThrows(): void
    {
        $adapter = $this->createMock(Adapter::class);

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

        $this->assertSame('<permissionCheck>', $sanitized['table_name']);
        $this->assertSame('<metadata>', $sanitized['meta_table']);
        $this->assertSame('<metadata>', $sanitized['nested']['inner_table']);

        $this->assertSame(['$id', '$createdAt', '$updatedAt', '$permissions', 'title'], $sanitized['projection']);
        $this->assertSame('$tenant', $sanitized['tenant_col']);

        // _id is MySQL's auto-increment column; not renamed.
        $this->assertSame(['$id', '_id'], $sanitized['nested']['cols']);
    }

    public function testRecordPlanActualsFillsLastEntry(): void
    {
        $adapter = $this->getMockBuilder(Adapter::class)
            ->onlyMethods([])
            ->getMockForAbstractClass();

        $buffer = new \ReflectionProperty(Adapter::class, 'explainBuffer');
        $buffer->setValue($adapter, [[
            'purpose' => 'find',
            'context' => ['collection' => 'movies'],
            'plan'    => ['engine' => 'mariadb', 'rowsScanned' => 10],
        ]]);

        (new \ReflectionMethod(Adapter::class, 'recordPlanActuals'))
            ->invoke($adapter, 7, 1.5);

        $captured = $buffer->getValue($adapter);
        $this->assertSame(7, $captured[0]['plan']['rowsReturned']);
        $this->assertSame(1.5, $captured[0]['plan']['executionTime']);
    }

    public function testRecordPlanActualsIgnoresFailedPlans(): void
    {
        $adapter = $this->getMockForAbstractClass(Adapter::class);

        $buffer = new \ReflectionProperty(Adapter::class, 'explainBuffer');
        // A failed EXPLAIN is stored as ['error' => ...]; actuals must not be
        // grafted onto an error entry as if it were a real plan.
        $buffer->setValue($adapter, [[
            'purpose' => 'find',
            'context' => [],
            'plan'    => ['error' => 'boom'],
        ]]);

        (new \ReflectionMethod(Adapter::class, 'recordPlanActuals'))
            ->invoke($adapter, 7, 1.5);

        $captured = $buffer->getValue($adapter);
        $this->assertArrayNotHasKey('rowsReturned', $captured[0]['plan']);
        $this->assertSame(['error' => 'boom'], $captured[0]['plan']);
    }

    public function testRecordPlanActualsNoopWhenNotCapturing(): void
    {
        $adapter = $this->getMockForAbstractClass(Adapter::class);

        // explainBuffer defaults to null (not capturing) — must be a safe no-op.
        (new \ReflectionMethod(Adapter::class, 'recordPlanActuals'))
            ->invoke($adapter, 7, 1.5);

        $buffer = new \ReflectionProperty(Adapter::class, 'explainBuffer');
        $this->assertNull($buffer->getValue($adapter));
    }

    public function testPostgresExtractionRecursesIntoChildPlans(): void
    {
        // Canonical pgvector plan: Limit -> Index Scan. The index name and the
        // scanned rows live on the CHILD; the root Limit only knows `k`.
        $tree = [
            'Plan' => [
                'Node Type' => 'Limit',
                'Plan Rows' => 5,
                'Total Cost' => 42.7,
                'Plans' => [
                    [
                        'Node Type' => 'Index Scan',
                        'Index Name' => 'movies_embedding_hnsw',
                        'Plan Rows' => 950,
                        'Total Cost' => 41.0,
                    ],
                ],
            ],
        ];

        $adapter = (new \ReflectionClass(Postgres::class))->newInstanceWithoutConstructor();
        $root = $tree['Plan'];

        $rows = (new \ReflectionMethod(Postgres::class, 'extractPgPlanRows'))->invoke($adapter, $root);
        $index = (new \ReflectionMethod(Postgres::class, 'extractPgIndexUsed'))->invoke($adapter, $root);

        // rowsScanned comes from the scan leaf (950), NOT the Limit's 5.
        $this->assertSame(950, $rows);
        $this->assertSame('movies_embedding_hnsw', $index);
    }

    public function testPostgresExtractionFallsBackToRootRowsWithoutScan(): void
    {
        $adapter = (new \ReflectionClass(Postgres::class))->newInstanceWithoutConstructor();
        $root = [
            'Node Type' => 'Result',
            'Plan Rows' => 1,
        ];

        $rows = (new \ReflectionMethod(Postgres::class, 'extractPgPlanRows'))->invoke($adapter, $root);
        $index = (new \ReflectionMethod(Postgres::class, 'extractPgIndexUsed'))->invoke($adapter, $root);

        $this->assertSame(1, $rows);
        $this->assertNull($index);
    }
}
