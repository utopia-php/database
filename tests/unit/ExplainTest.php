<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

class ExplainTest extends TestCase
{
    public function test_with_explain_toggles_capture_and_wraps_result(): void
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
                    'plan' => [
                        'engine' => 'sql',
                        'rowsScanned' => 25,
                        'indexUsed' => '_idx_status',
                        'estimatedCost' => 4.5,
                        'tree' => null,
                    ],
                ],
            ]);

        $db = new Database($adapter, new Cache(new None));

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

    public function test_stop_explain_capture_is_called_even_when_callback_throws(): void
    {
        $adapter = $this->createMock(Adapter::class);

        $adapter->expects($this->once())->method('startExplainCapture');
        $adapter->expects($this->once())->method('stopExplainCapture')->willReturn([]);

        $db = new Database($adapter, new Cache(new None));

        $this->expectException(\RuntimeException::class);
        $db->withExplain(function () {
            throw new \RuntimeException('boom');
        });
    }

    public function test_sanitize_plan_hides_internal_tables_and_renames_columns(): void
    {
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getDatabase')->willReturn('appwrite');
        $sanitize = (new \ReflectionMethod(Adapter::class, 'sanitizePlan'))
            ->getClosure($adapter);

        $sanitized = $sanitize([
            'table_name' => 'project_1_collection_5_perms',
            'meta_table' => 'appwrite__metadata',
            'projection' => ['_uid', '_createdAt', '_updatedAt', '_permissions', 'title'],
            'tenant_col' => '_tenant',
            // The perms/metadata tables also appear embedded in condition strings.
            'attached_condition' => "_45_abc123_perms._permission = 'read' and project_1__metadata._uid = '1'",
            // SQL plans qualify columns with the schema name.
            'index_condition' => "(`appwrite`.`main`.`status` = 'published')",
            'nested' => [
                'inner_table' => 'project_1__metadata',
                'cols' => ['_uid', '_id'],
            ],
        ]);

        $this->assertSame('<permissionCheck>', $sanitized['table_name']);
        $this->assertSame('<metadata>', $sanitized['meta_table']);
        $this->assertSame('<metadata>', $sanitized['nested']['inner_table']);

        $this->assertSame(['$id', '$createdAt', '$updatedAt', '$permissions', 'title'], $sanitized['projection']);
        $this->assertSame('$tenant', $sanitized['tenant_col']);

        // Embedded physical table tokens inside a condition string are rewritten too.
        $this->assertStringNotContainsString('_perms', $sanitized['attached_condition']);
        $this->assertStringNotContainsString('__metadata', $sanitized['attached_condition']);

        // The internal schema name is stripped from qualified column references.
        $this->assertStringNotContainsString('`appwrite`', $sanitized['index_condition']);
        $this->assertSame("(`main`.`status` = 'published')", $sanitized['index_condition']);

        // _id is MySQL's auto-increment column; not renamed.
        $this->assertSame(['$id', '_id'], $sanitized['nested']['cols']);
    }

    public function test_record_plan_actuals_fills_last_entry(): void
    {
        $adapter = $this->getMockBuilder(Adapter::class)
            ->onlyMethods([])
            ->getMockForAbstractClass();

        $buffer = new \ReflectionProperty(Adapter::class, 'explainBuffer');
        $buffer->setValue($adapter, [[
            'purpose' => 'find',
            'context' => ['collection' => 'movies'],
            'plan' => ['engine' => 'mariadb', 'rowsScanned' => 10],
        ]]);

        (new \ReflectionMethod(Adapter::class, 'recordPlanActuals'))
            ->invoke($adapter, 7, 1.5);

        $captured = $buffer->getValue($adapter);
        $this->assertSame(7, $captured[0]['plan']['rowsReturned']);
        $this->assertSame(1.5, $captured[0]['plan']['executionTime']);
    }

    public function test_record_plan_actuals_ignores_failed_plans(): void
    {
        $adapter = $this->getMockForAbstractClass(Adapter::class);

        $buffer = new \ReflectionProperty(Adapter::class, 'explainBuffer');
        // A failed EXPLAIN is stored as ['error' => ...]; actuals must not be
        // grafted onto an error entry as if it were a real plan.
        $buffer->setValue($adapter, [[
            'purpose' => 'find',
            'context' => [],
            'plan' => ['error' => 'boom'],
        ]]);

        (new \ReflectionMethod(Adapter::class, 'recordPlanActuals'))
            ->invoke($adapter, 7, 1.5);

        $captured = $buffer->getValue($adapter);
        $this->assertArrayNotHasKey('rowsReturned', $captured[0]['plan']);
        $this->assertSame(['error' => 'boom'], $captured[0]['plan']);
    }

    public function test_record_plan_actuals_noop_when_not_capturing(): void
    {
        $adapter = $this->getMockForAbstractClass(Adapter::class);

        // explainBuffer defaults to null (not capturing) — must be a safe no-op.
        (new \ReflectionMethod(Adapter::class, 'recordPlanActuals'))
            ->invoke($adapter, 7, 1.5);

        $buffer = new \ReflectionProperty(Adapter::class, 'explainBuffer');
        $this->assertNull($buffer->getValue($adapter));
    }

    public function test_postgres_extraction_recurses_into_child_plans(): void
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

    public function test_postgres_extraction_falls_back_to_root_rows_without_scan(): void
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

    public function test_pool_does_not_restart_capture_on_already_capturing_pinned_adapter(): void
    {
        // Reproduces the re-entrancy bug: while the pool is capturing, a nested
        // delegate() on the pinned (transaction) adapter — e.g. a before(find)
        // transformation issuing its own query — must NOT call
        // startExplainCapture() again, which would throw "cannot be nested".
        $inner = $this->getMockBuilder(Adapter::class)
            ->onlyMethods(['startExplainCapture', 'isExplainCapturing', 'ping'])
            ->getMockForAbstractClass();

        // The inner adapter is already mid-capture from the outer find().
        $inner->method('isExplainCapturing')->willReturn(true);
        $inner->method('ping')->willReturn(true);
        // The guard must prevent any re-start on the already-capturing adapter.
        $inner->expects($this->never())->method('startExplainCapture');

        $pool = (new \ReflectionClass(Pool::class))->newInstanceWithoutConstructor();

        // Pin the inner adapter (as withTransaction does) and turn on pool-level
        // capture so delegate() takes the capturing branch.
        (new \ReflectionProperty(Pool::class, 'pinnedAdapter'))->setValue($pool, $inner);
        (new \ReflectionProperty(Adapter::class, 'explainBuffer'))->setValue($pool, []);

        $result = $pool->delegate('ping', []);

        $this->assertTrue($result);
    }

    public function test_pool_clears_stale_timeout_on_borrowed_adapters(): void
    {
        $inner = $this->getMockBuilder(Adapter::class)
            ->onlyMethods(['clearTimeout', 'ping'])
            ->getMockForAbstractClass();

        $inner
            ->expects($this->once())
            ->method('clearTimeout')
            ->with(Database::EVENT_ALL);
        $inner->method('ping')->willReturn(true);

        $pool = new UtopiaPool(new Stack, 'mock', 1, fn () => $inner);
        $adapter = new Pool($pool);
        $adapter->setAuthorization(new Authorization);

        $this->assertTrue($adapter->ping());
    }

    public function test_pool_reapplies_timeout_across_delegated_calls(): void
    {
        // Regression: after setTimeout() on the Pool, a subsequent delegated call
        // (e.g. find) must re-apply the timeout to whatever adapter is borrowed
        // — not clear it because the Pool itself forgot the timeout was set.
        $inner = $this->getMockBuilder(Adapter::class)
            ->onlyMethods(['setTimeout', 'clearTimeout', 'ping'])
            ->getMockForAbstractClass();

        // Every setTimeout invocation must carry the live timeout, never clear it.
        $inner
            ->method('setTimeout')
            ->with(1000, Database::EVENT_ALL);
        // clearTimeout must NOT be called while the Pool still has a positive timeout.
        $inner->expects($this->never())->method('clearTimeout');
        $inner->method('ping')->willReturn(true);

        $pool = new UtopiaPool(new Stack, 'mock', 1, fn () => $inner);
        $adapter = new Pool($pool);
        $adapter->setAuthorization(new Authorization);

        $adapter->setTimeout(1000);
        $this->assertSame(1000, $adapter->getTimeout());
        $this->assertTrue($adapter->ping());
    }

    public function test_pool_reapplies_timeout_event_across_delegated_calls(): void
    {
        // Regression: Pool must remember the event too. Replaying a
        // document_find timeout as EVENT_ALL makes unrelated schema/metadata
        // operations pay the timeout on later borrowed adapters.
        $inner = $this->getMockBuilder(Adapter::class)
            ->onlyMethods(['setTimeout', 'clearTimeout', 'ping'])
            ->getMockForAbstractClass();

        $inner
            ->expects($this->exactly(3))
            ->method('setTimeout')
            ->with(1000, Database::EVENT_DOCUMENT_FIND);
        $inner->expects($this->never())->method('clearTimeout');
        $inner->method('ping')->willReturn(true);

        $pool = new UtopiaPool(new Stack, 'mock', 1, fn () => $inner);
        $adapter = new Pool($pool);
        $adapter->setAuthorization(new Authorization);

        $adapter->setTimeout(1000, Database::EVENT_DOCUMENT_FIND);
        $this->assertTrue($adapter->ping());
    }
}
