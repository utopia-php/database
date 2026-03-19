<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\QueryBuilder;

class QueryBuilderAdvancedTest extends TestCase
{
    private Database $db;

    protected function setUp(): void
    {
        $this->db = $this->createMock(Database::class);
    }

    public function testFilterAddsRawQueries(): void
    {
        $builder = new QueryBuilder($this->db, 'users');
        $rawQueries = [Query::equal('status', ['active']), Query::greaterThan('age', 18)];

        $queries = $builder->filter($rawQueries)->buildQueries();

        $this->assertCount(2, $queries);
        $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);
        $this->assertContains('equal', $methods);
        $this->assertContains('greaterThan', $methods);
    }

    public function testMultipleWhereClausesChain(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder
            ->where('status', 'active')
            ->where('role', 'admin')
            ->where('verified', true)
            ->buildQueries();

        $this->assertCount(3, $queries);
        $attributes = array_map(fn (Query $q) => $q->getAttribute(), $queries);
        $this->assertContains('status', $attributes);
        $this->assertContains('role', $attributes);
        $this->assertContains('verified', $attributes);
    }

    public function testWhereBetweenGeneratesBetweenQuery(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->whereBetween('age', 18, 65)->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('between', $queries[0]->getMethod()->value);
        $this->assertEquals('age', $queries[0]->getAttribute());
    }

    public function testWhereIsNullGeneratesIsNullQuery(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->whereIsNull('deleted_at')->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('isNull', $queries[0]->getMethod()->value);
        $this->assertEquals('deleted_at', $queries[0]->getAttribute());
    }

    public function testSearchGeneratesSearchQuery(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->search('content', 'hello world')->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('search', $queries[0]->getMethod()->value);
    }

    public function testGroupByGeneratesGroupByQueries(): void
    {
        $builder = new QueryBuilder($this->db, 'orders');

        $queries = $builder->groupBy(['status', 'region'])->buildQueries();

        $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);
        $this->assertContains('groupBy', $methods);
    }

    public function testHavingPassesThroughQueryObjects(): void
    {
        $havingQuery = Query::greaterThan('total', 100);
        $builder = new QueryBuilder($this->db, 'orders');

        $queries = $builder->having([$havingQuery])->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertSame($havingQuery, $queries[0]);
    }

    public function testSumDelegatesToDbSum(): void
    {
        $this->db->expects($this->once())
            ->method('sum')
            ->with('orders', 'amount', $this->isType('array'))
            ->willReturn(1500.50);

        $builder = new QueryBuilder($this->db, 'orders');
        $result = $builder->where('status', 'paid')->sum('amount');

        $this->assertEquals(1500.50, $result);
    }

    public function testOrderDescGeneratesOrderDescQueries(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->orderDesc('created_at')->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('orderDesc', $queries[0]->getMethod()->value);
    }

    public function testCursorYieldsDocumentsFromMultipleBatches(): void
    {
        $batch1 = [
            new Document(['$id' => 'd1']),
            new Document(['$id' => 'd2']),
        ];
        $batch2 = [
            new Document(['$id' => 'd3']),
        ];

        $this->db->expects($this->exactly(2))
            ->method('find')
            ->willReturnOnConsecutiveCalls($batch1, $batch2);

        $builder = new QueryBuilder($this->db, 'users');
        $collected = [];
        foreach ($builder->cursor(2) as $doc) {
            $collected[] = $doc->getId();
        }

        $this->assertEquals(['d1', 'd2', 'd3'], $collected);
    }

    public function testCursorStopsWhenBatchIsSmallerThanBatchSize(): void
    {
        $batch = [new Document(['$id' => 'd1'])];

        $this->db->expects($this->once())
            ->method('find')
            ->willReturn($batch);

        $builder = new QueryBuilder($this->db, 'users');
        $collected = [];
        foreach ($builder->cursor(10) as $doc) {
            $collected[] = $doc->getId();
        }

        $this->assertEquals(['d1'], $collected);
    }

    public function testCursorWithEmptyFirstBatchYieldsNothing(): void
    {
        $this->db->expects($this->once())
            ->method('find')
            ->willReturn([]);

        $builder = new QueryBuilder($this->db, 'users');
        $collected = [];
        foreach ($builder->cursor(10) as $doc) {
            $collected[] = $doc;
        }

        $this->assertEmpty($collected);
    }

    public function testCursorUsesCursorAfterForPagination(): void
    {
        $batch1 = [
            new Document(['$id' => 'd1']),
            new Document(['$id' => 'd2']),
        ];
        $batch2 = [];

        $calls = [];
        $this->db->method('find')
            ->willReturnCallback(function (string $collection, array $queries) use (&$calls, $batch1, $batch2) {
                $calls[] = $queries;

                return count($calls) === 1 ? $batch1 : $batch2;
            });

        $builder = new QueryBuilder($this->db, 'users');
        $collected = [];
        foreach ($builder->cursor(2) as $doc) {
            $collected[] = $doc->getId();
        }

        $this->assertCount(2, $calls);
        $secondCallMethods = array_map(fn (Query $q) => $q->getMethod()->value, $calls[1]);
        $this->assertContains('cursorAfter', $secondCallMethods);
    }

    public function testBuildQueriesIncludesAllConfiguredOptions(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder
            ->where('status', 'active')
            ->select(['name', 'email'])
            ->limit(10)
            ->offset(20)
            ->orderAsc('name')
            ->orderDesc('created_at')
            ->buildQueries();

        $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);
        $this->assertContains('equal', $methods);
        $this->assertContains('select', $methods);
        $this->assertContains('limit', $methods);
        $this->assertContains('offset', $methods);
        $this->assertContains('orderAsc', $methods);
        $this->assertContains('orderDesc', $methods);
    }

    public function testBuildQueriesWithNoConfigurationReturnsEmptyArray(): void
    {
        $builder = new QueryBuilder($this->db, 'users');
        $queries = $builder->buildQueries();
        $this->assertEmpty($queries);
    }

    public function testFilterMergesWithExistingFilters(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder
            ->where('status', 'active')
            ->filter([Query::greaterThan('age', 18)])
            ->buildQueries();

        $this->assertCount(2, $queries);
    }

    public function testWhereNotGeneratesNotEqualQuery(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->whereNot('status', 'banned')->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('notEqual', $queries[0]->getMethod()->value);
    }

    public function testWhereContainsGeneratesContainsQuery(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->whereContains('tags', 'php')->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('containsAny', $queries[0]->getMethod()->value);
    }

    public function testWhereIsNotNullGeneratesIsNotNullQuery(): void
    {
        $builder = new QueryBuilder($this->db, 'users');

        $queries = $builder->whereIsNotNull('email')->buildQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('isNotNull', $queries[0]->getMethod()->value);
    }

    public function testCursorWithOrderPreservesOrder(): void
    {
        $batch = [new Document(['$id' => 'd1'])];

        $this->db->method('find')
            ->willReturnCallback(function (string $collection, array $queries) use ($batch) {
                $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);
                $this->assertContains('orderAsc', $methods);

                return $batch;
            });

        $builder = new QueryBuilder($this->db, 'users');
        $builder->orderAsc('name');
        foreach ($builder->cursor(10) as $doc) {
            // just iterate
        }
    }
}
