<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\QueryBuilder;

class QueryBuilderTest extends TestCase
{
    public function testBuildQueriesBasic(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $builder = new QueryBuilder($db, 'users');

        $queries = $builder
            ->where('status', 'active')
            ->limit(10)
            ->offset(5)
            ->orderAsc('name')
            ->buildQueries();

        $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);

        $this->assertContains('equal', $methods);
        $this->assertContains('limit', $methods);
        $this->assertContains('offset', $methods);
        $this->assertContains('orderAsc', $methods);
    }

    public function testBuildQueriesWithSelect(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $builder = new QueryBuilder($db, 'users');

        $queries = $builder
            ->select(['name', 'email'])
            ->buildQueries();

        $selectQuery = null;
        foreach ($queries as $q) {
            if ($q->getMethod()->value === 'select') {
                $selectQuery = $q;
            }
        }

        $this->assertNotNull($selectQuery);
        $this->assertEquals(['name', 'email'], $selectQuery->getValues());
    }

    public function testBuildQueriesWithFilters(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $builder = new QueryBuilder($db, 'users');

        $queries = $builder
            ->whereGreaterThan('age', 18)
            ->whereLessThan('age', 65)
            ->whereIsNotNull('email')
            ->buildQueries();

        $this->assertCount(3, $queries);
    }

    public function testGetDelegatesToFind(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $db->expects($this->once())
            ->method('find')
            ->with('users', $this->isType('array'))
            ->willReturn([]);

        $builder = new QueryBuilder($db, 'users');
        $result = $builder->where('active', true)->get();

        $this->assertEquals([], $result);
    }

    public function testCountDelegatesToCount(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $db->expects($this->once())
            ->method('count')
            ->with('users', $this->isType('array'))
            ->willReturn(42);

        $builder = new QueryBuilder($db, 'users');
        $result = $builder->where('active', true)->count();

        $this->assertEquals(42, $result);
    }

    public function testFirstReturnsFirstResult(): void
    {
        $doc = new \Utopia\Database\Document(['$id' => 'first']);

        $db = $this->createMock(\Utopia\Database\Database::class);
        $db->expects($this->once())
            ->method('find')
            ->willReturn([$doc]);

        $builder = new QueryBuilder($db, 'users');
        $result = $builder->first();

        $this->assertEquals('first', $result->getId());
    }

    public function testFirstReturnsEmptyDocumentWhenNoResults(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $db->expects($this->once())
            ->method('find')
            ->willReturn([]);

        $builder = new QueryBuilder($db, 'users');
        $result = $builder->first();

        $this->assertTrue($result->isEmpty());
    }

    public function testChainableInterface(): void
    {
        $db = $this->createMock(\Utopia\Database\Database::class);
        $builder = new QueryBuilder($db, 'users');

        $result = $builder
            ->where('a', 1)
            ->whereNot('b', 2)
            ->whereGreaterThan('c', 3)
            ->whereLessThan('d', 4)
            ->whereBetween('e', 1, 10)
            ->whereContains('f', 'val')
            ->whereIsNull('g')
            ->whereIsNotNull('h')
            ->search('i', 'query')
            ->select(['a', 'b'])
            ->limit(10)
            ->offset(0)
            ->orderAsc('a')
            ->orderDesc('b')
            ->groupBy(['c'])
            ->eagerLoad(['rel1']);

        $this->assertInstanceOf(QueryBuilder::class, $result);
    }
}
