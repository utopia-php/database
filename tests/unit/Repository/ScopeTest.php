<?php

namespace Tests\Unit\Repository;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Repository\CompositeSpecification;
use Utopia\Database\Repository\Repository;
use Utopia\Database\Repository\Scope;
use Utopia\Database\Repository\Specification;

class ScopedRepository extends Repository
{
    public function collection(): string
    {
        return 'products';
    }
}

class ActiveScope implements Scope
{
    public function apply(): array
    {
        return [Query::equal('active', [true])];
    }
}

class TenantScope implements Scope
{
    public function __construct(private string $tenantId)
    {
    }

    public function apply(): array
    {
        return [Query::equal('tenantId', [$this->tenantId])];
    }
}

class PriceSpec implements Specification
{
    public function __construct(private int $maxPrice)
    {
    }

    public function toQueries(): array
    {
        return [Query::lessThanEqual('price', $this->maxPrice)];
    }

    public function and(Specification $other): Specification
    {
        return new CompositeSpecification([$this, $other], 'and');
    }

    public function or(Specification $other): Specification
    {
        return new CompositeSpecification([$this, $other], 'or');
    }
}

class ScopeTest extends TestCase
{
    protected Database $db;

    protected ScopedRepository $repo;

    protected function setUp(): void
    {
        $this->db = $this->createMock(Database::class);
        $this->repo = new ScopedRepository($this->db);
    }

    public function testAddScopeAddsScope(): void
    {
        $scope = new ActiveScope();
        $this->repo->addScope($scope);

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    return count($queries) === 1
                        && $queries[0]->getAttribute() === 'active';
                })
            )
            ->willReturn([]);

        $this->repo->findAll();
    }

    public function testFindAllAppliesGlobalScopes(): void
    {
        $this->repo->addScope(new ActiveScope());

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    $attrs = array_map(fn (Query $q) => $q->getAttribute(), $queries);

                    return in_array('active', $attrs);
                })
            )
            ->willReturn([new Document(['$id' => 'p1'])]);

        $results = $this->repo->findAll();
        $this->assertCount(1, $results);
    }

    public function testFindOneByAppliesGlobalScopes(): void
    {
        $this->repo->addScope(new ActiveScope());

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    $attrs = array_map(fn (Query $q) => $q->getAttribute(), $queries);

                    return in_array('active', $attrs) && in_array('name', $attrs);
                })
            )
            ->willReturn([new Document(['$id' => 'p1', 'name' => 'Widget'])]);

        $result = $this->repo->findOneBy('name', 'Widget');
        $this->assertEquals('p1', $result->getId());
    }

    public function testCountAppliesGlobalScopes(): void
    {
        $this->repo->addScope(new ActiveScope());

        $this->db->expects($this->once())
            ->method('count')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    $attrs = array_map(fn (Query $q) => $q->getAttribute(), $queries);

                    return in_array('active', $attrs);
                })
            )
            ->willReturn(5);

        $this->assertEquals(5, $this->repo->count());
    }

    public function testWithoutScopesBypassesGlobalScopes(): void
    {
        $this->repo->addScope(new ActiveScope());

        $this->db->expects($this->once())
            ->method('find')
            ->with('products', [])
            ->willReturn([new Document(['$id' => 'p1']), new Document(['$id' => 'p2'])]);

        $results = $this->repo->withoutScopes();
        $this->assertCount(2, $results);
    }

    public function testClearScopesRemovesAllScopes(): void
    {
        $this->repo->addScope(new ActiveScope());
        $this->repo->addScope(new TenantScope('t1'));

        $this->repo->clearScopes();

        $this->db->expects($this->once())
            ->method('find')
            ->with('products', [])
            ->willReturn([]);

        $this->repo->findAll();
    }

    public function testMultipleScopesMergeQueries(): void
    {
        $this->repo->addScope(new ActiveScope());
        $this->repo->addScope(new TenantScope('t1'));

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    $attrs = array_map(fn (Query $q) => $q->getAttribute(), $queries);

                    return in_array('active', $attrs) && in_array('tenantId', $attrs);
                })
            )
            ->willReturn([]);

        $this->repo->findAll();
    }

    public function testMatchingCombinesScopesWithSpecification(): void
    {
        $this->repo->addScope(new ActiveScope());

        $spec = new PriceSpec(100);

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    $attrs = array_map(fn (Query $q) => $q->getAttribute(), $queries);

                    return in_array('active', $attrs) && in_array('price', $attrs);
                })
            )
            ->willReturn([]);

        $this->repo->matching($spec);
    }

    public function testScopesAppliedWithExplicitQueries(): void
    {
        $this->repo->addScope(new ActiveScope());

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    return count($queries) === 2;
                })
            )
            ->willReturn([]);

        $this->repo->findAll([Query::orderAsc('name')]);
    }

    public function testWithoutScopesPassesCustomQueries(): void
    {
        $this->repo->addScope(new ActiveScope());

        $customQueries = [Query::equal('category', ['electronics'])];

        $this->db->expects($this->once())
            ->method('find')
            ->with('products', $customQueries)
            ->willReturn([]);

        $this->repo->withoutScopes($customQueries);
    }

    public function testCountWithScopesAndExplicitQueries(): void
    {
        $this->repo->addScope(new TenantScope('t2'));

        $this->db->expects($this->once())
            ->method('count')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    return count($queries) === 2;
                })
            )
            ->willReturn(3);

        $this->assertEquals(3, $this->repo->count([Query::equal('status', ['published'])]));
    }

    public function testClearScopesThenAddNewScope(): void
    {
        $this->repo->addScope(new ActiveScope());
        $this->repo->clearScopes();
        $this->repo->addScope(new TenantScope('t3'));

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'products',
                $this->callback(function (array $queries) {
                    $attrs = array_map(fn (Query $q) => $q->getAttribute(), $queries);

                    return in_array('tenantId', $attrs) && ! in_array('active', $attrs);
                })
            )
            ->willReturn([]);

        $this->repo->findAll();
    }
}
