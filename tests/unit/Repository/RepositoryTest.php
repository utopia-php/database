<?php

namespace Tests\Unit\Repository;

use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Repository\CompositeSpecification;
use Utopia\Database\Repository\Repository;
use Utopia\Database\Repository\Specification;

class TestRepository extends Repository
{
    public function collection(): string
    {
        return 'users';
    }
}

class ActiveSpecification implements Specification
{
    public function toQueries(): array
    {
        return [Query::equal('status', ['active'])];
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

class AdminSpecification implements Specification
{
    public function toQueries(): array
    {
        return [Query::equal('role', ['admin'])];
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

#[AllowMockObjectsWithoutExpectations]
class RepositoryTest extends TestCase
{
    private Database $db;

    private TestRepository $repo;

    protected function setUp(): void
    {
        $this->db = $this->createMock(Database::class);
        $this->repo = new TestRepository($this->db);
    }

    public function testFindByIdDelegatesToGetDocument(): void
    {
        $doc = new Document(['$id' => 'u1', 'name' => 'Alice']);

        $this->db->expects($this->once())
            ->method('getDocument')
            ->with('users', 'u1')
            ->willReturn($doc);

        $result = $this->repo->findById('u1');
        $this->assertSame($doc, $result);
    }

    public function testFindAllDelegatesToFind(): void
    {
        $docs = [new Document(['$id' => 'u1']), new Document(['$id' => 'u2'])];

        $this->db->expects($this->once())
            ->method('find')
            ->with('users', [])
            ->willReturn($docs);

        $result = $this->repo->findAll();
        $this->assertCount(2, $result);
    }

    public function testFindAllWithQueriesPassesThem(): void
    {
        $queries = [Query::equal('status', ['active'])];

        $this->db->expects($this->once())
            ->method('find')
            ->with('users', $queries)
            ->willReturn([]);

        $this->repo->findAll($queries);
    }

    public function testFindOneByCreatesEqualQueryWithLimit1(): void
    {
        $doc = new Document(['$id' => 'u1', 'email' => 'alice@test.com']);

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'users',
                $this->callback(function (array $queries) {
                    $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);

                    return in_array('equal', $methods) && in_array('limit', $methods);
                })
            )
            ->willReturn([$doc]);

        $result = $this->repo->findOneBy('email', 'alice@test.com');
        $this->assertEquals('u1', $result->getId());
    }

    public function testFindOneByReturnsEmptyDocumentWhenNoResults(): void
    {
        $this->db->method('find')->willReturn([]);

        $result = $this->repo->findOneBy('email', 'nonexistent@test.com');
        $this->assertTrue($result->isEmpty());
    }

    public function testCountDelegatesToCount(): void
    {
        $this->db->expects($this->once())
            ->method('count')
            ->with('users', [])
            ->willReturn(42);

        $this->assertEquals(42, $this->repo->count());
    }

    public function testCountWithQueries(): void
    {
        $queries = [Query::equal('status', ['active'])];

        $this->db->expects($this->once())
            ->method('count')
            ->with('users', $queries)
            ->willReturn(10);

        $this->assertEquals(10, $this->repo->count($queries));
    }

    public function testCreateDelegatesToCreateDocument(): void
    {
        $doc = new Document(['name' => 'Alice']);
        $created = new Document(['$id' => 'u1', 'name' => 'Alice']);

        $this->db->expects($this->once())
            ->method('createDocument')
            ->with('users', $doc)
            ->willReturn($created);

        $result = $this->repo->create($doc);
        $this->assertEquals('u1', $result->getId());
    }

    public function testUpdateDelegatesToUpdateDocument(): void
    {
        $doc = new Document(['$id' => 'u1', 'name' => 'Bob']);

        $this->db->expects($this->once())
            ->method('updateDocument')
            ->with('users', 'u1', $doc)
            ->willReturn($doc);

        $result = $this->repo->update('u1', $doc);
        $this->assertEquals('Bob', $result->getAttribute('name'));
    }

    public function testDeleteDelegatesToDeleteDocument(): void
    {
        $this->db->expects($this->once())
            ->method('deleteDocument')
            ->with('users', 'u1')
            ->willReturn(true);

        $this->assertTrue($this->repo->delete('u1'));
    }

    public function testMatchingAppliesSpecificationQueries(): void
    {
        $spec = new ActiveSpecification();

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'users',
                $this->callback(function (array $queries) {
                    return count($queries) === 1
                        && $queries[0]->getMethod()->value === 'equal';
                })
            )
            ->willReturn([]);

        $this->repo->matching($spec);
    }

    public function testCompositeSpecificationAndMergesQueries(): void
    {
        $activeSpec = new ActiveSpecification();
        $adminSpec = new AdminSpecification();

        $composite = $activeSpec->and($adminSpec);
        $queries = $composite->toQueries();

        $this->assertCount(2, $queries);
        $attributes = array_map(fn (Query $q) => $q->getAttribute(), $queries);
        $this->assertContains('status', $attributes);
        $this->assertContains('role', $attributes);
    }

    public function testCompositeSpecificationOrCreatesOrQueries(): void
    {
        $activeSpec = new ActiveSpecification();
        $adminSpec = new AdminSpecification();

        $composite = $activeSpec->or($adminSpec);
        $queries = $composite->toQueries();

        $this->assertNotEmpty($queries);
        $methods = array_map(fn (Query $q) => $q->getMethod()->value, $queries);
        $this->assertContains('or', $methods);
    }

    public function testSpecificationAndCreatesComposite(): void
    {
        $spec1 = new ActiveSpecification();
        $spec2 = new AdminSpecification();

        $composite = $spec1->and($spec2);
        $this->assertInstanceOf(Specification::class, $composite);
        $this->assertCount(2, $composite->toQueries());
    }

    public function testSpecificationOrCreatesComposite(): void
    {
        $spec1 = new ActiveSpecification();
        $spec2 = new AdminSpecification();

        $composite = $spec1->or($spec2);
        $this->assertInstanceOf(Specification::class, $composite);
    }

    public function testCustomSpecificationImplementingInterface(): void
    {
        $spec = new ActiveSpecification();
        $queries = $spec->toQueries();

        $this->assertCount(1, $queries);
        $this->assertEquals('status', $queries[0]->getAttribute());
    }

    public function testMatchingWithBaseQueriesMergesBoth(): void
    {
        $spec = new ActiveSpecification();
        $baseQueries = [Query::orderAsc('name')];

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'users',
                $this->callback(function (array $queries) {
                    return count($queries) === 2;
                })
            )
            ->willReturn([]);

        $this->repo->matching($spec, $baseQueries);
    }

    public function testFindOneByHandlesArrayValue(): void
    {
        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'users',
                $this->callback(function (array $queries) {
                    return $queries[0]->getValues() === ['admin', 'editor'];
                })
            )
            ->willReturn([]);

        $this->repo->findOneBy('role', ['admin', 'editor']);
    }

    public function testCompositeSpecificationAndCanChainFurther(): void
    {
        $spec1 = new ActiveSpecification();
        $spec2 = new AdminSpecification();
        $spec3 = new ActiveSpecification();

        $composite = $spec1->and($spec2)->and($spec3);
        $queries = $composite->toQueries();

        $this->assertGreaterThanOrEqual(3, count($queries));
    }

    public function testCompositeSpecificationOrCanChainFurther(): void
    {
        $spec1 = new ActiveSpecification();
        $spec2 = new AdminSpecification();
        $spec3 = new ActiveSpecification();

        $composite = $spec1->or($spec2)->or($spec3);
        $queries = $composite->toQueries();

        $this->assertNotEmpty($queries);
    }
}
