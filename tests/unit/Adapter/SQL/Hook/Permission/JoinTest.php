<?php

namespace Tests\Unit\Adapter\SQL\Hook\Permission;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQL\Hook\Permission\Filter;
use Utopia\Database\Adapter\SQL\Hook\Permission\Join;
use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class JoinTest extends TestCase
{
    public function testLeftJoinPlacesPermissionInOnClause(): void
    {
        $filter = $this->permissionFilter();
        $hook = new Join($filter, 'j0');

        $result = $hook->filterJoin('j0', JoinType::Left);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement);
        $this->assertStringContainsString('`j0`.`'.Storage::UID.'`', $result->condition->expression);
        $this->assertNull($hook->filterJoin('j1', JoinType::Left));
    }

    public function testInnerJoinPlacesPermissionInOnClause(): void
    {
        $hook = new Join($this->permissionFilter(), 'j0');
        $result = $hook->filterJoin('j0', JoinType::Inner);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement);
    }

    public function testRightJoinPlacesPermissionInWhereClause(): void
    {
        $hook = new Join($this->permissionFilter(), 'j0');
        $result = $hook->filterJoin('j0', JoinType::Right);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertStringContainsString('`j0`.`'.Storage::UID.'`', $result->condition->expression);
        $this->assertStringNotContainsString('IS NULL', $result->condition->expression);
    }

    public function testFullOuterJoinPlacesPermissionInWhereClauseAndAllowsNullUid(): void
    {
        $hook = new Join($this->permissionFilter(), 'j0');
        $result = $hook->filterJoin('j0', JoinType::FullOuter);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertStringContainsString('`j0`.`'.Storage::UID.'`', $result->condition->expression);
        $this->assertStringContainsString('IS NULL', $result->condition->expression);
        $this->assertSame($this->permissionFilter()->filter('j0')->bindings, $result->condition->bindings);
    }

    public function testCrossJoinPlacesPermissionInWhereClause(): void
    {
        $hook = new Join($this->permissionFilter(), 'j0');
        $result = $hook->filterJoin('j0', JoinType::Cross);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertStringNotContainsString('IS NULL', $result->condition->expression);
    }

    private function permissionFilter(): Filter
    {
        return new Filter(
            roles: ['any'],
            permissionsTable: static fn (string $table): string => 'perms_'.$table,
            documentColumn: 'j0.'.Storage::UID,
        );
    }
}
