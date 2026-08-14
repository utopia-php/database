<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Hook\PermissionJoinFilter;
use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class PermissionJoinFilterTest extends TestCase
{
    public function testLeftJoinPlacesPermissionInOnClause(): void
    {
        $filter = $this->permissionFilter();
        $hook = new PermissionJoinFilter($filter, 'j0');

        $result = $hook->filterJoin('j0', JoinType::Left);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement);
        $this->assertStringContainsString('j0.'.Storage::UID, $result->condition->expression);
        $this->assertNull($hook->filterJoin('j1', JoinType::Left));
    }

    public function testInnerJoinPlacesPermissionInWhereClause(): void
    {
        $hook = new PermissionJoinFilter($this->permissionFilter(), 'j0');
        $result = $hook->filterJoin('j0', JoinType::Inner);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
    }

    public function testFullOuterJoinPlacesPermissionInOnClause(): void
    {
        $hook = new PermissionJoinFilter($this->permissionFilter(), 'j0');
        $result = $hook->filterJoin('j0', JoinType::FullOuter);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement);
    }

    private function permissionFilter(): PermissionFilter
    {
        return new PermissionFilter(
            roles: ['any'],
            permissionsTable: static fn (string $table): string => 'perms_'.$table,
            documentColumn: 'j0.'.Storage::UID,
        );
    }
}
