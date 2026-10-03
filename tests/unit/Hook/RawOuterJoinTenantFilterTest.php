<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\RawOuterJoinTenantFilter;
use Utopia\Database\Hook\RawTenantFilter;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class RawOuterJoinTenantFilterTest extends TestCase
{
    public function testOnlyRightAndFullOuterJoinsNeedConditionsInTheirOwnOn(): void
    {
        $tenants = new RawTenantFilter(7, 'ns_authors', false, '`');
        $hook = new RawOuterJoinTenantFilter($tenants);

        foreach ([JoinType::Inner, JoinType::Left, JoinType::Cross, JoinType::Natural] as $type) {
            $tenants->filterJoin('Book', $type);
            $this->assertNull($hook->filterJoin('Book', $type), "A {$type->value} meets its condition where RawTenantFilter places it");
            $tenants->reset();
        }

        foreach ([JoinType::Right, JoinType::FullOuter] as $type) {
            $tenants->filterJoin('Book', $type);
            $result = $hook->filterJoin('Book', $type);

            $this->assertNotNull($result);
            $this->assertSame(Placement::On, $result->placement, 'Only ON decides which rows the join pairs');
            $this->assertSame($tenants->outerJoin('Book', $type)->expression, $result->condition->expression);
            $this->assertSame(
                '(`ns_authors`._tenant IN (?) OR `ns_authors`.`_uid` IS NULL) AND `Book`._tenant IN (?)',
                $result->condition->expression,
            );
            $tenants->reset();
        }
    }
}
