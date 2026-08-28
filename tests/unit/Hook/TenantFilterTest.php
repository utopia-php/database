<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Hook\TenantFilter;
use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class TenantFilterTest extends TestCase
{
    public function testMetadataPermissionRowsMayBeTenantless(): void
    {
        // A shared pool creates its system collections once with no tenant, so
        // the permission rows for those definitions are tenantless too. Scoping
        // the side table strictly to the writer's tenant matches neither the
        // read nor the delete, and revoking a permission on a shared definition
        // silently does nothing.
        $hook = new TenantFilter(989, Database::METADATA, Storage::permissionsTable(Database::METADATA));

        $condition = $hook->filter('perms');

        $this->assertStringContainsString('IS NULL', $condition->expression, 'The metadata permissions table must match tenantless rows');
        $this->assertSame([989], $condition->bindings);
    }

    public function testANonMetadataPermissionsTableStaysStrictlyTenanted(): void
    {
        $hook = new TenantFilter(989, Database::METADATA, Storage::permissionsTable('orders'));

        $condition = $hook->filter('perms');

        $this->assertStringNotContainsString('IS NULL', $condition->expression, 'A project collection must not leak across tenants');
        $this->assertSame([989], $condition->bindings);
    }

    public function testFilterDoesNotAllowNullTenantByDefault(): void
    {
        $hook = new TenantFilter(7, '', 'orders');

        $condition = $hook->filter('table_main');

        $this->assertSame('table_main.'.Storage::TENANT.' IN (?)', $condition->expression);
        $this->assertSame([7], $condition->bindings);
    }

    public function testFilterAllowsNullTenantWhenColumnProvided(): void
    {
        $hook = new TenantFilter(7, '', 'orders', 'table_main.'.Storage::TENANT);

        $condition = $hook->filter('table_main');

        $this->assertStringContainsString('table_main.'.Storage::TENANT.' IN (?)', $condition->expression);
        $this->assertStringContainsString('IS NULL', $condition->expression);
        $this->assertSame([7], $condition->bindings);
    }

    public function testFilterJoinLeftPlacesTenantInOnClause(): void
    {
        $hook = new TenantFilter(7);
        $result = $hook->filterJoin('j0', JoinType::Left);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement);
        $this->assertSame('j0.'.Storage::TENANT.' IN (?)', $result->condition->expression);
        $this->assertSame([7], $result->condition->bindings);
    }

    public function testFilterJoinRightPlacesTenantInWhereClause(): void
    {
        $hook = new TenantFilter(7);
        $result = $hook->filterJoin('j0', JoinType::Right);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertSame('j0.'.Storage::TENANT.' IN (?)', $result->condition->expression);
        $this->assertSame([7], $result->condition->bindings);
    }

    public function testFilterJoinFullOuterPlacesTenantInWhereClauseAndAllowsNull(): void
    {
        $hook = new TenantFilter(7);
        $result = $hook->filterJoin('j0', JoinType::FullOuter);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertStringContainsString('j0.'.Storage::TENANT.' IN (?)', $result->condition->expression);
        $this->assertStringContainsString('IS NULL', $result->condition->expression);
        $this->assertSame([7], $result->condition->bindings);
    }

    public function testFilterJoinInnerPlacesTenantInWhereClause(): void
    {
        $hook = new TenantFilter(7);
        $result = $hook->filterJoin('j0', JoinType::Inner);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertSame('j0.'.Storage::TENANT.' IN (?)', $result->condition->expression);
    }

    public function testFilterJoinCrossPlacesTenantInWhereClause(): void
    {
        $hook = new TenantFilter(7);
        $result = $hook->filterJoin('j0', JoinType::Cross);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertSame('j0.'.Storage::TENANT.' IN (?)', $result->condition->expression);
    }
}
