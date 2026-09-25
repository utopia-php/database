<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Storage;

final class PermissionFilterTest extends TestCase
{
    public function testWithoutSemiJoinHintsTheSubqueryAndKeepsItsConditions(): void
    {
        $filter = $this->permissionFilter();
        $merged = $filter->filter('j0');
        $apart = $filter->withoutSemiJoin()->filter('j0');

        $this->assertStringContainsString('IN (SELECT DISTINCT _document FROM', $merged->expression);
        $this->assertStringNotContainsString('NO_SEMIJOIN', $merged->expression);
        $this->assertSame(
            \str_replace('IN (SELECT DISTINCT', 'IN (SELECT /*+ NO_SEMIJOIN() */ DISTINCT', $merged->expression),
            $apart->expression,
        );
        $this->assertSame($merged->bindings, $apart->bindings);
    }

    public function testWithoutSemiJoinLeavesTheOriginalFilterMerged(): void
    {
        $filter = $this->permissionFilter();
        $filter->withoutSemiJoin();

        $this->assertStringNotContainsString('NO_SEMIJOIN', $filter->filter('j0')->expression);
    }

    public function testWithoutSemiJoinStillDeniesEveryRowWithoutRoles(): void
    {
        $filter = new PermissionFilter(
            roles: [],
            permissionsTable: static fn (string $table): string => 'perms_'.$table,
        );

        $this->assertSame('1 = 0', $filter->withoutSemiJoin()->filter('j0')->expression);
    }

    public function testASubclassSeesTheSemiJoinChoice(): void
    {
        $filter = new class (roles: ['any'], permissionsTable: static fn (string $table): string => 'perms_'.$table) extends PermissionFilter {
            public function mergesIntoTheJoin(): bool
            {
                return $this->semiJoin;
            }
        };

        $this->assertTrue($filter->mergesIntoTheJoin());
        $this->assertFalse($filter->withoutSemiJoin()->mergesIntoTheJoin());
    }

    private function permissionFilter(): PermissionFilter
    {
        return new PermissionFilter(
            roles: ['any', 'user:1'],
            permissionsTable: static fn (string $table): string => 'perms_'.$table,
            documentColumn: 'j0.'.Storage::UID,
            permDocumentColumn: Storage::PERM_DOCUMENT,
            permRoleColumn: Storage::PERM_PERMISSION,
            permTypeColumn: Storage::PERM_TYPE,
        );
    }
}
