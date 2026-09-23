<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\OuterJoinPermissionFilter;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Hook\PermissionJoinFilter;
use Utopia\Database\Hook\TenantFilter;
use Utopia\Database\Storage;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class OuterJoinPermissionFilterTest extends TestCase
{
    private const string SOURCE = 'main';

    private const string ALIAS = 'j0';

    /**
     * @return iterable<string, array{JoinType}>
     */
    public static function preservingJoins(): iterable
    {
        yield 'right join' => [JoinType::Right];
        yield 'full outer join' => [JoinType::FullOuter];
    }

    /**
     * @return iterable<string, array{JoinType}>
     */
    public static function otherJoins(): iterable
    {
        yield 'inner join' => [JoinType::Inner];
        yield 'left join' => [JoinType::Left];
        yield 'cross join' => [JoinType::Cross];
    }

    /**
     * @return iterable<string, array{JoinType, bool}>
     */
    public static function placements(): iterable
    {
        foreach ([JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::FullOuter, JoinType::Cross] as $joinType) {
            foreach (['without' => false, 'with' => true] as $label => $preservingOuterJoin) {
                yield "{$joinType->value} {$label} a preserving outer join" => [$joinType, $preservingOuterJoin];
            }
        }
    }

    #[DataProvider('preservingJoins')]
    public function testBothSidesAreCheckedInsideOn(JoinType $joinType): void
    {
        $result = (new OuterJoinPermissionFilter(self::SOURCE, [
            self::SOURCE => $this->permission(self::SOURCE)->filter(self::SOURCE),
            self::ALIAS => $this->permission(self::ALIAS)->filter(self::ALIAS),
        ]))->filterJoin(self::ALIAS, $joinType);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement, 'Only ON decides which rows the join pairs');
        $this->assertSame(
            '('.$this->permission(self::SOURCE)->filter(self::SOURCE)->expression.' OR `main`.`_uid` IS NULL) AND '
                .$this->permission(self::ALIAS)->filter(self::ALIAS)->expression,
            $result->condition->expression,
        );
        $this->assertSame(
            [...$this->permission(self::SOURCE)->filter(self::SOURCE)->bindings, ...$this->permission(self::ALIAS)->filter(self::ALIAS)->bindings],
            $result->condition->bindings,
        );
    }

    #[DataProvider('otherJoins')]
    public function testJoinsThatDropUnreadableRowsInTheirOwnPlacementNeedNothing(JoinType $joinType): void
    {
        $hook = new OuterJoinPermissionFilter(self::SOURCE, [
            self::SOURCE => new Condition('main.ok'),
            self::ALIAS => new Condition('j0.ok'),
        ]);

        $this->assertNull($hook->filterJoin(self::ALIAS, $joinType));
    }

    public function testATableReadThroughItsCollectionGrantIsNotChecked(): void
    {
        $sourceOnly = (new OuterJoinPermissionFilter(self::SOURCE, [self::SOURCE => new Condition('main.ok = ?', [1])], '"'))
            ->filterJoin(self::ALIAS, JoinType::Right);
        $joinedOnly = (new OuterJoinPermissionFilter(self::SOURCE, [self::ALIAS => new Condition('j0.ok = ?', [2])]))
            ->filterJoin(self::ALIAS, JoinType::Right);

        $this->assertNotNull($sourceOnly);
        $this->assertSame('(main.ok = ? OR "main"."_uid" IS NULL)', $sourceOnly->condition->expression);
        $this->assertSame([1], $sourceOnly->condition->bindings);
        $this->assertNotNull($joinedOnly);
        $this->assertSame('j0.ok = ?', $joinedOnly->condition->expression);
        $this->assertSame([2], $joinedOnly->condition->bindings);
        $this->assertNull((new OuterJoinPermissionFilter(self::SOURCE, []))->filterJoin(self::ALIAS, JoinType::FullOuter));
    }

    /**
     * Permission conditions are placed exactly where tenant conditions are, and let through the
     * same missing rows.
     */
    #[DataProvider('placements')]
    public function testPermissionJoinFilterPlacesItsConditionWhereTenantFilterDoes(JoinType $joinType, bool $preservingOuterJoin): void
    {
        $permission = (new PermissionJoinFilter($this->permission(self::ALIAS), self::ALIAS, preservingOuterJoin: $preservingOuterJoin))
            ->filterJoin(self::ALIAS, $joinType);
        $tenant = (new TenantFilter(7, allowNullColumn: $preservingOuterJoin ? self::SOURCE.'.'.Storage::UID : ''))
            ->filterJoin(self::ALIAS, $joinType);

        $this->assertNotNull($permission);
        $this->assertNotNull($tenant);
        $this->assertSame($tenant->placement, $permission->placement);
        $this->assertSame(
            \str_contains($tenant->condition->expression, '`j0`.`_uid` IS NULL'),
            \str_contains($permission->condition->expression, '`j0`.`_uid` IS NULL'),
        );
    }

    private function permission(string $alias): PermissionFilter
    {
        return new PermissionFilter(
            roles: ['any'],
            permissionsTable: static fn (string $table): string => 'perms_'.$table,
            documentColumn: $alias.'.'.Storage::UID,
        );
    }
}
