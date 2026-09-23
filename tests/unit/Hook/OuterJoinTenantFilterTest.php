<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Hook\OuterJoinTenantFilter;
use Utopia\Database\Hook\TenantFilter;
use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class OuterJoinTenantFilterTest extends TestCase
{
    private const string SOURCE = 'table_main';

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
        yield 'natural join' => [JoinType::Natural];
    }

    #[DataProvider('preservingJoins')]
    public function testBothSidesAreScopedToTheTenantInsideOn(JoinType $joinType): void
    {
        $filter = new TenantFilter(7, Database::METADATA, 'orders', self::SOURCE.'.'.Storage::UID);

        $result = (new OuterJoinTenantFilter($filter, self::SOURCE))->filterJoin(self::ALIAS, $joinType);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement, 'Only ON decides which rows the join pairs');
        $this->assertSame(
            '(`table_main`._tenant IN (?) OR `table_main`.`_uid` IS NULL) AND `j0`._tenant IN (?)',
            $result->condition->expression,
        );
        $this->assertSame([7, 7], $result->condition->bindings);
    }

    #[DataProvider('otherJoins')]
    public function testJoinsTenantFilterAlreadyScopesAreLeftToIt(JoinType $joinType): void
    {
        $filter = new TenantFilter(7, Database::METADATA, 'orders');

        $this->assertNull((new OuterJoinTenantFilter($filter, self::SOURCE))->filterJoin(self::ALIAS, $joinType));
    }

    public function testATenantlessMetadataRowStaysMatchable(): void
    {
        $filter = new TenantFilter(7, Database::METADATA, Database::METADATA);

        $result = (new OuterJoinTenantFilter($filter, self::SOURCE))->filterJoin(self::ALIAS, JoinType::Right);

        $this->assertNotNull($result);
        $this->assertSame(
            '(`table_main`._tenant IN (?) OR `table_main`._tenant IS NULL) AND `j0`._tenant IN (?)',
            $result->condition->expression,
            'A shared pool defines its metadata once, with no tenant, for every tenant to read',
        );
    }

    public function testEveryTenantOfACrossTenantReadIsBoundOnBothSides(): void
    {
        $filter = new TenantFilter([1, 2], Database::METADATA, 'orders');

        $result = (new OuterJoinTenantFilter($filter, self::SOURCE))->filterJoin(self::ALIAS, JoinType::FullOuter);

        $this->assertNotNull($result);
        $this->assertSame('`table_main`._tenant IN (?, ?) AND `j0`._tenant IN (?, ?)', $result->condition->expression);
        $this->assertSame([1, 2, 1, 2], $result->condition->bindings);
    }

    public function testAFullOuterJoinTreatsOnlyAMissingJoinedRowAsUnmatched(): void
    {
        $result = (new TenantFilter(7))->filterJoin(self::ALIAS, JoinType::FullOuter);

        $this->assertNotNull($result);
        $this->assertSame(Placement::Where, $result->placement);
        $this->assertSame(
            '(`j0`._tenant IN (?) OR `j0`.`_uid` IS NULL)',
            $result->condition->expression,
            'A stored row without a tenant is not a missing row, so the tenant column cannot tell them apart',
        );
    }
}
