<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\JoinChain;
use Utopia\Database\Hook\OuterJoinChainFilter;
use Utopia\Database\Hook\TenantFilter;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class OuterJoinChainFilterTest extends TestCase
{
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

    #[DataProvider('preservingJoins')]
    public function testEveryEarlierTableFilteredInWhereIsRepeatedInOnRelaxedForMissingRows(JoinType $joinType): void
    {
        $chain = new JoinChain([
            'b' => JoinType::Inner,
            'c' => JoinType::Right,
            'x' => JoinType::Cross,
            'd' => $joinType,
            'e' => JoinType::Right,
        ]);
        $tenants = new TenantFilter(7);
        $conditions = [];
        foreach (['b', 'c', 'x', 'd', 'e'] as $alias) {
            $conditions[$alias] = $tenants->joined($alias);
        }

        $result = (new OuterJoinChainFilter($chain, $conditions))->filterJoin('d', $joinType);

        $this->assertNotNull($result);
        $this->assertSame(Placement::On, $result->placement, 'Only ON decides which rows the join pairs');
        $this->assertSame(
            '(c._tenant IN (?) OR `c`.`_uid` IS NULL) AND (x._tenant IN (?) OR `x`.`_uid` IS NULL)',
            $result->condition->expression,
            'The inner-joined table meets its condition in its own ON; a table an earlier outer join left missing must not stop the pairing',
        );
        $this->assertSame([7, 7], $result->condition->bindings);
    }

    #[DataProvider('otherJoins')]
    public function testJoinsThatKeepNoUnmatchedRowsOfEarlierTablesNeedNothing(JoinType $joinType): void
    {
        $chain = new JoinChain(['c' => JoinType::Right, 'd' => $joinType]);

        $this->assertNull((new OuterJoinChainFilter($chain, ['c' => new Condition('c.ok')]))->filterJoin('d', $joinType));
    }

    public function testTheFirstOuterJoinAndTablesWithoutConditionsAddNothing(): void
    {
        $chain = new JoinChain(['c' => JoinType::Right, 'd' => JoinType::Right]);

        $this->assertNull((new OuterJoinChainFilter($chain, ['c' => new Condition('c.ok')]))->filterJoin('c', JoinType::Right));
        $this->assertNull((new OuterJoinChainFilter($chain, ['d' => new Condition('d.ok')]))->filterJoin('d', JoinType::Right));
    }

    public function testBindingsFollowTheConditionsInOrder(): void
    {
        $chain = new JoinChain(['c' => JoinType::FullOuter, 'x' => JoinType::Cross, 'd' => JoinType::Right]);

        $result = (new OuterJoinChainFilter($chain, [
            'x' => new Condition('x.role IN (?, ?)', ['x1', 'x2']),
            'c' => new Condition('c.role = ?', ['c1']),
        ], '"'))->filterJoin('d', JoinType::Right);

        $this->assertNotNull($result);
        $this->assertSame('(c.role = ? OR "c"."_uid" IS NULL) AND (x.role IN (?, ?) OR "x"."_uid" IS NULL)', $result->condition->expression);
        $this->assertSame(['c1', 'x1', 'x2'], $result->condition->bindings);
    }
}
