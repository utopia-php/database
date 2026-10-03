<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\JoinChain;
use Utopia\Database\Query;
use Utopia\Query\Builder\JoinType;

final class JoinChainTest extends TestCase
{
    public function testJoinsAreKeyedTheWayTheBuilderHandsThemToJoinFilters(): void
    {
        $chain = JoinChain::fromQueries([
            Query::equal('name', ['a']),
            Query::join('books', '$id', 'authorId', '=', 'book'),
            Query::crossJoin('extras', 'extra'),
            Query::rightJoin('reviews', '$id', 'authorId'),
            Query::fullOuterJoin('notes', '$id', 'authorId', '=', 'note'),
        ]);

        $this->assertSame(['extra', 'reviews'], $chain->preceding('note'), 'A join without an alias is keyed by its table');
        $this->assertTrue($chain->has(JoinType::Cross));
        $this->assertFalse($chain->has(JoinType::Left));
    }

    public function testOnlyTablesWhoseConditionsSitInWherePrecedeALaterJoin(): void
    {
        $chain = new JoinChain([
            'inner' => JoinType::Inner,
            'left' => JoinType::Left,
            'right' => JoinType::Right,
            'full' => JoinType::FullOuter,
            'cross' => JoinType::Cross,
            'last' => JoinType::Right,
        ]);

        $this->assertSame(['right', 'full', 'cross'], $chain->preceding('last'));
        $this->assertSame(['right'], $chain->preceding('full'));
        $this->assertSame([], $chain->preceding('right'), 'Inner and left joins meet their conditions in their own ON');
        $this->assertSame([], $chain->preceding('inner'));
    }

    public function testAnAliasOutsideTheChainHasNoPrecedingTables(): void
    {
        $chain = new JoinChain(['right' => JoinType::Right, 'last' => JoinType::Right]);

        $this->assertSame([], $chain->preceding('unknown'), 'Repeating conditions of tables the join may not follow would reference aliases it cannot see');
    }

    public function testOnlyRightAndFullOuterJoinsCanLeaveATableMissing(): void
    {
        $this->assertFalse((new JoinChain())->hasPreservingOuterJoin());
        $this->assertFalse((new JoinChain(['a' => JoinType::Inner, 'b' => JoinType::Left, 'c' => JoinType::Cross]))->hasPreservingOuterJoin());
        $this->assertTrue((new JoinChain(['a' => JoinType::Inner, 'b' => JoinType::Right]))->hasPreservingOuterJoin());
        $this->assertTrue((new JoinChain(['a' => JoinType::FullOuter]))->hasPreservingOuterJoin());
    }
}
