<?php

namespace Tests\Unit\Adapter\SQL\Hook\Join;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQL\Hook\Join\Chain;
use Utopia\Database\Query;
use Utopia\Query\Builder\JoinType;

final class ChainTest extends TestCase
{
    public function testJoinsAreKeyedTheWayTheBuilderHandsThemToJoinFilters(): void
    {
        $chain = Chain::fromQueries([
            Query::equal('name', ['a']),
            Query::join('books', 'book', [Query::on('$id', 'authorId')]),
            Query::crossJoin('extras', 'extra'),
            Query::rightJoin('reviews', 'review', [Query::on('$id', 'authorId')]),
            Query::fullOuterJoin('notes', 'note', [Query::on('$id', 'authorId')]),
        ]);

        $this->assertSame(['extra', 'review'], $chain->preceding('note'), 'Joins are keyed by their alias');
        $this->assertTrue($chain->has(JoinType::Cross));
        $this->assertFalse($chain->has(JoinType::Left));
    }

    public function testOnlyTablesWhoseConditionsSitInWherePrecedeALaterJoin(): void
    {
        $chain = new Chain([
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
        $chain = new Chain(['right' => JoinType::Right, 'last' => JoinType::Right]);

        $this->assertSame([], $chain->preceding('unknown'), 'Repeating conditions of tables the join may not follow would reference aliases it cannot see');
    }

    public function testOnlyRightAndFullOuterJoinsCanLeaveATableMissing(): void
    {
        $this->assertFalse((new Chain())->hasPreservingOuterJoin());
        $this->assertFalse((new Chain(['a' => JoinType::Inner, 'b' => JoinType::Left, 'c' => JoinType::Cross]))->hasPreservingOuterJoin());
        $this->assertTrue((new Chain(['a' => JoinType::Inner, 'b' => JoinType::Right]))->hasPreservingOuterJoin());
        $this->assertTrue((new Chain(['a' => JoinType::FullOuter]))->hasPreservingOuterJoin());
    }
}
