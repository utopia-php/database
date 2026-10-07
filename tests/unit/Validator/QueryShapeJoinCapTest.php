<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Capability;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Base;
use Utopia\Database\Validator\Queries\Document as DocumentQueries;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;

class QueryShapeJoinCapTest extends TestCase
{
    use QueryShapeAttributes;

    /**
     * @return list<Query>
     */
    private function crossJoins(int $count): array
    {
        return \array_map(fn (int $index): Query => Query::crossJoin('other', 'joined'.$index), \range(1, $count));
    }

    public function testAtMostEightJoinsPerQuery(): void
    {
        $validator = new Base([new Join(), new Limit()]);

        $this->assertTrue($validator->isValid($this->crossJoins(8)), $validator->getDescription());

        $this->assertFalse($validator->isValid($this->crossJoins(9)));
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());

        $this->assertFalse($validator->isValid([...$this->crossJoins(61), Query::limit(1)]));
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());

        $mixed = [
            ...$this->crossJoins(4),
            Query::join('orders', 'first', [Query::on('$id', 'customer')]),
            Query::leftJoin('orders', 'second', [Query::on('$id', 'customer')]),
            Query::rightJoin('orders', 'third', [Query::on('$id', 'customer')]),
            Query::fullOuterJoin('orders', 'fourth', [Query::on('$id', 'customer')]),
            Query::join('orders', 'fifth', [Query::on('$id', 'fifth.customer')]),
        ];
        $this->assertFalse($validator->isValid($mixed), 'every kind of join counts towards the cap');
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());
    }

    public function testJoinCapAppliesToSingleDocumentQueries(): void
    {
        $validator = new DocumentQueries($this->attributes(), Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::UnsignedBigInt, Capability::Joins]));

        $this->assertTrue($validator->isValid($this->crossJoins(8)), $validator->getDescription());
        $this->assertFalse($validator->isValid($this->crossJoins(9)));
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());
    }
}
