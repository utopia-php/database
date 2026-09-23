<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
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
        $validator = new Queries([new Join(), new Limit()]);

        $this->assertTrue($validator->isValid($this->crossJoins(8)), $validator->getDescription());

        $this->assertFalse($validator->isValid($this->crossJoins(9)));
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());

        $this->assertFalse($validator->isValid([...$this->crossJoins(61), Query::limit(1)]));
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());

        $mixed = [
            ...$this->crossJoins(4),
            Query::join('orders', '$id', 'customer', '=', 'first'),
            Query::leftJoin('orders', '$id', 'customer', '=', 'second'),
            Query::rightJoin('orders', '$id', 'customer', '=', 'third'),
            Query::fullOuterJoin('orders', '$id', 'customer', '=', 'fourth'),
            Query::join('orders', 'fifth', [Query::on('$id', 'fifth.customer')]),
        ];
        $this->assertFalse($validator->isValid($mixed), 'every kind of join counts towards the cap');
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());
    }

    public function testJoinCapAppliesToSingleDocumentQueries(): void
    {
        $validator = new DocumentQueries($this->attributes());

        $this->assertTrue($validator->isValid($this->crossJoins(8)), $validator->getDescription());
        $this->assertFalse($validator->isValid($this->crossJoins(9)));
        $this->assertSame('Too many joins: at most 8 are allowed', $validator->getDescription());
    }
}
