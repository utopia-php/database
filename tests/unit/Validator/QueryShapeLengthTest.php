<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Having;
use Utopia\Query\Schema\ColumnType;

class QueryShapeLengthTest extends TestCase
{
    use QueryShapeAttributes;

    /**
     * @return list<Query>
     */
    private function three(): array
    {
        return [Query::equal('name', ['a']), Query::equal('name', ['b']), Query::equal('name', ['c'])];
    }

    public function testLengthCapsEveryNestedGroup(): void
    {
        $validator = new Queries([new Filter($this->attributes(), ColumnType::Integer->value)], length: 2);

        $this->assertTrue($validator->isValid([Query::or([Query::equal('name', ['a']), Query::equal('name', ['b'])])]), $validator->getDescription());
        $this->assertFalse($validator->isValid($this->three()), 'the top level holds three queries');
        $this->assertFalse($validator->isValid([Query::or($this->three())]), 'an or group holds three queries');
        $this->assertFalse($validator->isValid([Query::and($this->three())]), 'an and group holds three queries');
        $this->assertFalse($validator->isValid([Query::and([Query::or($this->three()), Query::equal('name', ['d'])])]), 'a group two levels down holds three queries');

        $schemaless = new Queries([new Filter($this->attributes(), ColumnType::Integer->value, supportForAttributes: false)], length: 2);
        $this->assertTrue($schemaless->isValid([Query::elemMatch('items', [Query::equal('sku', ['a']), Query::equal('sku', ['b'])])]), $schemaless->getDescription());
        $this->assertFalse($schemaless->isValid([Query::elemMatch('items', [Query::equal('sku', ['a']), Query::equal('sku', ['b']), Query::equal('sku', ['c'])])]), 'an elemMatch group holds three queries');
    }

    public function testLengthRejectionsNameTheGroupThatIsTooLong(): void
    {
        $validator = new Queries([new Filter($this->attributes(), ColumnType::Integer->value)], length: 2);

        $this->assertFalse($validator->isValid($this->three()));
        $this->assertSame('Too many queries: at most 2 are allowed', $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::and([Query::or($this->three()), Query::equal('name', ['d'])])]));
        $this->assertSame('Too many queries in or: at most 2 are allowed', $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::and($this->three())]));
        $this->assertSame('Too many queries in and: at most 2 are allowed', $validator->getDescription());
    }

    public function testLengthCapsHavingConditions(): void
    {
        $attributes = $this->attributes();
        $validator = new Queries([
            new Filter($attributes, ColumnType::Integer->value),
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Having(),
        ], length: 3);

        $this->assertFalse($validator->isValid([
            Query::sum('price', 'total'),
            Query::groupBy(['name']),
            Query::having([Query::greaterThan('total', 1), Query::lessThan('total', 9), Query::equal('name', ['a']), Query::notEqual('name', 'b')]),
        ]));
        $this->assertSame('Too many queries in having: at most 3 are allowed', $validator->getDescription());
    }
}
