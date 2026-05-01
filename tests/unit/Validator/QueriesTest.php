<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Distinct;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Having;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Query\Schema\ColumnType;

class QueriesTest extends TestCase
{
    protected function setUp(): void
    {
    }

    protected function tearDown(): void
    {
    }

    public function test_empty_queries(): void
    {
        $validator = new Queries();

        $this->assertEquals(true, $validator->isValid([]));
    }

    public function test_invalid_method(): void
    {
        $validator = new Queries();
        $this->assertEquals(false, $validator->isValid([Query::equal('attr', ['value'])]));

        $validator = new Queries([new Limit()]);
        $this->assertEquals(false, $validator->isValid([Query::equal('attr', ['value'])]));
    }

    public function test_invalid_value(): void
    {
        $validator = new Queries([new Limit()]);
        $this->assertEquals(false, $validator->isValid([Query::limit(-1)]));
    }

    /**
     * @throws Exception
     */
    public function test_valid(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
            new Document([
                '$id' => 'meta',
                'key' => 'meta',
                'type' => ColumnType::Object->value,
                'array' => false,
            ]),
        ];

        $validator = new Queries(
            [
                new Cursor(),
                new Filter($attributes, ColumnType::Integer->value),
                new Limit(),
                new Offset(),
                new Order($attributes),
            ]
        );

        $this->assertEquals(true, $validator->isValid([Query::cursorAfter(new Document(['$id' => 'asdf']))]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::equal('name', ['value'])]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::limit(10)]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::offset(10)]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::orderAsc('name')]), $validator->getDescription());

        // Object attribute query: allowed shape
        $this->assertTrue(
            $validator->isValid([
                Query::equal('meta', [
                    ['a' => [1, 2]],
                    ['b' => [212]],
                ]),
            ]),
            $validator->getDescription()
        );

        // Object attribute query: disallowed nested multiple keys in same level
        $this->assertFalse(
            $validator->isValid([
                Query::equal('meta', [
                    ['a' => [1, 'b' => [212]]],
                ]),
            ])
        );

        // Object attribute query: disallowed complex multi-key nested structure
        $this->assertTrue(
            $validator->isValid([
                Query::contains('meta', [
                    [
                        'role' => [
                            'name' => ['test1', 'test2'],
                            'ex' => ['new' => 'test1'],
                        ],
                    ],
                ]),
            ])
        );
    }

    public function test_non_array_value_returns_false(): void
    {
        $validator = new Queries();

        $this->assertFalse($validator->isValid('not_an_array'));
        $this->assertEquals('Queries must be an array', $validator->getDescription());

        $this->assertFalse($validator->isValid(42));
        $this->assertFalse($validator->isValid(null));
    }

    public function test_query_count_exceeds_length(): void
    {
        $validator = new Queries([new Limit()], length: 2);

        $this->assertFalse($validator->isValid([
            Query::limit(10),
            Query::limit(20),
            Query::limit(30),
        ]));
    }

    public function test_aggregation_queries_add_aliases_to_order_validators(): void
    {
        $attributes = [
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'array' => false,
            ]),
        ];

        $validator = new Queries([
            new Aggregate(),
            new Order($attributes),
        ]);

        $this->assertTrue($validator->isValid([
            Query::avg('price', 'avg_price'),
            Query::orderAsc('avg_price'),
        ]));
    }

    public function test_variance_and_stddev_method_type_mapping(): void
    {
        $validator = new Queries([new Aggregate()]);

        $this->assertTrue($validator->isValid([Query::variance('col', 'var_col')]));
        $this->assertTrue($validator->isValid([Query::stddev('col', 'std_col')]));
    }

    public function test_distinct_method_type_mapping(): void
    {
        $validator = new Queries([new Distinct()]);

        $this->assertTrue($validator->isValid([Query::distinct()]));
    }

    public function test_group_by_method_type_mapping(): void
    {
        $validator = new Queries([new GroupBy()]);

        $this->assertTrue($validator->isValid([Query::groupBy(['category'])]));
    }

    public function test_having_method_type_mapping(): void
    {
        $validator = new Queries([new Having()]);

        $this->assertTrue($validator->isValid([Query::having([Query::greaterThan('count', 5)])]));
    }

    public function test_join_method_type_mapping(): void
    {
        $validator = new Queries([new Join()]);

        $this->assertTrue($validator->isValid([Query::join('orders', 'user_id', 'id')]));
    }

    public function test_is_array(): void
    {
        $validator = new Queries();

        $this->assertTrue($validator->isArray());
    }

    public function test_get_type(): void
    {
        $validator = new Queries();

        $this->assertEquals('object', $validator->getType());
    }
}
