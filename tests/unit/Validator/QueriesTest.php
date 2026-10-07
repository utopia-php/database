<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Base;
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
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Method;
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
        $validator = new Base();

        $this->assertEquals(true, $validator->isValid([]));
    }

    public function test_invalid_method(): void
    {
        $validator = new Base();
        $this->assertEquals(false, $validator->isValid([Query::equal('attr', ['value'])]));

        $validator = new Base([new Limit()]);
        $this->assertEquals(false, $validator->isValid([Query::equal('attr', ['value'])]));
    }

    public function test_invalid_value(): void
    {
        $validator = new Base([new Limit()]);
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

        $validator = new Base(
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
                Query::containsAny('meta', [
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
        $validator = new Base();

        $this->assertFalse($validator->isValid('not_an_array'));
        $this->assertEquals('Queries must be an array', $validator->getDescription());

        $this->assertFalse($validator->isValid(42));
        $this->assertFalse($validator->isValid(null));
    }

    public function test_query_count_exceeds_length(): void
    {
        $validator = new Base([new Limit()], length: 2);

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

        $validator = new Base([
            new Aggregate($attributes),
            new Order($attributes),
        ]);

        $this->assertTrue($validator->isValid([
            Query::avg('price', 'avg_price'),
            Query::orderAsc('avg_price'),
        ]));
    }

    public function test_variance_and_stddev_method_type_mapping(): void
    {
        $validator = new Base([new Aggregate(supportForAttributes: false)]);

        $this->assertTrue($validator->isValid([Query::variance('col', 'var_col')]));
        $this->assertTrue($validator->isValid([Query::stddev('col', 'std_col')]));
    }

    public function test_distinct_method_type_mapping(): void
    {
        $validator = new Base([new Distinct()]);

        $this->assertTrue($validator->isValid([Query::distinct()]));
    }

    public function test_group_by_method_type_mapping(): void
    {
        $validator = new Base([new GroupBy(supportForAttributes: false)]);

        $this->assertTrue($validator->isValid([Query::groupBy(['category'])]));
    }

    public function test_having_method_type_mapping(): void
    {
        $validator = new Base([new Having()]);

        $this->assertTrue($validator->isValid([Query::having([Query::greaterThan('count', 5)])]));
    }

    public function test_join_method_type_mapping(): void
    {
        $validator = new Base([new Join()]);

        $this->assertTrue($validator->isValid([Query::join('orders', 'j0', [Query::on('user_id', 'id')])]));
    }

    public function test_aggregate_and_group_by_accept_joined_attributes(): void
    {
        // `score` and `rev.score` live on the joined collection, so they are absent from
        // this collection's schema. Rejecting them broke every join aggregation test.
        $attributes = [
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Join(),
        ]);
        $validator->setJoinedCollections([new Document([
            '$id' => 'reviews',
            'attributes' => [
                new Document([
                    '$id' => 'score',
                    'key' => 'score',
                    'type' => ColumnType::Integer->value,
                    'array' => false,
                ]),
            ],
        ])]);

        $this->assertTrue($validator->isValid([
            Query::leftJoin('reviews', 'rev', [Query::on('productId', '$id')]),
            Query::sum('rev.score'),
            Query::groupBy(['score']),
        ]));
    }

    public function test_aggregate_and_group_by_reject_unknown_attributes_without_a_join(): void
    {
        $attributes = [
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Join(),
        ]);

        $this->assertFalse($validator->isValid([Query::sum('score')]));
        $this->assertFalse($validator->isValid([Query::groupBy(['score'])]));
    }

    public function test_aggregate_and_group_by_reject_an_undeclared_join_alias(): void
    {
        $attributes = [
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Join(),
        ]);

        $this->assertFalse($validator->isValid([
            Query::leftJoin('reviews', 'rev', [Query::on('productId', '$id')]),
            Query::sum('revv.score'),
        ]), 'an aggregate qualified with an undeclared alias must not pass');

        $this->assertFalse($validator->isValid([
            Query::leftJoin('reviews', 'rev', [Query::on('productId', '$id')]),
            Query::groupBy(['revv.score']),
        ]), 'a groupBy qualified with an undeclared alias must not pass');

        $this->assertFalse($validator->isValid([
            Query::leftJoin('reviews', 'rev', [Query::on('productId', '$id')]),
            Query::sum('rev.score.nested'),
        ]), 'a multi-segment join column must not pass');
    }

    public function test_join_alias_stand_down_does_not_leak_into_the_next_query_set(): void
    {
        $attributes = [
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::leftJoin('reviews', 'rev', [Query::on('productId', '$id')]),
            Query::sum('rev.score'),
        ]));

        $this->assertFalse($validator->isValid([
            Query::leftJoin('reviews', 'other', [Query::on('productId', '$id')]),
            Query::sum('rev.score'),
        ]), 'an alias from the previous query set must not stay valid');
    }

    public function test_joined_attribute_stand_down_does_not_leak_into_the_next_query_set(): void
    {
        // Queries caches its validators, so a join in one request must not leave the
        // schema check disabled for the next one.
        $attributes = [
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Join(),
        ]);
        $validator->setJoinedCollections([new Document([
            '$id' => 'reviews',
            'attributes' => [
                new Document([
                    '$id' => 'score',
                    'key' => 'score',
                    'type' => ColumnType::Integer->value,
                    'array' => false,
                ]),
            ],
        ])]);

        $this->assertTrue($validator->isValid([
            Query::leftJoin('reviews', 'rev', [Query::on('productId', '$id')]),
            Query::sum('score'),
        ]));

        $this->assertFalse($validator->isValid([Query::sum('score')]));
    }

    public function test_select_before_join_accepts_dotted_alias(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Select($attributes),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::select(['ord.amount']),
            Query::join('orders', 'ord', [Query::on('$id', 'customer_uid')]),
        ]), $validator->getDescription());
    }

    public function test_filter_before_join_accepts_dotted_alias(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::equal('sec.amount', [777]),
            Query::join('orders', 'sec', [Query::on('$id', 'customer_uid')]),
        ]), $validator->getDescription());
    }

    public function testNestedJoinAliasIsCollected(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Select($attributes),
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::select(['ord.amount']),
            Query::leftJoin('orders', 'ord', [
                Query::on('$id', 'customer_uid'),
                Query::equal('ord.status', ['paid']),
            ]),
        ]), $validator->getDescription());
    }

    public function testNestedJoinOnFilterUnknownAliasIsInvalid(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertFalse($validator->isValid([
            Query::leftJoin('orders', 'ord', [
                Query::on('$id', 'customer_uid'),
                Query::equal('missing.status', ['paid']),
            ]),
        ]));
        $this->assertStringContainsString('Attribute not found in schema', $validator->getDescription());
    }

    public function test_order_before_join_accepts_dotted_alias(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Order($attributes),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::orderAsc('sec.amount'),
            Query::join('orders', 'sec', [Query::on('$id', 'customer_uid')]),
        ]), $validator->getDescription());
    }

    public function test_unknown_join_alias_filter_is_rejected(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertFalse($validator->isValid([
            Query::equal('other.amount', [777]),
            Query::join('orders', 'sec', [Query::on('$id', 'customer_uid')]),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: other', $validator->getDescription());
    }

    public function test_nested_and_or_join_alias_is_accepted(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
            new Document([
                '$id' => 'rank',
                'key' => 'rank',
                'type' => ColumnType::Integer->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::join('meta', 'meta', [Query::on('$id', 'mainId')]),
            Query::and([
                Query::equal('name', ['Main']),
                Query::or([
                    Query::equal('meta.score', [10]),
                    Query::equal('rank', [2]),
                ]),
            ]),
        ]), $validator->getDescription());
    }

    public function test_nested_and_or_unknown_join_alias_is_rejected(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
            new Document([
                '$id' => 'rank',
                'key' => 'rank',
                'type' => ColumnType::Integer->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertFalse($validator->isValid([
            Query::join('meta', 'meta', [Query::on('$id', 'mainId')]),
            Query::and([
                Query::equal('name', ['Main']),
                Query::or([
                    Query::equal('other.score', [10]),
                    Query::equal('rank', [2]),
                ]),
            ]),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: other', $validator->getDescription());
    }

    public function test_nested_and_or_multi_segment_join_column_is_rejected(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
            new Document([
                '$id' => 'rank',
                'key' => 'rank',
                'type' => ColumnType::Integer->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertFalse($validator->isValid([
            Query::join('meta', 'meta', [Query::on('$id', 'mainId')]),
            Query::and([
                Query::equal('name', ['Main']),
                Query::or([
                    Query::equal('meta.foo.bar', [10]),
                    Query::equal('rank', [2]),
                ]),
            ]),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: meta', $validator->getDescription());
    }

    public function test_join_alias_reset_between_isValid_calls(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
            new Document([
                '$id' => 'rank',
                'key' => 'rank',
                'type' => ColumnType::Integer->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
            new Join(),
        ]);

        $this->assertTrue($validator->isValid([
            Query::join('meta', 'meta', [Query::on('$id', 'mainId')]),
            Query::equal('meta.score', [10]),
        ]), $validator->getDescription());

        $this->assertFalse($validator->isValid([
            Query::join('peer', 'peer', [Query::on('$id', 'mainId')]),
            Query::equal('meta.score', [8686]),
        ]));
        $this->assertStringContainsString('Attribute not found', $validator->getDescription());
        $this->assertStringContainsString('meta', $validator->getDescription());
    }

    public function test_nested_non_string_query_is_rejected(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $validator = new Base([
            new Filter($attributes, ColumnType::Integer->value),
        ]);

        $this->assertFalse($validator->isValid([
            new Query(Method::And, '', [123, Query::equal('name', ['Main'])]),
        ]));
        $this->assertSame('Invalid query: nested query must be a string', $validator->getDescription());
    }

    public function test_is_array(): void
    {
        $validator = new Base();

        $this->assertTrue($validator->isArray());
    }

    public function test_get_type(): void
    {
        $validator = new Base();

        $this->assertEquals('object', $validator->getType());
    }
}
