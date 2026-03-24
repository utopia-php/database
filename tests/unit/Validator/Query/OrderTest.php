<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Order;
use Utopia\Query\Schema\ColumnType;

class OrderTest extends TestCase
{
    protected Order $validator;

    /**
     * @throws Exception
     */
    protected function setUp(): void
    {
        $this->validator = new Order(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => '$sequence',
                    'key' => '$sequence',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
            ],
        );
    }

    public function test_value_success(): void
    {
        $this->assertTrue($this->validator->isValid(Query::orderAsc('attr')));
        $this->assertTrue($this->validator->isValid(Query::orderAsc()));
        $this->assertTrue($this->validator->isValid(Query::orderDesc('attr')));
        $this->assertTrue($this->validator->isValid(Query::orderDesc()));
    }

    public function test_value_failure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::limit(-1)));
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::limit(101)));
        $this->assertFalse($this->validator->isValid(Query::offset(-1)));
        $this->assertFalse($this->validator->isValid(Query::offset(5001)));
        $this->assertFalse($this->validator->isValid(Query::equal('attr', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::equal('dne', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::equal('', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::orderDesc('dne')));
        $this->assertFalse($this->validator->isValid(Query::orderAsc('dne')));
    }

    public function test_dotted_attribute_with_relationship_base(): void
    {
        $validator = new Order(
            attributes: [
                new Document([
                    '$id' => 'profile',
                    'key' => 'profile',
                    'type' => ColumnType::Relationship->value,
                    'array' => false,
                ]),
            ],
        );

        $this->assertFalse($validator->isValid(Query::orderAsc('profile.name')));
        $this->assertEquals('Cannot order by nested attribute: profile', $validator->getDescription());
    }

    public function test_dotted_attribute_not_in_schema(): void
    {
        $this->assertFalse($this->validator->isValid(Query::orderAsc('unknown.field')));
        $this->assertEquals('Attribute not found in schema: unknown', $this->validator->getDescription());
    }

    public function test_non_query_input_returns_false(): void
    {
        $this->assertFalse($this->validator->isValid('not_a_query'));
        $this->assertFalse($this->validator->isValid(42));
        $this->assertFalse($this->validator->isValid(null));
    }

    public function test_order_random_is_valid(): void
    {
        $query = Query::orderRandom();
        $this->assertTrue($this->validator->isValid($query));
    }

    public function test_add_aggregation_aliases(): void
    {
        $this->validator->addAggregationAliases(['total_count', 'avg_price']);

        $this->assertTrue($this->validator->isValid(Query::orderAsc('total_count')));
        $this->assertTrue($this->validator->isValid(Query::orderDesc('avg_price')));
    }
}
