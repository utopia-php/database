<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Operator as OperatorValidator;
use Utopia\Query\Schema\ColumnType;

class OperatorTest extends TestCase
{
    protected Document $collection;

    protected function setUp(): void
    {
        $this->collection = new Document([
            '$id' => 'test_collection',
            'attributes' => [
                new Document([
                    '$id' => 'count',
                    'key' => 'count',
                    'type' => ColumnType::Integer->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'score',
                    'key' => 'score',
                    'type' => ColumnType::Double->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'title',
                    'key' => 'title',
                    'type' => ColumnType::String->value,
                    'array' => false,
                    'size' => 100,
                ]),
                new Document([
                    '$id' => 'tags',
                    'key' => 'tags',
                    'type' => ColumnType::String->value,
                    'array' => true,
                ]),
                new Document([
                    '$id' => 'active',
                    'key' => 'active',
                    'type' => ColumnType::Boolean->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'createdAt',
                    'key' => 'createdAt',
                    'type' => ColumnType::Datetime->value,
                    'array' => false,
                ]),
            ],
        ]);
    }

    protected function tearDown(): void {}

    // Test parsing string operators (new functionality)
    public function test_parse_string_operator(): void
    {
        $validator = new OperatorValidator($this->collection);

        // Create an operator and serialize it to JSON
        $operator = Operator::increment(5);
        $operator->setAttribute('count');
        $json = $operator->toString();

        // Validator should accept JSON string and parse it
        $this->assertTrue($validator->isValid($json), $validator->getDescription());
    }

    public function test_parse_invalid_string_operator(): void
    {
        $validator = new OperatorValidator($this->collection);

        // Invalid JSON should fail
        $this->assertFalse($validator->isValid('invalid json'));
        $this->assertStringContainsString('Invalid operator:', $validator->getDescription());
    }

    public function test_parse_string_operator_with_invalid_method(): void
    {
        $validator = new OperatorValidator($this->collection);

        // Valid JSON but invalid method
        $invalidOperator = json_encode([
            'method' => 'invalidMethod',
            'attribute' => 'count',
            'values' => [1],
        ]);

        $this->assertFalse($validator->isValid($invalidOperator));
        $this->assertStringContainsString('Invalid operator method:', $validator->getDescription());
    }

    // Test numeric operators
    public function test_increment_operator(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::increment(5);
        $operator->setAttribute('count');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_increment_on_non_numeric(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::increment(5);
        $operator->setAttribute('title'); // String field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply increment operator to non-numeric field', $validator->getDescription());
    }

    public function test_decrement_operator(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::decrement(3);
        $operator->setAttribute('count');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_multiply_operator(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::multiply(2.5);
        $operator->setAttribute('score');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_divide_by_zero(): void
    {
        $validator = new OperatorValidator($this->collection);

        // The divide helper method throws exception before validator is called
        $this->expectException(\Utopia\Database\Exception\Operator::class);
        $this->expectExceptionMessage('Division by zero is not allowed');

        $operator = Operator::divide(0);
    }

    public function test_modulo_by_zero(): void
    {
        $validator = new OperatorValidator($this->collection);

        // The modulo helper method throws exception before validator is called
        $this->expectException(\Utopia\Database\Exception\Operator::class);
        $this->expectExceptionMessage('Modulo by zero is not allowed');

        $operator = Operator::modulo(0);
    }

    // Test array operators
    public function test_array_append(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayAppend(['new-tag']);
        $operator->setAttribute('tags');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_array_append_on_non_array(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayAppend(['value']);
        $operator->setAttribute('title'); // Non-array field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply arrayAppend operator to non-array field', $validator->getDescription());
    }

    public function test_array_unique(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayUnique();
        $operator->setAttribute('tags');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_array_unique_on_non_array(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayUnique();
        $operator->setAttribute('count'); // Non-array field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply arrayUnique operator to non-array field', $validator->getDescription());
    }

    public function test_array_intersect(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayIntersect(['tag1', 'tag2']);
        $operator->setAttribute('tags');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_array_intersect_with_empty_array(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayIntersect([]);
        $operator->setAttribute('tags');

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('requires a non-empty array value', $validator->getDescription());
    }

    public function test_array_diff(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayDiff(['unwanted']);
        $operator->setAttribute('tags');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_array_filter(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayFilter('equal', 'active');
        $operator->setAttribute('tags');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_array_filter_invalid_condition(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::arrayFilter('invalidCondition', 'value');
        $operator->setAttribute('tags');

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Invalid array filter condition', $validator->getDescription());
    }

    // Test string operators
    public function test_string_concat(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::stringConcat(' - Updated');
        $operator->setAttribute('title');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_string_concat_on_non_string(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::stringConcat(' suffix');
        $operator->setAttribute('count'); // Non-string field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply stringConcat operator to non-string field', $validator->getDescription());
    }

    public function test_string_replace(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::stringReplace('old', 'new');
        $operator->setAttribute('title');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    // Test boolean operators
    public function test_toggle(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::toggle();
        $operator->setAttribute('active');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_toggle_on_non_boolean(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::toggle();
        $operator->setAttribute('count'); // Non-boolean field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply toggle operator to non-boolean field', $validator->getDescription());
    }

    // Test date operators
    public function test_date_add_days(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::dateAddDays(7);
        $operator->setAttribute('createdAt');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_date_add_days_on_non_date_time(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::dateAddDays(7);
        $operator->setAttribute('count'); // Non-datetime field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply dateAddDays operator to non-datetime field', $validator->getDescription());
    }

    public function test_date_sub_days(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::dateSubDays(3);
        $operator->setAttribute('createdAt');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    public function test_date_sub_days_on_non_date_time(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::dateSubDays(3);
        $operator->setAttribute('title'); // Non-datetime field

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString('Cannot apply dateSubDays operator to non-datetime field', $validator->getDescription());
    }

    public function test_date_set_now(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::dateSetNow();
        $operator->setAttribute('createdAt');

        $this->assertTrue($validator->isValid($operator), $validator->getDescription());
    }

    // Test attribute validation
    public function test_non_existent_attribute(): void
    {
        $validator = new OperatorValidator($this->collection);

        $operator = Operator::increment(1);
        $operator->setAttribute('nonExistentField');

        $this->assertFalse($validator->isValid($operator));
        $this->assertStringContainsString("Attribute 'nonExistentField' does not exist in collection", $validator->getDescription());
    }

    // Test multiple operators as strings (like Query validator does)
    public function test_multiple_string_operators(): void
    {
        $validator = new OperatorValidator($this->collection);

        // Test various operators as JSON strings
        $operators = [
            Operator::increment(1),
            Operator::arrayAppend(['tag']),
            Operator::stringConcat(' suffix'),
            Operator::toggle(),
            Operator::dateAddDays(7),
        ];

        $attributes = ['count', 'tags', 'title', 'active', 'createdAt'];

        foreach ($operators as $index => $operator) {
            $operator->setAttribute($attributes[$index]);
            $json = $operator->toString();
            $this->assertTrue($validator->isValid($json), "Failed for operator {$attributes[$index]}: ".$validator->getDescription());
        }
    }
}
