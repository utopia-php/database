<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;

class OperatorTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testCreate(): void
    {
        // Test basic construction
        $operator = new Operator(Operator::TYPE_INCREMENT, 'count', [1]);

        $this->assertEquals(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertEquals('count', $operator->getAttribute());
        $this->assertEquals([1], $operator->getValues());
        $this->assertEquals(1, $operator->getValue());

        // Test with different types
        $operator = new Operator(Operator::TYPE_ARRAY_APPEND, 'tags', ['php', 'database']);

        $this->assertEquals(Operator::TYPE_ARRAY_APPEND, $operator->getMethod());
        $this->assertEquals('tags', $operator->getAttribute());
        $this->assertEquals(['php', 'database'], $operator->getValues());
        $this->assertEquals('php', $operator->getValue());
    }

    public function testHelperMethods(): void
    {
        // Test increment helper
        $operator = Operator::increment(5);
        $this->assertEquals(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute()); // Initially empty
        $this->assertEquals([5], $operator->getValues());

        // Test decrement helper
        $operator = Operator::decrement(1);
        $this->assertEquals(Operator::TYPE_DECREMENT, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute()); // Initially empty
        $this->assertEquals([1], $operator->getValues());

        // Test default increment value
        $operator = Operator::increment();
        $this->assertEquals(1, $operator->getValue());

        // Test string helpers
        $operator = Operator::concat(' - Updated');
        $this->assertEquals(Operator::TYPE_CONCAT, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([' - Updated'], $operator->getValues());

        $operator = Operator::replace('old', 'new');
        $this->assertEquals(Operator::TYPE_REPLACE, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals(['old', 'new'], $operator->getValues());

        // Test math helpers
        $operator = Operator::multiply(2, 1000);
        $this->assertEquals(Operator::TYPE_MULTIPLY, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([2, 1000], $operator->getValues());

        $operator = Operator::divide(2, 1);
        $this->assertEquals(Operator::TYPE_DIVIDE, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([2, 1], $operator->getValues());

        // Test boolean helper
        $operator = Operator::toggle();
        $this->assertEquals(Operator::TYPE_TOGGLE, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([], $operator->getValues());


        // Test utility helpers
        $operator = Operator::coalesce(['$field', 'default']);
        $this->assertEquals(Operator::TYPE_COALESCE, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([['$field', 'default']], $operator->getValues());

        $operator = Operator::dateSetNow();
        $this->assertEquals(Operator::TYPE_DATE_SET_NOW, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([], $operator->getValues());

        // Test concat helper
        $operator = Operator::concat(' - Updated');
        $this->assertEquals(Operator::TYPE_CONCAT, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([' - Updated'], $operator->getValues());

        // Test compute operator with callable
        $operator = Operator::compute(function ($doc) { return $doc->getAttribute('price') * 2; });
        $this->assertEquals(Operator::TYPE_COMPUTE, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertTrue(is_callable($operator->getValue()));

        // Test modulo and power operators
        $operator = Operator::modulo(3);
        $this->assertEquals(Operator::TYPE_MODULO, $operator->getMethod());
        $this->assertEquals([3], $operator->getValues());

        $operator = Operator::power(2, 1000);
        $this->assertEquals(Operator::TYPE_POWER, $operator->getMethod());
        $this->assertEquals([2, 1000], $operator->getValues());

        // Test new array helper methods
        $operator = Operator::arrayAppend(['new', 'values']);
        $this->assertEquals(Operator::TYPE_ARRAY_APPEND, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals(['new', 'values'], $operator->getValues());

        $operator = Operator::arrayPrepend(['first', 'second']);
        $this->assertEquals(Operator::TYPE_ARRAY_PREPEND, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals(['first', 'second'], $operator->getValues());

        $operator = Operator::arrayInsert(2, 'inserted');
        $this->assertEquals(Operator::TYPE_ARRAY_INSERT, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals([2, 'inserted'], $operator->getValues());

        $operator = Operator::arrayRemove('unwanted');
        $this->assertEquals(Operator::TYPE_ARRAY_REMOVE, $operator->getMethod());
        $this->assertEquals('', $operator->getAttribute());
        $this->assertEquals(['unwanted'], $operator->getValues());
    }

    public function testSetters(): void
    {
        $operator = new Operator(Operator::TYPE_INCREMENT, 'test', [1]);

        // Test setMethod
        $operator->setMethod(Operator::TYPE_DECREMENT);
        $this->assertEquals(Operator::TYPE_DECREMENT, $operator->getMethod());

        // Test setAttribute
        $operator->setAttribute('newAttribute');
        $this->assertEquals('newAttribute', $operator->getAttribute());

        // Test setValues
        $operator->setValues([10, 20]);
        $this->assertEquals([10, 20], $operator->getValues());

        // Test setValue
        $operator->setValue(50);
        $this->assertEquals([50], $operator->getValues());
        $this->assertEquals(50, $operator->getValue());
    }

    public function testTypeMethods(): void
    {
        // Test numeric operations
        $incrementOp = Operator::increment(1);
        $this->assertTrue($incrementOp->isNumericOperation());
        $this->assertFalse($incrementOp->isArrayOperation());

        $decrementOp = Operator::decrement(1);
        $this->assertTrue($decrementOp->isNumericOperation());
        $this->assertFalse($decrementOp->isArrayOperation());

        // Test string operations
        $concatOp = Operator::concat('suffix');
        $this->assertFalse($concatOp->isNumericOperation());
        $this->assertFalse($concatOp->isArrayOperation());
        $this->assertTrue($concatOp->isStringOperation());

        $concatOp = Operator::concat('suffix'); // Deprecated
        $this->assertFalse($concatOp->isNumericOperation());
        $this->assertFalse($concatOp->isArrayOperation());
        $this->assertTrue($concatOp->isStringOperation());

        $replaceOp = Operator::replace('old', 'new');
        $this->assertFalse($replaceOp->isNumericOperation());
        $this->assertFalse($replaceOp->isArrayOperation());
        $this->assertTrue($replaceOp->isStringOperation());

        // Test boolean operations
        $toggleOp = Operator::toggle();
        $this->assertFalse($toggleOp->isNumericOperation());
        $this->assertFalse($toggleOp->isArrayOperation());
        $this->assertTrue($toggleOp->isBooleanOperation());


        // Test conditional operations
        $coalesceOp = Operator::coalesce(['$field', 'default']);
        $this->assertFalse($coalesceOp->isNumericOperation());
        $this->assertFalse($coalesceOp->isArrayOperation());
        $this->assertTrue($coalesceOp->isConditionalOperation());

        $dateSetNowOp = Operator::dateSetNow();
        $this->assertFalse($dateSetNowOp->isNumericOperation());
        $this->assertFalse($dateSetNowOp->isArrayOperation());
        $this->assertTrue($dateSetNowOp->isDateOperation());

        // Test new array operations
        $arrayAppendOp = Operator::arrayAppend(['tag']);
        $this->assertFalse($arrayAppendOp->isNumericOperation());
        $this->assertTrue($arrayAppendOp->isArrayOperation());

        $arrayPrependOp = Operator::arrayPrepend(['tag']);
        $this->assertFalse($arrayPrependOp->isNumericOperation());
        $this->assertTrue($arrayPrependOp->isArrayOperation());

        $arrayInsertOp = Operator::arrayInsert(0, 'item');
        $this->assertFalse($arrayInsertOp->isNumericOperation());
        $this->assertTrue($arrayInsertOp->isArrayOperation());

        $arrayRemoveOp = Operator::arrayRemove('unwanted');
        $this->assertFalse($arrayRemoveOp->isNumericOperation());
        $this->assertTrue($arrayRemoveOp->isArrayOperation());
    }

    public function testIsMethod(): void
    {
        // Test valid methods
        $this->assertTrue(Operator::isMethod(Operator::TYPE_INCREMENT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_DECREMENT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_MULTIPLY));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_DIVIDE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_CONCAT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_REPLACE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_TOGGLE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_CONCAT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_COALESCE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_DATE_SET_NOW));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_MODULO));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_POWER));

        // Test new array methods
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_APPEND));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_PREPEND));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_INSERT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_REMOVE));

        // Test invalid methods
        $this->assertFalse(Operator::isMethod('invalid'));
        $this->assertFalse(Operator::isMethod(''));
        $this->assertFalse(Operator::isMethod('append')); // Old method should be false
        $this->assertFalse(Operator::isMethod('prepend')); // Old method should be false
        $this->assertFalse(Operator::isMethod('insert')); // Old method should be false
    }

    public function testIsOperator(): void
    {
        $operator = Operator::increment(1);
        $this->assertTrue(Operator::isOperator($operator));

        $this->assertFalse(Operator::isOperator('string'));
        $this->assertFalse(Operator::isOperator(123));
        $this->assertFalse(Operator::isOperator([]));
        $this->assertFalse(Operator::isOperator(null));
    }

    public function testExtractOperators(): void
    {
        $data = [
            'name' => 'John',
            'count' => Operator::increment(5),
            'tags' => Operator::arrayAppend(['new']),
            'age' => 30
        ];

        $result = Operator::extractOperators($data);

        $this->assertArrayHasKey('operators', $result);
        $this->assertArrayHasKey('updates', $result);

        $operators = $result['operators'];
        $updates = $result['updates'];

        // Check operators
        $this->assertCount(2, $operators);
        $this->assertInstanceOf(Operator::class, $operators['count']);
        $this->assertInstanceOf(Operator::class, $operators['tags']);

        // Check that attributes are set from document keys
        $this->assertEquals('count', $operators['count']->getAttribute());
        $this->assertEquals('tags', $operators['tags']->getAttribute());

        // Check updates
        $this->assertEquals(['name' => 'John', 'age' => 30], $updates);
    }

    public function testSerialization(): void
    {
        $operator = Operator::increment(10);
        $operator->setAttribute('score'); // Simulate setting attribute

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_INCREMENT,
            'attribute' => 'score',
            'values' => [10]
        ];
        $this->assertEquals($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertEquals($expected, $decoded);
    }

    public function testParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_INCREMENT,
            'attribute' => 'score',
            'values' => [5]
        ];

        $operator = Operator::parseOperator($array);
        $this->assertEquals(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertEquals('score', $operator->getAttribute());
        $this->assertEquals([5], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $operator = Operator::parse($json);
        $this->assertEquals(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertEquals('score', $operator->getAttribute());
        $this->assertEquals([5], $operator->getValues());
    }

    public function testParseOperators(): void
    {
        $operators = [
            json_encode(['method' => Operator::TYPE_INCREMENT, 'attribute' => 'count', 'values' => [1]]),
            json_encode(['method' => Operator::TYPE_ARRAY_APPEND, 'attribute' => 'tags', 'values' => ['new']])
        ];

        $parsed = Operator::parseOperators($operators);
        $this->assertCount(2, $parsed);
        $this->assertInstanceOf(Operator::class, $parsed[0]);
        $this->assertInstanceOf(Operator::class, $parsed[1]);
        $this->assertEquals(Operator::TYPE_INCREMENT, $parsed[0]->getMethod());
        $this->assertEquals(Operator::TYPE_ARRAY_APPEND, $parsed[1]->getMethod());
    }

    public function testClone(): void
    {
        $operator1 = Operator::increment(5);
        $operator2 = clone $operator1;

        $this->assertEquals($operator1->getMethod(), $operator2->getMethod());
        $this->assertEquals($operator1->getAttribute(), $operator2->getAttribute());
        $this->assertEquals($operator1->getValues(), $operator2->getValues());

        // Ensure they are different objects
        $operator2->setMethod(Operator::TYPE_DECREMENT);
        $this->assertEquals(Operator::TYPE_INCREMENT, $operator1->getMethod());
        $this->assertEquals(Operator::TYPE_DECREMENT, $operator2->getMethod());
    }

    public function testGetValueWithDefault(): void
    {
        $operator = Operator::increment(5);
        $this->assertEquals(5, $operator->getValue());
        $this->assertEquals(5, $operator->getValue('default'));

        $emptyOperator = new Operator(Operator::TYPE_INCREMENT, 'count', []);
        $this->assertEquals('default', $emptyOperator->getValue('default'));
        $this->assertNull($emptyOperator->getValue());
    }

    // Exception tests

    public function testParseInvalidJson(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid operator');
        Operator::parse('invalid json');
    }

    public function testParseNonArray(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid operator. Must be an array');
        Operator::parse('"string"');
    }

    public function testParseInvalidMethod(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid operator method. Must be a string');
        $array = ['method' => 123, 'attribute' => 'test', 'values' => []];
        Operator::parseOperator($array);
    }

    public function testParseUnsupportedMethod(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid operator method: invalid');
        $array = ['method' => 'invalid', 'attribute' => 'test', 'values' => []];
        Operator::parseOperator($array);
    }

    public function testParseInvalidAttribute(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid operator attribute. Must be a string');
        $array = ['method' => Operator::TYPE_INCREMENT, 'attribute' => 123, 'values' => []];
        Operator::parseOperator($array);
    }

    public function testParseInvalidValues(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid operator values. Must be an array');
        $array = ['method' => Operator::TYPE_INCREMENT, 'attribute' => 'test', 'values' => 'not array'];
        Operator::parseOperator($array);
    }

    public function testToStringInvalidJson(): void
    {
        // Create an operator with values that can't be JSON encoded
        $operator = new Operator(Operator::TYPE_INCREMENT, 'test', []);
        $operator->setValues([fopen('php://memory', 'r')]); // Resource can't be JSON encoded

        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Invalid Json');
        $operator->toString();
    }

    // New functionality tests

    public function testIncrementWithMax(): void
    {
        // Test increment with max limit
        $operator = Operator::increment(5, 10);
        $this->assertEquals(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertEquals([5, 10], $operator->getValues());

        // Test increment without max (should be same as original behavior)
        $operator = Operator::increment(5);
        $this->assertEquals([5], $operator->getValues());
    }

    public function testDecrementWithMin(): void
    {
        // Test decrement with min limit
        $operator = Operator::decrement(3, 0);
        $this->assertEquals(Operator::TYPE_DECREMENT, $operator->getMethod());
        $this->assertEquals([3, 0], $operator->getValues());

        // Test decrement without min (should be same as original behavior)
        $operator = Operator::decrement(3);
        $this->assertEquals([3], $operator->getValues());
    }

    public function testArrayRemove(): void
    {
        $operator = Operator::arrayRemove('spam');
        $this->assertEquals(Operator::TYPE_ARRAY_REMOVE, $operator->getMethod());
        $this->assertEquals(['spam'], $operator->getValues());
        $this->assertEquals('spam', $operator->getValue());
    }

    public function testExtractOperatorsWithNewMethods(): void
    {
        $data = [
            'name' => 'John',
            'count' => Operator::increment(5, 100), // with max
            'score' => Operator::decrement(1, 0),   // with min
            'tags' => Operator::arrayAppend(['new']),
            'items' => Operator::arrayPrepend(['first']),
            'list' => Operator::arrayInsert(2, 'value'),
            'blacklist' => Operator::arrayRemove('spam'),
            'title' => Operator::concat(' - Updated'),
            'content' => Operator::replace('old', 'new'),
            'views' => Operator::multiply(2, 1000),
            'rating' => Operator::divide(2, 1),
            'featured' => Operator::toggle(),
            'default_value' => Operator::coalesce(['$default_value', 'default']),
            'last_modified' => Operator::dateSetNow(),
            'title_prefix' => Operator::concat(' - Updated'),
            'views_modulo' => Operator::modulo(3),
            'score_power' => Operator::power(2, 1000),
            'age' => 30
        ];

        $result = Operator::extractOperators($data);

        $operators = $result['operators'];
        $updates = $result['updates'];

        // Check operators count
        $this->assertCount(16, $operators);

        // Check that array methods are properly extracted
        $this->assertInstanceOf(Operator::class, $operators['tags']);
        $this->assertEquals('tags', $operators['tags']->getAttribute());
        $this->assertEquals(Operator::TYPE_ARRAY_APPEND, $operators['tags']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['blacklist']);
        $this->assertEquals('blacklist', $operators['blacklist']->getAttribute());
        $this->assertEquals(Operator::TYPE_ARRAY_REMOVE, $operators['blacklist']->getMethod());

        // Check string operators
        $this->assertEquals(Operator::TYPE_CONCAT, $operators['title']->getMethod());
        $this->assertEquals(Operator::TYPE_REPLACE, $operators['content']->getMethod());

        // Check math operators
        $this->assertEquals(Operator::TYPE_MULTIPLY, $operators['views']->getMethod());
        $this->assertEquals(Operator::TYPE_DIVIDE, $operators['rating']->getMethod());

        // Check boolean operator
        $this->assertEquals(Operator::TYPE_TOGGLE, $operators['featured']->getMethod());

        // Check new operators
        $this->assertEquals(Operator::TYPE_CONCAT, $operators['title_prefix']->getMethod());
        $this->assertEquals(Operator::TYPE_MODULO, $operators['views_modulo']->getMethod());
        $this->assertEquals(Operator::TYPE_POWER, $operators['score_power']->getMethod());

        // Check utility operators
        $this->assertEquals(Operator::TYPE_COALESCE, $operators['default_value']->getMethod());
        $this->assertEquals(Operator::TYPE_DATE_SET_NOW, $operators['last_modified']->getMethod());

        // Check that max/min values are preserved
        $this->assertEquals([5, 100], $operators['count']->getValues());
        $this->assertEquals([1, 0], $operators['score']->getValues());

        // Check updates
        $this->assertEquals(['name' => 'John', 'age' => 30], $updates);
    }


    public function testParsingWithNewConstants(): void
    {
        // Test parsing new array methods
        $arrayRemove = [
            'method' => Operator::TYPE_ARRAY_REMOVE,
            'attribute' => 'blacklist',
            'values' => ['spam']
        ];

        $operator = Operator::parseOperator($arrayRemove);
        $this->assertEquals(Operator::TYPE_ARRAY_REMOVE, $operator->getMethod());
        $this->assertEquals('blacklist', $operator->getAttribute());
        $this->assertEquals(['spam'], $operator->getValues());

        // Test parsing increment with max
        $incrementWithMax = [
            'method' => Operator::TYPE_INCREMENT,
            'attribute' => 'score',
            'values' => [1, 10]
        ];

        $operator = Operator::parseOperator($incrementWithMax);
        $this->assertEquals([1, 10], $operator->getValues());
    }

    // Edge case tests

    public function testIncrementMaxLimitEdgeCases(): void
    {
        // Test that max limit is properly stored
        $operator = Operator::increment(5, 10);
        $values = $operator->getValues();
        $this->assertEquals(5, $values[0]); // increment value
        $this->assertEquals(10, $values[1]); // max limit

        // Test with float values
        $operator = Operator::increment(1.5, 9.9);
        $values = $operator->getValues();
        $this->assertEquals(1.5, $values[0]);
        $this->assertEquals(9.9, $values[1]);

        // Test with negative max (edge case)
        $operator = Operator::increment(1, -5);
        $values = $operator->getValues();
        $this->assertEquals(1, $values[0]);
        $this->assertEquals(-5, $values[1]);
    }

    public function testDecrementMinLimitEdgeCases(): void
    {
        // Test that min limit is properly stored
        $operator = Operator::decrement(3, 0);
        $values = $operator->getValues();
        $this->assertEquals(3, $values[0]); // decrement value
        $this->assertEquals(0, $values[1]); // min limit

        // Test with float values
        $operator = Operator::decrement(2.5, 0.1);
        $values = $operator->getValues();
        $this->assertEquals(2.5, $values[0]);
        $this->assertEquals(0.1, $values[1]);

        // Test with negative min
        $operator = Operator::decrement(1, -10);
        $values = $operator->getValues();
        $this->assertEquals(1, $values[0]);
        $this->assertEquals(-10, $values[1]);
    }

    public function testArrayRemoveEdgeCases(): void
    {
        // Test removing various types of values
        $operator = Operator::arrayRemove('string');
        $this->assertEquals('string', $operator->getValue());

        $operator = Operator::arrayRemove(42);
        $this->assertEquals(42, $operator->getValue());

        $operator = Operator::arrayRemove(null);
        $this->assertEquals(null, $operator->getValue());

        $operator = Operator::arrayRemove(true);
        $this->assertEquals(true, $operator->getValue());

        // Test removing array (nested array)
        $operator = Operator::arrayRemove(['nested']);
        $this->assertEquals(['nested'], $operator->getValue());
    }

    public function testOperatorCloningWithNewMethods(): void
    {
        // Test cloning increment with max
        $operator1 = Operator::increment(5, 10);
        $operator2 = clone $operator1;

        $this->assertEquals($operator1->getValues(), $operator2->getValues());
        $this->assertEquals([5, 10], $operator2->getValues());

        // Modify one to ensure they're separate objects
        $operator2->setValues([3, 8]);
        $this->assertEquals([5, 10], $operator1->getValues());
        $this->assertEquals([3, 8], $operator2->getValues());

        // Test cloning arrayRemove
        $removeOp1 = Operator::arrayRemove('spam');
        $removeOp2 = clone $removeOp1;

        $this->assertEquals($removeOp1->getValue(), $removeOp2->getValue());
        $removeOp2->setValue('ham');
        $this->assertEquals('spam', $removeOp1->getValue());
        $this->assertEquals('ham', $removeOp2->getValue());
    }

    public function testSerializationWithNewOperators(): void
    {
        // Test serialization of increment with max
        $operator = Operator::increment(5, 100);
        $operator->setAttribute('score');

        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_INCREMENT,
            'attribute' => 'score',
            'values' => [5, 100]
        ];
        $this->assertEquals($expected, $array);

        // Test serialization of arrayRemove
        $operator = Operator::arrayRemove('unwanted');
        $operator->setAttribute('blacklist');

        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_ARRAY_REMOVE,
            'attribute' => 'blacklist',
            'values' => ['unwanted']
        ];
        $this->assertEquals($expected, $array);

        // Ensure JSON serialization works
        $json = $operator->toString();
        $this->assertJson($json);
        $decoded = json_decode($json, true);
        $this->assertEquals($expected, $decoded);
    }

    public function testMixedOperatorTypes(): void
    {
        // Test that all new operator types can coexist
        $data = [
            'arrayAppend' => Operator::arrayAppend(['new']),
            'incrementWithMax' => Operator::increment(1, 10),
            'decrementWithMin' => Operator::decrement(2, 0),
            'multiply' => Operator::multiply(3, 100),
            'divide' => Operator::divide(2, 1),
            'concat' => Operator::concat(' suffix'),
            'replace' => Operator::replace('old', 'new'),
            'toggle' => Operator::toggle(),
            'coalesce' => Operator::coalesce(['$field', 'default']),
            'dateSetNow' => Operator::dateSetNow(),
            'concat' => Operator::concat(' suffix'),
            'modulo' => Operator::modulo(3),
            'power' => Operator::power(2),
            'remove' => Operator::arrayRemove('bad'),
        ];

        $result = Operator::extractOperators($data);
        $operators = $result['operators'];

        $this->assertCount(13, $operators);

        // Verify each operator type
        $this->assertEquals(Operator::TYPE_ARRAY_APPEND, $operators['arrayAppend']->getMethod());
        $this->assertEquals(Operator::TYPE_INCREMENT, $operators['incrementWithMax']->getMethod());
        $this->assertEquals([1, 10], $operators['incrementWithMax']->getValues());
        $this->assertEquals(Operator::TYPE_DECREMENT, $operators['decrementWithMin']->getMethod());
        $this->assertEquals([2, 0], $operators['decrementWithMin']->getValues());
        $this->assertEquals(Operator::TYPE_MULTIPLY, $operators['multiply']->getMethod());
        $this->assertEquals([3, 100], $operators['multiply']->getValues());
        $this->assertEquals(Operator::TYPE_DIVIDE, $operators['divide']->getMethod());
        $this->assertEquals([2, 1], $operators['divide']->getValues());
        $this->assertEquals(Operator::TYPE_CONCAT, $operators['concat']->getMethod());
        $this->assertEquals(Operator::TYPE_REPLACE, $operators['replace']->getMethod());
        $this->assertEquals(Operator::TYPE_TOGGLE, $operators['toggle']->getMethod());
        $this->assertEquals(Operator::TYPE_COALESCE, $operators['coalesce']->getMethod());
        $this->assertEquals(Operator::TYPE_DATE_SET_NOW, $operators['dateSetNow']->getMethod());
        $this->assertEquals(Operator::TYPE_CONCAT, $operators['concat']->getMethod());
        $this->assertEquals(Operator::TYPE_MODULO, $operators['modulo']->getMethod());
        $this->assertEquals(Operator::TYPE_POWER, $operators['power']->getMethod());
        $this->assertEquals(Operator::TYPE_ARRAY_REMOVE, $operators['remove']->getMethod());
    }

    public function testTypeValidationWithNewMethods(): void
    {
        // All new array methods should be detected as array operations
        $this->assertTrue(Operator::arrayAppend([])->isArrayOperation());
        $this->assertTrue(Operator::arrayPrepend([])->isArrayOperation());
        $this->assertTrue(Operator::arrayInsert(0, 'value')->isArrayOperation());
        $this->assertTrue(Operator::arrayRemove('value')->isArrayOperation());

        // None should be detected as numeric operations
        $this->assertFalse(Operator::arrayAppend([])->isNumericOperation());
        $this->assertFalse(Operator::arrayPrepend([])->isNumericOperation());
        $this->assertFalse(Operator::arrayInsert(0, 'value')->isNumericOperation());
        $this->assertFalse(Operator::arrayRemove('value')->isNumericOperation());

        // Test numeric operations
        $this->assertTrue(Operator::multiply(2)->isNumericOperation());
        $this->assertTrue(Operator::divide(2)->isNumericOperation());
        $this->assertFalse(Operator::multiply(2)->isArrayOperation());
        $this->assertFalse(Operator::divide(2)->isArrayOperation());

        // Test string operations
        $this->assertTrue(Operator::concat('test')->isStringOperation());
        $this->assertTrue(Operator::replace('old', 'new')->isStringOperation());
        $this->assertFalse(Operator::concat('test')->isNumericOperation());
        $this->assertFalse(Operator::replace('old', 'new')->isArrayOperation());

        // Test boolean operations
        $this->assertTrue(Operator::toggle()->isBooleanOperation());
        $this->assertFalse(Operator::toggle()->isNumericOperation());
        $this->assertFalse(Operator::toggle()->isArrayOperation());


        // Test conditional and date operations
        $this->assertTrue(Operator::coalesce(['$field', 'value'])->isConditionalOperation());
        $this->assertTrue(Operator::dateSetNow()->isDateOperation());
        $this->assertFalse(Operator::coalesce(['$field', 'value'])->isNumericOperation());
        $this->assertFalse(Operator::dateSetNow()->isNumericOperation());
    }

    // New comprehensive tests for all operators

    public function testStringOperators(): void
    {
        // Test concat operator
        $operator = Operator::concat(' - Updated');
        $this->assertEquals(Operator::TYPE_CONCAT, $operator->getMethod());
        $this->assertEquals([' - Updated'], $operator->getValues());
        $this->assertEquals(' - Updated', $operator->getValue());

        // Test concat operator (deprecated)
        $operator = Operator::concat(' - Updated');
        $this->assertEquals(Operator::TYPE_CONCAT, $operator->getMethod());
        $this->assertEquals([' - Updated'], $operator->getValues());
        $this->assertEquals(' - Updated', $operator->getValue());

        // Test replace operator
        $operator = Operator::replace('old', 'new');
        $this->assertEquals(Operator::TYPE_REPLACE, $operator->getMethod());
        $this->assertEquals(['old', 'new'], $operator->getValues());
        $this->assertEquals('old', $operator->getValue());
    }

    public function testMathOperators(): void
    {
        // Test multiply operator
        $operator = Operator::multiply(2.5, 100);
        $this->assertEquals(Operator::TYPE_MULTIPLY, $operator->getMethod());
        $this->assertEquals([2.5, 100], $operator->getValues());
        $this->assertEquals(2.5, $operator->getValue());

        // Test multiply without max
        $operator = Operator::multiply(3);
        $this->assertEquals([3], $operator->getValues());

        // Test divide operator
        $operator = Operator::divide(2, 1);
        $this->assertEquals(Operator::TYPE_DIVIDE, $operator->getMethod());
        $this->assertEquals([2, 1], $operator->getValues());
        $this->assertEquals(2, $operator->getValue());

        // Test divide without min
        $operator = Operator::divide(4);
        $this->assertEquals([4], $operator->getValues());

        // Test modulo operator
        $operator = Operator::modulo(3);
        $this->assertEquals(Operator::TYPE_MODULO, $operator->getMethod());
        $this->assertEquals([3], $operator->getValues());
        $this->assertEquals(3, $operator->getValue());

        // Test power operator
        $operator = Operator::power(2, 1000);
        $this->assertEquals(Operator::TYPE_POWER, $operator->getMethod());
        $this->assertEquals([2, 1000], $operator->getValues());
        $this->assertEquals(2, $operator->getValue());

        // Test power without max
        $operator = Operator::power(3);
        $this->assertEquals([3], $operator->getValues());
    }

    public function testDivideByZero(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Division by zero is not allowed');
        Operator::divide(0);
    }

    public function testModuloByZero(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Modulo by zero is not allowed');
        Operator::modulo(0);
    }

    public function testBooleanOperator(): void
    {
        $operator = Operator::toggle();
        $this->assertEquals(Operator::TYPE_TOGGLE, $operator->getMethod());
        $this->assertEquals([], $operator->getValues());
        $this->assertNull($operator->getValue());
    }


    public function testUtilityOperators(): void
    {
        // Test coalesce
        $operator = Operator::coalesce(['$field', 'default-value']);
        $this->assertEquals(Operator::TYPE_COALESCE, $operator->getMethod());
        $this->assertEquals([['$field', 'default-value']], $operator->getValues());
        $this->assertEquals(['$field', 'default-value'], $operator->getValue());

        // Test dateSetNow
        $operator = Operator::dateSetNow();
        $this->assertEquals(Operator::TYPE_DATE_SET_NOW, $operator->getMethod());
        $this->assertEquals([], $operator->getValues());
        $this->assertNull($operator->getValue());
    }


    public function testNewOperatorParsing(): void
    {
        // Test parsing all new operators
        $operators = [
            ['method' => Operator::TYPE_CONCAT, 'attribute' => 'title', 'values' => [' - Updated']],
            ['method' => Operator::TYPE_CONCAT, 'attribute' => 'subtitle', 'values' => [' - Updated']], // Deprecated
            ['method' => Operator::TYPE_REPLACE, 'attribute' => 'content', 'values' => ['old', 'new']],
            ['method' => Operator::TYPE_MULTIPLY, 'attribute' => 'score', 'values' => [2, 100]],
            ['method' => Operator::TYPE_DIVIDE, 'attribute' => 'rating', 'values' => [2, 1]],
            ['method' => Operator::TYPE_MODULO, 'attribute' => 'remainder', 'values' => [3]],
            ['method' => Operator::TYPE_POWER, 'attribute' => 'exponential', 'values' => [2, 1000]],
            ['method' => Operator::TYPE_TOGGLE, 'attribute' => 'active', 'values' => []],
            ['method' => Operator::TYPE_COALESCE, 'attribute' => 'default', 'values' => [['$default', 'value']]],
            ['method' => Operator::TYPE_DATE_SET_NOW, 'attribute' => 'updated', 'values' => []],
        ];

        foreach ($operators as $operatorData) {
            $operator = Operator::parseOperator($operatorData);
            $this->assertEquals($operatorData['method'], $operator->getMethod());
            $this->assertEquals($operatorData['attribute'], $operator->getAttribute());
            $this->assertEquals($operatorData['values'], $operator->getValues());

            // Test JSON serialization round-trip
            $json = $operator->toString();
            $parsed = Operator::parse($json);
            $this->assertEquals($operator->getMethod(), $parsed->getMethod());
            $this->assertEquals($operator->getAttribute(), $parsed->getAttribute());
            $this->assertEquals($operator->getValues(), $parsed->getValues());
        }
    }

    public function testOperatorCloning(): void
    {
        // Test cloning all new operator types
        $operators = [
            Operator::concat(' suffix'),
            Operator::concat(' suffix'), // Deprecated
            Operator::replace('old', 'new'),
            Operator::multiply(2, 100),
            Operator::divide(2, 1),
            Operator::modulo(3),
            Operator::power(2, 100),
            Operator::toggle(),
            Operator::coalesce(['$field', 'default']),
            Operator::dateSetNow(),
            Operator::compute(function ($doc) { return $doc->getAttribute('test'); }),
        ];

        foreach ($operators as $operator) {
            $cloned = clone $operator;
            $this->assertEquals($operator->getMethod(), $cloned->getMethod());
            $this->assertEquals($operator->getAttribute(), $cloned->getAttribute());
            $this->assertEquals($operator->getValues(), $cloned->getValues());

            // Ensure they are different objects
            $cloned->setAttribute('different');
            $this->assertNotEquals($operator->getAttribute(), $cloned->getAttribute());
        }
    }

    // Test edge cases and error conditions

    public function testOperatorEdgeCases(): void
    {
        // Test multiply with zero
        $operator = Operator::multiply(0);
        $this->assertEquals(0, $operator->getValue());

        // Test divide with fraction
        $operator = Operator::divide(0.5, 0.1);
        $this->assertEquals([0.5, 0.1], $operator->getValues());

        // Test concat with empty string
        $operator = Operator::concat('');
        $this->assertEquals('', $operator->getValue());

        // Test concat with empty string (deprecated)
        $operator = Operator::concat('');
        $this->assertEquals('', $operator->getValue());

        // Test replace with same strings
        $operator = Operator::replace('same', 'same');
        $this->assertEquals(['same', 'same'], $operator->getValues());

        // Test coalesce with multiple values
        $operator = Operator::coalesce(['$field1', '$field2', null]);
        $this->assertEquals(['$field1', '$field2', null], $operator->getValue());

        // Test modulo edge cases
        $operator = Operator::modulo(1.5);
        $this->assertEquals(1.5, $operator->getValue());

        // Test power with zero exponent
        $operator = Operator::power(0);
        $this->assertEquals(0, $operator->getValue());
    }

    public function testComputeOperator(): void
    {
        // Test compute operator with simple calculation
        $operator = Operator::compute(function ($doc) {
            return $doc->getAttribute('price') * $doc->getAttribute('quantity');
        });
        $this->assertEquals(Operator::TYPE_COMPUTE, $operator->getMethod());
        $this->assertTrue(is_callable($operator->getValue()));

        // Test compute operator with complex logic
        $operator = Operator::compute(function ($doc) {
            $price = $doc->getAttribute('price', 0);
            $discount = $doc->getAttribute('discount', 0);
            return $price * (1 - $discount / 100);
        });
        $this->assertTrue(is_callable($operator->getValue()));
        $this->assertEquals(1, count($operator->getValues())); // Should have exactly one callable
    }
}
