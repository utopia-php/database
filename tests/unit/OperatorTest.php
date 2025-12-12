<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;

class OperatorTest extends TestCase
{
    public function testCreate(): void
    {
        // Test basic construction
        $operator = new Operator(Operator::TYPE_INCREMENT, 'count', [1]);

        $this->assertSame(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertSame('count', $operator->getAttribute());
        $this->assertSame([1], $operator->getValues());
        $this->assertSame(1, $operator->getValue());

        // Test with different types
        $operator = new Operator(Operator::TYPE_ARRAY_APPEND, 'tags', ['php', 'database']);

        $this->assertSame(Operator::TYPE_ARRAY_APPEND, $operator->getMethod());
        $this->assertSame('tags', $operator->getAttribute());
        $this->assertSame(['php', 'database'], $operator->getValues());
        $this->assertSame('php', $operator->getValue());
    }

    public function testHelperMethods(): void
    {
        // Test increment helper
        $operator = Operator::increment(5);
        $this->assertSame(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute()); // Initially empty
        $this->assertSame([5], $operator->getValues());

        // Test decrement helper
        $operator = Operator::decrement(1);
        $this->assertSame(Operator::TYPE_DECREMENT, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute()); // Initially empty
        $this->assertSame([1], $operator->getValues());

        // Test default increment value
        $operator = Operator::increment();
        $this->assertSame(1, $operator->getValue());

        // Test string helpers
        $operator = Operator::stringConcat(' - Updated');
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([' - Updated'], $operator->getValues());

        $operator = Operator::stringReplace('old', 'new');
        $this->assertSame(Operator::TYPE_STRING_REPLACE, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['old', 'new'], $operator->getValues());

        // Test math helpers
        $operator = Operator::multiply(2, 1000);
        $this->assertSame(Operator::TYPE_MULTIPLY, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([2, 1000], $operator->getValues());

        $operator = Operator::divide(2, 1);
        $this->assertSame(Operator::TYPE_DIVIDE, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([2, 1], $operator->getValues());

        // Test boolean helper
        $operator = Operator::toggle();
        $this->assertSame(Operator::TYPE_TOGGLE, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([], $operator->getValues());

        $operator = Operator::dateSetNow();
        $this->assertSame(Operator::TYPE_DATE_SET_NOW, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([], $operator->getValues());

        // Test concat helper
        $operator = Operator::stringConcat(' - Updated');
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([' - Updated'], $operator->getValues());

        // Test modulo and power operators
        $operator = Operator::modulo(3);
        $this->assertSame(Operator::TYPE_MODULO, $operator->getMethod());
        $this->assertSame([3], $operator->getValues());

        $operator = Operator::power(2, 1000);
        $this->assertSame(Operator::TYPE_POWER, $operator->getMethod());
        $this->assertSame([2, 1000], $operator->getValues());

        // Test new array helper methods
        $operator = Operator::arrayAppend(['new', 'values']);
        $this->assertSame(Operator::TYPE_ARRAY_APPEND, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['new', 'values'], $operator->getValues());

        $operator = Operator::arrayPrepend(['first', 'second']);
        $this->assertSame(Operator::TYPE_ARRAY_PREPEND, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['first', 'second'], $operator->getValues());

        $operator = Operator::arrayInsert(2, 'inserted');
        $this->assertSame(Operator::TYPE_ARRAY_INSERT, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([2, 'inserted'], $operator->getValues());

        $operator = Operator::arrayRemove('unwanted');
        $this->assertSame(Operator::TYPE_ARRAY_REMOVE, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['unwanted'], $operator->getValues());
    }

    public function testSetters(): void
    {
        $operator = new Operator(Operator::TYPE_INCREMENT, 'test', [1]);

        // Test setMethod
        $operator->setMethod(Operator::TYPE_DECREMENT);
        $this->assertSame(Operator::TYPE_DECREMENT, $operator->getMethod());

        // Test setAttribute
        $operator->setAttribute('newAttribute');
        $this->assertSame('newAttribute', $operator->getAttribute());

        // Test setValues
        $operator->setValues([10, 20]);
        $this->assertSame([10, 20], $operator->getValues());

        // Test setValue
        $operator->setValue(50);
        $this->assertSame([50], $operator->getValues());
        $this->assertSame(50, $operator->getValue());
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
        $concatOp = Operator::stringConcat('suffix');
        $this->assertFalse($concatOp->isNumericOperation());
        $this->assertFalse($concatOp->isArrayOperation());
        $this->assertTrue($concatOp->isStringOperation());

        $replaceOp = Operator::stringReplace('old', 'new');
        $this->assertFalse($replaceOp->isNumericOperation());
        $this->assertFalse($replaceOp->isArrayOperation());
        $this->assertTrue($replaceOp->isStringOperation());

        // Test boolean operations
        $toggleOp = Operator::toggle();
        $this->assertFalse($toggleOp->isNumericOperation());
        $this->assertFalse($toggleOp->isArrayOperation());
        $this->assertTrue($toggleOp->isBooleanOperation());


        // Test date operations
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
        $this->assertTrue(Operator::isMethod(Operator::TYPE_STRING_CONCAT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_STRING_REPLACE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_TOGGLE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_STRING_CONCAT));
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
        $this->assertSame('count', $operators['count']->getAttribute());
        $this->assertSame('tags', $operators['tags']->getAttribute());

        // Check updates
        $this->assertSame(['name' => 'John', 'age' => 30], $updates);
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
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
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
        $this->assertSame(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertSame('score', $operator->getAttribute());
        $this->assertSame([5], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertSame('score', $operator->getAttribute());
        $this->assertSame([5], $operator->getValues());
    }

    public function testParseOperators(): void
    {
        $json1 = json_encode(['method' => Operator::TYPE_INCREMENT, 'attribute' => 'count', 'values' => [1]]);
        $json2 = json_encode(['method' => Operator::TYPE_ARRAY_APPEND, 'attribute' => 'tags', 'values' => ['new']]);

        $this->assertIsString($json1);
        $this->assertIsString($json2);

        $operators = [$json1, $json2];

        $parsed = Operator::parseOperators($operators);
        $this->assertCount(2, $parsed);
        $this->assertInstanceOf(Operator::class, $parsed[0]);
        $this->assertInstanceOf(Operator::class, $parsed[1]);
        $this->assertSame(Operator::TYPE_INCREMENT, $parsed[0]->getMethod());
        $this->assertSame(Operator::TYPE_ARRAY_APPEND, $parsed[1]->getMethod());
    }

    public function testClone(): void
    {
        $operator1 = Operator::increment(5);
        $operator2 = clone $operator1;

        $this->assertSame($operator1->getMethod(), $operator2->getMethod());
        $this->assertSame($operator1->getAttribute(), $operator2->getAttribute());
        $this->assertSame($operator1->getValues(), $operator2->getValues());

        // Ensure they are different objects
        $operator2->setMethod(Operator::TYPE_DECREMENT);
        $this->assertSame(Operator::TYPE_INCREMENT, $operator1->getMethod());
        $this->assertSame(Operator::TYPE_DECREMENT, $operator2->getMethod());
    }

    public function testGetValueWithDefault(): void
    {
        $operator = Operator::increment(5);
        $this->assertSame(5, $operator->getValue());
        $this->assertSame(5, $operator->getValue('default'));

        $emptyOperator = new Operator(Operator::TYPE_INCREMENT, 'count', []);
        $this->assertSame('default', $emptyOperator->getValue('default'));
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
        $this->assertSame(Operator::TYPE_INCREMENT, $operator->getMethod());
        $this->assertSame([5, 10], $operator->getValues());

        // Test increment without max (should be same as original behavior)
        $operator = Operator::increment(5);
        $this->assertSame([5], $operator->getValues());
    }

    public function testDecrementWithMin(): void
    {
        // Test decrement with min limit
        $operator = Operator::decrement(3, 0);
        $this->assertSame(Operator::TYPE_DECREMENT, $operator->getMethod());
        $this->assertSame([3, 0], $operator->getValues());

        // Test decrement without min (should be same as original behavior)
        $operator = Operator::decrement(3);
        $this->assertSame([3], $operator->getValues());
    }

    public function testArrayRemove(): void
    {
        $operator = Operator::arrayRemove('spam');
        $this->assertSame(Operator::TYPE_ARRAY_REMOVE, $operator->getMethod());
        $this->assertSame(['spam'], $operator->getValues());
        $this->assertSame('spam', $operator->getValue());
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
            'title' => Operator::stringConcat(' - Updated'),
            'content' => Operator::stringReplace('old', 'new'),
            'views' => Operator::multiply(2, 1000),
            'rating' => Operator::divide(2, 1),
            'featured' => Operator::toggle(),
            'last_modified' => Operator::dateSetNow(),
            'title_prefix' => Operator::stringConcat(' - Updated'),
            'views_modulo' => Operator::modulo(3),
            'score_power' => Operator::power(2, 1000),
            'age' => 30
        ];

        $result = Operator::extractOperators($data);

        $operators = $result['operators'];
        $updates = $result['updates'];

        // Check operators count (all fields except 'name' and 'age')
        $this->assertCount(15, $operators);

        // Check that array methods are properly extracted
        $this->assertInstanceOf(Operator::class, $operators['tags']);
        $this->assertSame('tags', $operators['tags']->getAttribute());
        $this->assertSame(Operator::TYPE_ARRAY_APPEND, $operators['tags']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['blacklist']);
        $this->assertSame('blacklist', $operators['blacklist']->getAttribute());
        $this->assertSame(Operator::TYPE_ARRAY_REMOVE, $operators['blacklist']->getMethod());

        // Check string operators
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operators['title']->getMethod());
        $this->assertSame(Operator::TYPE_STRING_REPLACE, $operators['content']->getMethod());

        // Check math operators
        $this->assertSame(Operator::TYPE_MULTIPLY, $operators['views']->getMethod());
        $this->assertSame(Operator::TYPE_DIVIDE, $operators['rating']->getMethod());

        // Check boolean operator
        $this->assertSame(Operator::TYPE_TOGGLE, $operators['featured']->getMethod());

        // Check new operators
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operators['title_prefix']->getMethod());
        $this->assertSame(Operator::TYPE_MODULO, $operators['views_modulo']->getMethod());
        $this->assertSame(Operator::TYPE_POWER, $operators['score_power']->getMethod());

        // Check date operator
        $this->assertSame(Operator::TYPE_DATE_SET_NOW, $operators['last_modified']->getMethod());

        // Check that max/min values are preserved
        $this->assertSame([5, 100], $operators['count']->getValues());
        $this->assertSame([1, 0], $operators['score']->getValues());

        // Check updates
        $this->assertSame(['name' => 'John', 'age' => 30], $updates);
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
        $this->assertSame(Operator::TYPE_ARRAY_REMOVE, $operator->getMethod());
        $this->assertSame('blacklist', $operator->getAttribute());
        $this->assertSame(['spam'], $operator->getValues());

        // Test parsing increment with max
        $incrementWithMax = [
            'method' => Operator::TYPE_INCREMENT,
            'attribute' => 'score',
            'values' => [1, 10]
        ];

        $operator = Operator::parseOperator($incrementWithMax);
        $this->assertSame([1, 10], $operator->getValues());
    }

    // Edge case tests

    public function testIncrementMaxLimitEdgeCases(): void
    {
        // Test that max limit is properly stored
        $operator = Operator::increment(5, 10);
        $values = $operator->getValues();
        $this->assertSame(5, $values[0]); // increment value
        $this->assertSame(10, $values[1]); // max limit

        // Test with float values
        $operator = Operator::increment(1.5, 9.9);
        $values = $operator->getValues();
        $this->assertSame(1.5, $values[0]);
        $this->assertSame(9.9, $values[1]);

        // Test with negative max (edge case)
        $operator = Operator::increment(1, -5);
        $values = $operator->getValues();
        $this->assertSame(1, $values[0]);
        $this->assertSame(-5, $values[1]);
    }

    public function testDecrementMinLimitEdgeCases(): void
    {
        // Test that min limit is properly stored
        $operator = Operator::decrement(3, 0);
        $values = $operator->getValues();
        $this->assertSame(3, $values[0]); // decrement value
        $this->assertSame(0, $values[1]); // min limit

        // Test with float values
        $operator = Operator::decrement(2.5, 0.1);
        $values = $operator->getValues();
        $this->assertSame(2.5, $values[0]);
        $this->assertSame(0.1, $values[1]);

        // Test with negative min
        $operator = Operator::decrement(1, -10);
        $values = $operator->getValues();
        $this->assertSame(1, $values[0]);
        $this->assertSame(-10, $values[1]);
    }

    public function testArrayRemoveEdgeCases(): void
    {
        // Test removing various types of values
        $operator = Operator::arrayRemove('string');
        $this->assertSame('string', $operator->getValue());

        $operator = Operator::arrayRemove(42);
        $this->assertSame(42, $operator->getValue());

        $operator = Operator::arrayRemove(null);
        $this->assertSame(null, $operator->getValue());

        $operator = Operator::arrayRemove(true);
        $this->assertSame(true, $operator->getValue());

        // Test removing array (nested array)
        $operator = Operator::arrayRemove(['nested']);
        $this->assertSame(['nested'], $operator->getValue());
    }

    public function testOperatorCloningWithNewMethods(): void
    {
        // Test cloning increment with max
        $operator1 = Operator::increment(5, 10);
        $operator2 = clone $operator1;

        $this->assertSame($operator1->getValues(), $operator2->getValues());
        $this->assertSame([5, 10], $operator2->getValues());

        // Modify one to ensure they're separate objects
        $operator2->setValues([3, 8]);
        $this->assertSame([5, 10], $operator1->getValues());
        $this->assertSame([3, 8], $operator2->getValues());

        // Test cloning arrayRemove
        $removeOp1 = Operator::arrayRemove('spam');
        $removeOp2 = clone $removeOp1;

        $this->assertSame($removeOp1->getValue(), $removeOp2->getValue());
        $removeOp2->setValue('ham');
        $this->assertSame('spam', $removeOp1->getValue());
        $this->assertSame('ham', $removeOp2->getValue());
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
        $this->assertSame($expected, $array);

        // Test serialization of arrayRemove
        $operator = Operator::arrayRemove('unwanted');
        $operator->setAttribute('blacklist');

        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_ARRAY_REMOVE,
            'attribute' => 'blacklist',
            'values' => ['unwanted']
        ];
        $this->assertSame($expected, $array);

        // Ensure JSON serialization works
        $json = $operator->toString();
        $this->assertJson($json);
        $decoded = json_decode($json, true);
        $this->assertSame($expected, $decoded);
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
            'concat' => Operator::stringConcat(' suffix'),
            'replace' => Operator::stringReplace('old', 'new'),
            'toggle' => Operator::toggle(),
            'dateSetNow' => Operator::dateSetNow(),
            'modulo' => Operator::modulo(3),
            'power' => Operator::power(2),
            'remove' => Operator::arrayRemove('bad'),
        ];

        $result = Operator::extractOperators($data);
        $operators = $result['operators'];

        $this->assertCount(12, $operators);

        // Verify each operator type
        $this->assertSame(Operator::TYPE_ARRAY_APPEND, $operators['arrayAppend']->getMethod());
        $this->assertSame(Operator::TYPE_INCREMENT, $operators['incrementWithMax']->getMethod());
        $this->assertSame([1, 10], $operators['incrementWithMax']->getValues());
        $this->assertSame(Operator::TYPE_DECREMENT, $operators['decrementWithMin']->getMethod());
        $this->assertSame([2, 0], $operators['decrementWithMin']->getValues());
        $this->assertSame(Operator::TYPE_MULTIPLY, $operators['multiply']->getMethod());
        $this->assertSame([3, 100], $operators['multiply']->getValues());
        $this->assertSame(Operator::TYPE_DIVIDE, $operators['divide']->getMethod());
        $this->assertSame([2, 1], $operators['divide']->getValues());
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operators['concat']->getMethod());
        $this->assertSame(Operator::TYPE_STRING_REPLACE, $operators['replace']->getMethod());
        $this->assertSame(Operator::TYPE_TOGGLE, $operators['toggle']->getMethod());
        $this->assertSame(Operator::TYPE_DATE_SET_NOW, $operators['dateSetNow']->getMethod());
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operators['concat']->getMethod());
        $this->assertSame(Operator::TYPE_MODULO, $operators['modulo']->getMethod());
        $this->assertSame(Operator::TYPE_POWER, $operators['power']->getMethod());
        $this->assertSame(Operator::TYPE_ARRAY_REMOVE, $operators['remove']->getMethod());
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
        $this->assertTrue(Operator::stringConcat('test')->isStringOperation());
        $this->assertTrue(Operator::stringReplace('old', 'new')->isStringOperation());
        $this->assertFalse(Operator::stringConcat('test')->isNumericOperation());
        $this->assertFalse(Operator::stringReplace('old', 'new')->isArrayOperation());

        // Test boolean operations
        $this->assertTrue(Operator::toggle()->isBooleanOperation());
        $this->assertFalse(Operator::toggle()->isNumericOperation());
        $this->assertFalse(Operator::toggle()->isArrayOperation());


        // Test date operations
        $this->assertTrue(Operator::dateSetNow()->isDateOperation());
        $this->assertFalse(Operator::dateSetNow()->isNumericOperation());
    }

    // New comprehensive tests for all operators

    public function testStringOperators(): void
    {
        // Test concat operator
        $operator = Operator::stringConcat(' - Updated');
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operator->getMethod());
        $this->assertSame([' - Updated'], $operator->getValues());
        $this->assertSame(' - Updated', $operator->getValue());
        $this->assertSame('', $operator->getAttribute());

        // Test concat with different values
        $operator = Operator::stringConcat('prefix-');
        $this->assertSame(Operator::TYPE_STRING_CONCAT, $operator->getMethod());
        $this->assertSame(['prefix-'], $operator->getValues());
        $this->assertSame('prefix-', $operator->getValue());

        // Test replace operator
        $operator = Operator::stringReplace('old', 'new');
        $this->assertSame(Operator::TYPE_STRING_REPLACE, $operator->getMethod());
        $this->assertSame(['old', 'new'], $operator->getValues());
        $this->assertSame('old', $operator->getValue());
    }

    public function testMathOperators(): void
    {
        // Test multiply operator
        $operator = Operator::multiply(2.5, 100);
        $this->assertSame(Operator::TYPE_MULTIPLY, $operator->getMethod());
        $this->assertSame([2.5, 100], $operator->getValues());
        $this->assertSame(2.5, $operator->getValue());

        // Test multiply without max
        $operator = Operator::multiply(3);
        $this->assertSame([3], $operator->getValues());

        // Test divide operator
        $operator = Operator::divide(2, 1);
        $this->assertSame(Operator::TYPE_DIVIDE, $operator->getMethod());
        $this->assertSame([2, 1], $operator->getValues());
        $this->assertSame(2, $operator->getValue());

        // Test divide without min
        $operator = Operator::divide(4);
        $this->assertSame([4], $operator->getValues());

        // Test modulo operator
        $operator = Operator::modulo(3);
        $this->assertSame(Operator::TYPE_MODULO, $operator->getMethod());
        $this->assertSame([3], $operator->getValues());
        $this->assertSame(3, $operator->getValue());

        // Test power operator
        $operator = Operator::power(2, 1000);
        $this->assertSame(Operator::TYPE_POWER, $operator->getMethod());
        $this->assertSame([2, 1000], $operator->getValues());
        $this->assertSame(2, $operator->getValue());

        // Test power without max
        $operator = Operator::power(3);
        $this->assertSame([3], $operator->getValues());
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
        $this->assertSame(Operator::TYPE_TOGGLE, $operator->getMethod());
        $this->assertSame([], $operator->getValues());
        $this->assertNull($operator->getValue());
    }


    public function testUtilityOperators(): void
    {
        // Test dateSetNow
        $operator = Operator::dateSetNow();
        $this->assertSame(Operator::TYPE_DATE_SET_NOW, $operator->getMethod());
        $this->assertSame([], $operator->getValues());
        $this->assertNull($operator->getValue());
    }


    public function testNewOperatorParsing(): void
    {
        // Test parsing all new operators
        $operators = [
            ['method' => Operator::TYPE_STRING_CONCAT, 'attribute' => 'title', 'values' => [' - Updated']],
            ['method' => Operator::TYPE_STRING_CONCAT, 'attribute' => 'subtitle', 'values' => [' - Updated']],
            ['method' => Operator::TYPE_STRING_REPLACE, 'attribute' => 'content', 'values' => ['old', 'new']],
            ['method' => Operator::TYPE_MULTIPLY, 'attribute' => 'score', 'values' => [2, 100]],
            ['method' => Operator::TYPE_DIVIDE, 'attribute' => 'rating', 'values' => [2, 1]],
            ['method' => Operator::TYPE_MODULO, 'attribute' => 'remainder', 'values' => [3]],
            ['method' => Operator::TYPE_POWER, 'attribute' => 'exponential', 'values' => [2, 1000]],
            ['method' => Operator::TYPE_TOGGLE, 'attribute' => 'active', 'values' => []],
            ['method' => Operator::TYPE_DATE_SET_NOW, 'attribute' => 'updated', 'values' => []],
        ];

        foreach ($operators as $operatorData) {
            $operator = Operator::parseOperator($operatorData);
            $this->assertSame($operatorData['method'], $operator->getMethod());
            $this->assertSame($operatorData['attribute'], $operator->getAttribute());
            $this->assertSame($operatorData['values'], $operator->getValues());

            // Test JSON serialization round-trip
            $json = $operator->toString();
            $parsed = Operator::parse($json);
            $this->assertSame($operator->getMethod(), $parsed->getMethod());
            $this->assertSame($operator->getAttribute(), $parsed->getAttribute());
            $this->assertSame($operator->getValues(), $parsed->getValues());
        }
    }

    public function testOperatorCloning(): void
    {
        // Test cloning all new operator types
        $operators = [
            Operator::stringConcat(' suffix'),
            Operator::stringReplace('old', 'new'),
            Operator::multiply(2, 100),
            Operator::divide(2, 1),
            Operator::modulo(3),
            Operator::power(2, 100),
            Operator::toggle(),
            Operator::dateSetNow(),
        ];

        foreach ($operators as $operator) {
            $cloned = clone $operator;
            $this->assertSame($operator->getMethod(), $cloned->getMethod());
            $this->assertSame($operator->getAttribute(), $cloned->getAttribute());
            $this->assertSame($operator->getValues(), $cloned->getValues());

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
        $this->assertSame(0, $operator->getValue());

        // Test divide with fraction
        $operator = Operator::divide(0.5, 0.1);
        $this->assertSame([0.5, 0.1], $operator->getValues());

        // Test concat with empty string
        $operator = Operator::stringConcat('');
        $this->assertSame('', $operator->getValue());

        // Test replace with same strings
        $operator = Operator::stringReplace('same', 'same');
        $this->assertSame(['same', 'same'], $operator->getValues());

        // Test modulo edge cases
        $operator = Operator::modulo(1.5);
        $this->assertSame(1.5, $operator->getValue());

        // Test power with zero exponent
        $operator = Operator::power(0);
        $this->assertSame(0, $operator->getValue());
    }

    public function testPowerOperatorWithMax(): void
    {
        // Test power with max limit
        $operator = Operator::power(2, 1000);
        $this->assertSame(Operator::TYPE_POWER, $operator->getMethod());
        $this->assertSame([2, 1000], $operator->getValues());

        // Test power without max
        $operator = Operator::power(3);
        $this->assertSame([3], $operator->getValues());
    }

    public function testOperatorTypeValidation(): void
    {
        // Test that operators have proper type checking methods
        $numericOp = Operator::power(2);
        $this->assertTrue($numericOp->isNumericOperation());
        $this->assertFalse($numericOp->isArrayOperation());
        $this->assertFalse($numericOp->isStringOperation());
        $this->assertFalse($numericOp->isBooleanOperation());
        $this->assertFalse($numericOp->isDateOperation());

        $moduloOp = Operator::modulo(5);
        $this->assertTrue($moduloOp->isNumericOperation());
        $this->assertFalse($moduloOp->isArrayOperation());
    }

    // Tests for arrayUnique() method
    public function testArrayUnique(): void
    {
        // Test basic creation
        $operator = Operator::arrayUnique();
        $this->assertSame(Operator::TYPE_ARRAY_UNIQUE, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([], $operator->getValues());
        $this->assertNull($operator->getValue());

        // Test type checking
        $this->assertTrue($operator->isArrayOperation());
        $this->assertFalse($operator->isNumericOperation());
        $this->assertFalse($operator->isStringOperation());
        $this->assertFalse($operator->isBooleanOperation());
        $this->assertFalse($operator->isDateOperation());
    }

    public function testArrayUniqueSerialization(): void
    {
        $operator = Operator::arrayUnique();
        $operator->setAttribute('tags');

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_ARRAY_UNIQUE,
            'attribute' => 'tags',
            'values' => []
        ];
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
    }

    public function testArrayUniqueParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_ARRAY_UNIQUE,
            'attribute' => 'items',
            'values' => []
        ];

        $operator = Operator::parseOperator($array);
        $this->assertSame(Operator::TYPE_ARRAY_UNIQUE, $operator->getMethod());
        $this->assertSame('items', $operator->getAttribute());
        $this->assertSame([], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_ARRAY_UNIQUE, $operator->getMethod());
        $this->assertSame('items', $operator->getAttribute());
        $this->assertSame([], $operator->getValues());
    }

    public function testArrayUniqueCloning(): void
    {
        $operator1 = Operator::arrayUnique();
        $operator1->setAttribute('original');
        $operator2 = clone $operator1;

        $this->assertSame($operator1->getMethod(), $operator2->getMethod());
        $this->assertSame($operator1->getAttribute(), $operator2->getAttribute());
        $this->assertSame($operator1->getValues(), $operator2->getValues());

        // Ensure they are different objects
        $operator2->setAttribute('cloned');
        $this->assertSame('original', $operator1->getAttribute());
        $this->assertSame('cloned', $operator2->getAttribute());
    }

    // Tests for arrayIntersect() method
    public function testArrayIntersect(): void
    {
        // Test basic creation
        $operator = Operator::arrayIntersect(['a', 'b', 'c']);
        $this->assertSame(Operator::TYPE_ARRAY_INTERSECT, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['a', 'b', 'c'], $operator->getValues());
        $this->assertSame('a', $operator->getValue());

        // Test type checking
        $this->assertTrue($operator->isArrayOperation());
        $this->assertFalse($operator->isNumericOperation());
        $this->assertFalse($operator->isStringOperation());
        $this->assertFalse($operator->isBooleanOperation());
        $this->assertFalse($operator->isDateOperation());
    }

    public function testArrayIntersectEdgeCases(): void
    {
        // Test with empty array
        $operator = Operator::arrayIntersect([]);
        $this->assertSame([], $operator->getValues());
        $this->assertNull($operator->getValue());

        // Test with numeric values
        $operator = Operator::arrayIntersect([1, 2, 3]);
        $this->assertSame([1, 2, 3], $operator->getValues());
        $this->assertSame(1, $operator->getValue());

        // Test with mixed types
        $operator = Operator::arrayIntersect(['string', 42, true, null]);
        $this->assertSame(['string', 42, true, null], $operator->getValues());
        $this->assertSame('string', $operator->getValue());

        // Test with nested arrays
        $operator = Operator::arrayIntersect([['nested'], ['array']]);
        $this->assertSame([['nested'], ['array']], $operator->getValues());
    }

    public function testArrayIntersectSerialization(): void
    {
        $operator = Operator::arrayIntersect(['x', 'y', 'z']);
        $operator->setAttribute('common');

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_ARRAY_INTERSECT,
            'attribute' => 'common',
            'values' => ['x', 'y', 'z']
        ];
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
    }

    public function testArrayIntersectParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_ARRAY_INTERSECT,
            'attribute' => 'allowed',
            'values' => ['admin', 'user']
        ];

        $operator = Operator::parseOperator($array);
        $this->assertSame(Operator::TYPE_ARRAY_INTERSECT, $operator->getMethod());
        $this->assertSame('allowed', $operator->getAttribute());
        $this->assertSame(['admin', 'user'], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_ARRAY_INTERSECT, $operator->getMethod());
        $this->assertSame('allowed', $operator->getAttribute());
        $this->assertSame(['admin', 'user'], $operator->getValues());
    }

    // Tests for arrayDiff() method
    public function testArrayDiff(): void
    {
        // Test basic creation
        $operator = Operator::arrayDiff(['remove', 'these']);
        $this->assertSame(Operator::TYPE_ARRAY_DIFF, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['remove', 'these'], $operator->getValues());
        $this->assertSame('remove', $operator->getValue());

        // Test type checking
        $this->assertTrue($operator->isArrayOperation());
        $this->assertFalse($operator->isNumericOperation());
        $this->assertFalse($operator->isStringOperation());
        $this->assertFalse($operator->isBooleanOperation());
        $this->assertFalse($operator->isDateOperation());
    }

    public function testArrayDiffEdgeCases(): void
    {
        // Test with empty array
        $operator = Operator::arrayDiff([]);
        $this->assertSame([], $operator->getValues());
        $this->assertNull($operator->getValue());

        // Test with single value
        $operator = Operator::arrayDiff(['only-one']);
        $this->assertSame(['only-one'], $operator->getValues());
        $this->assertSame('only-one', $operator->getValue());

        // Test with numeric values
        $operator = Operator::arrayDiff([10, 20, 30]);
        $this->assertSame([10, 20, 30], $operator->getValues());

        // Test with mixed types
        $operator = Operator::arrayDiff([false, 0, '']);
        $this->assertSame([false, 0, ''], $operator->getValues());
    }

    public function testArrayDiffSerialization(): void
    {
        $operator = Operator::arrayDiff(['spam', 'unwanted']);
        $operator->setAttribute('blocklist');

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_ARRAY_DIFF,
            'attribute' => 'blocklist',
            'values' => ['spam', 'unwanted']
        ];
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
    }

    public function testArrayDiffParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_ARRAY_DIFF,
            'attribute' => 'exclude',
            'values' => ['bad', 'invalid']
        ];

        $operator = Operator::parseOperator($array);
        $this->assertSame(Operator::TYPE_ARRAY_DIFF, $operator->getMethod());
        $this->assertSame('exclude', $operator->getAttribute());
        $this->assertSame(['bad', 'invalid'], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_ARRAY_DIFF, $operator->getMethod());
        $this->assertSame('exclude', $operator->getAttribute());
        $this->assertSame(['bad', 'invalid'], $operator->getValues());
    }

    // Tests for arrayFilter() method
    public function testArrayFilter(): void
    {
        // Test basic creation with equals condition
        $operator = Operator::arrayFilter('equals', 'active');
        $this->assertSame(Operator::TYPE_ARRAY_FILTER, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame(['equals', 'active'], $operator->getValues());
        $this->assertSame('equals', $operator->getValue());

        // Test type checking
        $this->assertTrue($operator->isArrayOperation());
        $this->assertFalse($operator->isNumericOperation());
        $this->assertFalse($operator->isStringOperation());
        $this->assertFalse($operator->isBooleanOperation());
        $this->assertFalse($operator->isDateOperation());
    }

    public function testArrayFilterConditions(): void
    {
        // Test different filter conditions
        $operator = Operator::arrayFilter('notEquals', 'inactive');
        $this->assertSame(['notEquals', 'inactive'], $operator->getValues());

        $operator = Operator::arrayFilter('greaterThan', 100);
        $this->assertSame(['greaterThan', 100], $operator->getValues());

        $operator = Operator::arrayFilter('lessThan', 50);
        $this->assertSame(['lessThan', 50], $operator->getValues());

        // Test null/notNull conditions (value parameter not used)
        $operator = Operator::arrayFilter('null');
        $this->assertSame(['null', null], $operator->getValues());

        $operator = Operator::arrayFilter('notNull');
        $this->assertSame(['notNull', null], $operator->getValues());

        // Test with explicit null value
        $operator = Operator::arrayFilter('null', null);
        $this->assertSame(['null', null], $operator->getValues());
    }

    public function testArrayFilterEdgeCases(): void
    {
        // Test with boolean value
        $operator = Operator::arrayFilter('equals', true);
        $this->assertSame(['equals', true], $operator->getValues());

        // Test with zero value
        $operator = Operator::arrayFilter('equals', 0);
        $this->assertSame(['equals', 0], $operator->getValues());

        // Test with empty string value
        $operator = Operator::arrayFilter('equals', '');
        $this->assertSame(['equals', ''], $operator->getValues());

        // Test with array value
        $operator = Operator::arrayFilter('equals', ['nested', 'array']);
        $this->assertSame(['equals', ['nested', 'array']], $operator->getValues());
    }

    public function testArrayFilterSerialization(): void
    {
        $operator = Operator::arrayFilter('greaterThan', 100);
        $operator->setAttribute('scores');

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_ARRAY_FILTER,
            'attribute' => 'scores',
            'values' => ['greaterThan', 100]
        ];
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
    }

    public function testArrayFilterParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_ARRAY_FILTER,
            'attribute' => 'ratings',
            'values' => ['lessThan', 3]
        ];

        $operator = Operator::parseOperator($array);
        $this->assertSame(Operator::TYPE_ARRAY_FILTER, $operator->getMethod());
        $this->assertSame('ratings', $operator->getAttribute());
        $this->assertSame(['lessThan', 3], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_ARRAY_FILTER, $operator->getMethod());
        $this->assertSame('ratings', $operator->getAttribute());
        $this->assertSame(['lessThan', 3], $operator->getValues());
    }

    // Tests for dateAddDays() method
    public function testDateAddDays(): void
    {
        // Test basic creation
        $operator = Operator::dateAddDays(7);
        $this->assertSame(Operator::TYPE_DATE_ADD_DAYS, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([7], $operator->getValues());
        $this->assertSame(7, $operator->getValue());

        // Test type checking
        $this->assertTrue($operator->isDateOperation());
        $this->assertFalse($operator->isNumericOperation());
        $this->assertFalse($operator->isArrayOperation());
        $this->assertFalse($operator->isStringOperation());
        $this->assertFalse($operator->isBooleanOperation());
    }

    public function testDateAddDaysEdgeCases(): void
    {
        // Test with zero days
        $operator = Operator::dateAddDays(0);
        $this->assertSame([0], $operator->getValues());
        $this->assertSame(0, $operator->getValue());

        // Test with negative days (should work per the docblock)
        $operator = Operator::dateAddDays(-5);
        $this->assertSame([-5], $operator->getValues());
        $this->assertSame(-5, $operator->getValue());

        // Test with large positive number
        $operator = Operator::dateAddDays(365);
        $this->assertSame([365], $operator->getValues());
        $this->assertSame(365, $operator->getValue());

        // Test with large negative number
        $operator = Operator::dateAddDays(-1000);
        $this->assertSame([-1000], $operator->getValues());
        $this->assertSame(-1000, $operator->getValue());
    }

    public function testDateAddDaysSerialization(): void
    {
        $operator = Operator::dateAddDays(30);
        $operator->setAttribute('expiresAt');

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_DATE_ADD_DAYS,
            'attribute' => 'expiresAt',
            'values' => [30]
        ];
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
    }

    public function testDateAddDaysParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_DATE_ADD_DAYS,
            'attribute' => 'scheduledFor',
            'values' => [14]
        ];

        $operator = Operator::parseOperator($array);
        $this->assertSame(Operator::TYPE_DATE_ADD_DAYS, $operator->getMethod());
        $this->assertSame('scheduledFor', $operator->getAttribute());
        $this->assertSame([14], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_DATE_ADD_DAYS, $operator->getMethod());
        $this->assertSame('scheduledFor', $operator->getAttribute());
        $this->assertSame([14], $operator->getValues());
    }

    public function testDateAddDaysCloning(): void
    {
        $operator1 = Operator::dateAddDays(10);
        $operator1->setAttribute('date1');
        $operator2 = clone $operator1;

        $this->assertSame($operator1->getMethod(), $operator2->getMethod());
        $this->assertSame($operator1->getAttribute(), $operator2->getAttribute());
        $this->assertSame($operator1->getValues(), $operator2->getValues());

        // Ensure they are different objects
        $operator2->setValues([20]);
        $this->assertSame([10], $operator1->getValues());
        $this->assertSame([20], $operator2->getValues());
    }

    // Tests for dateSubDays() method
    public function testDateSubDays(): void
    {
        // Test basic creation
        $operator = Operator::dateSubDays(3);
        $this->assertSame(Operator::TYPE_DATE_SUB_DAYS, $operator->getMethod());
        $this->assertSame('', $operator->getAttribute());
        $this->assertSame([3], $operator->getValues());
        $this->assertSame(3, $operator->getValue());

        // Test type checking
        $this->assertTrue($operator->isDateOperation());
        $this->assertFalse($operator->isNumericOperation());
        $this->assertFalse($operator->isArrayOperation());
        $this->assertFalse($operator->isStringOperation());
        $this->assertFalse($operator->isBooleanOperation());
    }

    public function testDateSubDaysEdgeCases(): void
    {
        // Test with zero days
        $operator = Operator::dateSubDays(0);
        $this->assertSame([0], $operator->getValues());
        $this->assertSame(0, $operator->getValue());

        // Test with single day
        $operator = Operator::dateSubDays(1);
        $this->assertSame([1], $operator->getValues());
        $this->assertSame(1, $operator->getValue());

        // Test with large number of days
        $operator = Operator::dateSubDays(90);
        $this->assertSame([90], $operator->getValues());
        $this->assertSame(90, $operator->getValue());

        // Test with very large number
        $operator = Operator::dateSubDays(10000);
        $this->assertSame([10000], $operator->getValues());
        $this->assertSame(10000, $operator->getValue());
    }

    public function testDateSubDaysSerialization(): void
    {
        $operator = Operator::dateSubDays(7);
        $operator->setAttribute('reminderDate');

        // Test toArray
        $array = $operator->toArray();
        $expected = [
            'method' => Operator::TYPE_DATE_SUB_DAYS,
            'attribute' => 'reminderDate',
            'values' => [7]
        ];
        $this->assertSame($expected, $array);

        // Test toString
        $string = $operator->toString();
        $this->assertJson($string);
        $decoded = json_decode($string, true);
        $this->assertSame($expected, $decoded);
    }

    public function testDateSubDaysParsing(): void
    {
        // Test parseOperator from array
        $array = [
            'method' => Operator::TYPE_DATE_SUB_DAYS,
            'attribute' => 'dueDate',
            'values' => [5]
        ];

        $operator = Operator::parseOperator($array);
        $this->assertSame(Operator::TYPE_DATE_SUB_DAYS, $operator->getMethod());
        $this->assertSame('dueDate', $operator->getAttribute());
        $this->assertSame([5], $operator->getValues());

        // Test parse from JSON string
        $json = json_encode($array);
        $this->assertIsString($json);
        $operator = Operator::parse($json);
        $this->assertSame(Operator::TYPE_DATE_SUB_DAYS, $operator->getMethod());
        $this->assertSame('dueDate', $operator->getAttribute());
        $this->assertSame([5], $operator->getValues());
    }

    public function testDateSubDaysCloning(): void
    {
        $operator1 = Operator::dateSubDays(15);
        $operator1->setAttribute('date1');
        $operator2 = clone $operator1;

        $this->assertSame($operator1->getMethod(), $operator2->getMethod());
        $this->assertSame($operator1->getAttribute(), $operator2->getAttribute());
        $this->assertSame($operator1->getValues(), $operator2->getValues());

        // Ensure they are different objects
        $operator2->setValues([25]);
        $this->assertSame([15], $operator1->getValues());
        $this->assertSame([25], $operator2->getValues());
    }

    // Integration tests for all six new operators
    public function testIsMethodForNewOperators(): void
    {
        // Test that all new operators are valid methods
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_UNIQUE));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_INTERSECT));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_DIFF));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_ARRAY_FILTER));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_DATE_ADD_DAYS));
        $this->assertTrue(Operator::isMethod(Operator::TYPE_DATE_SUB_DAYS));
    }

    public function testExtractOperatorsWithNewOperators(): void
    {
        $data = [
            'uniqueTags' => Operator::arrayUnique(),
            'commonItems' => Operator::arrayIntersect(['a', 'b']),
            'filteredList' => Operator::arrayDiff(['spam']),
            'activeUsers' => Operator::arrayFilter('equals', true),
            'expiry' => Operator::dateAddDays(30),
            'reminder' => Operator::dateSubDays(7),
            'name' => 'Regular value',
        ];

        $result = Operator::extractOperators($data);

        $operators = $result['operators'];
        $updates = $result['updates'];

        // Check operators count
        $this->assertCount(6, $operators);

        // Check each operator type
        $this->assertInstanceOf(Operator::class, $operators['uniqueTags']);
        $this->assertSame(Operator::TYPE_ARRAY_UNIQUE, $operators['uniqueTags']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['commonItems']);
        $this->assertSame(Operator::TYPE_ARRAY_INTERSECT, $operators['commonItems']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['filteredList']);
        $this->assertSame(Operator::TYPE_ARRAY_DIFF, $operators['filteredList']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['activeUsers']);
        $this->assertSame(Operator::TYPE_ARRAY_FILTER, $operators['activeUsers']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['expiry']);
        $this->assertSame(Operator::TYPE_DATE_ADD_DAYS, $operators['expiry']->getMethod());

        $this->assertInstanceOf(Operator::class, $operators['reminder']);
        $this->assertSame(Operator::TYPE_DATE_SUB_DAYS, $operators['reminder']->getMethod());

        // Check updates
        $this->assertSame(['name' => 'Regular value'], $updates);
    }
}
