<?php

namespace Tests\Unit\Operator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Validator\Operator as OperatorValidator;
use Utopia\Query\Schema\ColumnType;

class OperatorValidationTest extends TestCase
{
    private function makeCollection(array $attributes): Document
    {
        $attrDocs = [];
        foreach ($attributes as $attr) {
            $attrDocs[] = $attr->toDocument();
        }

        return new Document([
            '$id' => 'test_collection',
            '$collection' => Database::METADATA,
            'name' => 'test_collection',
            'attributes' => $attrDocs,
            'indexes' => [],
        ]);
    }

    private function makeValidator(array $attributes, ?Document $currentDoc = null): OperatorValidator
    {
        return new OperatorValidator($this->makeCollection($attributes), $currentDoc);
    }

    private function makeOperator(OperatorType $method, string $attribute, array $values = []): Operator
    {
        return new Operator($method, $attribute, $values);
    }

    public function testIncrementOnInteger(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'count', [3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testIncrementExceedsMax(): void
    {
        $currentDoc = new Document(['count' => Database::MAX_INT - 5]);
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Increment, 'count', [10]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('would overflow', $validator->getDescription());
    }

    public function testDecrementOnInteger(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Decrement, 'count', [3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDecrementBelowMin(): void
    {
        $currentDoc = new Document(['count' => Database::MIN_INT + 5]);
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Decrement, 'count', [10]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('would underflow', $validator->getDescription());
    }

    public function testMultiplyOnInteger(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Multiply, 'value', [3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testMultiplyOnFloat(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Multiply, 'score', [2.5]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testMultiplyViolatesRange(): void
    {
        $currentDoc = new Document(['value' => Database::MAX_INT]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Multiply, 'value', [2]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('would overflow', $validator->getDescription());
    }

    public function testMultiplyNegative(): void
    {
        $currentDoc = new Document(['value' => Database::MAX_INT]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Multiply, 'value', [-2]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('would underflow', $validator->getDescription());
    }

    public function testDivideOnInteger(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Divide, 'value', [2]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDivideOnFloat(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Divide, 'score', [3.0]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDivideByZero(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Division by zero is not allowed');
        Operator::divide(0);
    }

    public function testDivideByZeroValidator(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Divide, 'value', [0]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('division', $validator->getDescription());
    }

    public function testModuloOnInteger(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Modulo, 'value', [3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testModuloByZero(): void
    {
        $this->expectException(OperatorException::class);
        $this->expectExceptionMessage('Modulo by zero is not allowed');
        Operator::modulo(0);
    }

    public function testModuloByZeroValidator(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Modulo, 'value', [0]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('modulo', $validator->getDescription());
    }

    public function testModuloNegative(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Modulo, 'value', [-3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testPowerOnInteger(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Power, 'value', [3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testPowerFractional(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Power, 'value', [0.5]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testPowerNegativeExponent(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Power, 'value', [-2]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testPowerOverflow(): void
    {
        $currentDoc = new Document(['value' => 100]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Power, 'value', [10]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('would overflow', $validator->getDescription());
    }

    public function testStringConcat(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'title', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringConcat, 'title', [' World']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testStringConcatExceedsMaxLength(): void
    {
        $currentDoc = new Document(['title' => str_repeat('a', 95)]);
        $validator = $this->makeValidator([
            new Attribute(key: 'title', type: ColumnType::String, size: 100),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::StringConcat, 'title', [str_repeat('b', 10)]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('exceed maximum length', $validator->getDescription());
    }

    public function testStringConcatWithinMaxLength(): void
    {
        $currentDoc = new Document(['title' => str_repeat('a', 90)]);
        $validator = $this->makeValidator([
            new Attribute(key: 'title', type: ColumnType::String, size: 100),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::StringConcat, 'title', [str_repeat('b', 10)]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testStringConcatRequiresStringValue(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'title', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringConcat, 'title', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires a string value', $validator->getDescription());
    }

    public function testStringConcatNonStringValue(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'title', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringConcat, 'title', [123]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires a string value', $validator->getDescription());
    }

    public function testStringReplace(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringReplace, 'text', ['old', 'new']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testStringReplaceMultipleOccurrences(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringReplace, 'text', ['test', 'demo']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testStringReplaceValidation(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringReplace, 'text', ['only_search']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires exactly 2 string values', $validator->getDescription());
    }

    public function testStringReplaceWithNonStringValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringReplace, 'text', [123, 456]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires exactly 2 string values', $validator->getDescription());
    }

    public function testStringReplaceOnNonStringField(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'number', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::StringReplace, 'number', ['old', 'new']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-string field', $validator->getDescription());
    }

    public function testToggleBoolean(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'active', type: ColumnType::Boolean, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Toggle, 'active', []);
        $this->assertTrue($validator->isValid($op));
    }

    public function testToggleFromDefault(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'active', type: ColumnType::Boolean, size: 0, default: false),
        ]);

        $op = $this->makeOperator(OperatorType::Toggle, 'active', []);
        $this->assertTrue($validator->isValid($op));
    }

    public function testToggleOnNonBoolean(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Toggle, 'count', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-boolean field', $validator->getDescription());
    }

    public function testToggleOnStringField(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::Toggle, 'name', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-boolean field', $validator->getDescription());
    }

    public function testDateAddDays(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::DateAddDays, 'date', [5]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDateSubDays(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::DateSubDays, 'date', [3]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDateSetNow(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'timestamp', type: ColumnType::Datetime, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::DateSetNow, 'timestamp', []);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDateAtYearBoundaries(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::DateAddDays, 'date', [365]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::DateSubDays, 'date', [365]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::DateAddDays, 'date', [-365]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDateAddDaysOnNonDateField(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::DateAddDays, 'name', [5]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-datetime field', $validator->getDescription());
    }

    public function testDateAddDaysRequiresIntValue(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::DateAddDays, 'date', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires an integer number of days', $validator->getDescription());
    }

    public function testDateAddDaysNonIntegerValue(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::DateAddDays, 'date', [3.5]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires an integer number of days', $validator->getDescription());
    }

    public function testDateSetNowOnNonDateField(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::DateSetNow, 'name', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-datetime field', $validator->getDescription());
    }

    public function testArrayAppend(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayAppend, 'tags', ['new', 'items']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayAppendViolatesConstraints(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255, array: false),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayAppend, 'name', ['item']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array field', $validator->getDescription());
    }

    public function testArrayAppendIntegerBounds(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayAppend, 'numbers', [Database::MAX_INT + 1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('array items must be between', $validator->getDescription());
    }

    public function testArrayPrepend(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayPrepend, 'tags', ['first', 'second']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayPrependOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayPrepend, 'name', ['item']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array field', $validator->getDescription());
    }

    public function testArrayInsert(): void
    {
        $currentDoc = new Document(['numbers' => [1, 2, 3]]);
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::ArrayInsert, 'numbers', [1, 99]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayInsertAtBoundaries(): void
    {
        $currentDoc = new Document(['numbers' => [1, 2, 3]]);
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ], $currentDoc);

        $opStart = $this->makeOperator(OperatorType::ArrayInsert, 'numbers', [0, 0]);
        $this->assertTrue($validator->isValid($opStart));

        $opEnd = $this->makeOperator(OperatorType::ArrayInsert, 'numbers', [3, 4]);
        $this->assertTrue($validator->isValid($opEnd));
    }

    public function testArrayInsertOutOfBounds(): void
    {
        $currentDoc = new Document(['items' => ['a', 'b', 'c']]);
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::ArrayInsert, 'items', [10, 'new']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('index 10 is out of bounds for array of length 3', $validator->getDescription());
    }

    public function testArrayInsertNegativeIndex(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayInsert, 'items', [-1, 'new']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('index must be a non-negative integer', $validator->getDescription());
    }

    public function testArrayInsertMissingValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayInsert, 'items', [0]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires exactly 2 values', $validator->getDescription());
    }

    public function testArrayInsertOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayInsert, 'name', [0, 'val']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array field', $validator->getDescription());
    }

    public function testArrayRemove(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayRemove, 'tags', ['unwanted']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayRemoveOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayRemove, 'name', ['val']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array field', $validator->getDescription());
    }

    public function testArrayRemoveEmptyValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayRemove, 'tags', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires a value to remove', $validator->getDescription());
    }

    public function testArrayFilter(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['greaterThan', 5]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayFilterNumeric(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $opGt = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['greaterThan', 10]);
        $this->assertTrue($validator->isValid($opGt));

        $opLt = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['lessThan', 3]);
        $this->assertTrue($validator->isValid($opLt));

        $opGte = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['greaterThanEqual', 5]);
        $this->assertTrue($validator->isValid($opGte));

        $opLte = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['lessThanEqual', 5]);
        $this->assertTrue($validator->isValid($opLte));
    }

    public function testArrayFilterValidation(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['invalidCondition', 5]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('Invalid array filter condition', $validator->getDescription());
    }

    public function testArrayFilterOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'name', ['equal', 'test']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array field', $validator->getDescription());
    }

    public function testArrayFilterEmptyValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires 1 or 2 values', $validator->getDescription());
    }

    public function testArrayFilterTooManyValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', ['greaterThan', 5, 'extra']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires 1 or 2 values', $validator->getDescription());
    }

    public function testArrayFilterConditionNotString(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'numbers', [123, 5]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('condition must be a string', $validator->getDescription());
    }

    public function testArrayFilterNullConditions(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $opNull = $this->makeOperator(OperatorType::ArrayFilter, 'tags', ['isNull']);
        $this->assertTrue($validator->isValid($opNull));

        $opNotNull = $this->makeOperator(OperatorType::ArrayFilter, 'tags', ['isNotNull']);
        $this->assertTrue($validator->isValid($opNotNull));
    }

    public function testArrayDiff(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayDiff, 'tags', ['remove_me', 'and_me']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayDiffOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayDiff, 'name', ['val']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array attribute', $validator->getDescription());
    }

    public function testArrayIntersect(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayIntersect, 'items', ['a', 'b', 'c']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayIntersectEmpty(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayIntersect, 'items', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('requires a non-empty array value', $validator->getDescription());
    }

    public function testArrayIntersectOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayIntersect, 'name', ['val']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array attribute', $validator->getDescription());
    }

    public function testArrayUnique(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayUnique, 'items', []);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayUniqueOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayUnique, 'name', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-array field', $validator->getDescription());
    }

    public function testArrayOperationsOnEmpty(): void
    {
        $currentDoc = new Document(['items' => []]);
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ], $currentDoc);

        $opAppend = $this->makeOperator(OperatorType::ArrayAppend, 'items', ['first']);
        $this->assertTrue($validator->isValid($opAppend));

        $opPrepend = $this->makeOperator(OperatorType::ArrayPrepend, 'items', ['first']);
        $this->assertTrue($validator->isValid($opPrepend));

        $opInsert = $this->makeOperator(OperatorType::ArrayInsert, 'items', [0, 'first']);
        $this->assertTrue($validator->isValid($opInsert));

        $opInsertOOB = $this->makeOperator(OperatorType::ArrayInsert, 'items', [1, 'second']);
        $this->assertFalse($validator->isValid($opInsertOOB));
    }

    public function testArrayWithSingleElement(): void
    {
        $currentDoc = new Document(['items' => ['only']]);
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ], $currentDoc);

        $opInsert0 = $this->makeOperator(OperatorType::ArrayInsert, 'items', [0, 'before']);
        $this->assertTrue($validator->isValid($opInsert0));

        $opInsert1 = $this->makeOperator(OperatorType::ArrayInsert, 'items', [1, 'after']);
        $this->assertTrue($validator->isValid($opInsert1));

        $opInsertOOB = $this->makeOperator(OperatorType::ArrayInsert, 'items', [2, 'oob']);
        $this->assertFalse($validator->isValid($opInsertOOB));
    }

    public function testArrayWithNull(): void
    {
        $currentDoc = new Document(['items' => null]);
        $validator = $this->makeValidator([
            new Attribute(key: 'items', type: ColumnType::String, size: 50, array: true),
        ], $currentDoc);

        $opAppend = $this->makeOperator(OperatorType::ArrayAppend, 'items', ['first']);
        $this->assertTrue($validator->isValid($opAppend));
    }

    public function testIncrementOnFloat(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'score', [1.5]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testIncrementWithPreciseFloats(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'score', [0.1]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Increment, 'score', [PHP_FLOAT_EPSILON]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testFloatPrecisionLoss(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'score', [0.000000001]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Multiply, 'score', [1.0000000001]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testSequentialOperators(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $op1 = $this->makeOperator(OperatorType::Increment, 'count', [1]);
        $op2 = $this->makeOperator(OperatorType::Multiply, 'score', [2.0]);
        $op3 = $this->makeOperator(OperatorType::StringConcat, 'name', [' suffix']);

        $this->assertTrue($validator->isValid($op1));
        $this->assertTrue($validator->isValid($op2));
        $this->assertTrue($validator->isValid($op3));
    }

    public function testComplexScenarios(): void
    {
        $currentDoc = new Document([
            'count' => 50,
            'tags' => ['a', 'b', 'c'],
            'name' => 'Hello',
            'active' => false,
            'date' => '2023-01-01 00:00:00',
        ]);

        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
            new Attribute(key: 'active', type: ColumnType::Boolean, size: 0),
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ], $currentDoc);

        $this->assertTrue($validator->isValid($this->makeOperator(OperatorType::Increment, 'count', [10])));
        $this->assertTrue($validator->isValid($this->makeOperator(OperatorType::ArrayAppend, 'tags', ['new'])));
        $this->assertTrue($validator->isValid($this->makeOperator(OperatorType::StringConcat, 'name', [' World'])));
        $this->assertTrue($validator->isValid($this->makeOperator(OperatorType::Toggle, 'active', [])));
        $this->assertTrue($validator->isValid($this->makeOperator(OperatorType::DateAddDays, 'date', [7])));
    }

    public function testErrorHandling(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'nonexistent', [1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('does not exist', $validator->getDescription());
    }

    public function testNullValueHandling(): void
    {
        $currentDoc = new Document(['count' => null, 'name' => null]);
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'name', type: ColumnType::String, size: 100),
        ], $currentDoc);

        $opInc = $this->makeOperator(OperatorType::Increment, 'count', [5]);
        $this->assertTrue($validator->isValid($opInc));

        $opConcat = $this->makeOperator(OperatorType::StringConcat, 'name', ['hello']);
        $this->assertTrue($validator->isValid($opConcat));
    }

    public function testValueLimits(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'counter', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'counter', [5, 50]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Decrement, 'counter', [5, 0]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Multiply, 'counter', [2, 100]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Power, 'counter', [3, 1000]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testValueLimitsNonNumeric(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'counter', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'counter', [5, 'not_a_number']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('max/min limit must be numeric', $validator->getDescription());
    }

    public function testAttributeConstraints(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $opNumericOnArray = $this->makeOperator(OperatorType::Increment, 'tags', [1]);
        $this->assertFalse($validator->isValid($opNumericOnArray));

        $opArrayOnNumeric = $this->makeOperator(OperatorType::ArrayAppend, 'score', ['val']);
        $this->assertFalse($validator->isValid($opArrayOnNumeric));
    }

    public function testEmptyStrings(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 255),
        ]);

        $opConcat = $this->makeOperator(OperatorType::StringConcat, 'text', ['']);
        $this->assertTrue($validator->isValid($opConcat));

        $opReplace = $this->makeOperator(OperatorType::StringReplace, 'text', ['old', '']);
        $this->assertTrue($validator->isValid($opReplace));
    }

    public function testExtremeIntegerValues(): void
    {
        $currentDoc = new Document(['value' => 0]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $opMaxInc = $this->makeOperator(OperatorType::Increment, 'value', [Database::MAX_INT]);
        $this->assertTrue($validator->isValid($opMaxInc));

        $currentDoc2 = new Document(['value' => 1]);
        $validator2 = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc2);

        $opOverflow = $this->makeOperator(OperatorType::Increment, 'value', [Database::MAX_INT]);
        $this->assertFalse($validator2->isValid($opOverflow));
    }

    public function testUnicodeCharacters(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 255),
        ]);

        $op = $this->makeOperator(OperatorType::StringConcat, 'text', [' mundo']);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::StringReplace, 'text', ['hello', 'hola']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testVeryLongStrings(): void
    {
        $currentDoc = new Document(['text' => '']);
        $validator = $this->makeValidator([
            new Attribute(key: 'text', type: ColumnType::String, size: 100),
        ], $currentDoc);

        $opFits = $this->makeOperator(OperatorType::StringConcat, 'text', [str_repeat('x', 100)]);
        $this->assertTrue($validator->isValid($opFits));

        $opExceeds = $this->makeOperator(OperatorType::StringConcat, 'text', [str_repeat('x', 101)]);
        $this->assertFalse($validator->isValid($opExceeds));
        $this->assertStringContainsString('exceed maximum length', $validator->getDescription());
    }

    public function testZeroValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'count', [0]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Decrement, 'count', [0]);
        $this->assertTrue($validator->isValid($op));

        $op = $this->makeOperator(OperatorType::Multiply, 'score', [0]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testBatchOperators(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
            new Attribute(key: 'title', type: ColumnType::String, size: 255),
            new Attribute(key: 'active', type: ColumnType::Boolean, size: 0),
            new Attribute(key: 'date', type: ColumnType::Datetime, size: 0),
        ]);

        $operators = [
            $this->makeOperator(OperatorType::Increment, 'count', [5]),
            $this->makeOperator(OperatorType::Multiply, 'score', [2.0]),
            $this->makeOperator(OperatorType::ArrayAppend, 'tags', ['new']),
            $this->makeOperator(OperatorType::StringConcat, 'title', [' Updated']),
            $this->makeOperator(OperatorType::Toggle, 'active', []),
            $this->makeOperator(OperatorType::DateSetNow, 'date', []),
        ];

        foreach ($operators as $op) {
            $this->assertTrue($validator->isValid($op), "Failed for operator: {$op->getMethod()->value} on {$op->getAttribute()}");
        }
    }

    public function testIncrementOnTextAttribute(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'text_field', type: ColumnType::String, size: 100),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'text_field', [1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString("non-numeric field 'text_field'", $validator->getDescription());
    }

    public function testIncrementOnArrayAttribute(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'tags', [1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-numeric field', $validator->getDescription());
    }

    public function testIncrementOnBooleanAttribute(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'active', type: ColumnType::Boolean, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'active', [1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-numeric field', $validator->getDescription());
    }

    public function testNumericOperatorNonNumericValue(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'count', ['not_a_number']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('value must be numeric', $validator->getDescription());
    }

    public function testNumericOperatorEmptyValues(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'count', []);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('value must be numeric', $validator->getDescription());
    }

    public function testStringConcatOnNonStringField(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::StringConcat, 'count', [' suffix']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-string field', $validator->getDescription());
    }

    public function testStringConcatOnArrayField(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::StringConcat, 'tags', [' suffix']);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('non-string field', $validator->getDescription());
    }

    public function testArrayInsertIntegerBounds(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayInsert, 'numbers', [0, Database::MAX_INT + 1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('array items must be between', $validator->getDescription());
    }

    public function testDecrementUnderflow(): void
    {
        $currentDoc = new Document(['count' => Database::MIN_INT + 2]);
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Decrement, 'count', [5]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString('would underflow', $validator->getDescription());
    }

    public function testModuloOnFloat(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Modulo, 'score', [3.5]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testPowerWithMaxLimit(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Power, 'value', [2, 1000]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDivideWithMinLimit(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Double, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Divide, 'value', [2.0, 1.0]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testIncrementWithMaxCap(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'counter', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'counter', [100, 50]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDecrementWithMinCap(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'counter', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Decrement, 'counter', [100, 0]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testOperatorOnNonexistentAttribute(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
        ]);

        $op = $this->makeOperator(OperatorType::Increment, 'nonexistent', [1]);
        $this->assertFalse($validator->isValid($op));
        $this->assertStringContainsString("'nonexistent' does not exist", $validator->getDescription());
    }

    public function testAllNumericOperatorsOnString(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $numericTypes = [
            OperatorType::Increment,
            OperatorType::Decrement,
            OperatorType::Multiply,
            OperatorType::Divide,
            OperatorType::Modulo,
            OperatorType::Power,
        ];

        foreach ($numericTypes as $type) {
            $op = $this->makeOperator($type, 'name', [1]);
            $this->assertFalse($validator->isValid($op), "Expected {$type->value} to fail on string field");
            $this->assertStringContainsString('non-numeric field', $validator->getDescription());
        }
    }

    public function testAllArrayOperatorsOnNonArray(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        $opAppend = $this->makeOperator(OperatorType::ArrayAppend, 'name', ['val']);
        $this->assertFalse($validator->isValid($opAppend));

        $opPrepend = $this->makeOperator(OperatorType::ArrayPrepend, 'name', ['val']);
        $this->assertFalse($validator->isValid($opPrepend));

        $opInsert = $this->makeOperator(OperatorType::ArrayInsert, 'name', [0, 'val']);
        $this->assertFalse($validator->isValid($opInsert));

        $opRemove = $this->makeOperator(OperatorType::ArrayRemove, 'name', ['val']);
        $this->assertFalse($validator->isValid($opRemove));

        $opUnique = $this->makeOperator(OperatorType::ArrayUnique, 'name', []);
        $this->assertFalse($validator->isValid($opUnique));

        $opDiff = $this->makeOperator(OperatorType::ArrayDiff, 'name', ['val']);
        $this->assertFalse($validator->isValid($opDiff));

        $opIntersect = $this->makeOperator(OperatorType::ArrayIntersect, 'name', ['val']);
        $this->assertFalse($validator->isValid($opIntersect));

        $opFilter = $this->makeOperator(OperatorType::ArrayFilter, 'name', ['equal', 'val']);
        $this->assertFalse($validator->isValid($opFilter));
    }

    public function testDateOperatorsOnNonDateFields(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
            new Attribute(key: 'active', type: ColumnType::Boolean, size: 0),
        ]);

        foreach (['count', 'name', 'active'] as $field) {
            $op = $this->makeOperator(OperatorType::DateAddDays, $field, [5]);
            $this->assertFalse($validator->isValid($op));
            $this->assertStringContainsString('non-datetime field', $validator->getDescription());

            $op = $this->makeOperator(OperatorType::DateSubDays, $field, [5]);
            $this->assertFalse($validator->isValid($op));
            $this->assertStringContainsString('non-datetime field', $validator->getDescription());

            $op = $this->makeOperator(OperatorType::DateSetNow, $field, []);
            $this->assertFalse($validator->isValid($op));
            $this->assertStringContainsString('non-datetime field', $validator->getDescription());
        }
    }

    public function testExtractOperatorsAndValidate(): void
    {
        $data = [
            'count' => Operator::increment(5),
            'tags' => Operator::arrayAppend(['new']),
            'name' => 'Regular value',
        ];

        $result = Operator::extractOperators($data);
        $this->assertCount(2, $result['operators']);
        $this->assertCount(1, $result['updates']);

        $validator = $this->makeValidator([
            new Attribute(key: 'count', type: ColumnType::Integer, size: 0),
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
            new Attribute(key: 'name', type: ColumnType::String, size: 255),
        ]);

        foreach ($result['operators'] as $op) {
            $this->assertTrue($validator->isValid($op));
        }
    }

    public function testOperatorTypeClassificationMethods(): void
    {
        $this->assertTrue(OperatorType::Increment->isNumeric());
        $this->assertTrue(OperatorType::Decrement->isNumeric());
        $this->assertTrue(OperatorType::Multiply->isNumeric());
        $this->assertTrue(OperatorType::Divide->isNumeric());
        $this->assertTrue(OperatorType::Modulo->isNumeric());
        $this->assertTrue(OperatorType::Power->isNumeric());

        $this->assertTrue(OperatorType::ArrayAppend->isArray());
        $this->assertTrue(OperatorType::ArrayPrepend->isArray());
        $this->assertTrue(OperatorType::ArrayInsert->isArray());
        $this->assertTrue(OperatorType::ArrayRemove->isArray());
        $this->assertTrue(OperatorType::ArrayUnique->isArray());
        $this->assertTrue(OperatorType::ArrayIntersect->isArray());
        $this->assertTrue(OperatorType::ArrayDiff->isArray());
        $this->assertTrue(OperatorType::ArrayFilter->isArray());

        $this->assertTrue(OperatorType::StringConcat->isString());
        $this->assertTrue(OperatorType::StringReplace->isString());

        $this->assertTrue(OperatorType::Toggle->isBoolean());

        $this->assertTrue(OperatorType::DateAddDays->isDate());
        $this->assertTrue(OperatorType::DateSubDays->isDate());
        $this->assertTrue(OperatorType::DateSetNow->isDate());

        $this->assertFalse(OperatorType::Increment->isArray());
        $this->assertFalse(OperatorType::ArrayAppend->isNumeric());
        $this->assertFalse(OperatorType::StringConcat->isNumeric());
        $this->assertFalse(OperatorType::Toggle->isNumeric());
        $this->assertFalse(OperatorType::DateAddDays->isNumeric());
    }

    public function testOperatorHelperMethods(): void
    {
        $inc = Operator::increment(5, 100);
        $this->assertEquals(OperatorType::Increment, $inc->getMethod());
        $this->assertEquals([5, 100], $inc->getValues());

        $dec = Operator::decrement(3, 0);
        $this->assertEquals(OperatorType::Decrement, $dec->getMethod());
        $this->assertEquals([3, 0], $dec->getValues());

        $mul = Operator::multiply(2, 50);
        $this->assertEquals(OperatorType::Multiply, $mul->getMethod());
        $this->assertEquals([2, 50], $mul->getValues());

        $div = Operator::divide(4, 1);
        $this->assertEquals(OperatorType::Divide, $div->getMethod());
        $this->assertEquals([4, 1], $div->getValues());

        $mod = Operator::modulo(7);
        $this->assertEquals(OperatorType::Modulo, $mod->getMethod());
        $this->assertEquals([7], $mod->getValues());

        $pow = Operator::power(3, 999);
        $this->assertEquals(OperatorType::Power, $pow->getMethod());
        $this->assertEquals([3, 999], $pow->getValues());
    }

    public function testArrayFilterEqualCondition(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'tags', ['equal', 'active']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testArrayFilterNotEqualCondition(): void
    {
        $validator = $this->makeValidator([
            new Attribute(key: 'tags', type: ColumnType::String, size: 50, array: true),
        ]);

        $op = $this->makeOperator(OperatorType::ArrayFilter, 'tags', ['notEqual', 'inactive']);
        $this->assertTrue($validator->isValid($op));
    }

    public function testMultiplyByZero(): void
    {
        $currentDoc = new Document(['value' => 42]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Multiply, 'value', [0]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDecrementFromZero(): void
    {
        $currentDoc = new Document(['value' => 0]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Decrement, 'value', [1]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testIncrementFromMaxMinusOne(): void
    {
        $currentDoc = new Document(['value' => Database::MAX_INT - 1]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Increment, 'value', [1]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testDecrementFromMinPlusOne(): void
    {
        $currentDoc = new Document(['value' => Database::MIN_INT + 1]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Decrement, 'value', [1]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testFloatOperatorsSkipOverflowCheck(): void
    {
        $currentDoc = new Document(['score' => PHP_FLOAT_MAX / 2]);
        $validator = $this->makeValidator([
            new Attribute(key: 'score', type: ColumnType::Double, size: 0),
        ], $currentDoc);

        $op = $this->makeOperator(OperatorType::Increment, 'score', [PHP_FLOAT_MAX / 2]);
        $this->assertTrue($validator->isValid($op));
    }

    public function testIntegerOverflowWithMaxCap(): void
    {
        $currentDoc = new Document(['value' => Database::MAX_INT - 5]);
        $validator = $this->makeValidator([
            new Attribute(key: 'value', type: ColumnType::Integer, size: 0),
        ], $currentDoc);

        $opWithCap = $this->makeOperator(OperatorType::Increment, 'value', [100, Database::MAX_INT]);
        $this->assertTrue($validator->isValid($opWithCap));

        $opWithoutCap = $this->makeOperator(OperatorType::Increment, 'value', [100]);
        $this->assertFalse($validator->isValid($opWithoutCap));
    }
}
