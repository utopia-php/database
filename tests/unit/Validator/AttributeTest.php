<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\Attribute;

class AttributeTest extends TestCase
{
    public function testDuplicateAttributeId(): void
    {
        $validator = new Attribute(
            attributes: [
                new Document([
                    '$id' => ID::custom('title'),
                    'key' => 'title',
                    'type' => Database::VAR_STRING,
                    'size' => 255,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Attribute already exists in metadata');
        $validator->isValid($attribute);
    }

    public function testValidStringAttribute(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function testStringSizeTooLarge(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 1000,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 2000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for string is: 1,000');
        $validator->isValid($attribute);
    }

    public function testVarcharSizeTooLarge(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 1000,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_VARCHAR,
            'size' => 2000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for varchar is: 1,000');
        $validator->isValid($attribute);
    }

    public function testTextSizeTooLarge(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => Database::VAR_TEXT,
            'size' => 70000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for text is: 65535');
        $validator->isValid($attribute);
    }

    public function testMediumtextSizeTooLarge(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => Database::VAR_MEDIUMTEXT,
            'size' => 20000000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for mediumtext is: 16777215');
        $validator->isValid($attribute);
    }

    public function testIntegerSizeTooLarge(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: 100,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => Database::VAR_INTEGER,
            'size' => 200,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for int is: 50');
        $validator->isValid($attribute);
    }

    public function testUnknownType(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('test'),
            'key' => 'test',
            'type' => 'unknown_type',
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessageMatches('/Unknown attribute type: unknown_type/');
        $validator->isValid($attribute);
    }

    public function testRequiredFiltersForDatetime(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('created'),
            'key' => 'created',
            'type' => Database::VAR_DATETIME,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [], // Missing datetime filter
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Attribute of type: datetime requires the following filters: datetime');
        $validator->isValid($attribute);
    }

    public function testValidDatetimeWithFilter(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('created'),
            'key' => 'created',
            'type' => Database::VAR_DATETIME,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => ['datetime'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function testDefaultValueOnRequiredAttribute(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => true,
            'default' => 'default value',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cannot set a default value for a required attribute');
        $validator->isValid($attribute);
    }

    public function testDefaultValueTypeMismatch(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => Database::VAR_INTEGER,
            'size' => 4,
            'required' => false,
            'default' => 'not_an_integer',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value not_an_integer does not match given type integer');
        $validator->isValid($attribute);
    }

    public function testVectorNotSupported(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: false,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => Database::VAR_VECTOR,
            'size' => 128,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector types are not supported by the current database');
        $validator->isValid($attribute);
    }

    public function testVectorCannotBeArray(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embeddings'),
            'key' => 'embeddings',
            'type' => Database::VAR_VECTOR,
            'size' => 128,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => true,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector type cannot be an array');
        $validator->isValid($attribute);
    }

    public function testVectorInvalidDimensions(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => Database::VAR_VECTOR,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions must be a positive integer');
        $validator->isValid($attribute);
    }

    public function testVectorDimensionsExceedsMax(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => Database::VAR_VECTOR,
            'size' => 20000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions cannot exceed 16000');
        $validator->isValid($attribute);
    }

    public function testSpatialNotSupported(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: false,
        );

        $attribute = new Document([
            '$id' => ID::custom('location'),
            'key' => 'location',
            'type' => Database::VAR_POINT,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['point'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Spatial attributes are not supported');
        $validator->isValid($attribute);
    }

    public function testSpatialCannotBeArray(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('locations'),
            'key' => 'locations',
            'type' => Database::VAR_POINT,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => true,
            'filters' => ['point'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Spatial attributes cannot be arrays');
        $validator->isValid($attribute);
    }

    public function testSpatialMustHaveEmptySize(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('location'),
            'key' => 'location',
            'type' => Database::VAR_POINT,
            'size' => 100,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['point'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Size must be empty for spatial attributes');
        $validator->isValid($attribute);
    }

    public function testObjectNotSupported(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: false,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => Database::VAR_OBJECT,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['object'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Object attributes are not supported');
        $validator->isValid($attribute);
    }

    public function testObjectCannotBeArray(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => Database::VAR_OBJECT,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => true,
            'filters' => ['object'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Object attributes cannot be arrays');
        $validator->isValid($attribute);
    }

    public function testObjectMustHaveEmptySize(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => Database::VAR_OBJECT,
            'size' => 100,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['object'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Size must be empty for object attributes');
        $validator->isValid($attribute);
    }

    public function testAttributeLimitExceeded(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxAttributes: 5,
            maxWidth: 0,
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            attributeCountCallback: fn () => 10,
            attributeWidthCallback: fn () => 100,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Column limit reached');
        $validator->isValid($attribute);
    }

    public function testRowWidthLimitExceeded(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxAttributes: 100,
            maxWidth: 1000,
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            attributeCountCallback: fn () => 5,
            attributeWidthCallback: fn () => 1500,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Row width limit reached');
        $validator->isValid($attribute);
    }

    public function testVectorDefaultValueNotArray(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => Database::VAR_VECTOR,
            'size' => 3,
            'required' => false,
            'default' => 'not_an_array',
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector default value must be an array');
        $validator->isValid($attribute);
    }

    public function testVectorDefaultValueWrongElementCount(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => Database::VAR_VECTOR,
            'size' => 3,
            'required' => false,
            'default' => [1.0, 2.0],
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector default value must have exactly 3 elements');
        $validator->isValid($attribute);
    }

    public function testVectorDefaultValueNonNumericElements(): void
    {
        $validator = new Attribute(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => Database::VAR_VECTOR,
            'size' => 3,
            'required' => false,
            'default' => [1.0, 'not_a_number', 3.0],
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector default value must contain only numeric elements');
        $validator->isValid($attribute);
    }
}
