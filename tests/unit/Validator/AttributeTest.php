<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\Schema\Column;
use Utopia\Database\Validator\AttributeDefinition;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;

class AttributeTest extends TestCase
{
    public function testLegacyBigIntegerMetadataNormalizesToCanonicalType(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            '$id' => 'total',
            'type' => 'bigint',
            'size' => 8,
        ]));

        $this->assertSame(ColumnType::BigInteger, $attribute->type);
        $this->assertSame(8, $attribute->size);
        $this->assertSame('bigint', $attribute->toDocument()->getAttribute('type'));

        $arrayAttribute = Attribute::fromArray([
            '$id' => 'arrayTotal',
            'type' => 'bigint',
            'size' => 64,
        ]);

        $this->assertSame(ColumnType::BigInteger, $arrayAttribute->type);
        $this->assertSame(64, $arrayAttribute->size);
    }

    public function testBigIntegerDefaultsSupportNativeAndStringBoundaries(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxIntLength: 100,
            maxBigIntLength: 100,
            supportUnsignedBigInt: true,
        );

        $this->assertTrue($validator->isValid(Attribute::bigInteger(
            key: 'signed',
            default: PHP_INT_MAX,
        )));
        $this->assertTrue($validator->isValid(Attribute::bigInteger(
            key: 'signedMinimum',
            default: '-9223372036854775808',
        )));
        $this->assertTrue($validator->isValid(Attribute::bigInteger(
            key: 'unsigned',
            default: '18446744073709551615',
            signed: false,
        )));
        $this->assertTrue($validator->isValid(Attribute::bigInteger(
            key: 'values',
            default: ['-9223372036854775808', PHP_INT_MAX],
            array: true,
        )));
    }

    public function testBigIntegerDefaultRejectsValuesOutsideSignedRange(): void
    {
        $validator = new AttributeDefinition(attributes: []);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not match given type bigint');
        $validator->isValid(Attribute::bigInteger(
            key: 'total',
            default: '9223372036854775808',
        ));
    }

    public function testBigIntegerArrayDefaultValidatesEveryValue(): void
    {
        $validator = new AttributeDefinition(attributes: [], supportUnsignedBigInt: true);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not match given type bigint');
        $validator->isValid(Attribute::bigInteger(
            key: 'totals',
            default: ['1', '18446744073709551616'],
            signed: false,
            array: true,
        ));
    }

    public function test_duplicate_attribute_id(): void
    {
        $validator = new AttributeDefinition(
            attributes: [
                new Document([
                    '$id' => ID::custom('title'),
                    'key' => 'title',
                    'type' => ColumnType::String->value,
                    'size' => 255,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
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

    public function test_valid_string_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_string_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 1000,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
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

    public function test_varchar_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 1000,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::Varchar->value,
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

    public function test_text_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::Text->value,
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

    public function test_mediumtext_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::MediumText->value,
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

    public function test_integer_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: 100,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => ColumnType::Integer->value,
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

    public function test_unknown_type(): void
    {
        $validator = new AttributeDefinition(
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

    public function test_required_filters_for_datetime(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('created'),
            'key' => 'created',
            'type' => ColumnType::Datetime->value,
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

    public function test_valid_datetime_with_filter(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('created'),
            'key' => 'created',
            'type' => ColumnType::Datetime->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => ['datetime'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_default_value_on_required_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
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

    public function test_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => ColumnType::Integer->value,
            'size' => 4,
            'required' => false,
            'default' => 'not_an_integer',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value "not_an_integer" does not match given type integer');
        $validator->isValid($attribute);
    }

    public function test_vector_not_supported(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: false,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
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

    public function test_vector_cannot_be_array(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embeddings'),
            'key' => 'embeddings',
            'type' => ColumnType::Vector->value,
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

    public function test_vector_invalid_dimensions(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
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

    public function test_vector_dimensions_exceeds_max(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
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

    public function test_spatial_not_supported(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: false,
        );

        $attribute = new Document([
            '$id' => ID::custom('location'),
            'key' => 'location',
            'type' => ColumnType::Point->value,
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

    public function test_spatial_cannot_be_array(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('locations'),
            'key' => 'locations',
            'type' => ColumnType::Point->value,
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

    public function test_spatial_must_have_empty_size(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('location'),
            'key' => 'location',
            'type' => ColumnType::Point->value,
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

    public function test_object_not_supported(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: false,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => ColumnType::Object->value,
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

    public function test_object_cannot_be_array(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => ColumnType::Object->value,
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

    public function test_object_must_have_empty_size(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => ColumnType::Object->value,
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

    public function test_attribute_limit_exceeded(): void
    {
        $validator = new AttributeDefinition(
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
            'type' => ColumnType::String->value,
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

    public function test_row_width_limit_exceeded(): void
    {
        $validator = new AttributeDefinition(
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
            'type' => ColumnType::String->value,
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

    public function test_vector_default_value_not_array(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
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

    public function test_vector_default_value_wrong_element_count(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
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

    public function test_vector_default_value_non_numeric_elements(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
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

    public function test_longtext_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::LongText->value,
            'size' => 5000000000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for longtext is: 4294967295');
        $validator->isValid($attribute);
    }

    public function test_valid_varchar_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('name'),
            'key' => 'name',
            'type' => ColumnType::Varchar->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_text_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::Text->value,
            'size' => 65535,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_mediumtext_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::MediumText->value,
            'size' => 16777215,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_longtext_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::LongText->value,
            'size' => 4294967295,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_float_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('price'),
            'key' => 'price',
            'type' => ColumnType::Double->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_boolean_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('active'),
            'key' => 'active',
            'type' => ColumnType::Boolean->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_float_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('price'),
            'key' => 'price',
            'type' => ColumnType::Double->value,
            'size' => 0,
            'required' => false,
            'default' => 'not_a_float',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value "not_a_float" does not match given type double');
        $validator->isValid($attribute);
    }

    public function test_boolean_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('active'),
            'key' => 'active',
            'type' => ColumnType::Boolean->value,
            'size' => 0,
            'required' => false,
            'default' => 'not_a_boolean',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value "not_a_boolean" does not match given type boolean');
        $validator->isValid($attribute);
    }

    public function test_string_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => 123,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 123 does not match given type string');
        $validator->isValid($attribute);
    }

    public function test_valid_string_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => 'default title',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_integer_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => ColumnType::Integer->value,
            'size' => 4,
            'required' => false,
            'default' => 42,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_float_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('price'),
            'key' => 'price',
            'type' => ColumnType::Double->value,
            'size' => 0,
            'required' => false,
            'default' => 19.99,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_boolean_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('active'),
            'key' => 'active',
            'type' => ColumnType::Boolean->value,
            'size' => 0,
            'required' => false,
            'default' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_unsigned_integer_size_limit(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: 100,
        );

        // Unsigned allows double the size
        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => ColumnType::Integer->value,
            'size' => 80,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_unsigned_integer_size_too_large(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: 100,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => ColumnType::Integer->value,
            'size' => 150,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Max size allowed for int is: 100');
        $validator->isValid($attribute);
    }

    public function test_duplicate_attribute_id_case_insensitive(): void
    {
        $validator = new AttributeDefinition(
            attributes: [
                new Document([
                    '$id' => ID::custom('Title'),
                    'key' => 'Title',
                    'type' => ColumnType::String->value,
                    'size' => 255,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
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

    public function test_duplicate_in_schema(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            schemaAttributes: [
                new Column(name: 'existing_column', type: 'VARCHAR(255)', length: 255, nullable: true),
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSchemaAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('existing_column'),
            'key' => 'existing_column',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Attribute already exists in schema');
        $validator->isValid($attribute);
    }

    public function test_schema_check_skipped_when_migrating(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            schemaAttributes: [
                new Column(name: 'existing_column', type: 'VARCHAR(255)', length: 255, nullable: true),
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSchemaAttributes: true,
            isMigrating: true,
            sharedTables: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('existing_column'),
            'key' => 'existing_column',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_linestring_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('route'),
            'key' => 'route',
            'type' => ColumnType::Linestring->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['linestring'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_polygon_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('area'),
            'key' => 'area',
            'type' => ColumnType::Polygon->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['polygon'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_point_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSpatialAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('location'),
            'key' => 'location',
            'type' => ColumnType::Point->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['point'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_vector_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
            'size' => 128,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_vector_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('embedding'),
            'key' => 'embedding',
            'type' => ColumnType::Vector->value,
            'size' => 3,
            'required' => false,
            'default' => [1.0, 2.0, 3.0],
            'signed' => true,
            'array' => false,
            'filters' => ['vector'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_object_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForObject: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('metadata'),
            'key' => 'metadata',
            'type' => ColumnType::Object->value,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => ['object'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_array_string_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('tags'),
            'key' => 'tags',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_array_with_default_values(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('tags'),
            'key' => 'tags',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => ['tag1', 'tag2', 'tag3'],
            'signed' => true,
            'array' => true,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_array_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('tags'),
            'key' => 'tags',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => ['tag1', 123, 'tag3'],
            'signed' => true,
            'array' => true,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 123 does not match given type string');
        $validator->isValid($attribute);
    }

    public function test_datetime_default_value_must_be_string(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('created'),
            'key' => 'created',
            'type' => ColumnType::Datetime->value,
            'size' => 0,
            'required' => false,
            'default' => 12345,
            'signed' => false,
            'array' => false,
            'filters' => ['datetime'],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 12345 does not match given type datetime');
        $validator->isValid($attribute);
    }

    public function test_valid_datetime_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('created'),
            'key' => 'created',
            'type' => ColumnType::Datetime->value,
            'size' => 0,
            'required' => false,
            'default' => '2024-01-01T00:00:00.000Z',
            'signed' => false,
            'array' => false,
            'filters' => ['datetime'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_varchar_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('name'),
            'key' => 'name',
            'type' => ColumnType::Varchar->value,
            'size' => 255,
            'required' => false,
            'default' => 123,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 123 does not match given type varchar');
        $validator->isValid($attribute);
    }

    public function test_text_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::Text->value,
            'size' => 65535,
            'required' => false,
            'default' => 123,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 123 does not match given type text');
        $validator->isValid($attribute);
    }

    public function test_mediumtext_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::MediumText->value,
            'size' => 16777215,
            'required' => false,
            'default' => 123,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 123 does not match given type mediumtext');
        $validator->isValid($attribute);
    }

    public function test_longtext_default_value_type_mismatch(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::LongText->value,
            'size' => 4294967295,
            'required' => false,
            'default' => 123,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value 123 does not match given type longtext');
        $validator->isValid($attribute);
    }

    public function test_valid_varchar_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('name'),
            'key' => 'name',
            'type' => ColumnType::Varchar->value,
            'size' => 255,
            'required' => false,
            'default' => 'default name',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_text_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('content'),
            'key' => 'content',
            'type' => ColumnType::Text->value,
            'size' => 65535,
            'required' => false,
            'default' => 'default content',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_valid_integer_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('count'),
            'key' => 'count',
            'type' => ColumnType::Integer->value,
            'size' => 4,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_null_default_value_allowed(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_array_default_on_non_array_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('title'),
            'key' => 'title',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => ['not', 'allowed'],
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cannot set an array default value for a non-array attribute');
        $validator->isValid($attribute);
    }

    public function test_array_default_allowed_on_json_filter_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('services'),
            'key' => 'services',
            'type' => ColumnType::String->value,
            'size' => 16384,
            'required' => false,
            'default' => [],
            'signed' => true,
            'array' => false,
            'filters' => ['json'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_object_default_allowed_on_json_filter_attribute(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('data'),
            'key' => 'data',
            'type' => ColumnType::String->value,
            'size' => 65535,
            'required' => false,
            'default' => new \stdClass(),
            'signed' => true,
            'array' => false,
            'filters' => ['json', 'encrypt'],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_get_type(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $this->assertEquals('object', $validator->getType());
    }

    public function test_get_description(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $this->assertEquals('Invalid attribute', $validator->getDescription());
    }

    public function test_is_array(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $this->assertFalse($validator->isArray());
    }

    public function test_is_valid_with_attribute_vo_directly(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attrVO = Attribute::string(
            key: 'directAttr',
            size: 255,
            required: false,
            default: null,
            array: false,
            filters: [],
        );

        $this->assertTrue($validator->isValid($attrVO));
    }

    public function test_attribute_does_not_collide_with_schema(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            schemaAttributes: [
                new Column(name: 'existing_column', type: 'VARCHAR(255)', length: 255, nullable: true),
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSchemaAttributes: true,
        );

        $attribute = new Document([
            '$id' => ID::custom('new_column'),
            'key' => 'new_column',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->assertTrue($validator->isValid($attribute));
    }

    public function test_invalid_format_for_type(): void
    {
        Structure::addFormat('testformat', function (mixed $attribute) {
            return new \Utopia\Validator\Text(100);
        }, ColumnType::Integer);

        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attribute = new Document([
            '$id' => ID::custom('formatted'),
            'key' => 'formatted',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'format' => 'testformat',
            'filters' => [],
        ]);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Format ("testformat") not available for this attribute type ("string")');
        $validator->isValid($attribute);
    }

    public function test_id_type_attribute_validation(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attrVO = Attribute::id(
            key: 'myId',
            required: false,
            default: null,
            array: false,
        );

        $this->assertTrue($validator->isValid($attrVO));
    }

    public function test_unknown_column_type_in_check_type(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('Unknown attribute type: enum');
        $validator->isValid(Attribute::fromArray([
            'key' => 'badtype',
            'type' => ColumnType::Enum,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]));
    }

    public function test_null_default_value_in_validate_default_types(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attrVO = Attribute::string(
            key: 'nullableField',
            size: 255,
            required: false,
            default: null,
            array: false,
            filters: [],
        );

        $this->assertTrue($validator->isValid($attrVO));
    }

    public function test_vector_component_non_numeric_default_type(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attrVO = Attribute::vector(
            key: 'vec',
            dimensions: 3,
            required: false,
            default: [1.0, 2.0, 3.0],
        );

        $this->assertTrue($validator->isValid($attrVO));

        $validator2 = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForVectors: true,
        );

        $attrVO2 = Attribute::vector(
            key: 'vec2',
            dimensions: 3,
            required: false,
            default: [1.0, 'notANumber', 3.0],
        );

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector default value must contain only numeric elements');
        $validator2->isValid($attrVO2);
    }

    public function test_unknown_column_type_with_default_value(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('Unknown attribute type: enum');
        $validator->isValid(Attribute::fromArray([
            'key' => 'baddefault',
            'type' => ColumnType::Enum,
            'size' => 0,
            'required' => false,
            'default' => 'somevalue',
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]));
    }

    public function test_schema_duplicate_check_with_filter_callback(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            schemaAttributes: [
                new Column(name: '_prefix_column', type: 'VARCHAR(255)', length: 255, nullable: true),
            ],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
            supportForSchemaAttributes: true,
            filterCallback: fn (string $key) => str_replace('_prefix_', '', $key),
        );

        $attribute = new Document([
            '$id' => ID::custom('column'),
            'key' => 'column',
            'type' => ColumnType::String->value,
            'size' => 255,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Attribute already exists in schema');
        $validator->isValid($attribute);
    }

    public function test_relationship_type_passes_check_type(): void
    {
        $validator = new AttributeDefinition(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );

        $attrVO = Attribute::fromArray([
            'key' => 'parent',
            'type' => ColumnType::Relationship,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
            'options' => ['relatedCollection' => 'parents', 'relationType' => RelationshipType::ManyToOne->value, 'side' => RelationshipSide::Child->value],
        ]);

        $this->assertTrue($validator->isValid($attrVO));
    }

    public function testBigIntegerDefaultRejectsNonNumericString(): void
    {
        $validator = new AttributeDefinition(attributes: []);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not match given type bigint');
        $validator->isValid(Attribute::bigInteger(
            key: 'counter',
            default: 'not_a_bigint',
        ));
    }
}
