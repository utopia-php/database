<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\Attribute;
use Utopia\Query\Schema\ColumnType;

class AttributeTest extends TestCase
{
    public function test_duplicate_attribute_id(): void
    {
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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

    public function test_required_filters_for_datetime(): void
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
            attributes: [],
            schemaAttributes: [
                new Document([
                    '$id' => ID::custom('existing_column'),
                    'key' => 'existing_column',
                    'type' => ColumnType::String->value,
                    'size' => 255,
                ]),
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
        $validator = new Attribute(
            attributes: [],
            schemaAttributes: [
                new Document([
                    '$id' => ID::custom('existing_column'),
                    'key' => 'existing_column',
                    'type' => ColumnType::String->value,
                    'size' => 255,
                ]),
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
        $validator = new Attribute(
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
}
