<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Index;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class IndexTest extends TestCase
{
    protected function setUp(): void
    {
    }

    protected function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function test_attribute_not_found(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Key,
                attributes: ['not_exist'],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Invalid index attribute "not_exist" not found', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_fulltext_with_non_string(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'date',
                type: ColumnType::Datetime,
                size: 0,
                required: false,
                signed: false,
                array: false,
                format: '',
                filters: ['datetime'],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Fulltext,
                attributes: ['title', 'date'],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Attribute "date" cannot be part of a fulltext index, must be of type string', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_index_length(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 769,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Key,
                attributes: ['title'],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_multiple_index_length(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 256,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'description',
                type: ColumnType::String,
                size: 1024,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Fulltext,
                attributes: ['title'],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertTrue($validator->isValid($index));

        $index2 = new Index(
            key: 'index2',
            type: IndexType::Key,
            attributes: ['title', 'description'],
        );

        // Validator does not track new indexes added; just validate the new one
        $this->assertFalse($validator->isValid($index2));
        $this->assertEquals('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_empty_attributes(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 769,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Key,
                attributes: [],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('No attributes provided for index', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_object_index_validation(): void
    {
        $attributes = [
            new Attribute(
                key: 'data',
                type: ColumnType::Object,
                size: 0,
                required: true,
                signed: false,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'name',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForObjectIndexes enabled
        $validator = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, supportForObjectIndexes: true);

        // Valid: Object index on single VAR_OBJECT attribute
        $validIndex = new Index(
            key: 'idx_gin_valid',
            type: IndexType::Object,
            attributes: ['data'],
            lengths: [],
            orders: [],
        );
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: Object index on non-object attribute
        $invalidIndexType = new Index(
            key: 'idx_gin_invalid_type',
            type: IndexType::Object,
            attributes: ['name'],
            lengths: [],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Object index can only be created on object attributes', $validator->getDescription());

        // Invalid: Object index on multiple attributes
        $invalidIndexMulti = new Index(
            key: 'idx_gin_multi',
            type: IndexType::Object,
            attributes: ['data', 'name'],
            lengths: [],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('Object index can be created on a single object attribute', $validator->getDescription());

        // Invalid: Object index with orders
        $invalidIndexOrder = new Index(
            key: 'idx_gin_order',
            type: IndexType::Object,
            attributes: ['data'],
            lengths: [],
            orders: ['asc'],
        );
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Object index do not support explicit orders', $validator->getDescription());

        // Validator with supportForObjectIndexes disabled should reject GIN
        $validatorNoSupport = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('Object indexes are not supported', $validatorNoSupport->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_nested_object_path_index_validation(): void
    {
        $attributes = [
            new Attribute(
                key: 'data',
                type: ColumnType::Object,
                size: 0,
                required: true,
                signed: false,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'metadata',
                type: ColumnType::Object,
                size: 0,
                required: false,
                signed: false,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'name',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForObjectIndexes enabled
        $validator = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, true, true, true, true, supportForObjects: true);

        // InValid: INDEX_OBJECT on nested path (dot notation)
        $validNestedObjectIndex = new Index(
            key: 'idx_nested_object',
            type: IndexType::Object,
            attributes: ['data.key.nestedKey'],
            lengths: [],
            orders: [],
        );

        $this->assertFalse($validator->isValid($validNestedObjectIndex));

        // Valid: INDEX_UNIQUE on nested path (for Postgres/Mongo)
        $validNestedUniqueIndex = new Index(
            key: 'idx_nested_unique',
            type: IndexType::Unique,
            attributes: ['data.key.nestedKey'],
            lengths: [],
            orders: [],
        );
        $this->assertTrue($validator->isValid($validNestedUniqueIndex));

        // Valid: INDEX_KEY on nested path
        $validNestedKeyIndex = new Index(
            key: 'idx_nested_key',
            type: IndexType::Key,
            attributes: ['metadata.user.id'],
            lengths: [],
            orders: [],
        );
        $this->assertTrue($validator->isValid($validNestedKeyIndex));

        // Invalid: Nested path on non-object attribute
        $invalidNestedPath = new Index(
            key: 'idx_invalid_nested',
            type: IndexType::Object,
            attributes: ['name.key'],
            lengths: [],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidNestedPath));
        $this->assertStringContainsString('Index attribute "name.key" is only supported on object attributes', $validator->getDescription());

        // Invalid: Nested path with non-existent base attribute
        $invalidBaseAttribute = new Index(
            key: 'idx_invalid_base',
            type: IndexType::Object,
            attributes: ['nonexistent.key'],
            lengths: [],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidBaseAttribute));
        $this->assertStringContainsString('Invalid index attribute', $validator->getDescription());

        // Valid: Multiple nested paths in same index
        $validMultiNested = new Index(
            key: 'idx_multi_nested',
            type: IndexType::Key,
            attributes: ['data.key1', 'data.key2'],
            lengths: [],
            orders: [],
        );
        $this->assertTrue($validator->isValid($validMultiNested));
    }

    /**
     * @throws Exception
     */
    public function test_duplicated_attributes(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Fulltext,
                attributes: ['title', 'title'],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Duplicate attributes provided', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_duplicated_attributes_different_order(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Fulltext,
                attributes: ['title', 'title'],
                lengths: [],
                orders: ['asc', 'desc'],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function test_reserved_index_key(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'primary',
                type: IndexType::Fulltext,
                attributes: ['title'],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768, ['PRIMARY']);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function test_index_with_no_attribute_support(): void
    {
        $attributes = [
            new Attribute(
                key: 'title',
                type: ColumnType::String,
                size: 769,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        $indexes = [
            new Index(
                key: 'index1',
                type: IndexType::Key,
                attributes: ['new'],
                lengths: [],
                orders: [],
            ),
        ];

        $validator = new IndexValidator(attributes: $attributes, indexes: $indexes, maxLength: 768);
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));

        $validator = new IndexValidator(attributes: $attributes, indexes: $indexes, maxLength: 768, supportForAttributes: false);
        $index = $indexes[0];
        $this->assertTrue($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function test_trigram_index_validation(): void
    {
        $attributes = [
            new Attribute(
                key: 'name',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'description',
                type: ColumnType::String,
                size: 512,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
            new Attribute(
                key: 'age',
                type: ColumnType::Integer,
                size: 0,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForTrigramIndexes enabled
        $validator = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, false, false, false, false, supportForTrigramIndexes: true);

        // Valid: Trigram index on single VAR_STRING attribute
        $validIndex = new Index(
            key: 'idx_trigram_valid',
            type: IndexType::Trigram,
            attributes: ['name'],
            lengths: [],
            orders: [],
        );
        $this->assertTrue($validator->isValid($validIndex));

        // Valid: Trigram index on multiple string attributes
        $validIndexMulti = new Index(
            key: 'idx_trigram_multi_valid',
            type: IndexType::Trigram,
            attributes: ['name', 'description'],
            lengths: [],
            orders: [],
        );
        $this->assertTrue($validator->isValid($validIndexMulti));

        // Invalid: Trigram index on non-string attribute
        $invalidIndexType = new Index(
            key: 'idx_trigram_invalid_type',
            type: IndexType::Trigram,
            attributes: ['age'],
            lengths: [],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Trigram index can only be created on string type attributes', $validator->getDescription());

        // Invalid: Trigram index with mixed string and non-string attributes
        $invalidIndexMixed = new Index(
            key: 'idx_trigram_mixed',
            type: IndexType::Trigram,
            attributes: ['name', 'age'],
            lengths: [],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidIndexMixed));
        $this->assertStringContainsString('Trigram index can only be created on string type attributes', $validator->getDescription());

        // Invalid: Trigram index with orders
        $invalidIndexOrder = new Index(
            key: 'idx_trigram_order',
            type: IndexType::Trigram,
            attributes: ['name'],
            lengths: [],
            orders: ['asc'],
        );
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Invalid: Trigram index with lengths
        $invalidIndexLength = new Index(
            key: 'idx_trigram_length',
            type: IndexType::Trigram,
            attributes: ['name'],
            lengths: [128],
            orders: [],
        );
        $this->assertFalse($validator->isValid($invalidIndexLength));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Validator with supportForTrigramIndexes disabled should reject trigram
        $validatorNoSupport = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('Trigram indexes are not supported', $validatorNoSupport->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_ttl_index_validation(): void
    {
        $attributes = [
            new Attribute(
                key: 'expiresAt',
                type: ColumnType::Datetime,
                size: 0,
                required: false,
                signed: false,
                array: false,
                format: '',
                filters: ['datetime'],
            ),
            new Attribute(
                key: 'name',
                type: ColumnType::String,
                size: 255,
                required: false,
                signed: true,
                array: false,
                format: '',
                filters: [],
            ),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForTTLIndexes enabled
        $validator = new IndexValidator(
            $attributes,
            $emptyIndexes,
            768,
            [],
            false, // supportForArrayIndexes
            false, // supportForSpatialIndexNull
            false, // supportForSpatialIndexOrder
            false, // supportForVectorIndexes
            true,  // supportForAttributes
            true,  // supportForMultipleFulltextIndexes
            true,  // supportForIdenticalIndexes
            false, // supportForObjectIndexes
            false, // supportForTrigramIndexes
            false, // supportForSpatialIndexes
            true,  // supportForKeyIndexes
            true,  // supportForUniqueIndexes
            true,  // supportForFulltextIndexes
            true   // supportForTTLIndexes
        );

        // Valid: TTL index on single datetime attribute with valid TTL
        $validIndex = new Index(
            key: 'idx_ttl_valid',
            type: IndexType::Ttl,
            attributes: ['expiresAt'],
            lengths: [],
            orders: [OrderDirection::Asc->value],
            ttl: 3600,
        );
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: TTL index with ttl = 0
        $invalidIndexZero = new Index(
            key: 'idx_ttl_zero',
            type: IndexType::Ttl,
            attributes: ['expiresAt'],
            lengths: [],
            orders: [OrderDirection::Asc->value],
            ttl: 0,
        );
        $this->assertFalse($validator->isValid($invalidIndexZero));
        $this->assertEquals('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index with TTL < 0
        $invalidIndexNegative = new Index(
            key: 'idx_ttl_negative',
            type: IndexType::Ttl,
            attributes: ['expiresAt'],
            lengths: [],
            orders: [OrderDirection::Asc->value],
            ttl: -100,
        );
        $this->assertFalse($validator->isValid($invalidIndexNegative));
        $this->assertEquals('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index on non-datetime attribute
        $invalidIndexType = new Index(
            key: 'idx_ttl_invalid_type',
            type: IndexType::Ttl,
            attributes: ['name'],
            lengths: [],
            orders: [OrderDirection::Asc->value],
            ttl: 3600,
        );
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('TTL index can only be created on datetime attributes', $validator->getDescription());

        // Invalid: TTL index on multiple attributes
        $invalidIndexMulti = new Index(
            key: 'idx_ttl_multi',
            type: IndexType::Ttl,
            attributes: ['expiresAt', 'name'],
            lengths: [],
            orders: [OrderDirection::Asc->value, OrderDirection::Asc->value],
            ttl: 3600,
        );
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('TTL indexes must be created on a single datetime attribute', $validator->getDescription());

        // Valid: TTL index with minimum valid TTL (1 second)
        $validIndexMin = new Index(
            key: 'idx_ttl_min',
            type: IndexType::Ttl,
            attributes: ['expiresAt'],
            lengths: [],
            orders: [OrderDirection::Asc->value],
            ttl: 1,
        );
        $this->assertTrue($validator->isValid($validIndexMin));

        // Invalid: any additional TTL index when another TTL index already exists
        $indexesWithTTL = [$validIndex];
        $validatorWithExisting = new IndexValidator(
            $attributes,
            $indexesWithTTL,
            768,
            [],
            false, // supportForArrayIndexes
            false, // supportForSpatialIndexNull
            false, // supportForSpatialIndexOrder
            false, // supportForVectorIndexes
            true,  // supportForAttributes
            true,  // supportForMultipleFulltextIndexes
            true,  // supportForIdenticalIndexes
            false, // supportForObjectIndexes
            false, // supportForTrigramIndexes
            false, // supportForSpatialIndexes
            true,  // supportForKeyIndexes
            true,  // supportForUniqueIndexes
            true,  // supportForFulltextIndexes
            true   // supportForTTLIndexes
        );

        $duplicateTTLIndex = new Index(
            key: 'idx_ttl_duplicate',
            type: IndexType::Ttl,
            attributes: ['expiresAt'],
            lengths: [],
            orders: [OrderDirection::Asc->value],
            ttl: 7200,
        );
        $this->assertFalse($validatorWithExisting->isValid($duplicateTTLIndex));
        $this->assertEquals('There can be only one TTL index in a collection', $validatorWithExisting->getDescription());

        // Validator with supportForTTLIndexes disabled should reject TTL
        $validatorNoSupport = new IndexValidator($attributes, $indexesWithTTL, 768, [], false, false, false, false, false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('TTL indexes are not supported', $validatorNoSupport->getDescription());
    }
}
