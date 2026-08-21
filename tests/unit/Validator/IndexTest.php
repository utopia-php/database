<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Index;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Query\OrderDirection;

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
            Attribute::string(key: 'title', format: ''),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['not_exist']),
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
            Attribute::string(key: 'title', format: ''),
            Attribute::datetime(key: 'date', signed: false, format: '', filters: ['datetime']),
        ];

        $indexes = [
            Index::fullText(key: 'index1', attributes: ['title', 'date']),
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
            Attribute::string(key: 'title', size: 769, format: ''),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['title']),
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
            Attribute::string(key: 'title', size: 256, format: ''),
            Attribute::string(key: 'description', size: 1024, format: ''),
        ];

        $indexes = [
            Index::fullText(key: 'index1', attributes: ['title']),
        ];

        $validator = new IndexValidator($attributes, $indexes, 768);
        $index = $indexes[0];
        $this->assertTrue($validator->isValid($index));

        $index2 = Index::key(key: 'index2', attributes: ['title', 'description']);

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
            Attribute::string(key: 'title', size: 769, format: ''),
        ];

        $indexes = [
            Index::key(key: 'index1'),
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
            Attribute::object(key: 'data', required: true, signed: false, format: ''),
            Attribute::string(key: 'name', format: ''),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForObjectIndexes enabled
        $validator = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, supportForObjectIndexes: true);

        // Valid: Object index on single VAR_OBJECT attribute
        $validIndex = Index::object(key: 'idx_gin_valid', attributes: ['data']);
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: Object index on non-object attribute
        $invalidIndexType = Index::object(key: 'idx_gin_invalid_type', attributes: ['name']);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Object index can only be created on object attributes', $validator->getDescription());

        // Invalid: Object index on multiple attributes
        $invalidIndexMulti = Index::object(key: 'idx_gin_multi', attributes: ['data', 'name']);
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('Object index can be created on a single object attribute', $validator->getDescription());

        // Invalid: Object index with orders
        $invalidIndexOrder = Index::object(key: 'idx_gin_order', attributes: ['data'], orders: ['asc']);
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
            Attribute::object(key: 'data', required: true, signed: false, format: ''),
            Attribute::object(key: 'metadata', signed: false, format: ''),
            Attribute::string(key: 'name', format: ''),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForObjectIndexes enabled
        $validator = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, true, true, true, true, supportForObjects: true);

        // InValid: INDEX_OBJECT on nested path (dot notation)
        $validNestedObjectIndex = Index::object(key: 'idx_nested_object', attributes: ['data.key.nestedKey']);

        $this->assertFalse($validator->isValid($validNestedObjectIndex));

        // Valid: INDEX_UNIQUE on nested path (for Postgres/Mongo)
        $validNestedUniqueIndex = Index::unique(key: 'idx_nested_unique', attributes: ['data.key.nestedKey']);
        $this->assertTrue($validator->isValid($validNestedUniqueIndex));

        // Valid: INDEX_KEY on nested path
        $validNestedKeyIndex = Index::key(key: 'idx_nested_key', attributes: ['metadata.user.id']);
        $this->assertTrue($validator->isValid($validNestedKeyIndex));

        // Invalid: Nested path on non-object attribute
        $invalidNestedPath = Index::object(key: 'idx_invalid_nested', attributes: ['name.key']);
        $this->assertFalse($validator->isValid($invalidNestedPath));
        $this->assertStringContainsString('Index attribute "name.key" is only supported on object attributes', $validator->getDescription());

        // Invalid: Nested path with non-existent base attribute
        $invalidBaseAttribute = Index::object(key: 'idx_invalid_base', attributes: ['nonexistent.key']);
        $this->assertFalse($validator->isValid($invalidBaseAttribute));
        $this->assertStringContainsString('Invalid index attribute', $validator->getDescription());

        // Valid: Multiple nested paths in same index
        $validMultiNested = Index::key(key: 'idx_multi_nested', attributes: ['data.key1', 'data.key2']);
        $this->assertTrue($validator->isValid($validMultiNested));
    }

    /**
     * @throws Exception
     */
    public function test_duplicated_attributes(): void
    {
        $attributes = [
            Attribute::string(key: 'title', format: ''),
        ];

        $indexes = [
            Index::fullText(key: 'index1', attributes: ['title', 'title']),
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
            Attribute::string(key: 'title', format: ''),
        ];

        $indexes = [
            Index::fullText(key: 'index1', attributes: ['title', 'title'], orders: ['asc', 'desc']),
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
            Attribute::string(key: 'title', format: ''),
        ];

        $indexes = [
            Index::fullText(key: 'primary', attributes: ['title']),
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
            Attribute::string(key: 'title', size: 769, format: ''),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['new']),
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
            Attribute::string(key: 'name', format: ''),
            Attribute::string(key: 'description', size: 512, format: ''),
            Attribute::integer(key: 'age', format: ''),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForTrigramIndexes enabled
        $validator = new IndexValidator($attributes, $emptyIndexes, 768, [], false, false, false, false, false, false, false, false, supportForTrigramIndexes: true);

        // Valid: Trigram index on single VAR_STRING attribute
        $validIndex = Index::trigram(key: 'idx_trigram_valid', attributes: ['name']);
        $this->assertTrue($validator->isValid($validIndex));

        // Valid: Trigram index on multiple string attributes
        $validIndexMulti = Index::trigram(key: 'idx_trigram_multi_valid', attributes: ['name', 'description']);
        $this->assertTrue($validator->isValid($validIndexMulti));

        // Invalid: Trigram index on non-string attribute
        $invalidIndexType = Index::trigram(key: 'idx_trigram_invalid_type', attributes: ['age']);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Trigram index can only be created on string type attributes', $validator->getDescription());

        // Invalid: Trigram index with mixed string and non-string attributes
        $invalidIndexMixed = Index::trigram(key: 'idx_trigram_mixed', attributes: ['name', 'age']);
        $this->assertFalse($validator->isValid($invalidIndexMixed));
        $this->assertStringContainsString('Trigram index can only be created on string type attributes', $validator->getDescription());

        // Invalid: Trigram index with orders
        $invalidIndexOrder = Index::trigram(key: 'idx_trigram_order', attributes: ['name'], orders: ['asc']);
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Invalid: Trigram index with lengths
        $invalidIndexLength = Index::trigram(key: 'idx_trigram_length', attributes: ['name'], lengths: [128]);
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
            Attribute::datetime(key: 'expiresAt', signed: false, format: '', filters: ['datetime']),
            Attribute::string(key: 'name', format: ''),
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
        $validIndex = Index::ttl(key: 'idx_ttl_valid', attributes: ['expiresAt'], orders: [OrderDirection::Asc->value], ttl: 3600);
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: TTL index with ttl = 0
        $invalidIndexZero = Index::ttl(key: 'idx_ttl_zero', attributes: ['expiresAt'], orders: [OrderDirection::Asc->value], ttl: 0);
        $this->assertFalse($validator->isValid($invalidIndexZero));
        $this->assertEquals('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index with TTL < 0
        $invalidIndexNegative = Index::ttl(key: 'idx_ttl_negative', attributes: ['expiresAt'], orders: [OrderDirection::Asc->value], ttl: -100);
        $this->assertFalse($validator->isValid($invalidIndexNegative));
        $this->assertEquals('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index on non-datetime attribute
        $invalidIndexType = Index::ttl(key: 'idx_ttl_invalid_type', attributes: ['name'], orders: [OrderDirection::Asc->value], ttl: 3600);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('TTL index can only be created on datetime attributes', $validator->getDescription());

        // Invalid: TTL index on multiple attributes
        $invalidIndexMulti = Index::ttl(key: 'idx_ttl_multi', attributes: ['expiresAt', 'name'], orders: [OrderDirection::Asc->value, OrderDirection::Asc->value], ttl: 3600);
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('TTL indexes must be created on a single datetime attribute', $validator->getDescription());

        // Valid: TTL index with minimum valid TTL (1 second)
        $validIndexMin = Index::ttl(key: 'idx_ttl_min', attributes: ['expiresAt'], orders: [OrderDirection::Asc->value]);
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

        $duplicateTTLIndex = Index::ttl(key: 'idx_ttl_duplicate', attributes: ['expiresAt'], orders: [OrderDirection::Asc->value], ttl: 7200);
        $this->assertFalse($validatorWithExisting->isValid($duplicateTTLIndex));
        $this->assertEquals('There can be only one TTL index in a collection', $validatorWithExisting->getDescription());

        // Validator with supportForTTLIndexes disabled should reject TTL
        $validatorNoSupport = new IndexValidator($attributes, $indexesWithTTL, 768, [], false, false, false, false, false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('TTL indexes are not supported', $validatorNoSupport->getDescription());
    }
}
