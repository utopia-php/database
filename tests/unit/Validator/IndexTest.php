<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Index;
use Utopia\Database\Validator\IndexDefinition;
use Utopia\Database\Validator\Queries\Indexed;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\IndexType;

class IndexTest extends TestCase
{
    /**
     * What an index validator supported by default before it took a profile.
     */
    private const array CAPABILITIES = [
        Capability::DefinedAttributes,
        Capability::IndexFulltextMultiple,
        Capability::IndexIdentical,
        Capability::IndexKey,
        Capability::IndexUnique,
        Capability::IndexFulltext,
    ];

    #[\Override]
    protected function setUp(): void
    {
    }

    #[\Override]
    protected function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function test_attribute_not_found(): void
    {
        $attributes = [
            Attribute::string(key: 'title'),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['not_exist']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
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
            Attribute::string(key: 'title'),
            Attribute::datetime(key: 'date'),
        ];

        $indexes = [
            Index::fulltext(key: 'index1', attributes: ['title', 'date']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
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
            Attribute::string(key: 'title', size: 769),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['title']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
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
            Attribute::string(key: 'title', size: 256),
            Attribute::string(key: 'description', size: 1024),
        ];

        $indexes = [
            Index::fulltext(key: 'index1', attributes: ['title']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
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
            Attribute::string(key: 'title', size: 769),
        ];

        $indexes = [
            Index::fromArray(['key' => 'index1', 'type' => IndexType::Key]),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
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
            Attribute::object(key: 'data', required: true),
            Attribute::string(key: 'name'),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForObjectIndexes enabled
        $validator = new IndexDefinition($attributes, $emptyIndexes, Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexObject, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768));

        // Valid: Object index on single VAR_OBJECT attribute
        $validIndex = Index::object(key: 'idx_gin_valid', attribute: 'data');
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: Object index on non-object attribute
        $invalidIndexType = Index::object(key: 'idx_gin_invalid_type', attribute: 'name');
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Object index can only be created on object attributes', $validator->getDescription());

        // Invalid: Object index on multiple attributes
        $invalidIndexMulti = Index::fromArray(['key' => 'idx_gin_multi', 'type' => IndexType::Object, 'attributes' => ['data', 'name']]);
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('Object index can be created on a single object attribute', $validator->getDescription());

        // Invalid: Object index with orders
        $invalidIndexOrder = Index::fromArray(['key' => 'idx_gin_order', 'type' => IndexType::Object, 'attributes' => ['data'], 'orders' => [OrderDirection::Asc]]);
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Object index do not support explicit orders', $validator->getDescription());

        // Validator with supportForObjectIndexes disabled should reject GIN
        $validatorNoSupport = new IndexDefinition($attributes, $emptyIndexes, Profiles::of(capabilities: [Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768));
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('Object indexes are not supported', $validatorNoSupport->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_nested_object_path_index_validation(): void
    {
        $attributes = [
            Attribute::object(key: 'data', required: true),
            Attribute::object(key: 'metadata'),
            Attribute::string(key: 'name'),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForObjectIndexes enabled
        $validator = new IndexDefinition($attributes, $emptyIndexes, Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexObject, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext, Capability::Objects], indexLength: 768));

        // InValid: INDEX_OBJECT on nested path (dot notation)
        $validNestedObjectIndex = Index::object(key: 'idx_nested_object', attribute: 'data.key.nestedKey');

        $this->assertFalse($validator->isValid($validNestedObjectIndex));

        // Valid: INDEX_UNIQUE on nested path (for Postgres/Mongo)
        $validNestedUniqueIndex = Index::unique(key: 'idx_nested_unique', attributes: ['data.key.nestedKey']);
        $this->assertTrue($validator->isValid($validNestedUniqueIndex));

        // Valid: INDEX_KEY on nested path
        $validNestedKeyIndex = Index::key(key: 'idx_nested_key', attributes: ['metadata.user.id']);
        $this->assertTrue($validator->isValid($validNestedKeyIndex));

        // Invalid: Nested path on non-object attribute
        $invalidNestedPath = Index::object(key: 'idx_invalid_nested', attribute: 'name.key');
        $this->assertFalse($validator->isValid($invalidNestedPath));
        $this->assertStringContainsString('Index attribute "name.key" is only supported on object attributes', $validator->getDescription());

        // Invalid: Nested path with non-existent base attribute
        $invalidBaseAttribute = Index::object(key: 'idx_invalid_base', attribute: 'nonexistent.key');
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
            Attribute::string(key: 'title'),
        ];

        $indexes = [
            Index::fulltext(key: 'index1', attributes: ['title', 'title']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
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
            Attribute::string(key: 'title'),
        ];

        $indexes = [
            Index::fulltext(key: 'index1', attributes: ['title', 'title']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function test_reserved_index_key(): void
    {
        $attributes = [
            Attribute::string(key: 'title'),
        ];

        $indexes = [
            Index::fulltext(key: 'primary', attributes: ['title']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768, internalIndexKeys: ['PRIMARY']));
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function test_index_with_no_attribute_support(): void
    {
        $attributes = [
            Attribute::string(key: 'title', size: 769),
        ];

        $indexes = [
            Index::key(key: 'index1', attributes: ['new']),
        ];

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
        $index = $indexes[0];
        $this->assertFalse($validator->isValid($index));

        $validator = new IndexDefinition($attributes, $indexes, Profiles::of(capabilities: [Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768));
        $index = $indexes[0];
        $this->assertTrue($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function test_trigram_index_validation(): void
    {
        $attributes = [
            Attribute::string(key: 'name'),
            Attribute::string(key: 'description', size: 512),
            Attribute::integer(key: 'age'),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForTrigramIndexes enabled
        $validator = new IndexDefinition($attributes, $emptyIndexes, Profiles::of(capabilities: [Capability::IndexTrigram, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768));

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
        $invalidIndexOrder = Index::fromArray(['key' => 'idx_trigram_order', 'type' => IndexType::Trigram, 'attributes' => ['name'], 'orders' => [OrderDirection::Asc]]);
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Invalid: Trigram index with lengths
        $invalidIndexLength = Index::fromArray(['key' => 'idx_trigram_length', 'type' => IndexType::Trigram, 'attributes' => ['name'], 'lengths' => [128]]);
        $this->assertFalse($validator->isValid($invalidIndexLength));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Validator with supportForTrigramIndexes disabled should reject trigram
        $validatorNoSupport = new IndexDefinition($attributes, $emptyIndexes, Profiles::of(capabilities: [Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768));
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('Trigram indexes are not supported', $validatorNoSupport->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_ttl_index_validation(): void
    {
        $attributes = [
            Attribute::datetime(key: 'expiresAt'),
            Attribute::string(key: 'name'),
        ];

        /** @var array<Index> $emptyIndexes */
        $emptyIndexes = [];

        // Validator with supportForTTLIndexes enabled
        $validator = new IndexDefinition(
            $attributes,
            $emptyIndexes,
            Profiles::of(capabilities: [...self::CAPABILITIES, Capability::IndexTtl], indexLength: 768),
        );

        // Valid: TTL index on single datetime attribute with valid TTL
        $validIndex = Index::ttl(key: 'idx_ttl_valid', attribute: 'expiresAt', ttl: 3600);
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: TTL index with ttl = 0
        $this->assertTtlRefused(0);

        // Invalid: TTL index with TTL < 0
        $this->assertTtlRefused(-100);

        // Invalid: stored TTL index without a TTL
        $this->assertFalse($validator->isValid(new Document(['$id' => 'idx_ttl_missing', 'type' => IndexType::Ttl->value, 'attributes' => ['expiresAt']])));
        $this->assertSame('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index on non-datetime attribute
        $invalidIndexType = Index::ttl(key: 'idx_ttl_invalid_type', attribute: 'name', ttl: 3600);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('TTL index can only be created on datetime attributes', $validator->getDescription());

        // Invalid: TTL index on multiple attributes
        $invalidIndexMulti = Index::fromArray(['key' => 'idx_ttl_multi', 'type' => IndexType::Ttl, 'attributes' => ['expiresAt', 'name'], 'orders' => [OrderDirection::Asc, OrderDirection::Asc], 'ttl' => 3600]);
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('TTL indexes must be created on a single datetime attribute', $validator->getDescription());

        // Valid: TTL index with minimum valid TTL (1 second)
        $validIndexMin = Index::ttl(key: 'idx_ttl_min', attribute: 'expiresAt', ttl: 1);
        $this->assertTrue($validator->isValid($validIndexMin));

        // Invalid: any additional TTL index when another TTL index already exists
        $indexesWithTTL = [$validIndex];
        $validatorWithExisting = new IndexDefinition(
            $attributes,
            $indexesWithTTL,
            Profiles::of(capabilities: [...self::CAPABILITIES, Capability::IndexTtl], indexLength: 768),
        );

        $duplicateTTLIndex = Index::ttl(key: 'idx_ttl_duplicate', attribute: 'expiresAt', ttl: 7200);
        $this->assertFalse($validatorWithExisting->isValid($duplicateTTLIndex));
        $this->assertEquals('There can be only one TTL index in a collection', $validatorWithExisting->getDescription());

        // Validator with supportForTTLIndexes disabled should reject TTL
        $validatorNoSupport = new IndexDefinition($attributes, $indexesWithTTL, Profiles::of(capabilities: [Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768));
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('TTL indexes are not supported', $validatorNoSupport->getDescription());
    }

    public function testIndexWithoutATypeIsRejected(): void
    {
        $validator = new IndexDefinition([Attribute::string(key: 'title', size: 64)], [], Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));

        $this->assertFalse($validator->isValid(new Document([
            Document::ID => 'by_title',
            'attributes' => ['title'],
        ])));
        $this->assertStringStartsWith('Unknown index type: . Must be one of ', $validator->getDescription());
    }

    public function testTtlIndexWithoutATtlIsRejected(): void
    {
        $validator = new IndexDefinition(
            [Attribute::datetime(key: 'expiresAt')],
            [],
            Profiles::of(capabilities: [...self::CAPABILITIES, Capability::IndexTtl], indexLength: 768),
        );

        $this->assertFalse($validator->isValid(new Document([
            Document::ID => 'expiry',
            'type' => IndexType::Ttl->value,
            'attributes' => ['expiresAt'],
        ])));
        $this->assertSame('TTL must be at least 1 second', $validator->getDescription());
    }

    public function testUnknownIndexTypeIsAValidationFailure(): void
    {
        $validator = new IndexDefinition([Attribute::string(key: 'title', size: 64)], [], Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));

        $this->assertFalse($validator->isValid(new Document([
            Document::ID => 'by_title',
            'type' => 'bogus',
            'attributes' => ['title'],
        ])));
        $this->assertStringStartsWith('Unknown index type: bogus. Must be one of ', $validator->getDescription());
    }

    public function testStoredIndexOfAnUnknownTypeIsReadLeniently(): void
    {
        $stored = new Document([
            Document::ID => 'by_title',
            'type' => 'bogus',
            'attributes' => ['title'],
        ]);

        $index = Index::fromDocument($stored);

        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame(['title'], $index->attributes);
        $this->assertTrue((new Indexed([Attribute::string(key: 'title', size: 64)], [$stored]))->isValid([]));
    }

    public function testTextAttributeWithoutASizeIsJudgedAgainstTheTextMaximum(): void
    {
        $validator = new IndexDefinition([Attribute::text(key: 'body')], [], Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));

        $this->assertTrue($validator->isValid(Index::key(key: 'by_body', attributes: ['body'], lengths: [100])), $validator->getDescription());

        $this->assertFalse($validator->isValid(Index::key(key: 'by_body', attributes: ['body'])));
        $this->assertSame('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    public function testKeyAndUniqueIndexesAreRejectedWithoutAdapterSupport(): void
    {
        $validator = new IndexDefinition(
            [Attribute::string(key: 'title', size: 64)],
            [],
            Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexFulltext], indexLength: 768),
        );
        $key = Index::key(key: 'by_title', attributes: ['title']);
        $unique = Index::unique(key: 'by_title', attributes: ['title']);

        $this->assertFalse($validator->isValid($key));
        $this->assertSame('Key index is not supported', $validator->getDescription());
        $this->assertFalse($validator->checkKeyUniqueFulltextSupport($key));
        $this->assertSame('Key index is not supported', $validator->getDescription());

        $this->assertFalse($validator->isValid($unique));
        $this->assertSame('Unique index is not supported', $validator->getDescription());
        $this->assertFalse($validator->checkKeyUniqueFulltextSupport($unique));
        $this->assertSame('Unique index is not supported', $validator->getDescription());
    }

    public function testStoredLegacyIndexTypeIsValidatedAsAKeyIndex(): void
    {
        $validator = new IndexDefinition([Attribute::string(key: 'title', size: 64)], [], Profiles::of(capabilities: self::CAPABILITIES, indexLength: 768));
        $stored = new Document([Document::ID => 'by_title', 'type' => IndexType::Index->value, 'attributes' => ['title']]);

        $this->assertSame(IndexType::Key, Index::fromDocument($stored)->type);
        $this->assertTrue($validator->isValid($stored), $validator->getDescription());
    }

    public function testOrderOnAnArrayAttributeIsRejected(): void
    {
        $validator = new IndexDefinition(
            [Attribute::string(key: 'tags', size: 64, array: true)],
            [],
            Profiles::of(capabilities: [Capability::IndexArray, Capability::DefinedAttributes, Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext], indexLength: 768),
        );

        $this->assertFalse($validator->isValid(Index::key(key: 'by_tags', attributes: ['tags'], lengths: [64], orders: [OrderDirection::Asc])));
        $this->assertSame('Invalid index order "'.OrderDirection::Asc->value.'" on array attribute "tags"', $validator->getDescription());

        $this->assertTrue($validator->isValid(Index::key(key: 'by_tags', attributes: ['tags'], lengths: [64])), $validator->getDescription());
    }

    private function assertTtlRefused(int $ttl): void
    {
        try {
            Index::fromArray(['key' => 'idx_ttl', 'type' => IndexType::Ttl, 'attributes' => ['expiresAt'], 'ttl' => $ttl]);
            $this->fail('A TTL index with a TTL of '.$ttl.' must be refused');
        } catch (IndexException $error) {
            $this->assertSame('TTL must be at least 1 second', $error->getMessage());
        }
    }
}
