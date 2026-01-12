<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\Index;

class IndexTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function testAttributeNotFound(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_KEY,
                    'attributes' => ['not_exist'],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Invalid index attribute "not_exist" not found', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testFulltextWithNonString(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => ID::custom('date'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => false,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => ['datetime'],
                ]),
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_FULLTEXT,
                    'attributes' => ['title', 'date'],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Attribute "date" cannot be part of a fulltext index, must be of type string', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testIndexLength(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 769,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_KEY,
                    'attributes' => ['title'],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testMultipleIndexLength(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 256,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => ID::custom('description'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 1024,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_FULLTEXT,
                    'attributes' => ['title'],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertTrue($validator->isValid($index));

        $index = new Document([
            '$id' => ID::custom('index2'),
            'type' => Database::INDEX_KEY,
            'attributes' => ['title', 'description'],
        ]);

        $collection->setAttribute('indexes', $index, Document::SET_TYPE_APPEND);
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testEmptyAttributes(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 769,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_KEY,
                    'attributes' => [],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('No attributes provided for index', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testObjectIndexValidation(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('data'),
                    'type' => Database::VAR_OBJECT,
                    'format' => '',
                    'size' => 0,
                    'signed' => false,
                    'required' => true,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => ID::custom('name'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            'indexes' => []
        ]);

        // Validator with supportForObjectIndexes enabled
        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes', []), 768, [], false, false, false, false, supportForObjectIndexes:true);

        // Valid: Object index on single VAR_OBJECT attribute
        $validIndex = new Document([
            '$id' => ID::custom('idx_gin_valid'),
            'type' => Database::INDEX_OBJECT,
            'attributes' => ['data'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: Object index on non-object attribute
        $invalidIndexType = new Document([
            '$id' => ID::custom('idx_gin_invalid_type'),
            'type' => Database::INDEX_OBJECT,
            'attributes' => ['name'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Object index can only be created on object attributes', $validator->getDescription());

        // Invalid: Object index on multiple attributes
        $invalidIndexMulti = new Document([
            '$id' => ID::custom('idx_gin_multi'),
            'type' => Database::INDEX_OBJECT,
            'attributes' => ['data', 'name'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('Object index can be created on a single object attribute', $validator->getDescription());

        // Invalid: Object index with orders
        $invalidIndexOrder = new Document([
            '$id' => ID::custom('idx_gin_order'),
            'type' => Database::INDEX_OBJECT,
            'attributes' => ['data'],
            'lengths' => [],
            'orders' => ['asc'],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Object index do not support explicit orders', $validator->getDescription());

        // Validator with supportForObjectIndexes disabled should reject GIN
        $validatorNoSupport = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes', []), 768, [], false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('Object indexes are not supported', $validatorNoSupport->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testDuplicatedAttributes(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_FULLTEXT,
                    'attributes' => ['title', 'title'],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
        $this->assertEquals('Duplicate attributes provided', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testDuplicatedAttributesDifferentOrder(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_FULLTEXT,
                    'attributes' => ['title', 'title'],
                    'lengths' => [],
                    'orders' => ['asc', 'desc'],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function testReservedIndexKey(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('primary'),
                    'type' => Database::INDEX_FULLTEXT,
                    'attributes' => ['title'],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes'), 768, ['PRIMARY']);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));
    }

    /**
     * @throws Exception
    */
    public function testIndexWithNoAttributeSupport(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 769,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_KEY,
                    'attributes' => ['new'],
                    'lengths' => [],
                    'orders' => [],
                ]),
            ],
        ]);

        $validator = new Index(attributes: $collection->getAttribute('attributes'), indexes: $collection->getAttribute('indexes'), maxLength: 768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));

        $validator = new Index(attributes: $collection->getAttribute('attributes'), indexes: $collection->getAttribute('indexes'), maxLength: 768, supportForAttributes: false);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertTrue($validator->isValid($index));
    }

    /**
     * @throws Exception
     */
    public function testTrigramIndexValidation(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('name'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => ID::custom('description'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 512,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => ID::custom('age'),
                    'type' => Database::VAR_INTEGER,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => []
        ]);

        // Validator with supportForTrigramIndexes enabled
        $validator = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes', []), 768, [], false, false, false, false, false, false, false, false, supportForTrigramIndexes: true);

        // Valid: Trigram index on single VAR_STRING attribute
        $validIndex = new Document([
            '$id' => ID::custom('idx_trigram_valid'),
            'type' => Database::INDEX_TRIGRAM,
            'attributes' => ['name'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertTrue($validator->isValid($validIndex));

        // Valid: Trigram index on multiple string attributes
        $validIndexMulti = new Document([
            '$id' => ID::custom('idx_trigram_multi_valid'),
            'type' => Database::INDEX_TRIGRAM,
            'attributes' => ['name', 'description'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertTrue($validator->isValid($validIndexMulti));

        // Invalid: Trigram index on non-string attribute
        $invalidIndexType = new Document([
            '$id' => ID::custom('idx_trigram_invalid_type'),
            'type' => Database::INDEX_TRIGRAM,
            'attributes' => ['age'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('Trigram index can only be created on string type attributes', $validator->getDescription());

        // Invalid: Trigram index with mixed string and non-string attributes
        $invalidIndexMixed = new Document([
            '$id' => ID::custom('idx_trigram_mixed'),
            'type' => Database::INDEX_TRIGRAM,
            'attributes' => ['name', 'age'],
            'lengths' => [],
            'orders' => [],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexMixed));
        $this->assertStringContainsString('Trigram index can only be created on string type attributes', $validator->getDescription());

        // Invalid: Trigram index with orders
        $invalidIndexOrder = new Document([
            '$id' => ID::custom('idx_trigram_order'),
            'type' => Database::INDEX_TRIGRAM,
            'attributes' => ['name'],
            'lengths' => [],
            'orders' => ['asc'],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexOrder));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Invalid: Trigram index with lengths
        $invalidIndexLength = new Document([
            '$id' => ID::custom('idx_trigram_length'),
            'type' => Database::INDEX_TRIGRAM,
            'attributes' => ['name'],
            'lengths' => [128],
            'orders' => [],
        ]);
        $this->assertFalse($validator->isValid($invalidIndexLength));
        $this->assertStringContainsString('Trigram indexes do not support orders or lengths', $validator->getDescription());

        // Validator with supportForTrigramIndexes disabled should reject trigram
        $validatorNoSupport = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes', []), 768, [], false, false, false, false, false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('Trigram indexes are not supported', $validatorNoSupport->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testTTLIndexValidation(): void
    {
        $collection = new Document([
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                new Document([
                    '$id' => ID::custom('expiresAt'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => false,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => ['datetime'],
                ]),
                new Document([
                    '$id' => ID::custom('name'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => []
        ]);

        // Validator with supportForTTLIndexes enabled
        $validator = new Index(
            $collection->getAttribute('attributes'),
            $collection->getAttribute('indexes', []),
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
        $validIndex = new Document([
            '$id' => ID::custom('idx_ttl_valid'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC],
            'ttl' => 3600,
        ]);
        $this->assertTrue($validator->isValid($validIndex));

        // Invalid: TTL index with TTL = 0
        $invalidIndexZero = new Document([
            '$id' => ID::custom('idx_ttl_zero'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC],
            'ttl' => 0,
        ]);
        $this->assertFalse($validator->isValid($invalidIndexZero));
        $this->assertEquals('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index with TTL < 0
        $invalidIndexNegative = new Document([
            '$id' => ID::custom('idx_ttl_negative'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC],
            'ttl' => -100,
        ]);
        $this->assertFalse($validator->isValid($invalidIndexNegative));
        $this->assertEquals('TTL must be at least 1 second', $validator->getDescription());

        // Invalid: TTL index on non-datetime attribute
        $invalidIndexType = new Document([
            '$id' => ID::custom('idx_ttl_invalid_type'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['name'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC],
            'ttl' => 3600,
        ]);
        $this->assertFalse($validator->isValid($invalidIndexType));
        $this->assertStringContainsString('TTL index can only be created on datetime attributes', $validator->getDescription());

        // Invalid: TTL index on multiple attributes
        $invalidIndexMulti = new Document([
            '$id' => ID::custom('idx_ttl_multi'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt', 'name'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC, Database::ORDER_ASC],
            'ttl' => 3600,
        ]);
        $this->assertFalse($validator->isValid($invalidIndexMulti));
        $this->assertStringContainsString('TTL index can be created on a single datetime attribute', $validator->getDescription());

        // Invalid: TTL index without orders
        $invalidIndexNoOrders = new Document([
            '$id' => ID::custom('idx_ttl_no_orders'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [],
            'ttl' => 3600,
        ]);
        $this->assertFalse($validator->isValid($invalidIndexNoOrders));
        $this->assertEquals('TTL index need explicit orders. Add the orders to create this index.', $validator->getDescription());

        // Valid: TTL index with minimum valid TTL (1 second)
        $validIndexMin = new Document([
            '$id' => ID::custom('idx_ttl_min'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC],
            'ttl' => 1,
        ]);
        $this->assertTrue($validator->isValid($validIndexMin));

        // Invalid: TTL index on same attribute when another TTL index already exists
        $collection->setAttribute('indexes', $validIndex, Document::SET_TYPE_APPEND);
        $validatorWithExisting = new Index(
            $collection->getAttribute('attributes'),
            $collection->getAttribute('indexes', []),
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

        $duplicateTTLIndex = new Document([
            '$id' => ID::custom('idx_ttl_duplicate'),
            'type' => Database::INDEX_TTL,
            'attributes' => ['expiresAt'],
            'lengths' => [],
            'orders' => [Database::ORDER_ASC],
            'ttl' => 7200,
        ]);
        $this->assertFalse($validatorWithExisting->isValid($duplicateTTLIndex));
        $this->assertStringContainsString('There is already an index with the same attributes and orders', $validatorWithExisting->getDescription());

        // Validator with supportForTrigramIndexes disabled should reject TTL
        $validatorNoSupport = new Index($collection->getAttribute('attributes'), $collection->getAttribute('indexes', []), 768, [], false, false, false, false, false, false, false, false, false);
        $this->assertFalse($validatorNoSupport->isValid($validIndex));
        $this->assertEquals('TTL indexes are not supported', $validatorNoSupport->getDescription());
    }
}
