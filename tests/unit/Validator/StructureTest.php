<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Format;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\Structure;

class StructureTest extends TestCase
{
    /**
     * @var array<string, mixed>
     */
    protected array $collection = [
        '$id' => Database::METADATA,
        '$collection' => Database::METADATA,
        'name' => 'collections',
        'attributes' => [
            [
                '$id' => 'title',
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'description',
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 1000000,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'rating',
                'type' => Database::VAR_INTEGER,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'reviews',
                'type' => Database::VAR_INTEGER,
                'format' => '',
                'size' => 5,
                'required' => false,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
            [
                '$id' => 'price',
                'type' => Database::VAR_FLOAT,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => false,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'published',
                'type' => Database::VAR_BOOLEAN,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'tags',
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 55,
                'required' => false,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
            [
                '$id' => 'id',
                'type' => Database::VAR_ID,
                'format' => '',
                'size' => 0,
                'required' => false,
                'signed' => false,
                'array' => false,
                'filters' => [],
            ],
        ],
        'indexes' => [],
    ];

    public function setUp(): void
    {
        Structure::addFormat('email', function ($attribute) {
            $size = $attribute['size'] ?? 0;
            return new Format($size);
        }, Database::VAR_STRING);

        // Cannot encode format when defining constants
        // So add feedback attribute on startup
        $this->collection['attributes'][] = [
            '$id' => ID::custom('feedback'),
            'type' => Database::VAR_STRING,
            'format' => 'email',
            'size' => 55,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ];
    }

    public function tearDown(): void
    {
    }

    public function testDocumentInstance(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid('string'));
        $this->assertEquals(false, $validator->isValid(null));
        $this->assertEquals(false, $validator->isValid(false));
        $this->assertEquals(false, $validator->isValid(1));

        $this->assertEquals('Invalid document structure: Value must be an instance of Document', $validator->getDescription());
    }

    public function testCollectionAttribute(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document()));

        $this->assertEquals('Invalid document structure: Missing collection attribute $collection', $validator->getDescription());
    }

    public function testCollection(): void
    {
        $validator = new Structure(
            new Document(),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Collection not found', $validator->getDescription());
    }

    public function testRequiredKeys(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Missing required attribute "title"', $validator->getDescription());
    }

    public function testNullValues(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'id' => 1000,
        ])));

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', null, 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));
    }

    public function testUnknownKeys(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'titlex' => 'Unknown Attribute',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Unknown attribute: "titlex"', $validator->getDescription());
    }

    public function testIntegerAsString(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => '5',
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid integer', $validator->getDescription());
    }

    public function testValidDocument(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));
    }

    public function testStringValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 5,
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "title" has invalid type. Value must be a valid string and no longer than 256 chars', $validator->getDescription());
    }

    public function testArrayOfStringsValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [1, 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "tags[\'0\']" has invalid type. Value must be a valid string and no longer than 55 chars', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [true],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "tags[\'0\']" has invalid type. Value must be a valid string and no longer than 55 chars', $validator->getDescription());

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['too-long-tag-name-to-make-sure-the-length-validator-inside-string-attribute-type-fails-properly'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "tags[\'0\']" has invalid type. Value must be a valid string and no longer than 55 chars', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testArrayAsObjectValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['name' => 'dog'],
            'feedback' => 'team@appwrite.io'
        ])));
    }

    public function testArrayOfObjectsValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [['name' => 'dog']],
            'feedback' => 'team@appwrite.io'
        ])));
    }

    public function testIntegerValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => true,
            'price' => 1.99,
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid integer', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => '',
            'price' => 1.99,
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid integer', $validator->getDescription());
    }

    public function testArrayOfIntegersValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'reviews' => [3, 4, 4, 5],
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'reviews' => [],
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'reviews' => null,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'reviews' => ['', 4, 4, 5],
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "reviews[\'0\']" has invalid type. Value must be a valid integer', $validator->getDescription());
    }

    public function testFloatValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => '2.5',
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "price" has invalid type. Value must be a valid float', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => '',
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "price" has invalid type. Value must be a valid float', $validator->getDescription());
    }

    public function testBooleanValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => 1,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "published" has invalid type. Value must be a valid boolean', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => '',
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "published" has invalid type. Value must be a valid boolean', $validator->getDescription());
    }

    public function testFormatValidation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team_appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "feedback" has invalid format. Value must be a valid email address', $validator->getDescription());
    }

    public function testIntegerMaxRange(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => PHP_INT_MAX,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid range between -2,147,483,647 and 2,147,483,647', $validator->getDescription());
    }

    public function testDoubleUnsigned(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => -1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));

        $this->assertStringContainsString('Invalid document structure: Attribute "price" has invalid type. Value must be a valid range between 0 and ', $validator->getDescription());
    }

    public function testDoubleMaxRange(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 1,
            'price' => INF,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
        ])));
    }

    public function testId(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_INT
        );

        $id = 1000;
        $mongoid = '507f1f77bcf86cd799439011';

        /**
         * Sql
         */

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'id' => $id,
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'id' => $mongoid,
        ])));

        /**
         * Mongo
         */
        $validator = new Structure(
            new Document($this->collection),
            Database::VAR_ID_MONGO
        );

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'id' => $mongoid,
        ])));

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'id' => $mongoid,
        ])));
    }

}
