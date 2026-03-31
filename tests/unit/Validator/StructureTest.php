<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Format;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;

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
                'type' => ColumnType::String->value,
                'format' => '',
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'description',
                'type' => ColumnType::String->value,
                'format' => '',
                'size' => 1000000,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'rating',
                'type' => ColumnType::Integer->value,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'reviews',
                'type' => ColumnType::Integer->value,
                'format' => '',
                'size' => 5,
                'required' => false,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
            [
                '$id' => 'price',
                'type' => ColumnType::Double->value,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => false,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'published',
                'type' => ColumnType::Boolean->value,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'tags',
                'type' => ColumnType::String->value,
                'format' => '',
                'size' => 55,
                'required' => false,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
            [
                '$id' => 'id',
                'type' => ColumnType::Id->value,
                'format' => '',
                'size' => 0,
                'required' => false,
                'signed' => false,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'varchar_field',
                'type' => ColumnType::Varchar->value,
                'format' => '',
                'size' => 255,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'text_field',
                'type' => ColumnType::Text->value,
                'format' => '',
                'size' => 65535,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'mediumtext_field',
                'type' => ColumnType::MediumText->value,
                'format' => '',
                'size' => 16777215,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'longtext_field',
                'type' => ColumnType::LongText->value,
                'format' => '',
                'size' => 4294967295,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
        ],
        'indexes' => [],
    ];

    protected function setUp(): void
    {
        Structure::addFormat('email', function (mixed $attribute) {
            /** @var array<string, mixed> $attribute */
            $sizeRaw = $attribute['size'] ?? 0;
            $size = is_numeric($sizeRaw) ? (int) $sizeRaw : 0;

            return new Format($size);
        }, ColumnType::String);

        // Cannot encode format when defining constants
        // So add feedback attribute on startup
        /** @var array<int, array<string, mixed>> $attrs */
        $attrs = $this->collection['attributes'];
        $attrs[] = [
            '$id' => ID::custom('feedback'),
            'type' => ColumnType::String->value,
            'format' => 'email',
            'size' => 55,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ];
        $this->collection['attributes'] = $attrs;
    }

    protected function tearDown(): void
    {
    }

    public function test_document_instance(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        $this->assertEquals(false, $validator->isValid('string'));
        $this->assertEquals(false, $validator->isValid(null));
        $this->assertEquals(false, $validator->isValid(false));
        $this->assertEquals(false, $validator->isValid(1));

        $this->assertEquals('Invalid document structure: Value must be an instance of Document', $validator->getDescription());
    }

    public function test_collection_attribute(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        $this->assertEquals(false, $validator->isValid(new Document()));

        $this->assertEquals('Invalid document structure: Missing collection attribute $collection', $validator->getDescription());
    }

    public function test_collection(): void
    {
        $validator = new Structure(
            new Document(),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Collection not found', $validator->getDescription());
    }

    public function test_required_keys(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Missing required attribute "title"', $validator->getDescription());
    }

    public function test_null_values(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
            'id' => '1000',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));
    }

    public function test_unknown_keys(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Unknown attribute: "titlex"', $validator->getDescription());
    }

    public function test_integer_as_string(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid signed 32-bit integer between -2,147,483,648 and 2,147,483,647', $validator->getDescription());
    }

    public function test_valid_document(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));
    }

    public function test_string_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "title" has invalid type. Value must be a valid string and no longer than 256 chars', $validator->getDescription());
    }

    public function test_array_of_strings_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "tags[\'0\']" has invalid type. Value must be a valid string and no longer than 55 chars', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function test_array_as_object_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['name' => 'dog'],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));
    }

    public function test_array_of_objects_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [['name' => 'dog']],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));
    }

    public function test_integer_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid signed 32-bit integer between -2,147,483,648 and 2,147,483,647', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => '',
            'price' => 1.99,
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid signed 32-bit integer between -2,147,483,648 and 2,147,483,647', $validator->getDescription());
    }

    public function test_array_of_integers_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "reviews[\'0\']" has invalid type. Value must be a valid signed 32-bit integer between -2,147,483,648 and 2,147,483,647', $validator->getDescription());
    }

    public function test_float_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "price" has invalid type. Value must be a valid float', $validator->getDescription());
    }

    public function test_boolean_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "published" has invalid type. Value must be a valid boolean', $validator->getDescription());
    }

    public function test_format_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "feedback" has invalid format. Value must be a valid email address', $validator->getDescription());
    }

    public function test_integer_max_range(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid signed 32-bit integer between -2,147,483,648 and 2,147,483,647', $validator->getDescription());
    }

    public function test_double_unsigned(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertStringContainsString('Invalid document structure: Attribute "price" has invalid type. Value must be a valid range between 0 and ', $validator->getDescription());
    }

    public function test_double_max_range(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));
    }

    public function test_id(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        $sqlId = '1000';
        $mongoId = '0198fffb-d664-710a-9765-f922b3e81e3d';

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => null,
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
            'id' => $sqlId,
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
            'id' => $mongoId,
        ])));

        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Uuid7->value
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
            'id' => $mongoId,
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
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
            'id' => $mongoId,
        ])));
    }

    public function test_operators_skipped_during_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        // Operators should be skipped during structure validation
        $this->assertTrue($validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => 'Demo description',
            'rating' => Operator::increment(1), // Operator on required field
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])), $validator->getDescription());
    }

    public function test_multiple_operators_skipped_during_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        // Multiple operators should all be skipped
        $this->assertTrue($validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => Operator::stringConcat(' - Updated'),
            'description' => 'Demo description',
            'rating' => Operator::increment(1),
            'price' => Operator::multiply(2),
            'published' => Operator::toggle(),
            'tags' => Operator::arrayAppend(['new']),
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])), $validator->getDescription());
    }

    public function test_missing_required_field_without_operator(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
        );

        // Missing required field (not replaced by operator) should still fail
        $this->assertFalse($validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'My Title',
            'description' => 'Demo description',
            // 'rating' is missing entirely - should fail
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Missing required attribute "rating"', $validator->getDescription());
    }

    public function test_varchar_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            'varchar_field' => 'Short varchar text',
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'varchar_field' => 123,
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "varchar_field" has invalid type. Value must be a valid string and no longer than 255 chars', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'varchar_field' => \str_repeat('a', 256),
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "varchar_field" has invalid type. Value must be a valid string and no longer than 255 chars', $validator->getDescription());
    }

    public function test_text_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            'text_field' => \str_repeat('a', 65535),
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'text_field' => 123,
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "text_field" has invalid type. Value must be a valid string and no longer than 65535 chars', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'text_field' => \str_repeat('a', 65536),
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "text_field" has invalid type. Value must be a valid string and no longer than 65535 chars', $validator->getDescription());
    }

    public function test_mediumtext_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            'mediumtext_field' => \str_repeat('a', 100000),
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'mediumtext_field' => 123,
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "mediumtext_field" has invalid type. Value must be a valid string and no longer than 16777215 chars', $validator->getDescription());
    }

    public function test_longtext_validation(): void
    {
        $validator = new Structure(
            new Document($this->collection),
            ColumnType::Integer->value
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
            'longtext_field' => \str_repeat('a', 1000000),
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
            'feedback' => 'team@appwrite.io',
            'longtext_field' => 123,
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "longtext_field" has invalid type. Value must be a valid string and no longer than 4294967295 chars', $validator->getDescription());
    }

    public function test_string_type_array_validation(): void
    {
        $collection = [
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            'name' => 'collections',
            'attributes' => [
                [
                    '$id' => 'varchar_array',
                    'type' => ColumnType::Varchar->value,
                    'format' => '',
                    'size' => 128,
                    'required' => false,
                    'signed' => true,
                    'array' => true,
                    'filters' => [],
                ],
                [
                    '$id' => 'text_array',
                    'type' => ColumnType::Text->value,
                    'format' => '',
                    'size' => 65535,
                    'required' => false,
                    'signed' => true,
                    'array' => true,
                    'filters' => [],
                ],
            ],
            'indexes' => [],
        ];

        $validator = new Structure(
            new Document($collection),
            ColumnType::Integer->value
        );

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'varchar_array' => ['test1', 'test2', 'test3'],
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'varchar_array' => [123, 'test2', 'test3'],
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "varchar_array[\'0\']" has invalid type. Value must be a valid string and no longer than 128 chars', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => ID::custom('posts'),
            'varchar_array' => [\str_repeat('a', 129), 'test2'],
            '$createdAt' => '2000-04-01T12:00:00.000+00:00',
            '$updatedAt' => '2000-04-01T12:00:00.000+00:00',
        ])));

        $this->assertEquals('Invalid document structure: Attribute "varchar_array[\'0\']" has invalid type. Value must be a valid string and no longer than 128 chars', $validator->getDescription());
    }
}
