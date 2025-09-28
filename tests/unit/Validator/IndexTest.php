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

        $validator = new Index(attributes:$collection->getAttribute('attributes'), indexes:$collection->getAttribute('indexes'), maxLength:768);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertFalse($validator->isValid($index));

        $validator = new Index(attributes: $collection->getAttribute('attributes'), indexes:$collection->getAttribute('indexes'), maxLength: 768, supportForAttributes:false);
        $index = $collection->getAttribute('indexes')[0];
        $this->assertTrue($validator->isValid($index));
    }
}
