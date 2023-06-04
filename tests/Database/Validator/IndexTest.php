<?php

namespace Utopia\Tests\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\Index;
use Utopia\Database\Document;

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
    public function testFulltextWithNonString(): void
    {
        $validator = new Index(768);

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

        $this->assertFalse($validator->isValid($collection));
        $this->assertEquals('Attribute "date" cannot be part of a FULLTEXT index, must be of type string', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testIndexLength(): void
    {
        $validator = new Index(768);

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

        $this->assertFalse($validator->isValid($collection));
        $this->assertEquals('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testEmptyAttributes(): void
    {
        $validator = new Index(768);

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

        $this->assertFalse($validator->isValid($collection));
        $this->assertEquals('No attributes provided for index', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testDuplicatedAttributes(): void
    {
        $validator = new Index(768);

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

        $this->assertFalse($validator->isValid($collection));
        $this->assertEquals('Duplicate attributes provided', $validator->getDescription());
    }
}
