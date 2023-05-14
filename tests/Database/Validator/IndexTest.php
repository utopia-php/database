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
     * @param array<mixed> $collection
     * @return Document
     * @throws Exception
     */
    public function collectionArrayToDocuments(array $collection): Document
    {
        $document = new Document();

        foreach ($collection['attributes'] as $attribute) {
            $document->setAttribute('attributes', new Document([
                '$id' => ID::custom($attribute['$id']),
                'type' => $attribute['type'],
                'size' => $attribute['size'],
                'required' => $attribute['required'],
                'signed' => $attribute['signed'],
                'array' => $attribute['array'],
                'filters' => $attribute['filters'],
                'default' => $attribute['default'] ?? null,
                'format' => $attribute['format'] ?? ''
            ]), Document::SET_TYPE_APPEND);
        }

        foreach ($collection['indexes'] as $index) {
            $document->setAttribute('indexes', new Document([
                '$id' => ID::custom($index['$id']),
                'type' => $index['type'],
                'attributes' => $index['attributes'],
                'lengths' => $index['lengths'],
                'orders' => $index['orders'],
            ]), Document::SET_TYPE_APPEND);
        }

        return $document;
    }

    /**
     * @throws Exception
     */
    public function testAppwriteCollection(): void
    {
        // Todo: Move this test to Appwrite...

        $validator = new Index();

        /** @var array<mixed> $collections */
        $collections = include __DIR__ . '/config/collections.php';

        foreach ($collections as $collection) {
            $collection = $this->collectionArrayToDocuments($collection);
            $this->assertTrue($validator->isValid($collection));
        }
    }

    /**
     * @throws Exception
     */
    public function testFulltextWithNonString(): void
    {
        $validator = new Index();

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
        $this->assertEquals('Attribute "date" cannot be part of a FULLTEXT index', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testIndexLength(): void
    {
        $validator = new Index();

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
        $this->assertEquals('Index Length is longer that the max (768))', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testEmptyAttributes(): void
    {
        $validator = new Index();

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
}
