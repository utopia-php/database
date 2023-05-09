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
     * @param array $collection
     * @return Document
     * @throws Exception
     */
    public function convertToCollection(array $collection): Document
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
        $validator = new Index();

        /** @var array $configs */
        $collections = include __DIR__ . '/config/collections.php';

        foreach ($collections as $collection) {
            $collection = $this->convertToCollection($collection);
            $this->assertTrue($validator->isValid($collection));
        }
    }

    /**
     * @throws Exception
     */
    public function testFulltextWithNonString(): void
    {
        $validator = new Index();

        $collection = [
            '$id' => ID::custom('test'),
            'name' => 'test',
            'attributes' => [
                [
                    '$id' => ID::custom('title'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 255,
                    'signed' => true,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('date'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => false,
                    'required' => false,
                    'default' => null,
                    'array' => false,
                    'filters' => ['datetime'],
                ],
            ],
            'indexes' => [
                [
                    '$id' => ID::custom('index1'),
                    'type' => Database::INDEX_FULLTEXT,
                    'attributes' => ['title', 'date'],
                    'lengths' => [],
                    'orders' => [],
                ],
            ],
        ];

        $collection = $this->convertToCollection($collection);
        $this->assertFalse($validator->isValid($collection));
        $this->assertEquals('Attribute "date" cannot be part of a FULLTEXT index', $validator->getDescription());

    }

}
