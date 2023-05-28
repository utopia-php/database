<?php

namespace Utopia\Tests\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Documents;

class DocumentsQueriesTest extends TestCase
{
    /**
     * @var array<string, mixed>
     */
    protected array $collection = [];

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $this->collection = [
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            'name' => 'movies',
            'attributes' => [
                new Document([
                    '$id' => 'title',
                    'key' => 'title',
                    'type' => Database::VAR_STRING,
                    'size' => 256,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => 'description',
                    'key' => 'description',
                    'type' => Database::VAR_STRING,
                    'size' => 1000000,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => 'rating',
                    'key' => 'rating',
                    'type' => Database::VAR_INTEGER,
                    'size' => 5,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => 'price',
                    'key' => 'price',
                    'type' => Database::VAR_FLOAT,
                    'size' => 5,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => 'is_bool',
                    'key' => 'is_bool',
                    'type' => Database::VAR_BOOLEAN,
                    'size' => 0,
                    'required' => false,
                    'signed' => false,
                    'array' => false,
                    'filters' => [],
                ])
            ],
            'indexes' => [
                new Document([
                    '$id' => ID::custom('testindex2'),
                    'type' => 'key',
                    'attributes' => [
                        'title',
                        'description',
                        'price'
                    ],
                    'orders' => [
                        'ASC',
                        'DESC'
                    ],
                ]),
                new Document([
                    '$id' => ID::custom('testindex3'),
                    'type' => 'fulltext',
                    'attributes' => [
                        'title'
                    ],
                    'orders' => []
                ]),
            ],
        ];
    }

    public function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function testValidQueries(): void
    {
        $validator = new Documents($this->collection['attributes'], $this->collection['indexes']);

        $queries = [
            'notEqual("title", ["Iron Man", "Ant Man"])',
            'equal("description", "Best movie ever")',
            'equal("description", [""])',
            'lessThanEqual("price", 6.50)',
            'lessThan("price", 6.50)',
            'greaterThan("rating", 4)',
            'greaterThan("rating", 0)',
            'greaterThanEqual("rating", 6)',
            'between("price", 1.50, 6.50)',
            'search("title", "SEO")',
            'startsWith("title", "Good")',
            'endsWith("title", "Night")',
            'isNull("title")',
            'isNotNull("title")',
            'cursorAfter("a")',
            'cursorBefore("b")',
            'orderAsc("title")',
            'limit(10)',
            'offset(10)',
        ];

        $queries[] = Query::orderDesc('');
        $this->assertEquals(true, $validator->isValid($queries));

        $queries = ['equal("is_bool", false)'];
        $this->assertEquals(true, $validator->isValid($queries));
    }

    /**
     * @throws Exception
     */
    public function testInvalidQueries(): void
    {
        $validator = new Documents($this->collection['attributes'], $this->collection['indexes']);

        $queries = ['search("description", "iron")'];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Searching by attribute "description" requires a fulltext index.', $validator->getDescription());

        $queries = ['equal("not_found", 4)'];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Attribute not found in schema: not_found', $validator->getDescription());

        $queries = ['search("description", "iron")'];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Searching by attribute "description" requires a fulltext index.', $validator->getDescription());

        $queries = ['equal("not_found", 4)'];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Attribute not found in schema: not_found', $validator->getDescription());

        $queries = ['limit(-1)'];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Invalid limit: Value must be a valid range between 1 and 9,223,372,036,854,775,808', $validator->getDescription());

        $queries = ['equal("title", [])']; // empty array
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Equal queries require at least one value.', $validator->getDescription());
    }
}
