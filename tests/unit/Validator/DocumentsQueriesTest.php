<?php

namespace Tests\Unit\Validator;

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
            [
                'method' => 'equal',
                'attribute' => 'description',
                'values' => ['Best movie ever']
            ],
            [
                'method' => 'equal',
                'attribute' => 'description',
                'values' => ['']
            ],
            [
                'method' => 'lessThanEqual',
                'attribute' => 'price',
                'values' => [6.50]
            ],
            [
                'method' => 'lessThan',
                'attribute' => 'price',
                'values' => [6.50]
            ],
            [
                'method' => 'greaterThan',
                'attribute' => 'rating',
                'values' => [4]
            ],
            [
                'method' => 'greaterThan',
                'attribute' => 'rating',
                'values' => [0]
            ],
            [
                'method' => 'greaterThanEqual',
                'attribute' => 'rating',
                'values' => [6]
            ],
            [
                'method' => 'between',
                'attribute' => 'price',
                'values' => [1.50, 6.50]
            ],
            [
                'method' => 'search',
                'attribute' => 'title',
                'values' => ['SEO']
            ],
            [
                'method' => 'startsWith',
                'attribute' => 'title',
                'values' => ['Good']
            ],
            [
                'method' => 'endsWith',
                'attribute' => 'title',
                'values' => ['Night']
            ],
            [
                'method' => 'isNull',
                'attribute' => 'title',
            ],
            [
                'method' => 'isNotNull',
                'attribute' => 'title',
            ],
            [
                'method' => 'cursorAfter',
                'values' => ['a'],
            ],
            [
                'method' => 'cursorBefore',
                'values' => ['b'],
            ],
            [
                'method' => 'orderAsc',
                'attribute' => 'title',
            ],
            [
                'method' => 'limit',
                'values' => [10]
            ],
            [
                'method' => 'offset',
                'values' => [10]
            ]
        ];

        $queries[] = Query::orderDesc();
        $this->assertEquals(true, $validator->isValid($queries));

        $queries = [Query::equal('is_bool', [false])];
        $this->assertEquals(true, $validator->isValid($queries));
    }

    /**
     * @throws Exception
     */
    public function testInvalidQueries(): void
    {
        $validator = new Documents($this->collection['attributes'], $this->collection['indexes']);

        $queries = [[
            'method' => 'notEqual',
            'attribute' => 'title',
            'values' => ['Iron Man', 'Ant Man']
        ]];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: NotEqual queries require exactly one value.', $validator->getDescription());

        $queries = [[
            'method' => 'search',
            'attribute' => 'description',
            'values' => ['iron']
        ]];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Searching by attribute "description" requires a fulltext index.', $validator->getDescription());

        $queries = [[
            'method' => 'equal',
            'attribute' => 'not_found',
            'values' => [4]
        ]];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Attribute not found in schema: not_found', $validator->getDescription());

        $queries = [[
            'method' => 'limit',
            'values' => [-1]
        ]];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Invalid limit: Value must be a valid range between 1 and ' . number_format(PHP_INT_MAX), $validator->getDescription());

        $queries = [[
            'method' => 'equal',
            'attribute' => 'title',
            'values' => []
        ]]; // empty array
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Invalid query: Equal queries require at least one value.', $validator->getDescription());
    }
}
