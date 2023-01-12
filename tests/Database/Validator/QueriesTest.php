<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\ID;
use Utopia\Database\Validator\Query;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query as DatabaseQuery;
use Utopia\Database\Validator\Queries;

class QueriesTest extends TestCase
{
    /**
     * @var array
     */
    protected $collection = [];

    /**
     * @var DatabaseQuery[] $queries
     */
    protected $queries = [];

    /**
     * @var Query
     */
    protected $queryValidator = null;

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
                    '$id' => 'published',
                    'key' => 'published',
                    'type' => Database::VAR_BOOLEAN,
                    'size' => 5,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => 'tags',
                    'key' => 'tags',
                    'type' => Database::VAR_STRING,
                    'size' => 55,
                    'required' => true,
                    'signed' => true,
                    'array' => true,
                    'filters' => [],
                ]),
            ],
            'indexes' => [],
        ];

        $this->queryValidator = new Query($this->collection['attributes']);

        $query1 = 'notEqual("title", ["Iron Man", "Ant Man"])';
        $query2 = 'equal("description", "Best movie ever")';

        array_push($this->queries, $query1, $query2);

        // Constructor expects Document[] $indexes
        // Object property declaration cannot initialize a Document object
        // Add Document[] $indexes separately
        $index1 = new Document([
            '$id' => ID::custom('testindex'),
            'type' => 'key',
            'attributes' => [
                'title',
                'description'
            ],
            'orders' => [
                'ASC',
                'DESC'
            ],
        ]);

        $index2 = new Document([
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
        ]);

        $index3 = new Document([
            '$id' => ID::custom('testindex3'),
            'type' => 'fulltext',
            'attributes' => [
                'title'
            ],
            'orders' => []
        ]);

        $index4 = new Document([
            '$id' => 'testindex4',
            'type' => 'key',
            'attributes' => [
                'description'
            ],
            'orders' => []
        ]);

        $this->collection['indexes'] = [$index1, $index2, $index3, $index4];
    }

    public function tearDown(): void
    {
    }

    public function testQueries()
    {
        // test for SUCCESS
        $validator = new Queries($this->queryValidator, $this->collection['attributes'], $this->collection['indexes']);

        $this->assertEquals(true, $validator->isValid($this->queries), $validator->getDescription());

        $this->queries[] = 'lessThan("price", 6.50)';
        $this->queries[] = 'greaterThanEqual("price", 5.50)';
        $this->assertEquals(true, $validator->isValid($this->queries));

        $queries = [DatabaseQuery::orderDesc('')];
        $this->assertEquals(true, $validator->isValid($queries), $validator->getDescription());

        // test for FAILURE

        $this->queries[] = 'greaterThan("rating", 4)';

        $this->assertFalse($validator->isValid($this->queries));
        $this->assertEquals("Index not found: title,description,price,rating", $validator->getDescription());

        // test for queued index
        $query1 = 'lessThan("price", 6.50)';
        $query2 = 'notEqual("title", ["Iron Man", "Ant Man"])';

        $this->queries = [$query1, $query2];
        $this->assertEquals(false, $validator->isValid($this->queries));
        $this->assertEquals("Index not found: price,title", $validator->getDescription());

        // test fulltext

        $query3 = 'search("description", "iron")';
        $this->queries = [$query3];
        $this->assertEquals(false, $validator->isValid($this->queries));
        $this->assertEquals("Search method requires fulltext index: description", $validator->getDescription());
    }

    public function testLooseOrderQueries()
    {
        $validator = new Queries(
            $this->queryValidator, 
            [
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
            ],
            [
                new Document([
                    '$id' => 'testindex5',
                    'type' => 'key',
                    'attributes' => [
                        'title',
                        'price',
                        'rating'
                    ],
                    'orders' => []
                ])
            ], 
            true,
        );

        // Test for SUCCESS
        $this->assertTrue($validator->isValid([
            'sleep(1)',
        ]));

        // Test for SUCCESS
        $this->assertTrue($validator->isValid([
            'lessThanEqual("price", 6.50)',
            'lessThanEqual("title", "string")',
            'lessThanEqual("rating", 2002)',
        ]));

        $this->assertTrue($validator->isValid([
            'lessThanEqual("price", 6.50)',
            'lessThanEqual("title", "string")',
            'lessThanEqual("rating", 2002)',
        ]));

        $this->assertTrue($validator->isValid([
            'lessThanEqual("price", 6.50)',
            'lessThanEqual("rating", 2002)',
            'lessThanEqual("title", "string")',
        ]));

        $this->assertTrue($validator->isValid([
            'lessThanEqual("title", "string")',
            'lessThanEqual("price", 6.50)',
            'lessThanEqual("rating", 2002)',
        ]));

        $this->assertTrue($validator->isValid([
            'lessThanEqual("title", "string")',
            'lessThanEqual("rating", 2002)',
            'lessThanEqual("price", 6.50)',
        ]));

        $this->assertTrue($validator->isValid([
            'lessThanEqual("rating", 2002)',
            'lessThanEqual("title", "string")',
            'lessThanEqual("price", 6.50)',
        ]));

        $this->assertTrue($validator->isValid([
            'lessThanEqual("rating", 2002)',
            'lessThanEqual("price", 6.50)',
            'lessThanEqual("title", "string")',
        ]));
    }

    public function testIsStrict()
    {
        $validator = new Queries($this->queryValidator, $this->collection['attributes'], $this->collection['indexes']);

        $this->assertEquals(true, $validator->isStrict());

        $validator = new Queries($this->queryValidator, $this->collection['attributes'], $this->collection['indexes'], false);

        $this->assertEquals(false, $validator->isStrict());

    }
}
