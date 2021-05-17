<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\QueryValidator;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;

class QueriesTest extends TestCase
{
    /**
     * @var array
     */
    protected $collection = [
        '$id' => Database::COLLECTIONS,
        '$collection' => Database::COLLECTIONS,
        'name' => 'movies',
        'attributes' => [
            [
                '$id' => 'title',
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'description',
                'type' => Database::VAR_STRING,
                'size' => 1000000,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'rating',
                'type' => Database::VAR_INTEGER,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'price',
                'type' => Database::VAR_FLOAT,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'published',
                'type' => Database::VAR_BOOLEAN,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'tags',
                'type' => Database::VAR_STRING,
                'size' => 55,
                'required' => true,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
        ],
        'indexes' => [
            [
                '$id' => 'testindex',
                'type' => 'text',
                'attributes' => [
                    'title',
                    'description'
                ],
                'orders' => [
                    'ASC',
                    'DESC'
                ],
            ],
            [
                '$id' => 'testindex2',
                'type' => 'text',
                'attributes' => [
                    'title',
                    'description',
                    'price'
                ],
                'orders' => [
                    'ASC',
                    'DESC'
                ],
            ],
            [
                '$id' => 'testindex3',
                'type' => 'fulltext',
                'attributes' => [
                    'title'
                ],
                'orders' => []
            ],
            [
                '$id' => 'testindex4',
                'type' => 'text',
                'attributes' => [
                    'description'
                ],
                'orders' => []
            ],
        ],
        'indexesInQueue' => [
            [
                '$id' => 'testindex4',
                'type' => 'text',
                'attributes' => [
                    'price',
                    'title'
                ],
                'orders' => [
                    'ASC',
                    'DESC'
                ]
            ],
        ]
    ];


    /**
     * @var Query[] $queries
     */
    protected $queries = [];

    /**
     * @var QueryValidator
     */
    protected $queryValidator = null;

    public function setUp(): void
    {
        $this->queryValidator = new QueryValidator($this->collection['attributes']);

        $query1 = Query::parse('title.notEqual("Iron Man", "Ant Man")');
        $query2 = Query::parse('description.equal("Best movie ever")');

        array_push($this->queries, $query1, $query2);
    }

    public function tearDown(): void
    {
    }

    public function testQueries()
    {
        // test for SUCCESS
        $validator = new Queries($this->queryValidator, $this->collection['indexes'], $this->collection['indexesInQueue']);

        $this->assertEquals(true, $validator->isValid($this->queries));

        $this->queries[] = Query::parse('price.lesserEqual(6.50)');
        $this->assertEquals(true, $validator->isValid($this->queries));


        // test for FAILURE

        $this->queries[] = Query::parse('rating.greater(4)');

        $this->assertEquals(false, $validator->isValid($this->queries));
        $this->assertEquals("Index not found: title,description,price,rating", $validator->getDescription());

        // test for queued index
        $query1 = Query::parse('price.lesserEqual(6.50)');
        $query2 = Query::parse('title.notEqual("Iron Man", "Ant Man")');

        $this->queries = [$query1, $query2];
        $this->assertEquals(false, $validator->isValid($this->queries));
        $this->assertEquals("Index still in creation queue: price,title", $validator->getDescription());

        // test fulltext

        $query3 = Query::parse('description.search("iron")');
        $this->queries = [$query3];
        $this->assertEquals(false, $validator->isValid($this->queries));
        $this->assertEquals("Search operator requires fulltext index: description", $validator->getDescription());
    }

    public function testIsStrict()
    {
        $validator = new Queries($this->queryValidator, $this->collection['indexes'], $this->collection['indexesInQueue']);

        $this->assertEquals(true, $validator->isStrict());

        $validator = new Queries($this->queryValidator, $this->collection['indexes'], $this->collection['indexesInQueue'], false);

        $this->assertEquals(false, $validator->isStrict());
    }
}
