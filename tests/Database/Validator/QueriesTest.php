<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\QueryValidator;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Query;

class QueriesTest extends TestCase
{
    /**
     * @var array 
     */
    public $schema = [
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
    ];

    /**
     * @var array
     */
    protected $indexes = [
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
                'description'
            ],
            'orders' => [
                'ASC',
                'DESC'
            ],
        ],
    ];

    /**
     * @var Query[] $queries
     */
    protected $queries = [];

    /**
     * @var QueryValidator
     */
    protected $queryValidator;

    public function setUp(): void
    {
        $this->queryValidator= new QueryValidator($this->schema);

        $query1 = Query::parse('title.notEqual("Iron Man", "Ant Man")');
        $query2 = Query::parse('description.equal("Best movie ever")');
        $query3 = Query::parse('rating.greater(4)');
        $query4 = Query::parse('price.lesserEqual(6.50)');

        array_push($queries, $query1, $query2, $query3, $query4);
    }

    public function tearDown(): void
    {
    }

    public function testQueries()
    {
        $validator = new Queries($this->validator, $this->indexes);

        $this->assertEquals(1, 1);
    }
}
