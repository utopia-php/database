<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\QueryValidator;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;

class QueriesTest extends TestCase
{
    /**
     * @var array
     */
    protected $collection = [
        '$id' => Database::METADATA,
        '$collection' => Database::METADATA,
        'name' => 'movies',
        'attributes' => [
            [
                '$id' => 'title',
                'key' => 'title',
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'description',
                'key' => 'description',
                'type' => Database::VAR_STRING,
                'size' => 1000000,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'rating',
                'key' => 'rating',
                'type' => Database::VAR_INTEGER,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'price',
                'key' => 'price',
                'type' => Database::VAR_FLOAT,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'published',
                'key' => 'published',
                'type' => Database::VAR_BOOLEAN,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'tags',
                'key' => 'tags',
                'type' => Database::VAR_STRING,
                'size' => 55,
                'required' => true,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
        ],
        'indexes' => [],
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
        // Query validator expects Document[]
        $attributes = []; /** @var Document[] $attributes */
        foreach ($this->collection['attributes'] as $attribute) {
            $attributes[] = new Document($attribute);
        }

        $this->queryValidator = new QueryValidator($attributes);

        $query1 = Query::parse('title.notEqual("Iron Man", "Ant Man")');
        $query2 = Query::parse('description.equal("Best movie ever")');

        array_push($this->queries, $query1, $query2);

        // Constructor expects Document[] $indexes
        // Object property declaration cannot initialize a Document object
        // Add Document[] $indexes separately
        $index1 = new Document([
            '$id' => 'testindex',
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
            '$id' => 'testindex2',
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
            '$id' => 'testindex3',
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
        $validator = new Queries($this->queryValidator, $this->collection['indexes']);

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
        $this->assertEquals("Index not found: price,title", $validator->getDescription());

        // test fulltext

        $query3 = Query::parse('description.search("iron")');
        $this->queries = [$query3];
        $this->assertEquals(false, $validator->isValid($this->queries));
        $this->assertEquals("Search operator requires fulltext index: description", $validator->getDescription());
    }

    public function testIsStrict()
    {
        $validator = new Queries($this->queryValidator, $this->collection['indexes']);

        $this->assertEquals(true, $validator->isStrict());

        $validator = new Queries($this->queryValidator, $this->collection['indexes'], false);

        $this->assertEquals(false, $validator->isStrict());
    }
}
