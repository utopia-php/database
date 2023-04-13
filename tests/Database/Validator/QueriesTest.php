<?php

namespace Utopia\Tests\Validator;

use Exception;
use Utopia\Database\Helpers\ID;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Document as DocumentValidator;

class QueriesTest extends TestCase
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
    public function testQueries(): void
    {
        $validator = new DocumentValidator($this->collection['attributes'], $this->collection['indexes']);

        $queries = [
            'notEqual("title", ["Iron Man", "Ant Man"])',
            'equal("description", "Best movie ever")',
            'lessThan("price", 6.50)',
            'greaterThan("rating", 4)',
            'between("price", 1.50, 6.50)',
            'orderAsc("title")',
        ];

        $queries[] = Query::orderDesc('');
        $this->assertEquals(true, $validator->isValid($queries));

        $queries = ['search("description", "iron")'];
        $this->assertFalse($validator->isValid($queries));
        $this->assertEquals('Searching by attribute "description" requires a fulltext index.', $validator->getDescription());

        $queries = ['equal("not_found", 4)'];
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Query not valid: Attribute not found in schema: not_found', $validator->getDescription());
    }

}
