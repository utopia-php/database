<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Queries\V2 as DocumentsValidator;

class DocumentQueriesTest extends TestCase
{
    protected QueryContext $context;

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $collection = [
            '$collection' => ID::custom(Database::METADATA),
            '$id' => ID::custom('movies'),
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
                    '$id' => 'price',
                    'key' => 'price',
                    'type' => Database::VAR_FLOAT,
                    'size' => 5,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ])
            ]
        ];

        $collection = new Document($collection);

        $context = new QueryContext();
        $context->add($collection);

        $this->context = $context;
    }

    public function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function testValidQueries(): void
    {
        $validator = new DocumentsValidator(
            $this->context,
            Database::VAR_INTEGER
        );

        $queries = [
            Query::select('title'),
        ];

        $this->assertEquals(true, $validator->isValid($queries));

        /**
         * Check the top level is a relationship attribute
         */
        $queries[] = Query::select('price.relation');
        $this->assertEquals(false, $validator->isValid($queries));
        $this->assertEquals('Only nested relationships allowed', $validator->getDescription());
    }
}
