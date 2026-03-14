<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Document as DocumentQueries;
use Utopia\Query\Schema\ColumnType;

class DocumentQueriesTest extends TestCase
{
    /**
     * @var array<Document>
     */
    protected array $attributes = [];

    /**
     * @throws Exception
     */
    protected function setUp(): void
    {
        $this->attributes = [
            new Document([
                '$id' => 'title',
                'key' => 'title',
                'type' => ColumnType::String->value,
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => 'price',
                'key' => 'price',
                'type' => ColumnType::Double->value,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];
    }

    protected function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function test_valid_queries(): void
    {
        $validator = new DocumentQueries($this->attributes);

        $queries = [
            Query::select(['title']),
        ];

        $this->assertEquals(true, $validator->isValid($queries));

        $queries[] = Query::select(['price.relation']);
        $this->assertEquals(true, $validator->isValid($queries));
    }

    /**
     * @throws Exception
     */
    public function test_invalid_queries(): void
    {
        $validator = new DocumentQueries($this->attributes);
        $queries = [Query::limit(1)];
        $this->assertEquals(false, $validator->isValid($queries));
    }
}
