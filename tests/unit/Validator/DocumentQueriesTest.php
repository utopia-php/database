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

    public function testJoinIsValid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(true, $validator->isValid([
            Query::join('orders', '$id', 'customerId'),
        ]), $validator->getDescription());
    }

    public function testSelectWithJoinAliasIsValid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(true, $validator->isValid([
            Query::select(['ord.amount']),
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
        ]), $validator->getDescription());
    }

    public function testCountIsInvalid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(false, $validator->isValid([Query::count('*', 'cnt')]));
        $this->assertStringContainsString('Invalid query method', $validator->getDescription());
    }

    public function testGroupByIsInvalid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(false, $validator->isValid([Query::groupBy(['name'])]));
        $this->assertStringContainsString('Invalid query method', $validator->getDescription());
    }

    public function testHavingIsInvalid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(false, $validator->isValid([
            Query::having([Query::greaterThan('amount', 1)]),
        ]));
        $this->assertStringContainsString('Invalid query method', $validator->getDescription());
    }

    public function testDistinctIsInvalid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(false, $validator->isValid([Query::distinct()]));
        $this->assertStringContainsString('Invalid query method', $validator->getDescription());
    }

    public function testUnionIsInvalid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(false, $validator->isValid([
            Query::union([Query::equal('name', ['x'])]),
        ]));
        $this->assertStringContainsString('Invalid query method', $validator->getDescription());
    }

    public function testNaturalJoinIsInvalid(): void
    {
        $validator = new DocumentQueries($this->documentAttributes());

        $this->assertSame(false, $validator->isValid([Query::naturalJoin('orders')]));
        $this->assertStringContainsString('Natural joins are not supported', $validator->getDescription());
    }

    /**
     * @return array<Document>
     */
    private function documentAttributes(): array
    {
        return [
            new Document([
                'key' => 'name',
                'type' => ColumnType::String->value,
            ]),
            new Document([
                'key' => 'amount',
                'type' => ColumnType::Integer->value,
            ]),
        ];
    }
}
