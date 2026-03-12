<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\IndexedQueries;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class IndexedQueriesTest extends TestCase
{
    protected function setUp(): void
    {
    }

    protected function tearDown(): void
    {
    }

    public function test_empty_queries(): void
    {
        $validator = new IndexedQueries();

        $this->assertEquals(true, $validator->isValid([]));
    }

    public function test_invalid_query(): void
    {
        $validator = new IndexedQueries();

        $this->assertEquals(false, $validator->isValid(['this.is.invalid']));
    }

    public function test_invalid_method(): void
    {
        $validator = new IndexedQueries();
        $this->assertEquals(false, $validator->isValid(['equal("attr", "value")']));

        $validator = new IndexedQueries([], [], [new Limit()]);
        $this->assertEquals(false, $validator->isValid(['equal("attr", "value")']));
    }

    public function test_invalid_value(): void
    {
        $validator = new IndexedQueries([], [], [new Limit()]);
        $this->assertEquals(false, $validator->isValid(['limit(-1)']));
    }

    public function test_valid(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $indexes = [
            new Document([
                'type' => IndexType::Key->value,
                'attributes' => ['name'],
            ]),
            new Document([
                'type' => IndexType::Fulltext->value,
                'attributes' => ['name'],
            ]),
        ];

        $validator = new IndexedQueries(
            $attributes,
            $indexes,
            [
                new Cursor(),
                new Filter($attributes, ColumnType::Integer->value),
                new Limit(),
                new Offset(),
                new Order($attributes),
            ]
        );

        $query = Query::cursorAfter(new Document(['$id' => 'abc']));
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"cursorAfter","attribute":"","values":["abc"]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::parse('{"method":"cursorAfter","values":["abc"]}'); // No attribute required
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::equal('name', ['value']);
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"equal","attribute":"name","values":["value"]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::limit(10);
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"limit","values":[10]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::offset(10);
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"offset","values":[10]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::orderAsc('name');
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"orderAsc","attribute":"name"}'); // No values required
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::search('name', 'value');
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"search","attribute":"name","values":["value"]}');
        $this->assertEquals(true, $validator->isValid([$query]));
    }

    public function test_missing_index(): void
    {
        $attributes = [
            new Document([
                'key' => 'name',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $indexes = [
            new Document([
                'type' => IndexType::Key->value,
                'attributes' => ['name'],
            ]),
        ];

        $validator = new IndexedQueries(
            $attributes,
            $indexes,
            [
                new Cursor(),
                new Filter($attributes, ColumnType::Integer->value),
                new Limit(),
                new Offset(),
                new Order($attributes),
            ]
        );

        $query = Query::equal('dne', ['value']);
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::orderAsc('dne');
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::search('dne', 'phrase');
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::search('name', 'phrase');
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Searching by attribute "name" requires a fulltext index.', $validator->getDescription());
    }

    public function test_two_attributes_fulltext(): void
    {
        $attributes = [
            new Document([
                '$id' => 'ft1',
                'key' => 'ft1',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
            new Document([
                '$id' => 'ft2',
                'key' => 'ft2',
                'type' => ColumnType::String->value,
                'array' => false,
            ]),
        ];

        $indexes = [
            new Document([
                'type' => IndexType::Fulltext->value,
                'attributes' => ['ft1', 'ft2'],
            ]),
        ];

        $validator = new IndexedQueries(
            $attributes,
            $indexes,
            [
                new Cursor(),
                new Filter($attributes, ColumnType::Integer->value),
                new Limit(),
                new Offset(),
                new Order($attributes),
            ]
        );

        $this->assertEquals(false, $validator->isValid([Query::search('ft1', 'value')]));
    }

    public function test_json_parse(): void
    {
        try {
            Query::parse('{"method":"equal","attribute":"name","values":["value"]'); // broken Json;
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Invalid query: Syntax error', $e->getMessage());
        }
    }
}
