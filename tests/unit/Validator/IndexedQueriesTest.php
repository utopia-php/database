<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\IndexedQueries;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;

class IndexedQueriesTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testEmptyQueries(): void
    {
        $validator = new IndexedQueries();

        $this->assertSame(true, $validator->isValid([]));
    }

    public function testInvalidQuery(): void
    {
        $validator = new IndexedQueries();

        $this->assertSame(false, $validator->isValid(["this.is.invalid"]));
    }

    public function testInvalidMethod(): void
    {
        $validator = new IndexedQueries();
        $this->assertSame(false, $validator->isValid(['equal("attr", "value")']));

        $validator = new IndexedQueries([], [], [new Limit()]);
        $this->assertSame(false, $validator->isValid(['equal("attr", "value")']));
    }

    public function testInvalidValue(): void
    {
        $validator = new IndexedQueries([], [], [new Limit()]);
        $this->assertSame(false, $validator->isValid(['limit(-1)']));
    }

    public function testValid(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ];

        $indexes = [
            new Document([
                'type' => Database::INDEX_KEY,
                'attributes' => ['name'],
            ]),
            new Document([
                'type' => Database::INDEX_FULLTEXT,
                'attributes' => ['name'],
            ]),
        ];

        $validator = new IndexedQueries(
            $attributes,
            $indexes,
            [
                new Cursor(),
                new Filter($attributes, Database::VAR_INTEGER),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $query = Query::cursorAfter(new Document(['$id' => 'abc']));
        $this->assertSame(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"cursorAfter","attribute":"","values":["abc"]}');
        $this->assertSame(true, $validator->isValid([$query]));

        $query = Query::parse('{"method":"cursorAfter","values":["abc"]}'); // No attribute required
        $this->assertSame(true, $validator->isValid([$query]));

        $query = Query::equal('name', ['value']);
        $this->assertSame(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"equal","attribute":"name","values":["value"]}');
        $this->assertSame(true, $validator->isValid([$query]));

        $query = Query::limit(10);
        $this->assertSame(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"limit","values":[10]}');
        $this->assertSame(true, $validator->isValid([$query]));

        $query = Query::offset(10);
        $this->assertSame(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"offset","values":[10]}');
        $this->assertSame(true, $validator->isValid([$query]));

        $query = Query::orderAsc('name');
        $this->assertSame(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"orderAsc","attribute":"name"}'); // No values required
        $this->assertSame(true, $validator->isValid([$query]));

        $query = Query::search('name', 'value');
        $this->assertSame(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"search","attribute":"name","values":["value"]}');
        $this->assertSame(true, $validator->isValid([$query]));
    }

    public function testMissingIndex(): void
    {
        $attributes = [
            new Document([
                'key' => 'name',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ];

        $indexes = [
            new Document([
                'type' => Database::INDEX_KEY,
                'attributes' => ['name'],
            ]),
        ];

        $validator = new IndexedQueries(
            $attributes,
            $indexes,
            [
                new Cursor(),
                new Filter($attributes, Database::VAR_INTEGER),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $query = Query::equal('dne', ['value']);
        $this->assertSame(false, $validator->isValid([$query]));
        $this->assertSame('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::orderAsc('dne');
        $this->assertSame(false, $validator->isValid([$query]));
        $this->assertSame('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::search('dne', 'phrase');
        $this->assertSame(false, $validator->isValid([$query]));
        $this->assertSame('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::search('name', 'phrase');
        $this->assertSame(false, $validator->isValid([$query]));
        $this->assertSame('Searching by attribute "name" requires a fulltext index.', $validator->getDescription());
    }

    public function testTwoAttributesFulltext(): void
    {
        $attributes = [
            new Document([
                '$id' => 'ft1',
                'key' => 'ft1',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
            new Document([
                '$id' => 'ft2',
                'key' => 'ft2',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ];

        $indexes = [
            new Document([
                'type' => Database::INDEX_FULLTEXT,
                'attributes' => ['ft1','ft2'],
            ]),
        ];

        $validator = new IndexedQueries(
            $attributes,
            $indexes,
            [
                new Cursor(),
                new Filter($attributes, Database::VAR_INTEGER),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $this->assertSame(false, $validator->isValid([Query::search('ft1', 'value')]));
    }


    public function testJsonParse(): void
    {
        try {
            Query::parse('{"method":"equal","attribute":"name","values":["value"]'); // broken Json;
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertSame('Invalid query: Syntax error', $e->getMessage());
        }
    }
}
