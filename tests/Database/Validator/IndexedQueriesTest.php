<?php

namespace Utopia\Tests\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
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

        $this->assertEquals(true, $validator->isValid([]));
    }

    public function testInvalidQuery(): void
    {
        $validator = new IndexedQueries();

        $this->assertEquals(false, $validator->isValid(["this.is.invalid"]));
    }

    public function testInvalidMethod(): void
    {
        $validator = new IndexedQueries();
        $this->assertEquals(false, $validator->isValid(['equal("attr", "value")']));

        $validator = new IndexedQueries([], [], [new Limit()]);
        $this->assertEquals(false, $validator->isValid(['equal("attr", "value")']));
    }

    public function testInvalidValue(): void
    {
        $validator = new IndexedQueries([], [], [new Limit()]);
        $this->assertEquals(false, $validator->isValid(['limit(-1)']));
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
                new Filter($attributes),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $this->assertEquals(true, $validator->isValid([[
            'method' => 'cursorAfter',
            'attribute' => null,
            'values' => ['asdf'],
        ]]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::cursorAfter(new Document(['$id' => 'asdf']))]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([[
            'method' => 'equal',
            'attribute' => 'name',
            'values' => ['value'],
        ]]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::equal('name', ['value'])]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([[
            'method' => 'limit',
            'attribute' => null,
            'values' => [10],
        ]]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::limit(10)]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([[
            'method' => 'offset',
            'attribute' => null,
            'values' => [10],
        ]]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::offset(10)]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([[
            'method' => 'orderAsc',
            'attribute' => 'name',
            'values' => [],
        ]]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::orderAsc('name')]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([[
            'method' => 'search',
            'attribute' => 'name',
            'values' => ['value'],
        ]]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::search('name', 'value')]), $validator->getDescription());
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
                new Filter($attributes),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $this->assertEquals(false, $validator->isValid([[
            'type' => 'equal',
            'attribute' => 'dne',
            'values' => ['value']
        ]]), $validator->getDescription());
        $this->assertEquals(false, $validator->isValid([[
            'type' => 'orderAsc',
            'attribute' => 'dne',
        ]]), $validator->getDescription());
        $this->assertEquals(false, $validator->isValid([[
            'type' => 'search',
            'attribute' => 'name',
            'values' => ['value']
        ]]), $validator->getDescription());
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
                new Filter($attributes),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $this->assertEquals(false, $validator->isValid([Query::search('ft1', 'value')]));
    }
}
