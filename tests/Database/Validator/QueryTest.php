<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\Query;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query as DatabaseQuery;

class QueryTest extends TestCase
{
    /**
     * @var Document[]
     */
    protected $schema;

    /**
     * @var array
     */
    protected $attributes = [
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
        [
            '$id' => 'birthDay',
            'key' => 'birthDay',
            'type' => Database::VAR_DATETIME,
            'size' => 0,
            'required' => false,
            'signed' => false,
            'array' => false,
            'filters' => ['datetime'],
        ],
    ];

    public function setUp(): void
    {
        // Query validator expects Document[]
        foreach ($this->attributes as $attribute) {
            $this->schema[] = new Document($attribute);
        }
    }

    public function tearDown(): void
    {
    }

    public function testQuery()
    {
        $validator = new Query($this->schema);

        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('equal("$id", ["Iron Man", "Ant Man"])')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('notEqual("title", ["Iron Man", "Ant Man"])')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('equal("description", "Best movie ever")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('greaterThan("rating", 4)')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('lessThan("price", 6.50)')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('lessThanEqual("price", 6)')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('contains("tags", "action")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('cursorAfter("docId")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('cursorBefore("docId")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('orderAsc("title")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('orderDesc("title")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('isNull("title")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('isNotNull("title")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('between("price", [1.5, 10.9])')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('between("birthDay",["2024-01-01","2023-01-01"])')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('startsWith("title", "Fro")')));
        $this->assertEquals(true, $validator->isValid(DatabaseQuery::parse('endsWith("title", "Zen")')));
    }

    public function testInvalidMethod()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('eqqual("title", "Iron Man")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query method invalid: eqqual', $validator->getDescription());
    }

    public function testAttributeNotFound()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('equal("name", "Iron Man")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Attribute not found in schema: name', $validator->getDescription());

        $response = $validator->isValid(DatabaseQuery::parse('orderAsc("name")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Attribute not found in schema: name', $validator->getDescription());
    }

    public function testAttributeWrongType()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('equal("title", 1776)'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query type does not match expected: string', $validator->getDescription());
    }

    public function testMethodWrongType()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('contains("title", "Iron")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query method only supported on array attributes: contains', $validator->getDescription());
    }

    public function testQueryDate()
    {
        $validator = new Query($this->schema);
        $response = $validator->isValid(DatabaseQuery::parse('greaterThan("birthDay", "1960-01-01 10:10:10")'));
        $this->assertEquals(true, $response);
    }

    public function testQueryLimit()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('limit(25)'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(DatabaseQuery::parse('limit()'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(DatabaseQuery::parse('limit(-1)'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(DatabaseQuery::parse('limit(10000)'));
        $this->assertEquals(false, $response);
    }

    public function testQueryOffset()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('offset(25)'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(DatabaseQuery::parse('offset()'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(DatabaseQuery::parse('offset(-1)'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(DatabaseQuery::parse('offset(10000)'));
        $this->assertEquals(false, $response);
    }

    public function testQueryOrder()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('orderAsc("title")'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(DatabaseQuery::parse('orderAsc("")'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(DatabaseQuery::parse('orderAsc()'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(DatabaseQuery::parse('orderAsc("doesNotExist")'));
        $this->assertEquals(false, $response);
    }

    public function testQueryCursor()
    {
        $validator = new Query($this->schema);

        $response = $validator->isValid(DatabaseQuery::parse('cursorAfter("asdf")'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(DatabaseQuery::parse('cursorAfter()'));
        $this->assertEquals(false, $response);
    }
}
