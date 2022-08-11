<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\QueryValidator;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class QueryValidatorTest extends TestCase
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
        $validator = new QueryValidator($this->schema);

        $this->assertEquals(true, $validator->isValid(Query::parse('equal("$id", ["Iron Man", "Ant Man"])')));
        $this->assertEquals(true, $validator->isValid(Query::parse('notEqual("title", ["Iron Man", "Ant Man"])')));
        $this->assertEquals(true, $validator->isValid(Query::parse('equal("description", "Best movie ever")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('greaterThan("rating", 4)')), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid(Query::parse('lessThan("price", 6.50)')));
        $this->assertEquals(true, $validator->isValid(Query::parse('contains("tags", "action")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('cursorAfter("docId")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('cursorBefore("docId")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('orderAsc("title")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('orderDesc("title")')));
    }

    public function testInvalidMethod()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('eqqual("title", "Iron Man")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query method invalid: eqqual', $validator->getDescription());
    }

    public function testAttributeNotFound()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('equal("name", "Iron Man")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Attribute not found in schema: name', $validator->getDescription());

        $response = $validator->isValid(Query::parse('orderAsc("name")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Attribute not found in schema: name', $validator->getDescription());
    }

    public function testAttributeWrongType()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('equal("title", 1776)'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query type does not match expected: string', $validator->getDescription());
    }

    public function testMethodWrongType()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('contains("title", "Iron")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query method only supported on array attributes: contains', $validator->getDescription());
    }

    public function testQueryDate()
    {
        $validator = new QueryValidator($this->schema);
        $response = $validator->isValid(Query::parse('greaterThan("birthDay", "1960-01-01 10:10:10")'));
        $this->assertEquals(true, $response);
    }

    public function testQueryLimit()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('limit(25)'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(Query::parse('limit()'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(Query::parse('limit(-1)'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(Query::parse('limit(10000)'));
        $this->assertEquals(false, $response);
    }

    public function testQueryOffset()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('offset(25)'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(Query::parse('offset()'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(Query::parse('offset(-1)'));
        $this->assertEquals(false, $response);

        $response = $validator->isValid(Query::parse('offset(10000)'));
        $this->assertEquals(false, $response);
    }

    public function testQueryOrder()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('orderAsc("title")'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(Query::parse('orderAsc("")'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(Query::parse('orderAsc("doesNotExist")'));
        $this->assertEquals(false, $response);
    }

    public function testQueryCursor()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('cursorAfter("asdf")'));
        $this->assertEquals(true, $response);

        $response = $validator->isValid(Query::parse('cursorAfter()'));
        $this->assertEquals(false, $response);
    }
}
