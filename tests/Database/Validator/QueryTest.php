<?php

namespace Utopia\Tests\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Documents;

class QueryTest extends TestCase
{
    /**
     * @var array<Document>
     */
    protected array $attributes;

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $attributes = [
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

        foreach ($attributes as $attribute) {
            $this->attributes[] = new Document($attribute);
        }
    }

    public function tearDown(): void
    {
    }

    /**
     * @throws Exception
     */
    public function testQuery(): void
    {
        $validator = new Documents($this->attributes, []);

        $this->assertEquals(true, $validator->isValid([Query::parse('equal("$id", ["Iron Man", "Ant Man"])')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('equal("$id", "Iron Man")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('equal("description", "Best movie ever")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('greaterThan("rating", 4)')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('notEqual("title", ["Iron Man"])')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('lessThan("price", 6.50)')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('lessThanEqual("price", 6)')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('contains("tags", ["action1", "action2"])')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('contains("tags", "action1")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('cursorAfter("docId")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('cursorBefore("docId")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('orderAsc("title")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('orderDesc("title")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('isNull("title")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('isNotNull("title")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('between("price", 1.5, 10.9)')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('between("birthDay","2024-01-01", "2023-01-01")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('startsWith("title", "Fro")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('endsWith("title", "Zen")')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('select(["title", "description"])')]));
        $this->assertEquals(true, $validator->isValid([Query::parse('notEqual("title", [""])')]));
    }

    /**
     * @throws Exception
     */
    public function testInvalidMethod(): void
    {
        $validator = new Documents($this->attributes, []);

        $this->assertEquals(false, $validator->isValid([Query::parse('eqqual("title", "Iron Man")')]));
        $this->assertEquals('Invalid query method: eqqual', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid([Query::parse('notEqual("title", ["Iron Man", "Ant Man"])')]));
    }

    /**
     * @throws Exception
     */
    public function testAttributeNotFound(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::parse('equal("name", "Iron Man")')]);

        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Attribute not found in schema: name', $validator->getDescription());

        $response = $validator->isValid([Query::parse('orderAsc("name")')]);

        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Attribute not found in schema: name', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testAttributeWrongType(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::parse('equal("title", 1776)')]);

        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Query type does not match expected: string', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testQueryDate(): void
    {
        $validator = new Documents($this->attributes, []);
        $response = $validator->isValid([Query::parse('greaterThan("birthDay", "1960-01-01 10:10:10")')]);
        $this->assertEquals(true, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryLimit(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::parse('limit(25)')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::parse('limit()')]);
        $this->assertEquals(false, $response);

        $response = $validator->isValid([Query::parse('limit(-1)')]);
        $this->assertEquals(false, $response);

        $response = $validator->isValid([Query::parse('limit("aaa")')]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryOffset(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::parse('offset(25)')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::parse('offset()')]);
        $this->assertEquals(false, $response);

        $response = $validator->isValid([Query::parse('offset(-1)')]);
        $this->assertEquals(false, $response);

        $response = $validator->isValid([Query::parse('offset("aaa")')]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryOrder(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::parse('orderAsc("title")')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::parse('orderAsc("")')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::parse('orderAsc()')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::parse('orderAsc("doesNotExist")')]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryCursor(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::parse('cursorAfter("asdf")')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::parse('cursorAfter()')]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryGetByType(): void
    {
        $queries = [
            Query::equal('key', ['value']),
            Query::select(['attr1', 'attr2']),
            Query::cursorBefore(new Document([])),
            Query::cursorAfter(new Document([])),
        ];

        $queries = Query::getByType($queries, [Query::TYPE_CURSORAFTER, Query::TYPE_CURSORBEFORE]);
        $this->assertCount(2, $queries);
        foreach ($queries as $query) {
            $this->assertEquals(true, in_array($query->getMethod(), [Query::TYPE_CURSORAFTER, Query::TYPE_CURSORBEFORE]));
        }
    }

    /**
     * @throws Exception
     */
    public function testQueryEmpty(): void
    {
        $validator = new Documents($this->attributes, []);

        $response = $validator->isValid([Query::equal('title', [''])]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::equal('published', [false])]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::equal('price', [0])]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::greaterThan('price', 0)]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::greaterThan('published', false)]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::equal('price', [])]);
        $this->assertEquals(false, $response);

        $response = $validator->isValid([Query::greaterThan('price', null)]);
        $this->assertEquals(false, $response);

        $response = $validator->isValid([Query::isNull('price')]);
        $this->assertEquals(true, $response);
    }

    /**
     * @throws Exception
     */
    public function testOrQuery(): void
    {
        $validator = new Documents($this->attributes, []);

        $this->assertFalse($validator->isValid(
            [Query::or(
                [Query::equal('title', [''])]
            )]
        ));

        $this->assertEquals('Invalid query: Or queries require at least two queries', $validator->getDescription());

        $this->assertFalse($validator->isValid(
            [
                Query::or(
                    [
                    Query::equal('price', [0]),
                    Query::equal('not_found', [''])
                ]
                )]
        ));

        $this->assertEquals('Invalid query: Attribute not found in schema: not_found', $validator->getDescription());

        $this->assertFalse($validator->isValid(
            [
                Query::equal('price', [10]),
                Query::or(
                    [
                        Query::select(['price']),
                        Query::limit(1)
                    ]
                )]
        ));

        $this->assertEquals('Invalid query: Or queries requires only filters', $validator->getDescription());
    }
}
