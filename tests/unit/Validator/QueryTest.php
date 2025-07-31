<?php

namespace Tests\Unit\Validator;

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
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $this->assertEquals(true, $validator->isValid([Query::equal('$id', ['Iron Man', 'Ant Man'])]));
        $this->assertEquals(true, $validator->isValid([Query::equal('$id', ['Iron Man'])]));
        $this->assertEquals(true, $validator->isValid([Query::equal('description', ['Best movie ever'])]));
        $this->assertEquals(true, $validator->isValid([Query::greaterThan('rating', 4)]));
        $this->assertEquals(true, $validator->isValid([Query::notEqual('title', 'Iron Man')]));
        $this->assertEquals(true, $validator->isValid([Query::lessThan('price', 6.50)]));
        $this->assertEquals(true, $validator->isValid([Query::lessThanEqual('price', 6)]));
        $this->assertEquals(true, $validator->isValid([Query::contains('tags', ['action1', 'action2'])]));
        $this->assertEquals(true, $validator->isValid([Query::contains('tags', ['action1'])]));
        $this->assertEquals(true, $validator->isValid([Query::cursorAfter(new Document(['$id' => 'docId']))]));
        $this->assertEquals(true, $validator->isValid([Query::cursorBefore(new Document(['$id' => 'docId']))]));
        $this->assertEquals(true, $validator->isValid([Query::orderAsc('title')]));
        $this->assertEquals(true, $validator->isValid([Query::orderDesc('title')]));
        $this->assertEquals(true, $validator->isValid([Query::isNull('title')]));
        $this->assertEquals(true, $validator->isValid([Query::isNotNull('title')]));
        $this->assertEquals(true, $validator->isValid([Query::between('price', 1.5, 10.9)]));
        $this->assertEquals(true, $validator->isValid([Query::between('birthDay', '2024-01-01', '2023-01-01')]));
        $this->assertEquals(true, $validator->isValid([Query::startsWith('title', 'Fro')]));
        $this->assertEquals(true, $validator->isValid([Query::endsWith('title', 'Zen')]));
        $this->assertEquals(true, $validator->isValid([Query::select(['title', 'description'])]));
        $this->assertEquals(true, $validator->isValid([Query::notEqual('title', '')]));
    }

    /**
     * @throws Exception
     */
    public function testAttributeNotFound(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::equal('name', ['Iron Man'])]);
        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Attribute not found in schema: name', $validator->getDescription());

        $response = $validator->isValid([Query::orderAsc('name')]);
        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Attribute not found in schema: name', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testAttributeWrongType(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::equal('title', [1776])]);
        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Query value is invalid for attribute "title"', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testQueryDate(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::greaterThan('birthDay', '1960-01-01 10:10:10')]);
        $this->assertEquals(true, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryLimit(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::limit(25)]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::limit(-1)]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryOffset(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::offset(25)]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::offset(-1)]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryOrder(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::orderAsc('title')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::orderAsc('')]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::orderAsc()]);
        $this->assertEquals(true, $response);

        $response = $validator->isValid([Query::orderAsc('doesNotExist')]);
        $this->assertEquals(false, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryCursor(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

        $response = $validator->isValid([Query::cursorAfter(new Document(['$id' => 'asdf']))]);
        $this->assertEquals(true, $response);
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

        $queries = Query::getByType($queries, [Query::TYPE_CURSOR_AFTER, Query::TYPE_CURSOR_BEFORE]);
        $this->assertCount(2, $queries);
        foreach ($queries as $query) {
            $this->assertEquals(true, in_array($query->getMethod(), [Query::TYPE_CURSOR_AFTER, Query::TYPE_CURSOR_BEFORE]));
        }
    }

    /**
     * @throws Exception
     */
    public function testQueryEmpty(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

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

        $response = $validator->isValid([Query::isNull('price')]);
        $this->assertEquals(true, $response);
    }

    /**
     * @throws Exception
     */
    public function testOrQuery(): void
    {
        $validator = new Documents($this->attributes, [], Database::VAR_ID_INT);

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

        $this->assertEquals('Invalid query: Or queries can only contain filter queries', $validator->getDescription());
    }
}
