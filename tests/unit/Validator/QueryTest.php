<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Queries\V2 as DocumentsValidator;

class QueryTest extends TestCase
{
    protected QueryContext $context;

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
            [
                '$id' => 'meta',
                'key' => 'meta',
                'type' => Database::VAR_OBJECT,
                'array' => false,
            ]
        ];

        $attributes = array_map(
            fn ($attribute) => new Document($attribute),
            $attributes
        );

        $collection = new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            'name' => 'movies',
            'attributes' => $attributes,
            'indexes' => [],
        ]);

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
    public function testQuery(): void
    {
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
        $this->assertEquals(true, $validator->isValid([
            Query::select('title'),
            Query::select('description')
        ]));
        $this->assertEquals(true, $validator->isValid([Query::notEqual('title', '')]));
    }

    /**
     * @throws Exception
     */
    public function testAttributeNotFound(): void
    {
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

        $response = $validator->isValid([Query::equal('title', [1776])]);
        $this->assertEquals(false, $response);
        $this->assertEquals('Invalid query: Query value is invalid for attribute "title"', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testQueryDate(): void
    {
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

        $response = $validator->isValid([Query::greaterThan('birthDay', '1960-01-01 10:10:10')]);
        $this->assertEquals(true, $response);
    }

    /**
     * @throws Exception
     */
    public function testQueryLimit(): void
    {
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
            Query::select('attr1'),
            Query::select('attr2'),
            Query::cursorBefore(new Document([])),
            Query::cursorAfter(new Document([])),
        ];

        $query = Query::getCursorQueries($queries);

        $this->assertNotNull($query);
        $this->assertInstanceOf(Query::class, $query);
        $this->assertEquals($query->getMethod(), Query::TYPE_CURSOR_BEFORE);
        $this->assertNotEquals($query->getMethod(), Query::TYPE_CURSOR_AFTER);
    }

    /**
     * @throws Exception
     */
    public function testQueryEmpty(): void
    {
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

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
                        Query::select('price'),
                        Query::limit(1)
                    ]
                )]
        ));

        $this->assertEquals('Invalid query: Or queries can only contain filter queries', $validator->getDescription());
    }

    /**
     * @throws Exception
     */
    public function testObjectAttribute(): void
    {
        $validator = new DocumentsValidator($this->context, Database::VAR_INTEGER);

        // Object attribute query: allowed shape
        $this->assertTrue(
            $validator->isValid([
                Query::equal('meta', [
                    ['a' => [1, 2]],
                    ['b' => [212]],
                ]),
            ]),
            $validator->getDescription()
        );

        // Object attribute query: disallowed nested multiple keys in same level
        $this->assertFalse(
            $validator->isValid([
                Query::equal('meta', [
                    ['a' => [1, 'b' => [212]]],
                ]),
            ])
        );

        $this->assertEquals('Invalid object query structure for attribute "meta"', $validator->getDescription());

        // Object attribute query: allowed complex multi-key nested structure
        $this->assertTrue(
            $validator->isValid([
                Query::contains('meta', [
                    [
                        'role' => [
                            'name' => ['test1', 'test2'],
                            'ex' => ['new' => 'test1'],
                        ],
                    ],
                ]),
            ])
        );
    }
}
