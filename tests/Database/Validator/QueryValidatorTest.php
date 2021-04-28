<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\QueryValidator;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Query;

class QueryValidatorTest extends TestCase
{
    /**
     * @var array 
     */
    protected $schema = [
        [
            '$id' => 'title',
            'type' => Database::VAR_STRING,
            'size' => 256,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => 'description',
            'type' => Database::VAR_STRING,
            'size' => 1000000,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => 'rating',
            'type' => Database::VAR_INTEGER,
            'size' => 5,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => 'price',
            'type' => Database::VAR_FLOAT,
            'size' => 5,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => 'published',
            'type' => Database::VAR_BOOLEAN,
            'size' => 5,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => 'tags',
            'type' => Database::VAR_STRING,
            'size' => 55,
            'required' => true,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ],
    ];

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testQuery()
    {
        $validator = new QueryValidator($this->schema);

        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals(true, $validator->isValid($query));
    public function testInvalidOperator()
    {
        $validator = new QueryValidator($this->schema);

        $validator->isValid(Query::parse('title.eqqual("Iron Man")'));

        $this->assertEquals('Query operator invalid: eqqual', $validator->getDescription());
    }

    public function testAttributeNotFound()
    {
        $validator = new QueryValidator($this->schema);

        $validator->isValid(Query::parse('name.equal("Iron Man")'));

        $this->assertEquals('Attribute not found in schema: name', $validator->getDescription());
    }

    public function testAttributeWrongType()
    {
        $validator = new QueryValidator($this->schema);

        $query = Query::parse('title.equal(1776)');
        $validator->isValid($query);

        $this->assertEquals('Query type does not match expected: string', $validator->getDescription());
    }
}
