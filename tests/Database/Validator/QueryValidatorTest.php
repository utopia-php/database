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

        $this->assertEquals(true, $validator->isValid(Query::parse('$id.equal("Iron Man", "Ant Man")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('title.notEqual("Iron Man", "Ant Man")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('description.equal("Best movie ever")')));
        $this->assertEquals(true, $validator->isValid(Query::parse('rating.greater(4)')));
        $this->assertEquals(true, $validator->isValid(Query::parse('price.lesserEqual(6.50)')));
        $this->assertEquals(true, $validator->isValid(Query::parse('tags.contains("action")')));
    }

    public function testInvalidOperator()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('title.eqqual("Iron Man")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query operator invalid: eqqual', $validator->getDescription());
    }

    public function testAttributeNotFound()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('name.equal("Iron Man")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Attribute not found in schema: name', $validator->getDescription());
    }

    public function testAttributeWrongType()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('title.equal(1776)'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query type does not match expected: string', $validator->getDescription());
    }

    public function testOperatorWrongType()
    {
        $validator = new QueryValidator($this->schema);

        $response = $validator->isValid(Query::parse('title.contains("Iron")'));

        $this->assertEquals(false, $response);
        $this->assertEquals('Query operator only supported on array attributes: contains', $validator->getDescription());
    }
}
