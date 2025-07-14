<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Filter;

class FilterTest extends TestCase
{
    protected Base|null $validator = null;

    /**
     * @throws \Utopia\Database\Exception
     */
    public function setUp(): void
    {
        $attributes = [
            new Document([
                '$id' => 'string',
                'key' => 'string',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
            new Document([
                '$id' => 'string_array',
                'key' => 'string_array',
                'type' => Database::VAR_STRING,
                'array' => true,
            ]),
            new Document([
                '$id' => 'integer_array',
                'key' => 'integer_array',
                'type' => Database::VAR_INTEGER,
                'array' => true,
            ]),
            new Document([
                '$id' => 'integer',
                'key' => 'integer',
                'type' => Database::VAR_INTEGER,
                'array' => false,
            ]),
        ];

        $this->validator = new Filter($attributes, 'int');
    }

    public function testSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::between('string', '1975-12-06', '2050-12-06')));
        $this->assertTrue($this->validator->isValid(Query::isNotNull('string')));
        $this->assertTrue($this->validator->isValid(Query::isNull('string')));
        $this->assertTrue($this->validator->isValid(Query::startsWith('string', 'super')));
        $this->assertTrue($this->validator->isValid(Query::endsWith('string', 'man')));
        $this->assertTrue($this->validator->isValid(Query::contains('string_array', ['super'])));
        $this->assertTrue($this->validator->isValid(Query::contains('integer_array', [100,10,-1])));
        $this->assertTrue($this->validator->isValid(Query::contains('string_array', ["1","10","-1"])));
        $this->assertTrue($this->validator->isValid(Query::contains('string', ['super'])));
    }

    public function testFailure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::select(['attr'])));
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::limit(1)));
        $this->assertFalse($this->validator->isValid(Query::limit(0)));
        $this->assertFalse($this->validator->isValid(Query::limit(100)));
        $this->assertFalse($this->validator->isValid(Query::limit(-1)));
        $this->assertFalse($this->validator->isValid(Query::limit(101)));
        $this->assertFalse($this->validator->isValid(Query::offset(1)));
        $this->assertFalse($this->validator->isValid(Query::offset(0)));
        $this->assertFalse($this->validator->isValid(Query::offset(5000)));
        $this->assertFalse($this->validator->isValid(Query::offset(-1)));
        $this->assertFalse($this->validator->isValid(Query::offset(5001)));
        $this->assertFalse($this->validator->isValid(Query::equal('dne', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::equal('', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::orderAsc('string')));
        $this->assertFalse($this->validator->isValid(Query::orderDesc('string')));
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_CURSOR_AFTER, values: ['asdf'])));
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_CURSOR_BEFORE, values: ['asdf'])));
        $this->assertFalse($this->validator->isValid(Query::contains('integer', ['super'])));
        $this->assertFalse($this->validator->isValid(Query::equal('integer_array', [100,-1])));
        $this->assertFalse($this->validator->isValid(Query::contains('integer_array', [10.6])));
    }

    public function testTypeMismatch(): void
    {
        $this->assertFalse($this->validator->isValid(Query::equal('string', [false])));
        $this->assertEquals('Query value is invalid for attribute "string"', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::equal('string', [1])));
        $this->assertEquals('Query value is invalid for attribute "string"', $this->validator->getDescription());
    }

    public function testEmptyValues(): void
    {
        $this->assertFalse($this->validator->isValid(Query::contains('string', [])));
        $this->assertEquals('Contains queries require at least one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::equal('string', [])));
        $this->assertEquals('Equal queries require at least one value.', $this->validator->getDescription());
    }

    public function testMaxValuesCount(): void
    {
        $values = [];
        for ($i = 1; $i <= 200; $i++) {
            $values[] = $i;
        }

        $this->assertFalse($this->validator->isValid(Query::equal('integer', $values)));
        $this->assertEquals('Query on attribute has greater than 100 values: integer', $this->validator->getDescription());
    }
}
