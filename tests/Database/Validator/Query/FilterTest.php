<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Filter;

class FilterTest extends TestCase
{
    protected Base|null $validator = null;

    public function setUp(): void
    {
        $this->validator = new Filter(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                ]),
            ],
        );
    }

    public function testSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::between('attr', '1975-12-06', '2050-12-06')));
        $this->assertTrue($this->validator->isValid(Query::isNotNull('attr')));
        $this->assertTrue($this->validator->isValid(Query::isNull('attr')));
        $this->assertTrue($this->validator->isValid(Query::startsWith('attr', 'super')));
        $this->assertTrue($this->validator->isValid(Query::endsWith('attr', 'man')));
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
        $this->assertFalse($this->validator->isValid(Query::orderAsc('attr')));
        $this->assertFalse($this->validator->isValid(Query::orderDesc('attr')));
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_CURSORAFTER, values: ['asdf'])));
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: ['asdf'])));
    }

    public function testTypeMissmatch(): void
    {
        $this->assertFalse($this->validator->isValid(Query::parse('equal("attr", false)')));
        $this->assertEquals('Query type does not match expected: string', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('equal("attr", null)')));
        $this->assertEquals('Query type does not match expected: string', $this->validator->getDescription());
    }

    public function testEmptyValues(): void
    {
        $this->assertFalse($this->validator->isValid(Query::parse('notEqual("attr", [])')));
        $this->assertEquals('NotEqual can take only one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('contains("attr", [])')));
        $this->assertEquals('Contains queries require at least one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('equal("attr", [])')));
        $this->assertEquals('Equal queries require at least one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('lessThan("attr", [])')));
        $this->assertEquals('LessThan can take only one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('lessThanEqual("attr", [])')));
        $this->assertEquals('LessThanEqual can take only one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('search("attr", [])')));
        $this->assertEquals('Search can take only one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('greaterThanEqual("attr", [])')));
        $this->assertEquals('GreaterThanEqual can take only one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::parse('greaterThan("attr", [])')));
        $this->assertEquals('GreaterThan can take only one value.', $this->validator->getDescription());
    }
}
