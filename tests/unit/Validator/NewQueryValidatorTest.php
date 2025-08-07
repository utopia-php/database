<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Filter;

class NewQueryValidatorTest extends TestCase
{
    protected Filter $validator;

    public function setUp(): void
    {
        $attributes = [
            new Attribute('title', Database::VAR_STRING, 100, true, false, null, true, false),
            new Attribute('content', Database::VAR_STRING, 1000, true, false, null, true, false),
            new Attribute('tags', Database::VAR_STRING, 50, true, false, null, true, true), // array
            new Attribute('score', Database::VAR_INTEGER, 0, true, false, null, true, false),
            new Attribute('price', Database::VAR_FLOAT, 0, true, false, null, true, false),
            new Attribute('date', Database::VAR_DATETIME, 0, true, false, null, true, false),
            new Attribute('categories', Database::VAR_STRING, 50, true, false, null, true, true), // array
        ];

        $this->validator = new Filter($attributes, [], Database::INDEX_FULLTEXT);
    }

    public function testNotContainsValidation(): void
    {
        // Test valid notContains queries
        $this->assertTrue($this->validator->isValid(Query::notContains('title', ['unwanted'])));
        $this->assertTrue($this->validator->isValid(Query::notContains('tags', ['spam', 'unwanted'])));
        $this->assertTrue($this->validator->isValid(Query::notContains('categories', ['electronics'])));

        // Test invalid notContains queries (empty values)
        $this->assertFalse($this->validator->isValid(Query::notContains('title', [])));
        $this->assertEquals('NotContains queries require at least one value.', $this->validator->getMessage());
    }

    public function testNotSearchValidation(): void
    {
        // Test valid notSearch queries
        $this->assertTrue($this->validator->isValid(Query::notSearch('title', 'unwanted')));
        $this->assertTrue($this->validator->isValid(Query::notSearch('content', 'spam keyword')));

        // Test that arrays cannot use notSearch
        $this->assertFalse($this->validator->isValid(Query::notSearch('tags', 'unwanted')));
        $this->assertEquals('Cannot query notSearch on attribute "tags" because it is an array.', $this->validator->getMessage());
    }

    public function testNotStartsWithValidation(): void
    {
        // Test valid notStartsWith queries
        $this->assertTrue($this->validator->isValid(Query::notStartsWith('title', 'temp')));
        $this->assertTrue($this->validator->isValid(Query::notStartsWith('content', 'draft')));

        // Test that arrays cannot use notStartsWith
        $this->assertFalse($this->validator->isValid(Query::notStartsWith('tags', 'temp')));
        $this->assertEquals('Cannot query notStartsWith on attribute "tags" because it is an array.', $this->validator->getMessage());
    }

    public function testNotEndsWithValidation(): void
    {
        // Test valid notEndsWith queries
        $this->assertTrue($this->validator->isValid(Query::notEndsWith('title', '.tmp')));
        $this->assertTrue($this->validator->isValid(Query::notEndsWith('content', '_draft')));

        // Test that arrays cannot use notEndsWith
        $this->assertFalse($this->validator->isValid(Query::notEndsWith('categories', '.tmp')));
        $this->assertEquals('Cannot query notEndsWith on attribute "categories" because it is an array.', $this->validator->getMessage());
    }

    public function testNotBetweenValidation(): void
    {
        // Test valid notBetween queries
        $this->assertTrue($this->validator->isValid(Query::notBetween('score', 0, 50)));
        $this->assertTrue($this->validator->isValid(Query::notBetween('price', 9.99, 19.99)));
        $this->assertTrue($this->validator->isValid(Query::notBetween('date', '2023-01-01', '2023-12-31')));

        // Test that arrays cannot use notBetween
        $this->assertFalse($this->validator->isValid(Query::notBetween('tags', 'a', 'z')));
        $this->assertEquals('Cannot query notBetween on attribute "tags" because it is an array.', $this->validator->getMessage());
    }

    public function testNotContainsArraySupport(): void
    {
        // Test that notContains works with array attributes
        $this->assertTrue($this->validator->isValid(Query::notContains('tags', ['unwanted'])));
        $this->assertTrue($this->validator->isValid(Query::notContains('categories', ['spam', 'adult'])));
        
        // Test that notContains works with string attributes for substring matching
        $this->assertTrue($this->validator->isValid(Query::notContains('title', ['unwanted'])));
        $this->assertTrue($this->validator->isValid(Query::notContains('content', ['spam'])));
    }

    public function testValueCountValidation(): void
    {
        // notContains should allow multiple values (like contains)
        $this->assertTrue($this->validator->isValid(Query::notContains('tags', ['tag1', 'tag2', 'tag3'])));
        
        // notSearch, notStartsWith, notEndsWith should require exactly one value
        $this->assertFalse($this->validator->isValid(Query::notSearch('title', ['word1', 'word2'])));
        $this->assertFalse($this->validator->isValid(Query::notStartsWith('title', ['prefix1', 'prefix2'])));
        $this->assertFalse($this->validator->isValid(Query::notEndsWith('title', ['suffix1', 'suffix2'])));
        
        // notBetween should require exactly two values
        $this->assertFalse($this->validator->isValid(Query::notBetween('score', [10])));
        $this->assertFalse($this->validator->isValid(Query::notBetween('score', [10, 20, 30])));
    }

    public function testNonExistentAttributeValidation(): void
    {
        // Test that validation fails for non-existent attributes
        $this->assertFalse($this->validator->isValid(Query::notContains('nonexistent', ['value'])));
        $this->assertFalse($this->validator->isValid(Query::notSearch('nonexistent', 'value')));
        $this->assertFalse($this->validator->isValid(Query::notStartsWith('nonexistent', 'value')));
        $this->assertFalse($this->validator->isValid(Query::notEndsWith('nonexistent', 'value')));
        $this->assertFalse($this->validator->isValid(Query::notBetween('nonexistent', 1, 10)));
    }
}