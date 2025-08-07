<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;

class NewQueryTypesTest extends TestCase
{
    public function testNotContains(): void
    {
        // Test notContains with array values
        $query = Query::notContains('tags', ['tag1', 'tag2']);
        
        $this->assertEquals(Query::TYPE_NOT_CONTAINS, $query->getMethod());
        $this->assertEquals('tags', $query->getAttribute());
        $this->assertEquals(['tag1', 'tag2'], $query->getValues());
        
        // Test notContains with single value (should still be array)
        $query = Query::notContains('category', ['electronics']);
        
        $this->assertEquals(Query::TYPE_NOT_CONTAINS, $query->getMethod());
        $this->assertEquals('category', $query->getAttribute());
        $this->assertEquals(['electronics'], $query->getValues());
    }

    public function testNotSearch(): void
    {
        $query = Query::notSearch('content', 'keyword');
        
        $this->assertEquals(Query::TYPE_NOT_SEARCH, $query->getMethod());
        $this->assertEquals('content', $query->getAttribute());
        $this->assertEquals(['keyword'], $query->getValues());
        
        // Test with phrase
        $query = Query::notSearch('description', 'search phrase');
        
        $this->assertEquals(Query::TYPE_NOT_SEARCH, $query->getMethod());
        $this->assertEquals('description', $query->getAttribute());
        $this->assertEquals(['search phrase'], $query->getValues());
    }

    public function testNotStartsWith(): void
    {
        $query = Query::notStartsWith('title', 'prefix');
        
        $this->assertEquals(Query::TYPE_NOT_STARTS_WITH, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals(['prefix'], $query->getValues());
        
        // Test with empty string
        $query = Query::notStartsWith('name', '');
        
        $this->assertEquals(Query::TYPE_NOT_STARTS_WITH, $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
        $this->assertEquals([''], $query->getValues());
    }

    public function testNotEndsWith(): void
    {
        $query = Query::notEndsWith('filename', '.txt');
        
        $this->assertEquals(Query::TYPE_NOT_ENDS_WITH, $query->getMethod());
        $this->assertEquals('filename', $query->getAttribute());
        $this->assertEquals(['.txt'], $query->getValues());
        
        // Test with suffix
        $query = Query::notEndsWith('url', '/index.html');
        
        $this->assertEquals(Query::TYPE_NOT_ENDS_WITH, $query->getMethod());
        $this->assertEquals('url', $query->getAttribute());
        $this->assertEquals(['/index.html'], $query->getValues());
    }

    public function testNotBetween(): void
    {
        // Test with integers
        $query = Query::notBetween('score', 10, 20);
        
        $this->assertEquals(Query::TYPE_NOT_BETWEEN, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([10, 20], $query->getValues());
        
        // Test with floats
        $query = Query::notBetween('price', 9.99, 19.99);
        
        $this->assertEquals(Query::TYPE_NOT_BETWEEN, $query->getMethod());
        $this->assertEquals('price', $query->getAttribute());
        $this->assertEquals([9.99, 19.99], $query->getValues());
        
        // Test with strings (for date ranges, etc.)
        $query = Query::notBetween('date', '2023-01-01', '2023-12-31');
        
        $this->assertEquals(Query::TYPE_NOT_BETWEEN, $query->getMethod());
        $this->assertEquals('date', $query->getAttribute());
        $this->assertEquals(['2023-01-01', '2023-12-31'], $query->getValues());
    }

    public function testQueryTypeConstants(): void
    {
        // Test that all new constants are defined correctly
        $this->assertEquals('notContains', Query::TYPE_NOT_CONTAINS);
        $this->assertEquals('notSearch', Query::TYPE_NOT_SEARCH);
        $this->assertEquals('notStartsWith', Query::TYPE_NOT_STARTS_WITH);
        $this->assertEquals('notEndsWith', Query::TYPE_NOT_ENDS_WITH);
        $this->assertEquals('notBetween', Query::TYPE_NOT_BETWEEN);
    }

    public function testQueryTypeValidation(): void
    {
        // Test that all new query types are recognized as valid methods
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_CONTAINS));
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_SEARCH));
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_STARTS_WITH));
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_ENDS_WITH));
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_BETWEEN));
        
        // Test with string values too
        $this->assertTrue(Query::isMethod('notContains'));
        $this->assertTrue(Query::isMethod('notSearch'));
        $this->assertTrue(Query::isMethod('notStartsWith'));
        $this->assertTrue(Query::isMethod('notEndsWith'));
        $this->assertTrue(Query::isMethod('notBetween'));
    }

    public function testQueryCreationFromConstructor(): void
    {
        // Test creating queries using the constructor directly
        $query = new Query(Query::TYPE_NOT_CONTAINS, 'tags', ['unwanted']);
        
        $this->assertEquals(Query::TYPE_NOT_CONTAINS, $query->getMethod());
        $this->assertEquals('tags', $query->getAttribute());
        $this->assertEquals(['unwanted'], $query->getValues());
        
        $query = new Query(Query::TYPE_NOT_SEARCH, 'content', ['spam']);
        
        $this->assertEquals(Query::TYPE_NOT_SEARCH, $query->getMethod());
        $this->assertEquals('content', $query->getAttribute());
        $this->assertEquals(['spam'], $query->getValues());
        
        $query = new Query(Query::TYPE_NOT_STARTS_WITH, 'title', ['temp']);
        
        $this->assertEquals(Query::TYPE_NOT_STARTS_WITH, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals(['temp'], $query->getValues());
        
        $query = new Query(Query::TYPE_NOT_ENDS_WITH, 'file', '.tmp');
        
        $this->assertEquals(Query::TYPE_NOT_ENDS_WITH, $query->getMethod());
        $this->assertEquals('file', $query->getAttribute());
        $this->assertEquals(['.tmp'], $query->getValues());
        
        $query = new Query(Query::TYPE_NOT_BETWEEN, 'age', [18, 65]);
        
        $this->assertEquals(Query::TYPE_NOT_BETWEEN, $query->getMethod());
        $this->assertEquals('age', $query->getAttribute());
        $this->assertEquals([18, 65], $query->getValues());
    }

    public function testQuerySerialization(): void
    {
        // Test that new query types can be serialized and parsed correctly
        $originalQuery = Query::notContains('tags', ['unwanted', 'spam']);
        $serialized = $originalQuery->toString();
        $parsedQuery = Query::parse($serialized);
        
        $this->assertEquals($originalQuery->getMethod(), $parsedQuery->getMethod());
        $this->assertEquals($originalQuery->getAttribute(), $parsedQuery->getAttribute());
        $this->assertEquals($originalQuery->getValues(), $parsedQuery->getValues());
        
        $originalQuery = Query::notSearch('content', 'unwanted content');
        $serialized = $originalQuery->toString();
        $parsedQuery = Query::parse($serialized);
        
        $this->assertEquals($originalQuery->getMethod(), $parsedQuery->getMethod());
        $this->assertEquals($originalQuery->getAttribute(), $parsedQuery->getAttribute());
        $this->assertEquals($originalQuery->getValues(), $parsedQuery->getValues());
        
        $originalQuery = Query::notBetween('score', 0, 50);
        $serialized = $originalQuery->toString();
        $parsedQuery = Query::parse($serialized);
        
        $this->assertEquals($originalQuery->getMethod(), $parsedQuery->getMethod());
        $this->assertEquals($originalQuery->getAttribute(), $parsedQuery->getAttribute());
        $this->assertEquals($originalQuery->getValues(), $parsedQuery->getValues());
    }

    public function testNewQueryTypesInTypesArray(): void
    {
        // Test that all new query types are included in the TYPES array
        $this->assertContains(Query::TYPE_NOT_CONTAINS, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_SEARCH, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_STARTS_WITH, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_ENDS_WITH, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_BETWEEN, Query::TYPES);
    }
}