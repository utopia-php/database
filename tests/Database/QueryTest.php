<?php

namespace Utopia\Tests;

use Utopia\Database\Query;
use PHPUnit\Framework\TestCase;

class QueryTest extends TestCase
{

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testCreate(): void
    {
        $query = new Query('title', 'equal', ['Iron Man']);

        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('equal', $query->getOperator());
        $this->assertContains('Iron Man', $query->getValues());
    }

    public function testParse()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('equal', $query->getOperator());
        $this->assertContains('Iron Man', $query->getValues());
        
        $query = Query::parse('year.lesser(2001)'); 

        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals('lesser', $query->getOperator());
        $this->assertContains(2001, $query->getValues());

        $query = Query::parse('published.equal(true)');

        $this->assertEquals('published', $query->getAttribute());
        $this->assertEquals('equal', $query->getOperator());
        $this->assertContains(true, $query->getValues());

        $query = Query::parse('published.equal(false)');

        $this->assertEquals('published', $query->getAttribute());
        $this->assertEquals('equal', $query->getOperator());
        $this->assertContains(false, $query->getValues());

        $query = Query::parse('actors.notContains( " Johnny Depp ",  " Brad Pitt" , "Al Pacino")');

        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals('notContains', $query->getOperator());
        $this->assertContains(' Johnny Depp ', $query->getValues());
        $this->assertContains(' Brad Pitt', $query->getValues());
        $this->assertContains('Al Pacino', $query->getValues());

        $query = Query::parse('actors.equal("Brad Pitt", "Johnny Depp")');

        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals('equal', $query->getOperator());
        $this->assertContains('Brad Pitt', $query->getValues());
        $this->assertContains('Johnny Depp', $query->getValues());

        $query = Query::parse('writers.contains("Tim O\'Reilly")');

        $this->assertEquals('writers', $query->getAttribute());
        $this->assertEquals('contains', $query->getOperator());
        $this->assertContains("Tim O'Reilly", $query->getValues());

        $query = Query::parse('score.greater(8.5)');

        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals('greater', $query->getOperator());
        $this->assertContains(8.5, $query->getValues());

        $query = Query::parse('director.notEqual("null")');

        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals('notEqual', $query->getOperator());
        $this->assertContains('null', $query->getValues());

        $query = Query::parse('director.notEqual(null)');

        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals('notEqual', $query->getOperator());
        $this->assertContains(null, $query->getValues());
    }

    public function testGetAttribute()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('title', $query->getAttribute());
    }

    public function testGetOperator()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('equal', $query->getOperator());
    }

    public function testGetValue()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertContains('Iron Man', $query->getValues());
    }

    public function testGetQuery()
    {
        $query = Query::parse('title.equal("Iron Man")')->getQuery();

        $this->assertEquals('title', $query['attribute']);
        $this->assertEquals('equal', $query['operator']);
        $this->assertContains('Iron Man', $query['values']);
    }

    public function testIsOperator()
    {
        $this->assertEquals(true, Query::isOperator('equal'));
        $this->assertEquals(true, Query::isOperator('notEqual'));
        $this->assertEquals(true, Query::isOperator('lesser'));
        $this->assertEquals(true, Query::isOperator('lesserEqual'));
        $this->assertEquals(true, Query::isOperator('greater'));
        $this->assertEquals(true, Query::isOperator('greaterEqual'));
        $this->assertEquals(true, Query::isOperator('contains'));
        $this->assertEquals(true, Query::isOperator('search'));

        $this->assertEquals(true, Query::isOperator(Query::TYPE_EQUAL));
        $this->assertEquals(true, Query::isOperator(Query::TYPE_NOTEQUAL));
        $this->assertEquals(true, Query::isOperator(Query::TYPE_LESSER));
        $this->assertEquals(true, Query::isOperator(Query::TYPE_LESSEREQUAL));
        $this->assertEquals(true, Query::isOperator(Query::TYPE_GREATER));
        $this->assertEquals(true, Query::isOperator(Query::TYPE_GREATEREQUAL));
        $this->assertEquals(true, Query::isOperator(Query::TYPE_CONTAINS));
        $this->assertEquals(true, Query::isOperator(QUERY::TYPE_SEARCH));

        $this->assertEquals(false, Query::isOperator('invalid'));
    }
}