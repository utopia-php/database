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
        $query = new Query('equal', ['title', 'Iron Man']);

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('title', $query->getParams()[0]);
        $this->assertEquals('Iron Man', $query->getParams()[1]);
    }

    public function testParse()
    {
        $query = Query::parse('equal("title", "Iron Man")');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('title', $query->getParams()[0]);
        $this->assertEquals('Iron Man', $query->getParams()[1]);
        
        $query = Query::parse('lesser("year", 2001)'); 

        $this->assertEquals('lesser', $query->getMethod());
        $this->assertEquals('year', $query->getParams()[0]);
        $this->assertEquals(2001, $query->getParams()[1]);

        $query = Query::parse('equal("published", true)');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getParams()[0]);
        $this->assertEquals(true, $query->getParams()[1]);

        $query = Query::parse('equal("published", false)');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getParams()[0]);
        $this->assertEquals(false, $query->getParams()[1]);

        $query = Query::parse('notContains("actors", [ " Johnny Depp ",  " Brad Pitt" , \'Al Pacino \' ])');

        $this->assertEquals('notContains', $query->getMethod());
        $this->assertEquals('actors', $query->getParams()[0]);
        $this->assertEquals(" Johnny Depp ", $query->getParams()[1][0]);
        $this->assertEquals(" Brad Pitt", $query->getParams()[1][1]);
        $this->assertEquals("Al Pacino ", $query->getParams()[1][2]);

        $query = Query::parse('equal("actors", ["Brad Pitt", "Johnny Depp"])');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getParams()[0]);
        $this->assertEquals("Brad Pitt", $query->getParams()[1][0]);
        $this->assertEquals("Johnny Depp", $query->getParams()[1][1]);

        $query = Query::parse('contains("writers","Tim O\'Reilly")');

        $this->assertEquals('contains', $query->getMethod());
        $this->assertEquals('writers', $query->getParams()[0]);
        $this->assertEquals("Tim O'Reilly", $query->getParams()[1]);

        $query = Query::parse('greater("score", 8.5)');

        $this->assertEquals('greater', $query->getMethod());
        $this->assertEquals('score', $query->getParams()[0]);
        $this->assertEquals(8.5, $query->getParams()[1]);

        $query = Query::parse('notEqual("director", "null")');

        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getParams()[0]);
        $this->assertEquals('null', $query->getParams()[1]);

        $query = Query::parse('notEqual("director", null)');

        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getParams()[0]);
        $this->assertEquals(null, $query->getParams()[1]);
    }

    public function testParseV2() {
        $query = Query::parse('equal(1)');
        $this->assertEquals(1, $query->getParams()[0]);

        $query = Query::parse('equal(1, ["[Hello] World"])');
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("[Hello] World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["(Hello) World"])');
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("(Hello) World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["Hello , World"])');
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello , World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["Hello , World"])');
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello , World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["Hello /\ World"])');
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello /\ World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["I\'m [**awesome**], \"Dev\"eloper"])');
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("I'm [**awesome**], \"Dev\"eloper", $query->getParams()[1][0]);
    }

    public function testParseComplex()
    {
        $queries = [
            Query::parse('equal("One",3,[55.55,\'Works\',true],false,null)'),
            // Same query with random spaces
            Query::parse('equal("One" , 3 , [55.55, \'Works\',true], false, null)')
        ];

        foreach ($queries as $query) {
            $this->assertEquals('equal', $query->getMethod());
            $this->assertCount(5, $query->getParams());
    
            $this->assertIsString($query->getParams()[0]);
            $this->assertEquals('One', $query->getParams()[0]);
    
            $this->assertIsNumeric($query->getParams()[1]);
            $this->assertEquals(3, $query->getParams()[1]);

            $this->assertIsArray($query->getParams()[2]);
            $this->assertCount(3, $query->getParams()[2]);
            $this->assertIsNumeric($query->getParams()[2][0]);
            $this->assertEquals(55.55, $query->getParams()[2][0]);
            $this->assertIsString($query->getParams()[2][1]);
            $this->assertEquals('Works', $query->getParams()[2][1]);
            $this->assertTrue($query->getParams()[2][2]);
    
            $this->assertFalse($query->getParams()[3]);
    
            $this->assertNull($query->getParams()[4]);
        }
    }

    public function testGetAttribute()
    {
        $query = Query::parse('equal("title", "Iron Man")');

        $this->assertIsArray($query->getParams());
        $this->assertCount(2, $query->getParams());
        $this->assertEquals('title', $query->getParams()[0]);
        $this->assertEquals('Iron Man', $query->getParams()[1]);
    }

    public function testHelperMethods()
    {
        $query = Query::parse('equal("title", "Iron Man")');

        $this->assertEquals('title', $query->getFirstParam());
        $this->assertEquals('title', $query->getParams()[0]);

        $this->assertIsArray($query->getArrayParam(1));
        $this->assertCount(1, $query->getArrayParam(1));


        $query->setFirstParam("name");

        $this->assertEquals('name', $query->getFirstParam());
        $this->assertEquals('name', $query->getParams()[0]);

        $query = Query::parse('equal("title", ["Iron Man", "Spider Man"])');

        $this->assertIsArray($query->getArrayParam(1));
        $this->assertCount(2, $query->getArrayParam(1));
    }

    public function testGetMethod()
    {
        $query = Query::parse('equal("title", "Iron Man")');

        $this->assertEquals('equal', $query->getMethod());
    }

    public function testisMethod()
    {

        $this->assertEquals(true, Query::isMethod('equal'));
        $this->assertEquals(true, Query::isMethod('notEqual'));
        $this->assertEquals(true, Query::isMethod('lessThan'));
        $this->assertEquals(true, Query::isMethod('lessThanEqual'));
        $this->assertEquals(true, Query::isMethod('greaterThan'));
        $this->assertEquals(true, Query::isMethod('greaterThanEqual'));
        $this->assertEquals(true, Query::isMethod('contains'));
        $this->assertEquals(true, Query::isMethod('search'));
        $this->assertEquals(true, Query::isMethod('orderDesc'));
        $this->assertEquals(true, Query::isMethod('orderAsc'));
        $this->assertEquals(true, Query::isMethod('limit'));
        $this->assertEquals(true, Query::isMethod('offset'));
        $this->assertEquals(true, Query::isMethod('cursorAfter'));
        $this->assertEquals(true, Query::isMethod('cursorBefore'));

        $this->assertEquals(true, Query::isMethod(Query::TYPE_EQUAL));
        $this->assertEquals(true, Query::isMethod(Query::TYPE_NOTEQUAL));
        $this->assertEquals(true, Query::isMethod(Query::TYPE_LESSER));
        $this->assertEquals(true, Query::isMethod(Query::TYPE_LESSEREQUAL));
        $this->assertEquals(true, Query::isMethod(Query::TYPE_GREATER));
        $this->assertEquals(true, Query::isMethod(Query::TYPE_GREATEREQUAL));
        $this->assertEquals(true, Query::isMethod(Query::TYPE_CONTAINS));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_SEARCH));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_ORDERASC));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_ORDERDESC));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_LIMIT));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_OFFSET));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_CURSORAFTER));
        $this->assertEquals(true, Query::isMethod(QUERY::TYPE_CURSORBEFORE));

        $this->assertEquals(true, Query::isMethod('lt'));
        $this->assertEquals(true, Query::isMethod('lte'));
        $this->assertEquals(true, Query::isMethod('gt'));
        $this->assertEquals(true, Query::isMethod('gte'));
        $this->assertEquals(true, Query::isMethod('eq'));
        $this->assertEquals(true, Query::isMethod('page'));

        $this->assertEquals(false, Query::isMethod('invalid'));
    }
}