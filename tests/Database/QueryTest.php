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
        $this->assertTrue($query->getParams()[1]);

        $query = Query::parse('equal("published", false)');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getParams()[0]);
        $this->assertFalse($query->getParams()[1]);

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

    public function testParseV2()
    {
        $query = Query::parse('equal(1)');
        $this->assertCount(1, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);

        $query = Query::parse('equal(1, ["[Hello] World"])');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("[Hello] World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, , , ["[Hello] World"], , , )');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("[Hello] World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["(Hello) World"])');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("(Hello) World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["Hello , World"])');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello , World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["Hello , World"])');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello , World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["Hello /\ World"])');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello /\ World", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, ["I\'m [**awesome**], \"Dev\"eloper"])');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("I'm [**awesome**], \"Dev\"eloper", $query->getParams()[1][0]);

        $query = Query::parse('equal(1, "\\\\")');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("\\\\", $query->getParams()[1]);
        
        $query = Query::parse('equal(1, "Hello\\\\")');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello\\\\", $query->getParams()[1]);

        $query = Query::parse('equal(1, "Hello\\\\", "World")');
        $this->assertCount(3, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello\\\\", $query->getParams()[1]);
        $this->assertEquals("World", $query->getParams()[2]);

        $query = Query::parse('equal(1, "Hello\\", World")');
        $this->assertCount(2, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello\", World", $query->getParams()[1]);

        $query = Query::parse('equal(1, "Hello\\\\\\", ", "World")');
        $this->assertCount(3, $query->getParams());
        $this->assertEquals(1, $query->getParams()[0]);
        $this->assertEquals("Hello\\\\\", ", $query->getParams()[1]);
        $this->assertEquals("World", $query->getParams()[2]);
    }

    /*
    Tests for aliases if we enable them:
    public function testAlias()
    {
        $query = Query::parse('eq(1)');
        $this->assertEquals(Query::TYPE_EQUAL, $query->getMethod());
        $query = Query::parse('lt(1)');
        $this->assertEquals(Query::TYPE_LESSER, $query->getMethod());
        $query = Query::parse('lte(1)');
        $this->assertEquals(Query::TYPE_LESSEREQUAL, $query->getMethod());
        $query = Query::parse('gt(1)');
        $this->assertEquals(Query::TYPE_GREATER, $query->getMethod());
        $query = Query::parse('gte(1)');
        $this->assertEquals(Query::TYPE_GREATEREQUAL, $query->getMethod());
    }
    */

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
        $this->assertTrue(Query::isMethod('equal'));
        $this->assertTrue(Query::isMethod('notEqual'));
        $this->assertTrue(Query::isMethod('lessThan'));
        $this->assertTrue(Query::isMethod('lessThanEqual'));
        $this->assertTrue(Query::isMethod('greaterThan'));
        $this->assertTrue(Query::isMethod('greaterThanEqual'));
        $this->assertTrue(Query::isMethod('contains'));
        $this->assertTrue(Query::isMethod('search'));
        $this->assertTrue(Query::isMethod('orderDesc'));
        $this->assertTrue(Query::isMethod('orderAsc'));
        $this->assertTrue(Query::isMethod('limit'));
        $this->assertTrue(Query::isMethod('offset'));
        $this->assertTrue(Query::isMethod('cursorAfter'));
        $this->assertTrue(Query::isMethod('cursorBefore'));

        $this->assertTrue(Query::isMethod(Query::TYPE_EQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_NOTEQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_LESSER));
        $this->assertTrue(Query::isMethod(Query::TYPE_LESSEREQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_GREATER));
        $this->assertTrue(Query::isMethod(Query::TYPE_GREATEREQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_CONTAINS));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_SEARCH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDERASC));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDERDESC));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_LIMIT));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_OFFSET));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_CURSORAFTER));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_CURSORBEFORE));

        /*
        Tests for aliases if we enable them:
        $this->assertTrue(Query::isMethod('lt'));
        $this->assertTrue(Query::isMethod('lte'));
        $this->assertTrue(Query::isMethod('gt'));
        $this->assertTrue(Query::isMethod('gte'));
        $this->assertTrue(Query::isMethod('eq'));
        */

        $this->assertFalse(Query::isMethod('invalid'));
        $this->assertFalse(Query::isMethod('lte '));
    }
}
