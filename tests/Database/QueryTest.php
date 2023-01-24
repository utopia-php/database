<?php

namespace Utopia\Tests;

use Utopia\Database\Document;
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
        $query = new Query(Query::TYPE_EQUAL, 'title', ['Iron Man']);

        $this->assertEquals(Query::TYPE_EQUAL, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = new Query(Query::TYPE_ORDERDESC, 'score');

        $this->assertEquals(Query::TYPE_ORDERDESC, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = new Query(Query::TYPE_LIMIT, values: [10]);

        $this->assertEquals(Query::TYPE_LIMIT, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(10, $query->getValues()[0]);

        $query = Query::equal('title', ['Iron Man']);

        $this->assertEquals(Query::TYPE_EQUAL, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = Query::greaterThan('score', 10);

        $this->assertEquals(Query::TYPE_GREATER, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals(10, $query->getValues()[0]);

        $query = Query::search('search', 'John Doe');

        $this->assertEquals(Query::TYPE_SEARCH, $query->getMethod());
        $this->assertEquals('search', $query->getAttribute());
        $this->assertEquals('John Doe', $query->getValues()[0]);

        $query = Query::orderAsc('score');

        $this->assertEquals(Query::TYPE_ORDERASC, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::limit(10);

        $this->assertEquals(Query::TYPE_LIMIT, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([10], $query->getValues());

        $cursor = new Document();
        $query = Query::cursorAfter($cursor);

        $this->assertEquals(Query::TYPE_CURSORAFTER, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([$cursor], $query->getValues());
    }

    public function testParse()
    {
        $query = Query::parse('equal("title", "Iron Man")');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = Query::parse('lessThan("year", 2001)');

        $this->assertEquals('lessThan', $query->getMethod());
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals(2001, $query->getValues()[0]);

        $query = Query::parse('equal("published", true)');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertTrue($query->getValues()[0]);

        $query = Query::parse('equal("published", false)');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertFalse($query->getValues()[0]);

        $query = Query::parse('equal("actors", [ " Johnny Depp ",  " Brad Pitt" , \'Al Pacino \' ])');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals(" Johnny Depp ", $query->getValues()[0]);
        $this->assertEquals(" Brad Pitt", $query->getValues()[1]);
        $this->assertEquals("Al Pacino ", $query->getValues()[2]);

        $query = Query::parse('equal("actors", ["Brad Pitt", "Johnny Depp"])');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals("Brad Pitt", $query->getValues()[0]);
        $this->assertEquals("Johnny Depp", $query->getValues()[1]);

        $query = Query::parse('contains("writers","Tim O\'Reilly")');

        $this->assertEquals('contains', $query->getMethod());
        $this->assertEquals('writers', $query->getAttribute());
        $this->assertEquals("Tim O'Reilly", $query->getValues()[0]);

        $query = Query::parse('greaterThan("score", 8.5)');

        $this->assertEquals('greaterThan', $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals(8.5, $query->getValues()[0]);

        $query = Query::parse('notEqual("director", "null")');

        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals('null', $query->getValues()[0]);

        $query = Query::parse('notEqual("director", null)');

        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(null, $query->getValues()[0]);
    }

    public function testParseV2()
    {
        $query = Query::parse('equal("attr", 1)');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("attr", $query->getAttribute());
        $this->assertEquals([1], $query->getValues());

        $query = Query::parse('equal("attr", [0])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("attr", $query->getAttribute());
        $this->assertEquals([0], $query->getValues());

        $query = Query::parse('equal("attr", 0,)');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("attr", $query->getAttribute());
        $this->assertEquals([0], $query->getValues());

        $query = Query::parse('equal("attr", ["0"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("attr", $query->getAttribute());
        $this->assertEquals(["0"], $query->getValues());

        $query = Query::parse('equal(1, ["[Hello] World"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("[Hello] World", $query->getValues()[0]);

        $query = Query::parse('equal(1, , , ["[Hello] World"], , , )');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("[Hello] World", $query->getValues()[0]);

        $query = Query::parse('equal(1, ["(Hello) World"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("(Hello) World", $query->getValues()[0]);

        $query = Query::parse('equal(1, ["Hello , World"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello , World", $query->getValues()[0]);

        $query = Query::parse('equal(1, ["Hello , World"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello , World", $query->getValues()[0]);

        $query = Query::parse('equal(1, ["Hello /\ World"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello /\ World", $query->getValues()[0]);

        $query = Query::parse('equal(1, ["I\'m [**awesome**], \"Dev\"eloper"])');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("I'm [**awesome**], \"Dev\"eloper", $query->getValues()[0]);

        $query = Query::parse('equal(1, "\\\\")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("\\\\", $query->getValues()[0]);

        $query = Query::parse('equal(1, "Hello\\\\")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello\\\\", $query->getValues()[0]);

        $query = Query::parse('equal(1, "Hello\\\\", "World")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello\\\\", $query->getValues()[0]);

        $query = Query::parse('equal(1, "Hello\\", World")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello\", World", $query->getValues()[0]);

        $query = Query::parse('equal(1, "Hello\\\\\\", ", "World")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("Hello\\\\\", ", $query->getValues()[0]);

        $query = Query::parse('equal()');
        $this->assertCount(0, $query->getValues());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(null, $query->getValue());

        $query = Query::parse('limit()');
        $this->assertCount(0, $query->getValues());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(null, $query->getValue());

        $query = Query::parse('offset()');
        $this->assertCount(0, $query->getValues());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(null, $query->getValue());

        $query = Query::parse('cursorAfter()');
        $this->assertCount(0, $query->getValues());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(null, $query->getValue());

        $query = Query::parse('orderDesc()');
        $this->assertCount(0, $query->getValues());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(null, $query->getValue());

        $query = Query::parse('equal("count", 0)');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("count", $query->getAttribute());
        $this->assertEquals(0, $query->getValue());

        $query = Query::parse('equal("value", "NormalString")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals("NormalString", $query->getValue());

        $query = Query::parse('equal("value", "{"type":"json","somekey":"someval"}")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('{"type":"json","somekey":"someval"}', $query->getValue());

        $query = Query::parse('equal("value", "{ NormalStringInBraces }")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('{ NormalStringInBraces }', $query->getValue());

        $query = Query::parse('equal("value", ""NormalStringInDoubleQuotes"")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('"NormalStringInDoubleQuotes"', $query->getValue());

        $query = Query::parse('equal("value", "{"NormalStringInDoubleQuotesAndBraces"}")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('{"NormalStringInDoubleQuotesAndBraces"}', $query->getValue());

        $query = Query::parse('equal("value", "\'NormalStringInSingleQuotes\'")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('\'NormalStringInSingleQuotes\'', $query->getValue());

        $query = Query::parse('equal("value", "{\'NormalStringInSingleQuotesAndBraces\'}")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('{\'NormalStringInSingleQuotesAndBraces\'}', $query->getValue());

        $query = Query::parse('equal("value", "SingleQuote\'InMiddle")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('SingleQuote\'InMiddle', $query->getValue());

        $query = Query::parse('equal("value", "DoubleQuote"InMiddle")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('DoubleQuote"InMiddle', $query->getValue());

        $query = Query::parse('equal("value", "Slash/InMiddle")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('Slash/InMiddle', $query->getValue());

        $query = Query::parse('equal("value", "Backslash\InMiddle")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('Backslash\InMiddle', $query->getValue());

        $query = Query::parse('equal("value", "Colon:InMiddle")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('Colon:InMiddle', $query->getValue());

        $query = Query::parse('equal("value", ""quoted":"colon"")');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("value", $query->getAttribute());
        $this->assertEquals('"quoted":"colon"', $query->getValue());
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
            Query::parse('equal("One",[55.55,\'Works\',true])'),
            // Same query with random spaces
            Query::parse('equal("One" , [55.55, \'Works\',true])')
        ];

        foreach ($queries as $query) {
            $this->assertEquals('equal', $query->getMethod());

            $this->assertIsString($query->getAttribute());
            $this->assertEquals('One', $query->getAttribute());

            $this->assertCount(3, $query->getValues());

            $this->assertIsNumeric($query->getValues()[0]);
            $this->assertEquals(55.55, $query->getValues()[0]);

            $this->assertEquals('Works', $query->getValues()[1]);

            $this->assertTrue($query->getValues()[2]);
        }
    }

    public function testGetAttribute()
    {
        $query = Query::parse('equal("title", "Iron Man")');

        $this->assertIsArray($query->getValues());
        $this->assertCount(1, $query->getValues());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);
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
