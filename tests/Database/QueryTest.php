<?php

namespace Utopia\Tests;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Exception\Query as QueryException;
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

        $query = new Query(Query::TYPE_ORDER_DESC, 'score');

        $this->assertEquals(Query::TYPE_ORDER_DESC, $query->getMethod());
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

        $this->assertEquals(Query::TYPE_ORDER_ASC, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::limit(10);

        $this->assertEquals(Query::TYPE_LIMIT, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([10], $query->getValues());

        $cursor = new Document();
        $query = Query::cursorAfter($cursor);

        $this->assertEquals(Query::TYPE_CURSOR_AFTER, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([$cursor], $query->getValues());

        $query = Query::isNull('title');

        $this->assertEquals(Query::TYPE_IS_NULL, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::isNotNull('title');

        $this->assertEquals(Query::TYPE_IS_NOT_NULL, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testParse(): void
    {
        $query = Query::parse('{
  			"method": "equal",
  			"attribute": "title",
  			"values": ["Iron Man"]
		}');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "lessThan",
		  "attribute": "year",
		  "values": [2001]
		}');

        $this->assertEquals('lessThan', $query->getMethod());
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals(2001, $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": "published",
		  "values": [true]
		}');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertTrue($query->getValues()[0]);

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": "published",
		  "values": [false]
		}');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertFalse($query->getValues()[0]);

        $query = Query::parse('{
		    "method": "equal",
		    "attribute": "actors",
		    "values": [" Johnny Depp ", " Brad Pitt", "Al Pacino "]
		}');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals(" Johnny Depp ", $query->getValues()[0]);
        $this->assertEquals(" Brad Pitt", $query->getValues()[1]);
        $this->assertEquals("Al Pacino ", $query->getValues()[2]);

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": "actors",
		  "values": ["Brad Pitt", "Johnny Depp"]
		}');

        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals("Brad Pitt", $query->getValues()[0]);
        $this->assertEquals("Johnny Depp", $query->getValues()[1]);

        $query = Query::parse('{
		  "method": "contains",
		  "attribute": "writers",
		  "values": ["Tim O\'Reilly"]
		}');

        $this->assertEquals('contains', $query->getMethod());
        $this->assertEquals('writers', $query->getAttribute());
        $this->assertEquals("Tim O'Reilly", $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "greaterThan",
		  "attribute": "score",
		  "values": [8.5]
		}');

        $this->assertEquals('greaterThan', $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals(8.5, $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "notEqual",
		  "attribute": "director",
		  "values": ["null"]
		}');

        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals('null', $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "notEqual",
		  "attribute": "director",
		  "values": [null]
		}');

        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(null, $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "isNull",
		  "attribute": "director",
		  "values": []
		}');

        $this->assertEquals('isNull', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::parse('{
		  "method": "isNotNull",
		  "attribute": "director",
		  "values": []
		}');

        $this->assertEquals('isNotNull', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::parse('{
		  "method": "startsWith",
		  "attribute": "director",
		  "values": ["Quentin"]
		}');

        $this->assertEquals('startsWith', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(['Quentin'], $query->getValues());

        $query = Query::parse('{
		  "method": "endsWith",
		  "attribute": "director",
		  "values": ["Tarantino"]
		}');

        $this->assertEquals('endsWith', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(['Tarantino'], $query->getValues());

        $query = Query::parse('{
		  "method": "select",
		  "attribute": null,
		  "values": ["title", "director"]
		}');

        $this->assertEquals('select', $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals('title', $query->getValues()[0]);
        $this->assertEquals('director', $query->getValues()[1]);

        $query = Query::parse('{
		  "method": "between",
		  "attribute": "age",
		  "values": [15, 18]
		}');

        $this->assertEquals('between', $query->getMethod());
        $this->assertEquals('age', $query->getAttribute());
        $this->assertEquals(15, $query->getValues()[0]);
        $this->assertEquals(18, $query->getValues()[1]);

        $query = Query::parse('{
		  "method": "between",
		  "attribute": "lastUpdate",
		  "values": ["DATE1", "DATE2"]
		}');

        $this->assertEquals('between', $query->getMethod());
        $this->assertEquals('lastUpdate', $query->getAttribute());
        $this->assertEquals('DATE1', $query->getValues()[0]);
        $this->assertEquals('DATE2', $query->getValues()[1]);

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": "attr",
		  "values": [1]
		}');

        $this->assertCount(1, $query->getValues());
        $this->assertEquals("attr", $query->getAttribute());
        $this->assertEquals([1], $query->getValues());

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": "attr",
		  "values": [0]
		}');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals("attr", $query->getAttribute());
        $this->assertEquals([0], $query->getValues());

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": 1,
		  "values": ["[Hello] World"]
		}');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("[Hello] World", $query->getValues()[0]);

        $query = Query::parse('{
		  "method": "equal",
		  "attribute": 1,
		  "values": ["(Hello) World"]
		}');
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals("(Hello) World", $query->getValues()[0]);

        try {
            Query::parse('{
			    "method": "equal",
			    "attribute": 1,
			    "values": ["Hello /\\ World"]
			}');
            $this->fail('Failed to throw exception');
        } catch (QueryException) {
            $this->assertTrue(true);
        }
    }

    public function testisMethod(): void
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
        $this->assertTrue(Query::isMethod('isNull'));
        $this->assertTrue(Query::isMethod('isNotNull'));
        $this->assertTrue(Query::isMethod('between'));
        $this->assertTrue(Query::isMethod('select'));
        $this->assertTrue(Query::isMethod('or'));

        $this->assertTrue(Query::isMethod(Query::TYPE_EQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_EQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_LESSER));
        $this->assertTrue(Query::isMethod(Query::TYPE_LESSER_EQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_GREATER));
        $this->assertTrue(Query::isMethod(Query::TYPE_GREATER_EQUAL));
        $this->assertTrue(Query::isMethod(Query::TYPE_CONTAINS));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_SEARCH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDER_ASC));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDER_DESC));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_LIMIT));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_OFFSET));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_CURSOR_AFTER));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_CURSOR_BEFORE));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_IS_NULL));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_IS_NOT_NULL));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_BETWEEN));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_SELECT));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_OR));

        $this->assertFalse(Query::isMethod('invalid'));
        $this->assertFalse(Query::isMethod('lte '));
    }
}
