<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Exception\Query as QueryException;

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

    /**
     * @return void
     * @throws QueryException
     */
    public function testParse(): void
    {
        $jsonString = Query::equal('title', ['Iron Man'])->toString();
        $query = Query::parse($jsonString);
        $this->assertEquals('{"method":"equal","attribute":"title","values":["Iron Man"]}', $jsonString);
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = Query::parse(Query::lessThan('year', 2001)->toString());
        $this->assertEquals('lessThan', $query->getMethod());
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals(2001, $query->getValues()[0]);

        $query = Query::parse(Query::equal('published', [true])->toString());
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertTrue($query->getValues()[0]);

        $query = Query::parse(Query::equal('published', [false])->toString());
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertFalse($query->getValues()[0]);

        $query = Query::parse(Query::equal('actors', [' Johnny Depp ', ' Brad Pitt', 'Al Pacino '])->toString());
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals(' Johnny Depp ', $query->getValues()[0]);
        $this->assertEquals(' Brad Pitt', $query->getValues()[1]);
        $this->assertEquals('Al Pacino ', $query->getValues()[2]);

        $query = Query::parse(Query::equal('actors', ['Brad Pitt', 'Johnny Depp'])->toString());
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals('Brad Pitt', $query->getValues()[0]);
        $this->assertEquals('Johnny Depp', $query->getValues()[1]);

        $query = Query::parse(Query::contains('writers', ['Tim O\'Reilly'])->toString());
        $this->assertEquals('contains', $query->getMethod());
        $this->assertEquals('writers', $query->getAttribute());
        $this->assertEquals('Tim O\'Reilly', $query->getValues()[0]);

        $query = Query::parse(Query::greaterThan('score', 8.5)->toString());
        $this->assertEquals('greaterThan', $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals(8.5, $query->getValues()[0]);

        $query = Query::parse(Query::notEqual('director', 'null')->toString());
        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals('null', $query->getValues()[0]);

        $query = Query::parse(Query::isNull('director')->toString());
        $this->assertEquals('isNull', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::parse(Query::isNotNull('director')->toString());
        $this->assertEquals('isNotNull', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::parse(Query::startsWith('director', 'Quentin')->toString());
        $this->assertEquals('startsWith', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(['Quentin'], $query->getValues());

        $query = Query::parse(Query::endsWith('director', 'Tarantino')->toString());
        $this->assertEquals('endsWith', $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(['Tarantino'], $query->getValues());

        $query = Query::parse(Query::select(['title', 'director'])->toString());
        $this->assertEquals('select', $query->getMethod());
        $this->assertEquals(null, $query->getAttribute());
        $this->assertEquals(['title', 'director'], $query->getValues());

        $query = Query::parse(Query::between('age', 15, 18)->toString());
        $this->assertEquals('between', $query->getMethod());
        $this->assertEquals('age', $query->getAttribute());
        $this->assertEquals([15, 18], $query->getValues());

        $query = Query::parse(Query::between('lastUpdate', 'DATE1', 'DATE2')->toString());
        $this->assertEquals('between', $query->getMethod());
        $this->assertEquals('lastUpdate', $query->getAttribute());
        $this->assertEquals(['DATE1', 'DATE2'], $query->getValues());

        $query = Query::parse(Query::equal('attr', [1])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertEquals('attr', $query->getAttribute());
        $this->assertEquals([1], $query->getValues());

        $query = Query::parse(Query::equal('attr', [0])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertEquals('attr', $query->getAttribute());
        $this->assertEquals([0], $query->getValues());

        $query = Query::parse(Query::equal('1', ['[Hello] World'])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertEquals('1', $query->getAttribute());
        $this->assertEquals(['[Hello] World'], $query->getValues());

        $query = Query::parse(Query::equal('1', ['Hello /\\ World'])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertEquals(1, $query->getAttribute());
        $this->assertEquals(['Hello /\ World'], $query->getValues());

        $json = Query::or([
            Query::equal('actors', ['Brad Pitt']),
            Query::equal('actors', ['Johnny Depp'])
        ])->toString();

        $query = Query::parse($json);

        /** @var array<Query> $queries */
        $queries = $query->getValues();
        $this->assertCount(2, $query->getValues());
        $this->assertEquals(Query::TYPE_OR, $query->getMethod());
        $this->assertEquals(Query::TYPE_EQUAL, $queries[0]->getMethod());
        $this->assertEquals('actors', $queries[0]->getAttribute());
        $this->assertEquals($json, '{"method":"or","values":[{"method":"equal","attribute":"actors","values":["Brad Pitt"]},{"method":"equal","attribute":"actors","values":["Johnny Depp"]}]}');
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
        $this->assertTrue(Query::isMethod('and'));

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
        $this->assertTrue(Query::isMethod(QUERY::TYPE_AND));

        $this->assertFalse(Query::isMethod('invalid'));
        $this->assertFalse(Query::isMethod('lte '));
    }
}
