<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;

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

        $this->assertSame(Query::TYPE_EQUAL, $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame('Iron Man', $query->getValues()[0]);

        $query = new Query(Query::TYPE_ORDER_DESC, 'score');

        $this->assertSame(Query::TYPE_ORDER_DESC, $query->getMethod());
        $this->assertSame('score', $query->getAttribute());
        $this->assertSame([], $query->getValues());

        $query = new Query(Query::TYPE_LIMIT, values: [10]);

        $this->assertSame(Query::TYPE_LIMIT, $query->getMethod());
        $this->assertSame('', $query->getAttribute());
        $this->assertSame(10, $query->getValues()[0]);

        $query = Query::equal('title', ['Iron Man']);

        $this->assertSame(Query::TYPE_EQUAL, $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame('Iron Man', $query->getValues()[0]);

        $query = Query::greaterThan('score', 10);

        $this->assertSame(Query::TYPE_GREATER, $query->getMethod());
        $this->assertSame('score', $query->getAttribute());
        $this->assertSame(10, $query->getValues()[0]);

        // Test vector queries
        $vector = [0.1, 0.2, 0.3];

        $query = Query::vectorDot('embedding', $vector);
        $this->assertSame(Query::TYPE_VECTOR_DOT, $query->getMethod());
        $this->assertSame('embedding', $query->getAttribute());
        $this->assertSame([$vector], $query->getValues());

        $query = Query::vectorCosine('embedding', $vector);
        $this->assertSame(Query::TYPE_VECTOR_COSINE, $query->getMethod());
        $this->assertSame('embedding', $query->getAttribute());
        $this->assertSame([$vector], $query->getValues());

        $query = Query::vectorEuclidean('embedding', $vector);
        $this->assertSame(Query::TYPE_VECTOR_EUCLIDEAN, $query->getMethod());
        $this->assertSame('embedding', $query->getAttribute());
        $this->assertSame([$vector], $query->getValues());

        $query = Query::search('search', 'John Doe');

        $this->assertSame(Query::TYPE_SEARCH, $query->getMethod());
        $this->assertSame('search', $query->getAttribute());
        $this->assertSame('John Doe', $query->getValues()[0]);

        $query = Query::orderAsc('score');

        $this->assertSame(Query::TYPE_ORDER_ASC, $query->getMethod());
        $this->assertSame('score', $query->getAttribute());
        $this->assertSame([], $query->getValues());

        $query = Query::limit(10);

        $this->assertSame(Query::TYPE_LIMIT, $query->getMethod());
        $this->assertSame('', $query->getAttribute());
        $this->assertSame([10], $query->getValues());

        $cursor = new Document();
        $query = Query::cursorAfter($cursor);

        $this->assertSame(Query::TYPE_CURSOR_AFTER, $query->getMethod());
        $this->assertSame('', $query->getAttribute());
        $this->assertSame([$cursor], $query->getValues());

        $query = Query::isNull('title');

        $this->assertSame(Query::TYPE_IS_NULL, $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame([], $query->getValues());

        $query = Query::isNotNull('title');

        $this->assertSame(Query::TYPE_IS_NOT_NULL, $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame([], $query->getValues());

        $query = Query::notContains('tags', ['test', 'example']);

        $this->assertSame(Query::TYPE_NOT_CONTAINS, $query->getMethod());
        $this->assertSame('tags', $query->getAttribute());
        $this->assertSame(['test', 'example'], $query->getValues());

        $query = Query::notSearch('content', 'keyword');

        $this->assertSame(Query::TYPE_NOT_SEARCH, $query->getMethod());
        $this->assertSame('content', $query->getAttribute());
        $this->assertSame(['keyword'], $query->getValues());

        $query = Query::notStartsWith('title', 'prefix');

        $this->assertSame(Query::TYPE_NOT_STARTS_WITH, $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame(['prefix'], $query->getValues());

        $query = Query::notEndsWith('url', '.html');

        $this->assertSame(Query::TYPE_NOT_ENDS_WITH, $query->getMethod());
        $this->assertSame('url', $query->getAttribute());
        $this->assertSame(['.html'], $query->getValues());

        $query = Query::notBetween('score', 10, 20);

        $this->assertSame(Query::TYPE_NOT_BETWEEN, $query->getMethod());
        $this->assertSame('score', $query->getAttribute());
        $this->assertSame([10, 20], $query->getValues());

        // Test new date query wrapper methods
        $query = Query::createdBefore('2023-01-01T00:00:00.000Z');

        $this->assertSame(Query::TYPE_LESSER, $query->getMethod());
        $this->assertSame('$createdAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::createdAfter('2023-01-01T00:00:00.000Z');

        $this->assertSame(Query::TYPE_GREATER, $query->getMethod());
        $this->assertSame('$createdAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::updatedBefore('2023-12-31T23:59:59.999Z');

        $this->assertSame(Query::TYPE_LESSER, $query->getMethod());
        $this->assertSame('$updatedAt', $query->getAttribute());
        $this->assertSame(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::updatedAfter('2023-12-31T23:59:59.999Z');

        $this->assertSame(Query::TYPE_GREATER, $query->getMethod());
        $this->assertSame('$updatedAt', $query->getAttribute());
        $this->assertSame(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::createdBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z');

        $this->assertSame(Query::TYPE_BETWEEN, $query->getMethod());
        $this->assertSame('$createdAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::updatedBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z');

        $this->assertSame(Query::TYPE_BETWEEN, $query->getMethod());
        $this->assertSame('$updatedAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        // Test orderRandom query
        $query = Query::orderRandom();
        $this->assertSame(Query::TYPE_ORDER_RANDOM, $query->getMethod());
        $this->assertSame('', $query->getAttribute());
        $this->assertSame([], $query->getValues());
    }

    /**
     * @return void
     * @throws QueryException
     */
    public function testParse(): void
    {
        $jsonString = Query::equal('title', ['Iron Man'])->toString();
        $query = Query::parse($jsonString);
        $this->assertSame('{"method":"equal","attribute":"title","values":["Iron Man"]}', $jsonString);
        $this->assertSame('equal', $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame('Iron Man', $query->getValues()[0]);

        $query = Query::parse(Query::lessThan('year', 2001)->toString());
        $this->assertSame('lessThan', $query->getMethod());
        $this->assertSame('year', $query->getAttribute());
        $this->assertSame(2001, $query->getValues()[0]);

        $query = Query::parse(Query::equal('published', [true])->toString());
        $this->assertSame('equal', $query->getMethod());
        $this->assertSame('published', $query->getAttribute());
        $this->assertTrue($query->getValues()[0]);

        $query = Query::parse(Query::equal('published', [false])->toString());
        $this->assertSame('equal', $query->getMethod());
        $this->assertSame('published', $query->getAttribute());
        $this->assertFalse($query->getValues()[0]);

        $query = Query::parse(Query::equal('actors', [' Johnny Depp ', ' Brad Pitt', 'Al Pacino '])->toString());
        $this->assertSame('equal', $query->getMethod());
        $this->assertSame('actors', $query->getAttribute());
        $this->assertSame(' Johnny Depp ', $query->getValues()[0]);
        $this->assertSame(' Brad Pitt', $query->getValues()[1]);
        $this->assertSame('Al Pacino ', $query->getValues()[2]);

        $query = Query::parse(Query::equal('actors', ['Brad Pitt', 'Johnny Depp'])->toString());
        $this->assertSame('equal', $query->getMethod());
        $this->assertSame('actors', $query->getAttribute());
        $this->assertSame('Brad Pitt', $query->getValues()[0]);
        $this->assertSame('Johnny Depp', $query->getValues()[1]);

        $query = Query::parse(Query::contains('writers', ['Tim O\'Reilly'])->toString());
        $this->assertSame('contains', $query->getMethod());
        $this->assertSame('writers', $query->getAttribute());
        $this->assertSame('Tim O\'Reilly', $query->getValues()[0]);

        $query = Query::parse(Query::greaterThan('score', 8.5)->toString());
        $this->assertSame('greaterThan', $query->getMethod());
        $this->assertSame('score', $query->getAttribute());
        $this->assertSame(8.5, $query->getValues()[0]);

        $query = Query::parse(Query::notContains('tags', ['unwanted', 'spam'])->toString());
        $this->assertSame('notContains', $query->getMethod());
        $this->assertSame('tags', $query->getAttribute());
        $this->assertSame(['unwanted', 'spam'], $query->getValues());

        $query = Query::parse(Query::notSearch('content', 'unwanted content')->toString());
        $this->assertSame('notSearch', $query->getMethod());
        $this->assertSame('content', $query->getAttribute());
        $this->assertSame(['unwanted content'], $query->getValues());

        $query = Query::parse(Query::notStartsWith('title', 'temp')->toString());
        $this->assertSame('notStartsWith', $query->getMethod());
        $this->assertSame('title', $query->getAttribute());
        $this->assertSame(['temp'], $query->getValues());

        $query = Query::parse(Query::notEndsWith('filename', '.tmp')->toString());
        $this->assertSame('notEndsWith', $query->getMethod());
        $this->assertSame('filename', $query->getAttribute());
        $this->assertSame(['.tmp'], $query->getValues());

        $query = Query::parse(Query::notBetween('score', 0, 50)->toString());
        $this->assertSame('notBetween', $query->getMethod());
        $this->assertSame('score', $query->getAttribute());
        $this->assertSame([0, 50], $query->getValues());

        $query = Query::parse(Query::notEqual('director', 'null')->toString());
        $this->assertSame('notEqual', $query->getMethod());
        $this->assertSame('director', $query->getAttribute());
        $this->assertSame('null', $query->getValues()[0]);

        $query = Query::parse(Query::isNull('director')->toString());
        $this->assertSame('isNull', $query->getMethod());
        $this->assertSame('director', $query->getAttribute());
        $this->assertSame([], $query->getValues());

        $query = Query::parse(Query::isNotNull('director')->toString());
        $this->assertSame('isNotNull', $query->getMethod());
        $this->assertSame('director', $query->getAttribute());
        $this->assertSame([], $query->getValues());

        $query = Query::parse(Query::startsWith('director', 'Quentin')->toString());
        $this->assertSame('startsWith', $query->getMethod());
        $this->assertSame('director', $query->getAttribute());
        $this->assertSame(['Quentin'], $query->getValues());

        $query = Query::parse(Query::endsWith('director', 'Tarantino')->toString());
        $this->assertSame('endsWith', $query->getMethod());
        $this->assertSame('director', $query->getAttribute());
        $this->assertSame(['Tarantino'], $query->getValues());

        $query = Query::parse(Query::select(['title', 'director'])->toString());
        $this->assertSame('select', $query->getMethod());
        $this->assertSame(null, $query->getAttribute());
        $this->assertSame(['title', 'director'], $query->getValues());

        // Test new date query wrapper methods parsing
        $query = Query::parse(Query::createdBefore('2023-01-01T00:00:00.000Z')->toString());
        $this->assertSame('lessThan', $query->getMethod());
        $this->assertSame('$createdAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::parse(Query::createdAfter('2023-01-01T00:00:00.000Z')->toString());
        $this->assertSame('greaterThan', $query->getMethod());
        $this->assertSame('$createdAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::parse(Query::updatedBefore('2023-12-31T23:59:59.999Z')->toString());
        $this->assertSame('lessThan', $query->getMethod());
        $this->assertSame('$updatedAt', $query->getAttribute());
        $this->assertSame(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::updatedAfter('2023-12-31T23:59:59.999Z')->toString());
        $this->assertSame('greaterThan', $query->getMethod());
        $this->assertSame('$updatedAt', $query->getAttribute());
        $this->assertSame(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::createdBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z')->toString());
        $this->assertSame('between', $query->getMethod());
        $this->assertSame('$createdAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::updatedBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z')->toString());
        $this->assertSame('between', $query->getMethod());
        $this->assertSame('$updatedAt', $query->getAttribute());
        $this->assertSame(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::between('age', 15, 18)->toString());
        $this->assertSame('between', $query->getMethod());
        $this->assertSame('age', $query->getAttribute());
        $this->assertSame([15, 18], $query->getValues());

        $query = Query::parse(Query::between('lastUpdate', 'DATE1', 'DATE2')->toString());
        $this->assertSame('between', $query->getMethod());
        $this->assertSame('lastUpdate', $query->getAttribute());
        $this->assertSame(['DATE1', 'DATE2'], $query->getValues());

        $query = Query::parse(Query::equal('attr', [1])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertSame('attr', $query->getAttribute());
        $this->assertSame([1], $query->getValues());

        $query = Query::parse(Query::equal('attr', [0])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertSame('attr', $query->getAttribute());
        $this->assertSame([0], $query->getValues());

        $query = Query::parse(Query::equal('1', ['[Hello] World'])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertSame('1', $query->getAttribute());
        $this->assertSame(['[Hello] World'], $query->getValues());

        $query = Query::parse(Query::equal('1', ['Hello /\\ World'])->toString());
        $this->assertCount(1, $query->getValues());
        $this->assertSame(1, $query->getAttribute());
        $this->assertSame(['Hello /\ World'], $query->getValues());

        $json = Query::or([
            Query::equal('actors', ['Brad Pitt']),
            Query::equal('actors', ['Johnny Depp'])
        ])->toString();

        $query = Query::parse($json);

        /** @var array<Query> $queries */
        $queries = $query->getValues();
        $this->assertCount(2, $query->getValues());
        $this->assertSame(Query::TYPE_OR, $query->getMethod());
        $this->assertSame(Query::TYPE_EQUAL, $queries[0]->getMethod());
        $this->assertSame('actors', $queries[0]->getAttribute());
        $this->assertSame($json, '{"method":"or","values":[{"method":"equal","attribute":"actors","values":["Brad Pitt"]},{"method":"equal","attribute":"actors","values":["Johnny Depp"]}]}');

        try {
            Query::parse('{"method":["equal"],"attribute":["title"],"values":["test"]}');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertSame('Invalid query method. Must be a string, got array', $e->getMessage());
        }

        try {
            Query::parse('{"method":"equal","attribute":["title"],"values":["test"]}');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertSame('Invalid query attribute. Must be a string, got array', $e->getMessage());
        }

        try {
            Query::parse('{"method":"equal","attribute":"title","values":"test"}');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertSame('Invalid query values. Must be an array, got string', $e->getMessage());
        }

        try {
            Query::parse('false');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertSame('Invalid query. Must be an array, got boolean', $e->getMessage());
        }

        // Test orderRandom query parsing
        $query = Query::parse(Query::orderRandom()->toString());
        $this->assertSame('orderRandom', $query->getMethod());
        $this->assertSame('', $query->getAttribute());
        $this->assertSame([], $query->getValues());
    }

    public function testIsMethod(): void
    {
        $this->assertTrue(Query::isMethod('equal'));
        $this->assertTrue(Query::isMethod('notEqual'));
        $this->assertTrue(Query::isMethod('lessThan'));
        $this->assertTrue(Query::isMethod('lessThanEqual'));
        $this->assertTrue(Query::isMethod('greaterThan'));
        $this->assertTrue(Query::isMethod('greaterThanEqual'));
        $this->assertTrue(Query::isMethod('contains'));
        $this->assertTrue(Query::isMethod('notContains'));
        $this->assertTrue(Query::isMethod('search'));
        $this->assertTrue(Query::isMethod('notSearch'));
        $this->assertTrue(Query::isMethod('startsWith'));
        $this->assertTrue(Query::isMethod('notStartsWith'));
        $this->assertTrue(Query::isMethod('endsWith'));
        $this->assertTrue(Query::isMethod('notEndsWith'));
        $this->assertTrue(Query::isMethod('orderDesc'));
        $this->assertTrue(Query::isMethod('orderAsc'));
        $this->assertTrue(Query::isMethod('limit'));
        $this->assertTrue(Query::isMethod('offset'));
        $this->assertTrue(Query::isMethod('cursorAfter'));
        $this->assertTrue(Query::isMethod('cursorBefore'));
        $this->assertTrue(Query::isMethod('orderRandom'));
        $this->assertTrue(Query::isMethod('isNull'));
        $this->assertTrue(Query::isMethod('isNotNull'));
        $this->assertTrue(Query::isMethod('between'));
        $this->assertTrue(Query::isMethod('notBetween'));
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
        $this->assertTrue(Query::isMethod(Query::TYPE_NOT_CONTAINS));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_SEARCH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_NOT_SEARCH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_STARTS_WITH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_NOT_STARTS_WITH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ENDS_WITH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_NOT_ENDS_WITH));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDER_ASC));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDER_DESC));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_LIMIT));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_OFFSET));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_CURSOR_AFTER));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_CURSOR_BEFORE));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_ORDER_RANDOM));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_IS_NULL));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_IS_NOT_NULL));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_BETWEEN));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_NOT_BETWEEN));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_SELECT));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_OR));
        $this->assertTrue(Query::isMethod(QUERY::TYPE_AND));

        $this->assertFalse(Query::isMethod('invalid'));
        $this->assertFalse(Query::isMethod('lte '));
    }

    public function testNewQueryTypesInTypesArray(): void
    {
        $this->assertContains(Query::TYPE_NOT_CONTAINS, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_SEARCH, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_STARTS_WITH, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_ENDS_WITH, Query::TYPES);
        $this->assertContains(Query::TYPE_NOT_BETWEEN, Query::TYPES);
        $this->assertContains(Query::TYPE_ORDER_RANDOM, Query::TYPES);
    }
}
