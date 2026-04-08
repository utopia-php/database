<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Query\Method;

class QueryTest extends TestCase
{
    protected function setUp(): void
    {
    }

    protected function tearDown(): void
    {
    }

    public function test_create(): void
    {
        $query = new Query(Method::Equal, 'title', ['Iron Man']);

        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = new Query(Method::OrderDesc, 'score');

        $this->assertEquals(Method::OrderDesc, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = new Query(Method::Limit, values: [10]);

        $this->assertEquals(Method::Limit, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(10, $query->getValues()[0]);

        $query = Query::equal('title', ['Iron Man']);

        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = Query::greaterThan('score', 10);

        $this->assertEquals(Method::GreaterThan, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals(10, $query->getValues()[0]);

        // Test vector queries
        $vector = [0.1, 0.2, 0.3];

        $query = Query::vectorDot('embedding', $vector);
        $this->assertEquals(Method::VectorDot, $query->getMethod());
        $this->assertEquals('embedding', $query->getAttribute());
        $this->assertEquals([$vector], $query->getValues());

        $query = Query::vectorCosine('embedding', $vector);
        $this->assertEquals(Method::VectorCosine, $query->getMethod());
        $this->assertEquals('embedding', $query->getAttribute());
        $this->assertEquals([$vector], $query->getValues());

        $query = Query::vectorEuclidean('embedding', $vector);
        $this->assertEquals(Method::VectorEuclidean, $query->getMethod());
        $this->assertEquals('embedding', $query->getAttribute());
        $this->assertEquals([$vector], $query->getValues());

        $query = Query::search('search', 'John Doe');

        $this->assertEquals(Method::Search, $query->getMethod());
        $this->assertEquals('search', $query->getAttribute());
        $this->assertEquals('John Doe', $query->getValues()[0]);

        $query = Query::orderAsc('score');

        $this->assertEquals(Method::OrderAsc, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::limit(10);

        $this->assertEquals(Method::Limit, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([10], $query->getValues());

        $cursor = new Document();
        $query = Query::cursorAfter($cursor);

        $this->assertEquals(Method::CursorAfter, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([$cursor], $query->getValues());

        $query = Query::isNull('title');

        $this->assertEquals(Method::IsNull, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::isNotNull('title');

        $this->assertEquals(Method::IsNotNull, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::notContains('tags', ['test', 'example']);

        $this->assertEquals(Method::NotContains, $query->getMethod());
        $this->assertEquals('tags', $query->getAttribute());
        $this->assertEquals(['test', 'example'], $query->getValues());

        $query = Query::notSearch('content', 'keyword');

        $this->assertEquals(Method::NotSearch, $query->getMethod());
        $this->assertEquals('content', $query->getAttribute());
        $this->assertEquals(['keyword'], $query->getValues());

        $query = Query::notStartsWith('title', 'prefix');

        $this->assertEquals(Method::NotStartsWith, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals(['prefix'], $query->getValues());

        $query = Query::notEndsWith('url', '.html');

        $this->assertEquals(Method::NotEndsWith, $query->getMethod());
        $this->assertEquals('url', $query->getAttribute());
        $this->assertEquals(['.html'], $query->getValues());

        $query = Query::notBetween('score', 10, 20);

        $this->assertEquals(Method::NotBetween, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([10, 20], $query->getValues());

        // Test new date query wrapper methods
        $query = Query::createdBefore('2023-01-01T00:00:00.000Z');

        $this->assertEquals(Method::LessThan, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::createdAfter('2023-01-01T00:00:00.000Z');

        $this->assertEquals(Method::GreaterThan, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::updatedBefore('2023-12-31T23:59:59.999Z');

        $this->assertEquals(Method::LessThan, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
        $this->assertEquals(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::updatedAfter('2023-12-31T23:59:59.999Z');

        $this->assertEquals(Method::GreaterThan, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
        $this->assertEquals(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::createdBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z');

        $this->assertEquals(Method::Between, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::updatedBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z');

        $this->assertEquals(Method::Between, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        // Test orderRandom query
        $query = Query::orderRandom();
        $this->assertEquals(Method::OrderRandom, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    /**
     * @throws QueryException
     */
    public function test_parse(): void
    {
        $jsonString = Query::equal('title', ['Iron Man'])->toString();
        $query = Query::parse($jsonString);
        $this->assertEquals('{"method":"equal","attribute":"title","values":["Iron Man"]}', $jsonString);
        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('Iron Man', $query->getValues()[0]);

        $query = Query::parse(Query::lessThan('year', 2001)->toString());
        $this->assertEquals(Method::LessThan, $query->getMethod());
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals(2001, $query->getValues()[0]);

        $query = Query::parse(Query::equal('published', [true])->toString());
        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertTrue($query->getValues()[0]);

        $query = Query::parse(Query::equal('published', [false])->toString());
        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('published', $query->getAttribute());
        $this->assertFalse($query->getValues()[0]);

        $query = Query::parse(Query::equal('actors', [' Johnny Depp ', ' Brad Pitt', 'Al Pacino '])->toString());
        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals(' Johnny Depp ', $query->getValues()[0]);
        $this->assertEquals(' Brad Pitt', $query->getValues()[1]);
        $this->assertEquals('Al Pacino ', $query->getValues()[2]);

        $query = Query::parse(Query::equal('actors', ['Brad Pitt', 'Johnny Depp'])->toString());
        $this->assertEquals(Method::Equal, $query->getMethod());
        $this->assertEquals('actors', $query->getAttribute());
        $this->assertEquals('Brad Pitt', $query->getValues()[0]);
        $this->assertEquals('Johnny Depp', $query->getValues()[1]);

        $query = Query::parse(Query::contains('writers', ['Tim O\'Reilly'])->toString());
        $this->assertEquals(Method::Contains, $query->getMethod());
        $this->assertEquals('writers', $query->getAttribute());
        $this->assertEquals('Tim O\'Reilly', $query->getValues()[0]);

        $query = Query::parse(Query::greaterThan('score', 8.5)->toString());
        $this->assertEquals(Method::GreaterThan, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals(8.5, $query->getValues()[0]);

        $query = Query::parse(Query::notContains('tags', ['unwanted', 'spam'])->toString());
        $this->assertEquals(Method::NotContains, $query->getMethod());
        $this->assertEquals('tags', $query->getAttribute());
        $this->assertEquals(['unwanted', 'spam'], $query->getValues());

        $query = Query::parse(Query::notSearch('content', 'unwanted content')->toString());
        $this->assertEquals(Method::NotSearch, $query->getMethod());
        $this->assertEquals('content', $query->getAttribute());
        $this->assertEquals(['unwanted content'], $query->getValues());

        $query = Query::parse(Query::notStartsWith('title', 'temp')->toString());
        $this->assertEquals(Method::NotStartsWith, $query->getMethod());
        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals(['temp'], $query->getValues());

        $query = Query::parse(Query::notEndsWith('filename', '.tmp')->toString());
        $this->assertEquals(Method::NotEndsWith, $query->getMethod());
        $this->assertEquals('filename', $query->getAttribute());
        $this->assertEquals(['.tmp'], $query->getValues());

        $query = Query::parse(Query::notBetween('score', 0, 50)->toString());
        $this->assertEquals(Method::NotBetween, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
        $this->assertEquals([0, 50], $query->getValues());

        $query = Query::parse(Query::notEqual('director', 'null')->toString());
        $this->assertEquals(Method::NotEqual, $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals('null', $query->getValues()[0]);

        $query = Query::parse(Query::isNull('director')->toString());
        $this->assertEquals(Method::IsNull, $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::parse(Query::isNotNull('director')->toString());
        $this->assertEquals(Method::IsNotNull, $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals([], $query->getValues());

        $query = Query::parse(Query::startsWith('director', 'Quentin')->toString());
        $this->assertEquals(Method::StartsWith, $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(['Quentin'], $query->getValues());

        $query = Query::parse(Query::endsWith('director', 'Tarantino')->toString());
        $this->assertEquals(Method::EndsWith, $query->getMethod());
        $this->assertEquals('director', $query->getAttribute());
        $this->assertEquals(['Tarantino'], $query->getValues());

        $query = Query::parse(Query::select(['title', 'director'])->toString());
        $this->assertEquals(Method::Select, $query->getMethod());
        $this->assertEquals(null, $query->getAttribute());
        $this->assertEquals(['title', 'director'], $query->getValues());

        // Test new date query wrapper methods parsing
        $query = Query::parse(Query::createdBefore('2023-01-01T00:00:00.000Z')->toString());
        $this->assertEquals(Method::LessThan, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::parse(Query::createdAfter('2023-01-01T00:00:00.000Z')->toString());
        $this->assertEquals(Method::GreaterThan, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z'], $query->getValues());

        $query = Query::parse(Query::updatedBefore('2023-12-31T23:59:59.999Z')->toString());
        $this->assertEquals(Method::LessThan, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
        $this->assertEquals(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::updatedAfter('2023-12-31T23:59:59.999Z')->toString());
        $this->assertEquals(Method::GreaterThan, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
        $this->assertEquals(['2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::createdBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z')->toString());
        $this->assertEquals(Method::Between, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::updatedBetween('2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z')->toString());
        $this->assertEquals(Method::Between, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
        $this->assertEquals(['2023-01-01T00:00:00.000Z', '2023-12-31T23:59:59.999Z'], $query->getValues());

        $query = Query::parse(Query::between('age', 15, 18)->toString());
        $this->assertEquals(Method::Between, $query->getMethod());
        $this->assertEquals('age', $query->getAttribute());
        $this->assertEquals([15, 18], $query->getValues());

        $query = Query::parse(Query::between('lastUpdate', 'DATE1', 'DATE2')->toString());
        $this->assertEquals(Method::Between, $query->getMethod());
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
            Query::equal('actors', ['Johnny Depp']),
        ])->toString();

        $query = Query::parse($json);

        /** @var array<Query> $queries */
        $queries = $query->getValues();
        $this->assertCount(2, $query->getValues());
        $this->assertEquals(Method::Or, $query->getMethod());
        $this->assertEquals(Method::Equal, $queries[0]->getMethod());
        $this->assertEquals('actors', $queries[0]->getAttribute());
        $this->assertEquals($json, '{"method":"or","values":[{"method":"equal","attribute":"actors","values":["Brad Pitt"]},{"method":"equal","attribute":"actors","values":["Johnny Depp"]}]}');

        try {
            Query::parse('{"method":["equal"],"attribute":["title"],"values":["test"]}');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertEquals('Invalid query method. Must be a string, got array', $e->getMessage());
        }

        try {
            Query::parse('{"method":"equal","attribute":["title"],"values":["test"]}');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertEquals('Invalid query attribute. Must be a string, got array', $e->getMessage());
        }

        try {
            Query::parse('{"method":"equal","attribute":"title","values":"test"}');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertEquals('Invalid query values. Must be an array, got string', $e->getMessage());
        }

        try {
            Query::parse('false');
            $this->fail('Failed to throw exception');
        } catch (QueryException $e) {
            $this->assertEquals('Invalid query. Must be an array, got boolean', $e->getMessage());
        }

        // Test orderRandom query parsing
        $query = Query::parse(Query::orderRandom()->toString());
        $this->assertEquals(Method::OrderRandom, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function test_is_method(): void
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

        $this->assertTrue(Query::isMethod(Method::Equal));
        $this->assertTrue(Query::isMethod(Method::NotEqual));
        $this->assertTrue(Query::isMethod(Method::LessThan));
        $this->assertTrue(Query::isMethod(Method::LessThanEqual));
        $this->assertTrue(Query::isMethod(Method::GreaterThan));
        $this->assertTrue(Query::isMethod(Method::GreaterThanEqual));
        $this->assertTrue(Query::isMethod(Method::Contains));
        $this->assertTrue(Query::isMethod(Method::NotContains));
        $this->assertTrue(Query::isMethod(Method::Search));
        $this->assertTrue(Query::isMethod(Method::NotSearch));
        $this->assertTrue(Query::isMethod(Method::StartsWith));
        $this->assertTrue(Query::isMethod(Method::NotStartsWith));
        $this->assertTrue(Query::isMethod(Method::EndsWith));
        $this->assertTrue(Query::isMethod(Method::NotEndsWith));
        $this->assertTrue(Query::isMethod(Method::OrderAsc));
        $this->assertTrue(Query::isMethod(Method::OrderDesc));
        $this->assertTrue(Query::isMethod(Method::Limit));
        $this->assertTrue(Query::isMethod(Method::Offset));
        $this->assertTrue(Query::isMethod(Method::CursorAfter));
        $this->assertTrue(Query::isMethod(Method::CursorBefore));
        $this->assertTrue(Query::isMethod(Method::OrderRandom));
        $this->assertTrue(Query::isMethod(Method::IsNull));
        $this->assertTrue(Query::isMethod(Method::IsNotNull));
        $this->assertTrue(Query::isMethod(Method::Between));
        $this->assertTrue(Query::isMethod(Method::NotBetween));
        $this->assertTrue(Query::isMethod(Method::Select));
        $this->assertTrue(Query::isMethod(Method::Or));
        $this->assertTrue(Query::isMethod(Method::And));

        $this->assertFalse(Query::isMethod('invalid'));
        $this->assertFalse(Query::isMethod('lte '));
    }

    public function test_new_query_types_in_types_array(): void
    {
        $allMethods = Method::cases();
        $this->assertContains(Method::NotContains, $allMethods);
        $this->assertContains(Method::NotSearch, $allMethods);
        $this->assertContains(Method::NotStartsWith, $allMethods);
        $this->assertContains(Method::NotEndsWith, $allMethods);
        $this->assertContains(Method::NotBetween, $allMethods);
        $this->assertContains(Method::OrderRandom, $allMethods);
    }
}
