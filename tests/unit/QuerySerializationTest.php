<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

final class QuerySerializationTest extends TestCase
{
    /**
     * @return array<string, array{string}>
     */
    public static function nestedQueriesWithoutAList(): array
    {
        return [
            'and with a string' => ['{"method":"and","values":"x"}'],
            'or with a number' => ['{"method":"or","values":5}'],
            'elemMatch with an object' => ['{"method":"elemMatch","attribute":"items","values":{"method":"equal"}}'],
        ];
    }

    #[DataProvider('nestedQueriesWithoutAList')]
    public function testParseRejectsANestedQueryWhoseValuesAreNotAList(string $json): void
    {
        $this->expectException(QueryException::class);

        Query::parse($json);
    }

    public function testParseDecodesStringChildrenOfANestedQuery(): void
    {
        $query = Query::parse('{"method":"and","values":["{\"method\":\"equal\",\"attribute\":\"a\",\"values\":[1]}",{"method":"equal","attribute":"b","values":[2]}]}');

        $children = $query->getValues();
        $this->assertCount(2, $children);
        $this->assertInstanceOf(Query::class, $children[0]);
        $this->assertSame('a', $children[0]->getAttribute());
        $this->assertInstanceOf(Query::class, $children[1]);
        $this->assertSame('b', $children[1]->getAttribute());
    }

    /**
     * @return array<string, array{Method, mixed, string}>
     */
    public static function nonQueryChildren(): array
    {
        return [
            'or with a string' => [Method::Or, 'x', 'string'],
            'and with an array' => [Method::And, ['method' => 'equal'], 'array'],
            'having with an integer' => [Method::Having, 7, 'int'],
        ];
    }

    #[DataProvider('nonQueryChildren')]
    public function testToArrayRejectsANonQueryChild(Method $method, mixed $child, string $type): void
    {
        $query = new Query($method, '', [Query::equal('a', ['x']), $child]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage("Invalid child query in {$method->value} at index 1: expected Query, got {$type}");

        $query->toArray();
    }

    public function testToStringRejectsANonQueryChild(): void
    {
        $query = new Query(Method::Or, '', [Query::equal('a', ['x']), 'x']);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid child query in or at index 1: expected Query, got string');

        $query->toString();
    }

    public function testJoinToArrayWritesTheAliasAndEveryOnCondition(): void
    {
        $join = Query::leftJoin('orders', 'ord', [Query::on('$id', 'ord.user'), Query::equal('ord.status', ['paid'])]);

        $this->assertSame([
            'method' => 'leftJoin',
            'attribute' => 'orders',
            'alias' => 'ord',
            'values' => [
                ['method' => 'on', 'values' => ['$id', '=', 'ord.user']],
                ['method' => 'equal', 'attribute' => 'ord.status', 'values' => ['paid']],
            ],
        ], $join->toArray());
    }

    public function testParsedJoinKeepsItsAliasAndOnConditions(): void
    {
        $join = Query::leftJoin('orders', 'ord', [Query::on('$id', 'ord.user', '!='), Query::equal('ord.status', ['paid'])]);

        $parsed = Query::parse($join->toString());

        $this->assertSame(Method::LeftJoin, $parsed->getMethod());
        $this->assertSame('orders', $parsed->getAttribute());
        $this->assertSame('ord', $parsed->getAlias());
        $on = $parsed->getJoinOnQueries();
        $this->assertCount(2, $on);
        $this->assertInstanceOf(Query::class, $on[0]);
        $this->assertSame(['$id', '!=', 'ord.user'], $on[0]->getValues());
        $this->assertSame(Method::Equal, $on[1]->getMethod());
        $this->assertSame($join->toArray(), $parsed->toArray());
    }

    public function testParsedCrossJoinKeepsItsAlias(): void
    {
        $parsed = Query::parseQuery(Query::crossJoin('regions', 'r')->toArray());

        $this->assertSame(Method::CrossJoin, $parsed->getMethod());
        $this->assertSame('r', $parsed->getAlias());
        $this->assertSame([], $parsed->getValues());
    }

    public function testParsedColumnFormJoinBecomesAnOnCondition(): void
    {
        $parsed = Query::parseQuery(['method' => 'join', 'attribute' => 'orders', 'values' => ['$id', '>', 'ord.total', 'ord']]);

        $this->assertSame('ord', $parsed->getAlias());
        $this->assertSame(
            Query::join('orders', 'ord', [Query::on('$id', 'ord.total', '>')])->toArray(),
            $parsed->toArray(),
        );
    }

    /**
     * @return iterable<string, array{array<string, mixed>, Method}>
     */
    public static function joinsWithoutAlias(): iterable
    {
        $on = ['method' => 'on', 'values' => ['$id', '=', 'user']];

        yield 'the nested ON form' => [['method' => 'join', 'attribute' => 'orders', 'values' => [$on]], Method::Join];
        yield 'an empty alias property' => [['method' => 'leftJoin', 'attribute' => 'orders', 'alias' => '', 'values' => [$on]], Method::LeftJoin];
        yield 'an empty legacy alias first' => [['method' => 'join', 'attribute' => 'orders', 'values' => ['', $on]], Method::Join];
        yield 'the column form' => [['method' => 'join', 'attribute' => 'orders', 'values' => ['$id', '=', 'user']], Method::Join];
        yield 'a cross join' => [['method' => 'crossJoin', 'attribute' => 'orders', 'values' => []], Method::CrossJoin];
    }

    /**
     * @param  array<string, mixed>  $array
     */
    #[DataProvider('joinsWithoutAlias')]
    public function testParsedJoinWithoutAnAliasIsReadUnderItsCollection(array $array, Method $method): void
    {
        $parsed = Query::parseQuery($array);

        $this->assertSame($method, $parsed->getMethod());
        $this->assertSame('orders', $parsed->getAlias());
        $this->assertTrue($parsed->isAliasDefaulted());

        $serialized = $parsed->toArray();
        $this->assertSame('orders', $serialized['alias']);
        $this->assertSame($serialized, Query::parseQuery($serialized)->toArray());
        $this->assertTrue(Query::parse($parsed->toString())->isAliasDefaulted());
    }

    public function testAJoinBuiltWithoutAnAliasIsReadUnderItsCollection(): void
    {
        $join = new Query(Method::Join, 'orders', [Query::on('$id', 'user')]);

        $this->assertSame('orders', $join->getAlias());
        $this->assertTrue($join->isAliasDefaulted());
        $this->assertFalse(Query::join('orders', 'ord', [Query::on('$id', 'ord.user')])->isAliasDefaulted());
    }

    public function testADefaultedAliasSurvivesRenamingTheJoinedCollection(): void
    {
        $renamed = (clone Query::join('orders', '', [Query::on('$id', 'user')]))->setAttribute('database_1_collection_2');

        $this->assertSame('orders', $renamed->getAlias());
        $this->assertTrue($renamed->isAliasDefaulted());
    }

    public function testParsedJoinWithoutAnAliasOrCollectionIsAQueryException(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Join alias is required: join ');

        Query::parseQuery(['method' => 'join', 'values' => [['method' => 'on', 'values' => ['$id', '=', 'user']]]]);
    }

    /**
     * @return array<string, array{array<string, mixed>, string}>
     */
    public static function aggregates(): array
    {
        return [
            'alias property' => [Query::sum('price', 'revenue')->toArray(), 'revenue'],
            'legacy alias value' => [['method' => 'sum', 'attribute' => 'price', 'values' => ['revenue']], 'revenue'],
            'no alias' => [Query::sum('price')->toArray(), ''],
        ];
    }

    /**
     * @param  array<string, mixed>  $array
     */
    #[DataProvider('aggregates')]
    public function testParsedAggregateKeepsItsAlias(array $array, string $alias): void
    {
        $parsed = Query::parseQuery($array);

        $this->assertSame(Method::Sum, $parsed->getMethod());
        $this->assertSame('price', $parsed->getAttribute());
        $this->assertSame($alias, $parsed->getAlias());
        $this->assertSame([], $parsed->getValues());
    }

    public function testDocumentCursorSerialisesToItsId(): void
    {
        $cursor = Query::cursorAfter(new Document(['$id' => 'doc1']));

        $this->assertSame(['method' => 'cursorAfter', 'values' => ['doc1']], $cursor->toArray());
        $this->assertTrue((new Cursor())->isValid($cursor));
    }

    public function testArrayCursorIsRefusedByTheCursorValidator(): void
    {
        $cursor = Query::cursorBefore(['$id' => 'doc1']);

        $validator = new Cursor();

        $this->assertFalse($validator->isValid($cursor));
        $this->assertStringStartsWith('Invalid cursor: ', $validator->getDescription());
    }

    /**
     * @return array<string, array{string, bool}>
     */
    public static function attributeTypes(): array
    {
        return [
            'point' => [ColumnType::Point->value, true],
            'linestring' => [ColumnType::Linestring->value, true],
            'polygon' => [ColumnType::Polygon->value, true],
            'string' => [ColumnType::String->value, false],
            'vector' => [ColumnType::Vector->value, false],
            'unset' => ['', false],
            'unknown' => ['circle', false],
        ];
    }

    #[DataProvider('attributeTypes')]
    public function testIsSpatialAttributeFollowsTheAttributeType(string $type, bool $spatial): void
    {
        $query = Query::equal('shape', ['x']);
        $query->setAttributeType($type);

        $this->assertSame($spatial, $query->isSpatialAttribute());
    }
}
