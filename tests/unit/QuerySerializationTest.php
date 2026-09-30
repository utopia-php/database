<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
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
