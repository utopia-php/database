<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Query\Schema\ColumnType;

class DocumentsQueriesGrammarTest extends TestCase
{
    /**
     * @var array<Document>
     */
    private array $attributes;

    protected function setUp(): void
    {
        $this->attributes = [
            new Document([
                '$id' => 'rating',
                'key' => 'rating',
                'type' => ColumnType::Integer->value,
                'size' => 5,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];
    }

    /**
     * @return array<string, array{Query, string}>
     */
    public static function joinQueries(): array
    {
        return [
            'join' => [Query::join('orders', '$id', 'customerId', '=', 'o'), 'join'],
            'left join' => [Query::leftJoin('orders', '$id', 'customerId', '=', 'o'), 'leftJoin'],
            'right join' => [Query::rightJoin('orders', '$id', 'customerId', '=', 'o'), 'rightJoin'],
            'cross join' => [Query::crossJoin('orders', 'o'), 'crossJoin'],
            'full outer join' => [Query::fullOuterJoin('orders', '$id', 'customerId', '=', 'o'), 'fullOuterJoin'],
        ];
    }

    /**
     * @return array<string, array{Query, string}>
     */
    public static function aggregationQueries(): array
    {
        return [
            'count' => [Query::count('*', 'total'), 'count'],
            'sum' => [Query::sum('rating', 'total'), 'sum'],
            'group by' => [Query::groupBy(['rating']), 'groupBy'],
            'having' => [Query::having([Query::greaterThan('rating', 1)]), 'having'],
            'distinct' => [Query::distinct(), 'distinct'],
        ];
    }

    #[DataProvider('joinQueries')]
    #[DataProvider('aggregationQueries')]
    public function test_default_grammar_rejects_extended_methods(Query $query, string $method): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
        );

        $this->assertFalse($validator->isValid([$query]));
        $this->assertSame('Invalid query method: '.$method, $validator->getDescription());
    }

    #[DataProvider('joinQueries')]
    public function test_joins_are_accepted_when_enabled(Query $query, string $method): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportForJoins: true,
        );

        $this->assertTrue($validator->isValid([$query]), $method.': '.$validator->getDescription());
    }

    #[DataProvider('aggregationQueries')]
    public function test_aggregations_are_accepted_when_enabled(Query $query, string $method): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportForAggregations: true,
        );

        $this->assertTrue($validator->isValid([$query]), $method.': '.$validator->getDescription());
    }

    #[DataProvider('aggregationQueries')]
    public function test_enabling_joins_does_not_enable_aggregations(Query $query, string $method): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportForJoins: true,
        );

        $this->assertFalse($validator->isValid([$query]));
        $this->assertSame('Invalid query method: '.$method, $validator->getDescription());
    }

    #[DataProvider('joinQueries')]
    public function test_enabling_aggregations_does_not_enable_joins(Query $query, string $method): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportForAggregations: true,
        );

        $this->assertFalse($validator->isValid([$query]));
        $this->assertSame('Invalid query method: '.$method, $validator->getDescription());
    }
}
