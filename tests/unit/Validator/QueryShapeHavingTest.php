<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Query\Schema\IndexType;

class QueryShapeHavingTest extends TestCase
{
    use QueryShapeAttributes;

    /**
     * @return list<string>
     */
    private function values(int $count): array
    {
        return \array_map(fn (int $index): string => 'value'.$index, \range(1, $count));
    }

    /**
     * @return array<string, array{0: list<Query>}>
     */
    public static function validHavingProvider(): array
    {
        return [
            'alias greater than' => [[Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::greaterThan('total', 100)])]],
            'alias between' => [[Query::count('*', 'rows'), Query::groupBy(['name']), Query::having([Query::between('rows', 1, 5)])]],
            'alias equal to several values' => [[Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::equal('total', [1, 2.5])])]],
            'alias is null' => [[Query::avg('rating', 'mean'), Query::groupBy(['name']), Query::having([Query::isNull('mean')])]],
            'maximum compared as its attribute' => [[Query::max('name', 'last'), Query::groupBy(['price']), Query::having([Query::greaterThan('last', 'm')])]],
            'grouped attribute' => [[Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::equal('name', ['a', 'b'])])]],
            'grouped attributes in a logical group' => [[Query::sum('price', 'total'), Query::groupBy(['name', 'price']), Query::having([Query::or([Query::equal('name', ['a']), Query::greaterThan('price', 5)])])]],
            'several conditions' => [[Query::count('*', 'rows'), Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::greaterThanEqual('rows', 2), Query::lessThan('total', 500)])]],
            'grouped attribute of a join' => [[Query::leftJoin('orders', '$id', 'customer', '=', 'ord'), Query::count('*', 'rows'), Query::groupBy(['ord.status']), Query::having([Query::equal('ord.status', ['paid'])])]],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('validHavingProvider')]
    public function testHavingAcceptsAliasesAndGroupedAttributes(array $queries): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid($queries), $validator->getDescription());
    }

    /**
     * @return array<string, array{0: list<Query>, 1: string}>
     */
    public static function invalidHavingProvider(): array
    {
        return [
            'attribute outside the schema' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::equal('no_such_attribute', ['x'])])],
                'Having can only compare an aggregate alias or a groupBy attribute: no_such_attribute',
            ],
            'attribute that is not grouped' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::equal('body', ['x'])])],
                'Having can only compare an aggregate alias or a groupBy attribute: body',
            ],
            'attribute without any aggregation' => [
                [Query::having([Query::equal('name', ['x'])])],
                'Having can only compare an aggregate alias or a groupBy attribute: name',
            ],
            'grouped attribute with a value of the wrong type' => [
                [Query::sum('price', 'total'), Query::groupBy(['price']), Query::having([Query::equal('price', ['abc'])])],
                'Query value is invalid for attribute "price"',
            ],
            'grouped boolean with a value of the wrong type' => [
                [Query::count('*', 'rows'), Query::groupBy(['active']), Query::having([Query::greaterThan('active', 'yes')])],
                'Query value is invalid for attribute "active"',
            ],
            'numeric alias compared with text' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::greaterThan('total', 'abc')])],
                'Query value is invalid for aggregate alias "total"',
            ],
            'numeric alias compared with a boolean' => [
                [Query::count('*', 'rows'), Query::groupBy(['name']), Query::having([Query::equal('rows', [true])])],
                'Query value is invalid for aggregate alias "rows"',
            ],
            'maximum compared with a value its attribute cannot hold' => [
                [Query::max('name', 'last'), Query::groupBy(['price']), Query::having([Query::greaterThan('last', 5)])],
                'Query value is invalid for attribute "name"',
            ],
            'operator an aggregate cannot take' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::startsWith('total', '1')])],
                'Aggregate alias "total" cannot be compared with startsWith',
            ],
            'alias inside a logical group' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::or([Query::greaterThan('total', 10), Query::lessThan('total', 1)])])],
                'Aggregate alias "total" can only be compared at the top level of having',
            ],
            'alias comparison with too many values' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([new Query(Method::GreaterThan, 'total', [1, 2])])],
                'GreaterThan queries require exactly one value.',
            ],
            'alias between with one value' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([new Query(Method::Between, 'total', [1])])],
                'Between queries require exactly two values.',
            ],
            'alias equal without values' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([new Query(Method::Equal, 'total', [])])],
                'Equal queries require at least one value.',
            ],
            'aggregate as a condition' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::sum('price', 'again')])],
                'Having conditions must be filter queries',
            ],
            'nested having' => [
                [Query::sum('price', 'total'), Query::groupBy(['name']), Query::having([Query::having([Query::greaterThan('total', 1)])])],
                'Having conditions must be filter queries',
            ],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('invalidHavingProvider')]
    public function testHavingRejectsConditionsTheFilterRulesRefuse(array $queries, string $message): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid($queries));
        $this->assertSame('Invalid query: '.$message, $validator->getDescription());
    }

    public function testHavingRequiresAFulltextIndexToSearch(): void
    {
        $queries = [
            Query::count('*', 'rows'),
            Query::groupBy(['body']),
            Query::having([Query::search('body', 'word')]),
        ];

        $validator = $this->validator();
        $this->assertFalse($validator->isValid($queries));
        $this->assertSame('Searching by attribute "body" requires a fulltext index.', $validator->getDescription());

        $indexed = $this->validator([
            new Document(['$id' => 'body_fulltext', 'type' => IndexType::Fulltext->value, 'attributes' => ['body']]),
        ]);
        $this->assertTrue($indexed->isValid($queries), $indexed->getDescription());
    }

    public function testHavingCapsTheValuesOfEveryCondition(): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid([
            Query::count('*', 'rows'),
            Query::groupBy(['name']),
            Query::having([Query::equal('name', $this->values(self::MAX_VALUES))]),
        ]), $validator->getDescription());

        $this->assertFalse($validator->isValid([
            Query::count('*', 'rows'),
            Query::groupBy(['name']),
            Query::having([Query::equal('name', $this->values(self::MAX_VALUES + 1))]),
        ]));
        $this->assertSame('Invalid query: Query on attribute has greater than '.self::MAX_VALUES.' values: name', $validator->getDescription());

        $this->assertFalse($validator->isValid([
            Query::count('*', 'rows'),
            Query::groupBy(['name']),
            Query::having([Query::equal('rows', \range(1, self::MAX_VALUES + 1))]),
        ]));
        $this->assertSame('Invalid query: Query on aggregate alias has greater than '.self::MAX_VALUES.' values: rows', $validator->getDescription());
    }

    public function testHavingStateDoesNotLeakIntoTheNextQuerySet(): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid([
            Query::sum('price', 'total'),
            Query::groupBy(['name']),
            Query::having([Query::greaterThan('total', 1), Query::equal('name', ['a'])]),
        ]), $validator->getDescription());

        $this->assertFalse($validator->isValid([
            Query::groupBy(['price']),
            Query::having([Query::greaterThan('total', 1)]),
        ]), 'an alias declared by the previous query set must not stay valid');

        $this->assertFalse($validator->isValid([
            Query::sum('price', 'total'),
            Query::having([Query::equal('name', ['a'])]),
        ]), 'a groupBy attribute of the previous query set must not stay valid');
    }
}
