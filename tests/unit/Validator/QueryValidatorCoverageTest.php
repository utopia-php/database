<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Query;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\IndexedQueries;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Distinct;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Having;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

final class QueryValidatorCoverageTest extends TestCase
{
    private const int MAX_VALUES = 2;

    public function testAJoinAliasOfAnUnknownCollectionKeepsTheValueLimit(): void
    {
        $filter = $this->filter();
        $filter->allowJoinAliases(['joined']);

        $this->assertTrue($filter->isValid(Query::equal('joined.anything', ['a', 'b'])), $filter->getDescription());
        $this->assertFalse($filter->isValid(Query::equal('joined.anything', ['a', 'b', 'c'])));
        $this->assertSame('Query on attribute has greater than 2 values: joined.anything', $filter->getDescription());
    }

    public function testAnUndeclaredAttributeInSchemalessModeKeepsTheValueLimit(): void
    {
        $filter = $this->filter(supportForAttributes: false);

        $this->assertTrue($filter->isValid(Query::equal('undeclared', ['a', 'b'])), $filter->getDescription());
        $this->assertFalse($filter->isValid(Query::equal('undeclared', ['a', 'b', 'c'])));
        $this->assertSame('Query on attribute has greater than 2 values: undeclared', $filter->getDescription());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function typesWithoutAValueRule(): array
    {
        return [
            'decimal' => [ColumnType::Decimal->value],
            'json' => [ColumnType::Json->value],
            'unknown name' => ['mystery'],
        ];
    }

    #[DataProvider('typesWithoutAValueRule')]
    public function testAnAttributeTypeWithoutAValueRuleIsRefused(string $type): void
    {
        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('Unknown attribute type: '.$type);

        new Filter([new Document([
            Document::ID => 'amount',
            'key' => 'amount',
            'type' => $type,
            'array' => false,
        ])], ColumnType::Integer->value, self::MAX_VALUES);
    }

    public function testAnElemMatchWithAnInvalidNestedFilterIsRejected(): void
    {
        $filter = $this->filter(supportForAttributes: false);

        $this->assertTrue($filter->isValid(new Query(Method::ElemMatch, 'items', [Query::equal('sku', ['a'])])), $filter->getDescription());

        $this->assertFalse($filter->isValid(new Query(Method::ElemMatch, 'items', [Query::equal('sku', ['a']), new Query(Method::Equal, 'sku', [])])));
        $this->assertSame('Equal queries require at least one value.', $filter->getDescription());
    }

    public function testAnElemMatchChildMustBeAQuery(): void
    {
        $filter = $this->filter(supportForAttributes: false);

        $this->assertFalse($filter->isValid(new Query(Method::ElemMatch, 'items', ['{"method":"equal","attribute":"sku","values":["a"]}'])));
        $this->assertSame('elemMatch queries can only contain filter queries', $filter->getDescription());
    }

    /**
     * @return array<string, array{Method}>
     */
    public static function spatialMethods(): array
    {
        return [
            'crosses' => [Method::Crosses],
            'intersects' => [Method::Intersects],
            'overlaps' => [Method::Overlaps],
            'touches' => [Method::NotTouches],
            'covers' => [Method::Covers],
            'spatial equals' => [Method::SpatialEquals],
        ];
    }

    #[DataProvider('spatialMethods')]
    public function testASpatialQueryNeedsAValue(Method $method): void
    {
        $filter = $this->filter();

        $this->assertFalse($filter->isValid(new Query($method, 'location', [])));
        $this->assertSame(\ucfirst($method->value).' queries require at least one value.', $filter->getDescription());

        $this->assertTrue($filter->isValid(new Query($method, 'location', [[1.0, 2.0]])), $filter->getDescription());
    }

    public function testAVectorQueryTakesExactlyOneVector(): void
    {
        $filter = $this->filter();

        $this->assertTrue($filter->isValid(new Query(Method::VectorDot, 'embedding', [[1.0, 2.0, 3.0]])), $filter->getDescription());

        $this->assertFalse($filter->isValid(new Query(Method::VectorDot, 'embedding', [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])));
        $this->assertSame('VectorDot queries require exactly one vector value.', $filter->getDescription());
    }

    public function testAVectorQueryOnAJoinedAttributeTakesExactlyOneVectorAndIsThenRefused(): void
    {
        $filter = $this->filter();
        $filter->allowJoinAliases(['joined']);

        $this->assertFalse($filter->isValid(new Query(Method::VectorCosine, 'joined.embedding', [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])));
        $this->assertSame('VectorCosine queries require exactly one vector value.', $filter->getDescription());

        $this->assertFalse($filter->isValid(new Query(Method::VectorCosine, 'joined.embedding', [[1.0, 2.0, 3.0]])));
        $this->assertSame('Vector queries cannot be used on a joined attribute: joined.embedding', $filter->getDescription());
    }

    public function testAVectorQueryOnARelationshipPathIsCheckedAgainstTheRelationship(): void
    {
        $filter = $this->filter();

        $this->assertFalse($filter->isValid(new Query(Method::VectorEuclidean, 'author.embedding', [[1.0, 2.0, 3.0]])));
        $this->assertSame('Vector queries can only be used on vector attributes', $filter->getDescription());
    }

    public function testStringQueriesAreCheckedAgainstTheIndexesLikeQueryObjects(): void
    {
        $search = '{"method":"search","attribute":"name","values":["phrase"]}';
        $attributes = [new Document([Document::ID => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'array' => false])];

        $withoutFulltext = new IndexedQueries($attributes, [], [new Filter($attributes, ColumnType::Integer->value)]);
        $this->assertFalse($withoutFulltext->isValid([$search]));
        $this->assertSame('Searching by attribute "name" requires a fulltext index.', $withoutFulltext->getDescription());
        $this->assertFalse($withoutFulltext->isValid([Query::parse($search)]));
        $this->assertSame('Searching by attribute "name" requires a fulltext index.', $withoutFulltext->getDescription());

        $withFulltext = new IndexedQueries(
            $attributes,
            [new Document(['type' => IndexType::Fulltext->value, 'attributes' => ['name']])],
            [new Filter($attributes, ColumnType::Integer->value)],
        );
        $this->assertTrue($withFulltext->isValid([$search]), $withFulltext->getDescription());
        $this->assertFalse($withFulltext->isValid(['{"method":"search"']));
        $this->assertStringStartsWith('Invalid query: ', $withFulltext->getDescription());
    }

    public function testStringChildrenOfALogicalQueryAreParsedAndAnUnparseableOneIsRejected(): void
    {
        $validator = new Queries([$this->filter()]);
        $child = '{"method":"equal","attribute":"embedding","values":[[1,2,3]]}';

        $this->assertFalse($validator->isValid([new Query(Method::Or, '', [$child, '{"method":"equal"'])]));
        $this->assertStringStartsWith('Invalid query: ', $validator->getDescription());
        $this->assertStringNotContainsString('can only contain filter queries', $validator->getDescription());

        $this->assertFalse($validator->isValid([new Query(Method::And, '', [$child, 5])]));
        $this->assertSame('Invalid query: nested query must be a string', $validator->getDescription());

        $this->assertFalse($validator->isValid([new Query(Method::Or, '', [$child, $child])]));
        $this->assertStringContainsString('Or queries can only contain filter queries', $validator->getDescription());
    }

    /**
     * @return array<string, array{\Closure(): Base, mixed}>
     */
    public static function validatorsGivenANonQuery(): array
    {
        $validators = [
            'aggregate' => static fn (): Base => new Aggregate(),
            'distinct' => static fn (): Base => new Distinct(),
            'group by' => static fn (): Base => new GroupBy(),
            'having' => static fn (): Base => new Having(),
            'join' => static fn (): Base => new Join(),
        ];

        $cases = [];
        foreach ($validators as $name => $validator) {
            $cases["{$name} given a string"] = [$validator, 'limit(1)'];
            $cases["{$name} given an array"] = [$validator, ['method' => 'limit', 'values' => [1]]];
            $cases["{$name} given null"] = [$validator, null];
        }

        return $cases;
    }

    /**
     * @param  \Closure(): Base  $validator
     */
    #[DataProvider('validatorsGivenANonQuery')]
    public function testAQueryMethodValidatorGivenANonQueryRefusesIt(\Closure $validator, mixed $value): void
    {
        $instance = $validator();

        $this->assertFalse($instance->isValid($value));
        $this->assertSame('Value must be a Query', $instance->getDescription());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function missingTableNames(): array
    {
        return [
            'empty' => [''],
            'zero' => ['0'],
        ];
    }

    #[DataProvider('missingTableNames')]
    public function testAJoinWithoutATableNameIsRefused(string $table): void
    {
        $validator = new Join();

        $this->assertFalse($validator->isValid(Query::join($table, 'authorId', 'id', alias: 'author')));
        $this->assertSame('Join requires a table name', $validator->getDescription());

        $this->assertTrue($validator->isValid(Query::join('authors', 'authorId', 'id', alias: 'author')), $validator->getDescription());
    }

    public function testTheLimitValidatorRefusesAnotherMethodAndANonNumericLimit(): void
    {
        $validator = new Limit();

        $this->assertFalse($validator->isValid(Query::offset(5)));
        $this->assertSame('Invalid query method: offset', $validator->getDescription());

        $this->assertFalse($validator->isValid(new Query(Method::Limit, '', ['abc'])));
        $this->assertStringStartsWith('Invalid limit: ', $validator->getDescription());

        $this->assertFalse($validator->isValid('limit(5)'));
        $this->assertTrue($validator->isValid(Query::limit(5)), $validator->getDescription());
    }

    private function filter(bool $supportForAttributes = true): Filter
    {
        return new Filter(
            [
                new Document([Document::ID => 'location', 'key' => 'location', 'type' => ColumnType::Point->value, 'array' => false]),
                new Document([Document::ID => 'embedding', 'key' => 'embedding', 'type' => ColumnType::Vector->value, 'size' => 3, 'array' => false]),
                new Document([
                    Document::ID => 'author',
                    'key' => 'author',
                    'type' => ColumnType::Relationship->value,
                    'array' => false,
                    'options' => [
                        'relatedCollection' => 'authors',
                        'relationType' => RelationshipType::ManyToOne->value,
                        'twoWay' => false,
                        'twoWayKey' => 'books',
                        'side' => RelationshipSide::Parent->value,
                    ],
                ]),
            ],
            ColumnType::Integer->value,
            self::MAX_VALUES,
            supportForAttributes: $supportForAttributes,
        );
    }
}
