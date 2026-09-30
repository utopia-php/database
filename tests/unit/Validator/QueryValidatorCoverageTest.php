<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

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
    public function testAnAttributeTypeWithoutAValueRuleIsAnUnknownDataType(string $type): void
    {
        $filter = new Filter([new Document([
            Document::ID => 'amount',
            'key' => 'amount',
            'type' => $type,
            'array' => false,
        ])], ColumnType::Integer->value, self::MAX_VALUES);

        $this->assertFalse($filter->isValid(Query::equal('amount', ['1.5'])));
        $this->assertSame('Unknown Data type', $filter->getDescription());
        $this->assertTrue($filter->isValid(Query::isNull('amount')), 'a query without values never reaches the type rule');
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
                        'relationType' => RelationType::ManyToOne->value,
                        'twoWay' => false,
                        'twoWayKey' => 'books',
                        'side' => RelationSide::Parent->value,
                    ],
                ]),
            ],
            ColumnType::Integer->value,
            self::MAX_VALUES,
            supportForAttributes: $supportForAttributes,
        );
    }
}
