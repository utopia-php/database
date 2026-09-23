<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\RawTenantFilter;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Placement;

final class RawTenantFilterTest extends TestCase
{
    private const string TABLE = 'appwrite.ns_authors';

    private const int TENANT = 7;

    public function testInnerAndLeftJoinsMeetTheirConditionInOn(): void
    {
        foreach ([JoinType::Inner, JoinType::Left] as $type) {
            $result = $this->filter()->filterJoin('Book', $type);

            $this->assertSame(Placement::On, $result->placement, "A {$type->value} only pairs the tenant's rows");
            $this->assertSame('`Book`._tenant IN (?)', $result->condition->expression);
            $this->assertSame([self::TENANT], $result->condition->bindings);
        }
    }

    public function testOtherJoinsMeetTheirConditionInWhereLettingMissingRowsThrough(): void
    {
        foreach ([JoinType::Right, JoinType::FullOuter, JoinType::Cross, JoinType::Natural] as $type) {
            $result = $this->filter()->filterJoin('Book', $type);

            $this->assertSame(Placement::Where, $result->placement, "ON cannot drop the rows a {$type->value} keeps");
            $this->assertSame(
                '(`Book`._tenant IN (?) OR `Book`.`_uid` IS NULL)',
                $result->condition->expression,
                'A later outer join may leave the table missing from a row',
            );
            $this->assertSame([self::TENANT], $result->condition->bindings);
        }
    }

    public function testTheMainTableIsNamedAsTheBuilderNamesIt(): void
    {
        $condition = $this->filter()->filter(self::TABLE);

        $this->assertSame('`appwrite`.`ns_authors`._tenant IN (?)', $condition->expression, 'A bare column is ambiguous once a join is added');
        $this->assertSame([self::TENANT], $condition->bindings);
        $this->assertSame('`author`._tenant IN (?)', $this->filter()->filter('author')->expression, 'A main table the caller aliases is named by its alias');
    }

    /**
     * @return iterable<string, array{JoinType, bool}>
     */
    public static function joinTypes(): iterable
    {
        yield 'inner' => [JoinType::Inner, false];
        yield 'left' => [JoinType::Left, false];
        yield 'cross' => [JoinType::Cross, false];
        yield 'natural' => [JoinType::Natural, false];
        yield 'right' => [JoinType::Right, true];
        yield 'full outer' => [JoinType::FullOuter, true];
    }

    #[DataProvider('joinTypes')]
    public function testTheMainTableLetsMissingRowsThroughOnlyAfterARightOrFullOuterJoin(JoinType $type, bool $preserving): void
    {
        $filter = $this->filter();
        $filter->filterJoin('Book', $type);

        $this->assertSame(
            $preserving
                ? '(`appwrite`.`ns_authors`._tenant IN (?) OR `appwrite`.`ns_authors`.`_uid` IS NULL)'
                : '`appwrite`.`ns_authors`._tenant IN (?)',
            $filter->filter(self::TABLE)->expression,
        );
    }

    public function testMetadataKeepsTheDefinitionsAPoolSharesWithoutATenant(): void
    {
        $filter = new RawTenantFilter(self::TENANT, self::TABLE, true, '`');

        $this->assertSame(
            '(`appwrite`.`ns_authors`._tenant IN (?) OR `appwrite`.`ns_authors`._tenant IS NULL)',
            $filter->filter(self::TABLE)->expression,
        );
        $this->assertSame('`Book`._tenant IN (?)', $filter->filterJoin('Book', JoinType::Inner)->condition->expression, 'Only the main table is metadata');
    }

    public function testEachStatementLearnsItsOwnJoins(): void
    {
        $filter = $this->filter();
        $filter->filterJoin('Book', JoinType::Right);
        $filter->filter(self::TABLE);

        $this->assertSame(
            '`appwrite`.`ns_authors`._tenant IN (?)',
            $filter->filter(self::TABLE)->expression,
            'An update after a read with a right join has no join of its own',
        );

        $filter->filterJoin('Book', JoinType::Right);
        $filter->reset();

        $this->assertSame('`appwrite`.`ns_authors`._tenant IN (?)', $filter->filter(self::TABLE)->expression, 'A build starts without the joins an abandoned one learned');
    }

    public function testARightOrFullOuterJoinRefusesARenamedMainTable(): void
    {
        foreach ([JoinType::Right, JoinType::FullOuter] as $type) {
            $filter = $this->filter();
            $filter->filterJoin('Book', $type);

            try {
                $filter->filter('author');
                $this->fail("A {$type->value} named the main table as Database::from() names it in its ON");
            } catch (QueryException $exception) {
                $this->assertStringContainsString(self::TABLE, $exception->getMessage());
            }
        }

        $filter = $this->filter();
        $filter->filterJoin('Book', JoinType::Inner);
        $this->assertSame('`author`._tenant IN (?)', $filter->filter('author')->expression);
    }

    public function testAStatementWithoutATableIsRefused(): void
    {
        $this->expectException(QueryException::class);
        $this->filter()->filter('');
    }

    public function testARightOrFullOuterJoinPairsOnlyRowsTheTenantCouldRead(): void
    {
        $filter = $this->filter();
        $filter->filterJoin('Extra', JoinType::Cross);
        $filter->filterJoin('Note', JoinType::Inner);
        $filter->filterJoin('Review', JoinType::Right);

        $condition = $filter->outerJoin('Review', JoinType::Right);

        $this->assertSame(
            '(`appwrite`.`ns_authors`._tenant IN (?) OR `appwrite`.`ns_authors`.`_uid` IS NULL)'
            .' AND `Review`._tenant IN (?)'
            .' AND (`Extra`._tenant IN (?) OR `Extra`.`_uid` IS NULL)',
            $condition->expression,
            'The main table, the join itself and the earlier table whose condition sits in WHERE; the inner join met its own in ON',
        );
        $this->assertSame([self::TENANT, self::TENANT, self::TENANT], $condition->bindings);
    }

    public function testPostgresQuotesEveryTableItNames(): void
    {
        $filter = new RawTenantFilter(self::TENANT, 'appwrite.ns_authors', false, '"');
        $filter->filterJoin('Book', JoinType::FullOuter);

        $this->assertSame('("Review"._tenant IN (?) OR "Review"."_uid" IS NULL)', $filter->filterJoin('Review', JoinType::Right)->condition->expression);
        $this->assertSame(
            '("appwrite"."ns_authors"._tenant IN (?) OR "appwrite"."ns_authors"."_uid" IS NULL) AND "Review"._tenant IN (?) AND ("Book"._tenant IN (?) OR "Book"."_uid" IS NULL)',
            $filter->outerJoin('Review', JoinType::Right)->expression,
        );
    }

    private function filter(): RawTenantFilter
    {
        return new RawTenantFilter(self::TENANT, self::TABLE, false, '`');
    }
}
