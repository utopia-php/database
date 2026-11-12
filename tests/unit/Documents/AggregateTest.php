<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;

/**
 * aggregate() returns one plain row per group, with an unaliased aggregate under `<method>_<attribute>` (or
 * `<method>` for count('*')), and is the only way to run aggregate and groupBy queries.
 */
final class AggregateTest extends TestCase
{
    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->database = HookFixture::sqlite();
        HookFixture::seed($this->database, ['first', 'second', 'third']);
    }

    public function testAnUnaliasedAggregateComesBackUnderItsDefaultAlias(): void
    {
        $rows = $this->database->aggregate(HookFixture::COLLECTION, [
            Query::count(),
            Query::sum('views'),
            Query::max('views'),
        ]);

        $this->assertSame([['count' => 3, 'sum_views' => 6, 'max_views' => 3]], $rows);
    }

    public function testAnAliasIsKept(): void
    {
        $rows = $this->database->aggregate(HookFixture::COLLECTION, [Query::count('*', 'total'), Query::sum('views')]);

        $this->assertSame([['total' => 3, 'sum_views' => 6]], $rows);
    }

    /**
     * @return iterable<string, array{list<Query>, string}>
     */
    public static function duplicateDefaultAliases(): iterable
    {
        yield 'the same aggregate twice' => [[Query::sum('views'), Query::sum('views')], 'sum_views'];
        yield 'an explicit alias equal to a default one' => [[Query::count('*', 'sum_views'), Query::sum('views')], 'sum_views'];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('duplicateDefaultAliases')]
    public function testADefaultAliasTakenTwiceIsRefused(array $queries, string $alias): void
    {
        try {
            $this->database->aggregate(HookFixture::COLLECTION, $queries);
            $this->fail('Two aggregates came back under one alias');
        } catch (QueryException $error) {
            $this->assertSame("The default aggregate alias '{$alias}' is used by more than one aggregate; give each of them an explicit alias", $error->getMessage());
        }
    }

    public function testADefaultAliasTakenTwiceIsRefusedWithValidationOff(): void
    {
        try {
            $this->database->skipValidation(fn (): array => $this->database->aggregate(HookFixture::COLLECTION, [Query::max('views'), Query::max('views')]));
            $this->fail('Two aggregates came back under one alias');
        } catch (QueryException $error) {
            $this->assertSame("The default aggregate alias 'max_views' is used by more than one aggregate; give each of them an explicit alias", $error->getMessage());
        }
    }

    public function testAnExplicitAliasGivenTwiceIsRefusedTheSameWithValidationOff(): void
    {
        $queries = [Query::sum('views', 'total'), Query::max('views', 'total')];
        $message = 'Invalid query: Aggregate alias "total" is given to more than one aggregate';

        try {
            $this->database->aggregate(HookFixture::COLLECTION, $queries);
            $this->fail('Two aggregates came back under one alias');
        } catch (QueryException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        try {
            $this->database->skipValidation(fn (): array => $this->database->aggregate(HookFixture::COLLECTION, $queries));
            $this->fail('Two aggregates came back under one alias with validation off');
        } catch (QueryException $error) {
            $this->assertSame($message, $error->getMessage());
        }
    }

    public function testExplicitAliasesSeparateTheSameAggregate(): void
    {
        $rows = $this->database->aggregate(HookFixture::COLLECTION, [Query::sum('views', 'first'), Query::sum('views', 'second')]);

        $this->assertSame([['first' => 6, 'second' => 6]], $rows);
    }

    public function testEachGroupIsOneRow(): void
    {
        $this->database->updateDocument(HookFixture::COLLECTION, 'third', new Document(['title' => 'first']));

        $rows = $this->database->aggregate(HookFixture::COLLECTION, [
            Query::count(),
            Query::groupBy(['title']),
            Query::orderAsc('title'),
        ]);

        $this->assertSame([['count' => 2, 'title' => 'first'], ['count' => 1, 'title' => 'second']], $rows);
    }

    public function testFindRefusesAggregateAndGroupByQueries(): void
    {
        foreach ([[Query::count()], [Query::groupBy(['title'])]] as $queries) {
            try {
                $this->database->find(HookFixture::COLLECTION, $queries);
                $this->fail('find() ran an aggregation');
            } catch (QueryException $error) {
                $this->assertSame('find() does not run aggregate or groupBy queries: use aggregate()', $error->getMessage());
            }
        }
    }

    public function testAggregateNeedsAnAggregation(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('aggregate() needs an aggregate or groupBy query');

        $this->database->aggregate(HookFixture::COLLECTION, [Query::equal('title', ['first'])]);
    }
}
