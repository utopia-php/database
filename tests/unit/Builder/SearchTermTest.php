<?php

namespace Tests\Unit\Builder;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Builder\Filtering;
use Utopia\Database\Builder\MariaDB;
use Utopia\Database\Builder\MySQL;
use Utopia\Database\Builder\Postgres;
use Utopia\Database\Query;

final class SearchTermTest extends TestCase
{
    /**
     * @return array<string, array{string, string}>
     */
    public static function postgreSQLTerms(): array
    {
        return [
            'slash separates words' => ['foo/bar', "'foo or bar'"],
            'comma separates words' => ['foo,bar', "'foo or bar'"],
            'semicolon separates words' => ['foo;bar', "'foo or bar'"],
            'percent separates words' => ['foo%bar', "'foo or bar'"],
            'equals separates words' => ['foo=bar', "'foo or bar'"],
            'question mark separates words' => ['foo?bar', "'foo or bar'"],
            'hash separates words' => ['foo#bar', "'foo or bar'"],
            'colon separates words' => ['foo:bar', "'foo or bar'"],
            'pipe separates words' => ['foo|bar', "'foo or bar'"],
            'ampersand separates words' => ['foo&bar', "'foo or bar'"],
            'exclamation mark separates words' => ['foo!bar', "'foo or bar'"],
            'underscore stays inside a word' => ['foo_bar', "'foo_bar'"],
            'mixed separators and spaces' => ['baz, foo/bar;  qux', "'baz or foo or bar or qux'"],
            'operators are dropped' => ['+foo -bar* @3 <baz> ~qux (quux)', "'foo or bar or 3 or baz or qux or quux'"],
            'accented words are kept' => ['@García!', "'García'"],
            'exact term matches every word in single quotes' => ['"foo/bar baz"', "'foo bar baz'"],
            'unbalanced quote is not exact' => ['"foo/bar', "'foo or bar'"],
        ];
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function mySQLTerms(): array
    {
        return [
            'slash separates words' => ['foo/bar', 'foo bar*'],
            'comma separates words' => ['foo,bar', 'foo bar*'],
            'period separates words' => ['foo.bar', 'foo bar*'],
            'apostrophe separates words' => ["foo'bar", 'foo bar*'],
            'trailing punctuation keeps the prefix match on the word' => ['foo.', 'foo*'],
            'underscore stays inside a word' => ['foo_bar', 'foo_bar*'],
            'mixed separators and spaces' => ['baz, foo/bar;  qux', 'baz foo bar qux*'],
            'non-operator punctuation is dropped' => ['!!!foo...###', 'foo*'],
            'exact phrase keeps its quotes' => ['"foo/bar baz"', '"foo bar baz"'],
        ];
    }

    /**
     * @return array<string, array{string}>
     */
    public static function termsWithoutWords(): array
    {
        return [
            'separators only' => ['/,;%=?#'],
            'operators only' => ['+-*@<>~()'],
            'punctuation only' => ['!!!...###'],
            'quoted punctuation' => ['"/"'],
            'whitespace only' => ["  \t "],
        ];
    }

    #[DataProvider('postgreSQLTerms')]
    public function testPostgreSQLSearchesEachWordOfATerm(string $term, string $bound): void
    {
        $this->assertSame([$bound], $this->bindings(new Postgres(), Query::search('title', $term)));
        $this->assertSame([$bound], $this->bindings(new Postgres(), Query::notSearch('title', $term)));
    }

    public function testPostgreSQLMatchesTheTermWithWebsearchToTsquery(): void
    {
        $search = (new Postgres())->compileFilters([Query::search('title', '"foo bar"')]);
        $notSearch = (new Postgres())->compileFilters([Query::notSearch('title', '"foo bar"')]);

        $this->assertStringContainsString("@@ websearch_to_tsquery(?)", $search->expression);
        $this->assertStringStartsWith('NOT (', $notSearch->expression);
        $this->assertSame(["'foo bar'"], $search->bindings);
    }

    #[DataProvider('mySQLTerms')]
    public function testMySQLSearchesEachWordOfATerm(string $term, string $bound): void
    {
        $this->assertSame([$bound], $this->bindings(new MySQL(), Query::search('title', $term)));
        $this->assertSame([$bound], $this->bindings(new MySQL(), Query::notSearch('title', $term)));
    }

    #[DataProvider('mySQLTerms')]
    public function testMariaDBSearchesEachWordOfATerm(string $term, string $bound): void
    {
        $this->assertSame([$bound], $this->bindings(new MariaDB(), Query::search('title', $term)));
        $this->assertSame([$bound], $this->bindings(new MariaDB(), Query::notSearch('title', $term)));
    }

    #[DataProvider('termsWithoutWords')]
    public function testATermWithoutWordsBindsNothing(string $term): void
    {
        foreach ([new Postgres(), new MySQL(), new MariaDB()] as $builder) {
            $this->assertSame([], $this->bindings($builder, Query::search('title', $term)), $builder::class);
            $this->assertSame([], $this->bindings($builder, Query::notSearch('title', $term)), $builder::class);
        }
    }

    public function testATermIsAlwaysBoundAndNeverWrittenIntoTheCondition(): void
    {
        $term = "foo'); DROP TABLE docs; --";

        foreach ([new Postgres(), new MySQL(), new MariaDB()] as $builder) {
            $condition = $builder->compileFilters([Query::search('title', $term)]);

            $this->assertStringNotContainsString('DROP', $condition->expression, $builder::class);
            $this->assertStringNotContainsString('foo', $condition->expression, $builder::class);
            $this->assertCount(1, $condition->bindings, $builder::class);
        }
    }

    /**
     * @return array<mixed>
     */
    private function bindings(Filtering $builder, Query $query): array
    {
        return $builder->compileFilters([$query])->bindings;
    }
}
