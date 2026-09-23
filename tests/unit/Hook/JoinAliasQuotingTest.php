<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Hook\JoinChain;
use Utopia\Database\Hook\OuterJoinChainFilter;
use Utopia\Database\Hook\OuterJoinPermissionFilter;
use Utopia\Database\Hook\OuterJoinTenantFilter;
use Utopia\Database\Hook\PermissionAllowNullUid;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Hook\PermissionJoinFilter;
use Utopia\Database\Hook\TenantFilter;
use Utopia\Database\Storage;
use Utopia\Query\Builder\JoinType;

/**
 * The builder declares every table alias quoted, so a hook has to name it quoted too, with the
 * quote character it is given: PostgreSQL folds an unquoted mixed-case alias to lower case and
 * then finds no table by that name, and a reserved word is an identifier only once quoted.
 * Expectations are written with PostgreSQL's quote and translated for each engine.
 */
final class JoinAliasQuotingTest extends TestCase
{
    private const string SOURCE = 'Main';

    private const string ALIAS = 'Book';

    private const string EARLIER = 'Extra';

    /**
     * @return iterable<string, array{string}>
     */
    public static function quoteCharacters(): iterable
    {
        yield 'PostgreSQL' => ['"'];
        yield 'MariaDB and MySQL' => ['`'];
    }

    /**
     * @return iterable<string, array{JoinType, string}>
     */
    public static function joins(): iterable
    {
        foreach (self::quoteCharacters() as $engine => [$quote]) {
            foreach ([JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::FullOuter, JoinType::Cross] as $joinType) {
                yield "{$joinType->value}, {$engine}" => [$joinType, $quote];
            }
        }
    }

    #[DataProvider('quoteCharacters')]
    public function testTenantFilterQuotesTheTableItQualifies(string $quote): void
    {
        $filter = new TenantFilter(7, Database::METADATA, 'authors', quoteChar: $quote);

        $this->assertSame($this->quoted('"Main"._tenant IN (?)', $quote), $filter->filter(self::SOURCE)->expression);
        $this->assertSame($this->quoted('"Book"._tenant IN (?)', $quote), $filter->joined(self::ALIAS)->expression);
        $this->assertSame([7], $filter->joined(self::ALIAS)->bindings);
    }

    #[DataProvider('quoteCharacters')]
    public function testTenantFilterQuotesTheTableOfATenantlessMetadataRow(string $quote): void
    {
        $filter = new TenantFilter(7, Database::METADATA, Database::METADATA, quoteChar: $quote);

        $this->assertSame(
            $this->quoted('("Main"._tenant IN (?) OR "Main"._tenant IS NULL)', $quote),
            $filter->filter(self::SOURCE)->expression,
        );
    }

    /**
     * A table named with its database, or quoted already, is a raw table name rather than an alias.
     */
    #[DataProvider('quoteCharacters')]
    public function testTenantFilterDoesNotQualifyWithARawTableName(string $quote): void
    {
        $filter = new TenantFilter(7, Database::METADATA, 'authors', quoteChar: $quote);

        $this->assertSame('_tenant IN (?)', $filter->filter('database.namespace_authors')->expression);
        $this->assertSame('_tenant IN (?)', $filter->filter($quote.'namespace_authors'.$quote)->expression);
    }

    #[DataProvider('joins')]
    public function testTenantFilterQuotesTheAliasOfEveryJoin(JoinType $joinType, string $quote): void
    {
        $filter = new TenantFilter(7, allowNullColumn: self::SOURCE.'.'.Storage::UID, quoteChar: $quote);

        $result = $filter->filterJoin(self::ALIAS, $joinType);

        $this->assertNotNull($result);
        $this->assertSame(
            $this->quoted(match ($joinType) {
                JoinType::Inner, JoinType::Left => '"Book"._tenant IN (?)',
                default => '("Book"._tenant IN (?) OR "Book"."_uid" IS NULL)',
            }, $quote),
            $result->condition->expression,
        );
    }

    #[DataProvider('quoteCharacters')]
    public function testOuterJoinTenantFilterQuotesBothTables(string $quote): void
    {
        $filter = new TenantFilter(7, Database::METADATA, 'authors', self::SOURCE.'.'.Storage::UID, $quote);

        $result = (new OuterJoinTenantFilter($filter, self::SOURCE))->filterJoin(self::ALIAS, JoinType::Right);

        $this->assertNotNull($result);
        $this->assertSame(
            $this->quoted('("Main"._tenant IN (?) OR "Main"."_uid" IS NULL) AND "Book"._tenant IN (?)', $quote),
            $result->condition->expression,
        );
    }

    #[DataProvider('quoteCharacters')]
    public function testOuterJoinChainFilterQuotesEveryEarlierTable(string $quote): void
    {
        $chain = new JoinChain([self::EARLIER => JoinType::Cross, self::ALIAS => JoinType::Right]);
        $tenants = new TenantFilter(7, quoteChar: $quote);
        $permission = $this->permission(self::EARLIER, $quote);

        $tenant = (new OuterJoinChainFilter($chain, [self::EARLIER => $tenants->joined(self::EARLIER)], $quote))
            ->filterJoin(self::ALIAS, JoinType::Right);
        $permitted = (new OuterJoinChainFilter($chain, [self::EARLIER => $permission->filter(self::EARLIER)], $quote))
            ->filterJoin(self::ALIAS, JoinType::Right);

        $this->assertNotNull($tenant);
        $this->assertSame($this->quoted('("Extra"._tenant IN (?) OR "Extra"."_uid" IS NULL)', $quote), $tenant->condition->expression);
        $this->assertNotNull($permitted);
        $this->assertOnlyQuoted(self::EARLIER, $permitted->condition->expression, $quote);
    }

    #[DataProvider('quoteCharacters')]
    public function testPermissionFilterQuotesItsDocumentColumn(string $quote): void
    {
        $this->assertSame(
            $this->quoted('"Book"."_uid" IN (SELECT DISTINCT _document FROM "database"."namespace_books_perms" WHERE _permission IN (?) AND _type = ?)', $quote),
            $this->permission(self::ALIAS, $quote)->filter(self::ALIAS)->expression,
        );
    }

    #[DataProvider('joins')]
    public function testPermissionJoinFilterQuotesTheAliasOfEveryJoin(JoinType $joinType, string $quote): void
    {
        $hook = new PermissionJoinFilter($this->permission(self::ALIAS, $quote), self::ALIAS, $quote, preservingOuterJoin: true);

        $result = $hook->filterJoin(self::ALIAS, $joinType);

        $this->assertNotNull($result);
        $this->assertOnlyQuoted(self::ALIAS, $result->condition->expression, $quote);
    }

    #[DataProvider('quoteCharacters')]
    public function testPermissionAllowNullUidQuotesTheMainTableOnBothSides(string $quote): void
    {
        $hook = new PermissionAllowNullUid($this->permission(self::SOURCE, $quote), self::SOURCE.'.'.Storage::UID, $quote);

        $this->assertSame(
            $this->quoted('("Main"."_uid" IN (SELECT DISTINCT _document FROM "database"."namespace_books_perms" WHERE _permission IN (?) AND _type = ?) OR "Main"."_uid" IS NULL)', $quote),
            $hook->filter(self::SOURCE)->expression,
        );
    }

    #[DataProvider('quoteCharacters')]
    public function testOuterJoinPermissionFilterQuotesBothTables(string $quote): void
    {
        $hook = new OuterJoinPermissionFilter(self::SOURCE, [
            self::SOURCE => $this->permission(self::SOURCE, $quote)->filter(self::SOURCE),
            self::ALIAS => $this->permission(self::ALIAS, $quote)->filter(self::ALIAS),
        ], $quote);

        $result = $hook->filterJoin(self::ALIAS, JoinType::FullOuter);

        $this->assertNotNull($result);
        $this->assertOnlyQuoted(self::SOURCE, $result->condition->expression, $quote);
        $this->assertOnlyQuoted(self::ALIAS, $result->condition->expression, $quote);
    }

    private function permission(string $alias, string $quote): PermissionFilter
    {
        return new PermissionFilter(
            roles: ['any'],
            permissionsTable: static fn (string $table): string => 'database.namespace_books_perms',
            documentColumn: $alias.'.'.Storage::UID,
            permDocumentColumn: Storage::PERM_DOCUMENT,
            permRoleColumn: Storage::PERM_PERMISSION,
            permTypeColumn: Storage::PERM_TYPE,
            quoteChar: $quote,
        );
    }

    private function quoted(string $expression, string $quote): string
    {
        return \strtr($expression, ['"' => $quote]);
    }

    private function assertOnlyQuoted(string $alias, string $expression, string $quote): void
    {
        $this->assertStringContainsString($quote.$alias.$quote.'.', $expression, "{$alias} must be named quoted");
        $this->assertDoesNotMatchRegularExpression(
            '/(?<!'.\preg_quote($quote, '/').')\b'.$alias.'\b(?!'.\preg_quote($quote, '/').')/',
            $expression,
            "{$alias} must never be named unquoted",
        );
    }
}
