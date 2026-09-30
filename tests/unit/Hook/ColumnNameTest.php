<?php

namespace Tests\Unit\Hook;

use Closure;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\AllowNullColumn;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Query\Builder\Condition;

final class ColumnNameTest extends TestCase
{
    public function testWrapAllowsNullInTheQuotedColumn(): void
    {
        $condition = AllowNullColumn::wrap(new Condition('x = ?', [1]), 'alias._uid');

        $this->assertSame('(x = ? OR `alias`.`_uid` IS NULL)', $condition->expression);
        $this->assertSame([1], $condition->bindings);
    }

    #[DataProvider('invalidColumns')]
    public function testWrapRejectsAColumnOutsideTheIdentifierPattern(string $column): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid column name: '.$column);

        AllowNullColumn::wrap(new Condition('x = 1'), $column);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidColumns(): iterable
    {
        yield 'a space' => ['a b'];
        yield 'a statement separator' => ['x;y'];
        yield 'a quote character' => ['x`y'];
        yield 'an empty name' => [''];
    }

    /**
     * @param  Closure(string): PermissionFilter  $construct
     */
    #[DataProvider('permissionFilterColumns')]
    public function testPermissionFilterRejectsAColumnOutsideTheIdentifierPattern(Closure $construct, string $column): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid column name: '.$column);

        $construct($column);
    }

    /**
     * @return iterable<string, array{Closure(string): PermissionFilter, string}>
     */
    public static function permissionFilterColumns(): iterable
    {
        $permissionsTable = static fn (string $table): string => $table.'_perms';
        $constructors = [
            'documentColumn' => static fn (string $column): PermissionFilter => new PermissionFilter(['any'], $permissionsTable, documentColumn: $column),
            'permDocumentColumn' => static fn (string $column): PermissionFilter => new PermissionFilter(['any'], $permissionsTable, permDocumentColumn: $column),
            'permRoleColumn' => static fn (string $column): PermissionFilter => new PermissionFilter(['any'], $permissionsTable, permRoleColumn: $column),
            'permTypeColumn' => static fn (string $column): PermissionFilter => new PermissionFilter(['any'], $permissionsTable, permTypeColumn: $column),
            'permColumnColumn' => static fn (string $column): PermissionFilter => new PermissionFilter(['any'], $permissionsTable, permColumnColumn: $column),
        ];
        foreach ($constructors as $parameter => $construct) {
            foreach (self::invalidColumns() as $label => [$column]) {
                yield $parameter.' with '.$label => [$construct, $column];
            }
        }
    }

    public function testPermissionFilterWithoutRolesMatchesNothing(): void
    {
        $filter = new PermissionFilter([], static fn (string $table): string => $table.'_perms');

        $condition = $filter->filter('posts');

        $this->assertSame('1 = 0', $condition->expression);
        $this->assertSame([], $condition->bindings);
    }

    public function testPermissionFilterRejectsAPermissionsTableOutsideTheIdentifierPattern(): void
    {
        $filter = new PermissionFilter(['any'], static fn (string $table): string => $table.' perms');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid permissions table name: posts perms');

        $filter->filter('posts');
    }

    public function testPermissionFilterWithNoColumnsMatchesOnlyCollectionWidePermissions(): void
    {
        $filter = new PermissionFilter(['any'], static fn (string $table): string => $table.'_perms', columns: []);

        $condition = $filter->filter('posts');

        $this->assertStringEndsWith(' AND type = ? AND column IS NULL)', $condition->expression);
        $this->assertSame(['any', 'read'], $condition->bindings);
    }

    public function testPermissionFilterWithColumnsMatchesThemOrCollectionWidePermissions(): void
    {
        $filter = new PermissionFilter(['any', 'users'], static fn (string $table): string => $table.'_perms', columns: ['title', 'body']);

        $condition = $filter->filter('posts');

        $this->assertStringEndsWith(' AND type = ? AND (column IS NULL OR column IN (?, ?)))', $condition->expression);
        $this->assertSame(['any', 'users', 'read', 'title', 'body'], $condition->bindings);
    }

    public function testWrapAcceptsADigitOrHyphenLeadingColumnAndQuotesIt(): void
    {
        $this->assertSame('(x = 1 OR `1db`.`_uid` IS NULL)', AllowNullColumn::wrap(new Condition('x = 1'), '1db._uid')->expression);
        $this->assertSame('(x = 1 OR `-ns`.`_uid` IS NULL)', AllowNullColumn::wrap(new Condition('x = 1'), '-ns._uid')->expression);
    }

    public function testPermissionFilterAcceptsADigitLeadingPermissionsTableAndQuotesIt(): void
    {
        $filter = new PermissionFilter(['any'], static fn (string $table): string => '1db.ns_'.$table.'_perms');

        $condition = $filter->filter('posts');

        $this->assertStringContainsString(' FROM `1db`.`ns_posts_perms` WHERE ', $condition->expression);
        $this->assertSame(['any', 'read'], $condition->bindings);
    }

    public function testPermissionFilterStillRefusesADigitLeadingUnquotedColumn(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid column name: 1role');

        new PermissionFilter(['any'], static fn (string $table): string => $table.'_perms', permRoleColumn: '1role');
    }
}
