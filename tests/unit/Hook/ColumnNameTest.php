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
}
