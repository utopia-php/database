<?php

namespace Tests\Unit\Adapter\SQL\Hook\Permission;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQL\Hook\Permission\AllowNullUid;
use Utopia\Database\Adapter\SQL\Hook\Permission\Filter as PermissionFilter;
use Utopia\Database\Storage;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

final class AllowNullUidTest extends TestCase
{
    public function testWrapsPermissionConditionWithNullUid(): void
    {
        $inner = new PermissionFilter(
            roles: ['any'],
            permissionsTable: static fn (string $table): string => 'perms_'.$table,
            documentColumn: 'table_main.'.Storage::UID,
        );
        $hook = new AllowNullUid($inner, 'table_main.'.Storage::UID);

        $condition = $hook->filter('movies');

        $this->assertSame(
            '('.$inner->filter('movies')->expression.' OR `table_main`.`'.Storage::UID.'` IS NULL)',
            $condition->expression,
        );
        $this->assertSame($inner->filter('movies')->bindings, $condition->bindings);
    }

    public function testQuotesPostgresStyleIdentifiers(): void
    {
        $inner = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('inner_expr', ['role']);
            }
        };
        $hook = new AllowNullUid($inner, 'table_main.'.Storage::UID, '"');

        $condition = $hook->filter('movies');

        $this->assertSame('(inner_expr OR "table_main"."'.Storage::UID.'" IS NULL)', $condition->expression);
        $this->assertSame(['role'], $condition->bindings);
    }

    public function testRejectsInvalidDocumentColumn(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new AllowNullUid(
            new PermissionFilter(
                roles: ['any'],
                permissionsTable: static fn (string $table): string => 'perms_'.$table,
            ),
            'table_main._uid; DROP TABLE',
        );
    }
}
