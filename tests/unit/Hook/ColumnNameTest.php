<?php

namespace Tests\Unit\Hook;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\AllowNullColumn;
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
}
