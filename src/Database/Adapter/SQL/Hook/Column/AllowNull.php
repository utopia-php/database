<?php

namespace Utopia\Database\Hook;

use InvalidArgumentException;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

final readonly class AllowNullColumn implements Filter
{
    private const IDENTIFIER_PATTERN = '/^[a-zA-Z0-9_\-][a-zA-Z0-9_.\-]*$/';

    public function __construct(
        private Filter $filter,
        private string $column,
        private string $quoteCharacter = '`',
    ) {
        if (! \preg_match(self::IDENTIFIER_PATTERN, $column)) {
            throw new InvalidArgumentException('Invalid column name: '.$column);
        }
    }

    public function filter(string $table): Condition
    {
        return self::wrap($this->filter->filter($table), $this->column, $this->quoteCharacter);
    }

    public static function wrap(Condition $condition, string $column, string $quoteCharacter = '`'): Condition
    {
        if (! \preg_match(self::IDENTIFIER_PATTERN, $column)) {
            throw new DatabaseException('Invalid column name: '.$column);
        }

        return new Condition(
            '('.$condition->expression.' OR '.self::quote($column, $quoteCharacter).' IS NULL)',
            $condition->bindings,
        );
    }

    public static function quote(string $identifier, string $quoteCharacter = '`'): string
    {
        $parts = \explode('.', $identifier);
        $quoted = \array_map(
            fn (string $part): string => $quoteCharacter.\str_replace($quoteCharacter, $quoteCharacter.$quoteCharacter, $part).$quoteCharacter,
            $parts,
        );

        return \implode('.', $quoted);
    }
}
