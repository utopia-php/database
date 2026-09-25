<?php

namespace Utopia\Database\Hook;

use InvalidArgumentException;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

final readonly class AllowNullColumn implements Filter
{
    private const IDENTIFIER_PATTERN = '/^[a-zA-Z_][a-zA-Z0-9_.\-]*$/';

    public function __construct(
        private Filter $filter,
        private string $column,
        private string $quoteChar = '`',
    ) {
        if (! \preg_match(self::IDENTIFIER_PATTERN, $column)) {
            throw new InvalidArgumentException('Invalid column name: '.$column);
        }
    }

    public function filter(string $table): Condition
    {
        return self::wrap($this->filter->filter($table), $this->column, $this->quoteChar);
    }

    public static function wrap(Condition $condition, string $column, string $quoteChar = '`'): Condition
    {
        if (! \preg_match(self::IDENTIFIER_PATTERN, $column)) {
            throw new InvalidArgumentException('Invalid column name: '.$column);
        }

        return new Condition(
            '('.$condition->expression.' OR '.self::quote($column, $quoteChar).' IS NULL)',
            $condition->bindings,
        );
    }

    public static function quote(string $identifier, string $quoteChar = '`'): string
    {
        $parts = \explode('.', $identifier);
        $quoted = \array_map(
            fn (string $part): string => $quoteChar.\str_replace($quoteChar, $quoteChar.$quoteChar, $part).$quoteChar,
            $parts,
        );

        return \implode('.', $quoted);
    }
}
