<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\SQLite as Base;

/**
 * SQLite has no default LIKE escape character, so every LIKE declares the
 * backslash that escapeLikeValue() puts in front of `%`, `_` and `\`.
 */
class SQLite extends Base
{
    private const string ESCAPE = " ESCAPE '\\'";

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileLike(string $attribute, array $values, string $prefix, string $suffix, bool $not, ?string $column = null): string
    {
        return parent::compileLike($attribute, $values, $prefix, $suffix, $not, $column).self::ESCAPE;
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileContains(string $attribute, array $values, ?string $column = null): string
    {
        $predicates = $this->compileSubstrings($attribute, $values, false, $column);

        return \count($predicates) === 1 ? $predicates[0] : '('.\implode(' OR ', $predicates).')';
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileContainsAll(string $attribute, array $values, ?string $column = null): string
    {
        return '('.\implode(' AND ', $this->compileSubstrings($attribute, $values, false, $column)).')';
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileNotContains(string $attribute, array $values, ?string $column = null): string
    {
        $predicates = $this->compileSubstrings($attribute, $values, true, $column);

        return \count($predicates) === 1 ? $predicates[0] : '('.\implode(' AND ', $predicates).')';
    }

    /**
     * @param  array<mixed>  $values
     * @return list<string>
     */
    private function compileSubstrings(string $attribute, array $values, bool $not, ?string $column): array
    {
        return \array_map(
            fn (mixed $value): string => $this->compileLike($attribute, [$value], '%', '%', $not, $column),
            \array_values($values),
        );
    }
}
