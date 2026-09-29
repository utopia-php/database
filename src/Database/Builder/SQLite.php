<?php

namespace Utopia\Database\Builder;

use JsonException;
use Utopia\Query\Builder\SQLite as Base;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * SQLite has no default LIKE escape character, so every LIKE declares the
 * backslash that escapeLikeValue() puts in front of `%`, `_` and `\`.
 */
class SQLite extends Base
{
    private const string ESCAPE = " ESCAPE '\\'";

    private const string ELEMENT_MATCH = "EXISTS (SELECT 1 FROM json_each(%s) WHERE json_each.value = json_extract(?, '$'))";

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

    #[\Override]
    protected function compileArrayFilter(Method $method, string $attribute, Query $query): string
    {
        if ($method === Method::NotContains) {
            return $this->compileNotContaining($attribute, $this->compileJsonOverlapsExpr($attribute, [$query->getValues()]));
        }

        return parent::compileArrayFilter($method, $attribute, $query);
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileJsonContainsExpr(string $attribute, array $values, bool $not): string
    {
        $expression = '('.\implode(' AND ', $this->compileElementMatches($attribute, $values[0])).')';

        return $not ? $this->compileNotContaining($attribute, $expression) : $expression;
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileJsonOverlapsExpr(string $attribute, array $values): string
    {
        return '('.\implode(' OR ', $this->compileElementMatches($attribute, $values[0])).')';
    }

    private function compileNotContaining(string $attribute, string $expression): string
    {
        return '('.$attribute.' IS NOT NULL AND NOT '.$expression.')';
    }

    /**
     * @return list<string>
     */
    private function compileElementMatches(string $attribute, mixed $needles): array
    {
        $matches = [];
        foreach ((array) $needles as $needle) {
            try {
                $this->addBinding(\json_encode($needle, JSON_THROW_ON_ERROR));
            } catch (JsonException $exception) {
                throw new ValidationException('Invalid JSON payload: '.$exception->getMessage());
            }
            $matches[] = \sprintf(self::ELEMENT_MATCH, $attribute);
        }

        return $matches;
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
