<?php

namespace Utopia\Database\Builder;

use JsonException;
use Utopia\Database\Storage;
use Utopia\Query\Builder\SQLite as Base;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * SQLite has no default LIKE escape character, so every LIKE declares the
 * backslash that escapeLikeValue() puts in front of `%`, `_` and `\`.
 *
 * REGEXP resolves to the user function the SQLite adapter registers.
 *
 * Document ids are unique in the COLLATION of their unique indexes, and SQLite
 * only uses an index for a comparison made in the index's collation, so every
 * equality on an id column compares in that collation.
 */
class SQLite extends Base
{
    public const string COLLATION = 'NOCASE';

    private const string COLLATE = ' COLLATE '.self::COLLATION;

    private const array COLLATED_COLUMNS = [Storage::UID, Storage::PERM_DOCUMENT];

    private const array EQUALITY_OPERATORS = ['=', '!=', '<>'];

    private const string ESCAPE = " ESCAPE '\\'";

    private const string ELEMENT_MATCH = "EXISTS (SELECT 1 FROM json_each(%s) WHERE json_each.value = json_extract(?, '$'))";

    #[\Override]
    public function compileJoin(Query $query): string
    {
        $sql = parent::compileJoin($query);

        foreach ($this->joinComparisons($query) as [$left, $operator, $right]) {
            if (! \in_array($operator, self::EQUALITY_OPERATORS, true) || (! $this->isCollated($left) && ! $this->isCollated($right))) {
                continue;
            }

            $comparison = ' '.$operator.' '.$this->resolveAndWrap($right);
            $wrappedLeft = $this->resolveAndWrap($left);
            $sql = \str_replace($wrappedLeft.$comparison, $wrappedLeft.self::COLLATE.$comparison, $sql);
        }

        return $sql;
    }

    #[\Override]
    public function whereColumn(string $left, string $operator, string $right): static
    {
        if (! \in_array($operator, self::EQUALITY_OPERATORS, true) || (! $this->isCollated($left) && ! $this->isCollated($right))) {
            return parent::whereColumn($left, $operator, $right);
        }

        return $this->whereRaw($this->resolveAndWrap($left).self::COLLATE.' '.$operator.' '.$this->resolveAndWrap($right));
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileIn(string $attribute, array $values, ?string $column = null): string
    {
        return parent::compileIn($this->collate($attribute, $column), $values, $column);
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileNotIn(string $attribute, array $values, ?string $column = null): string
    {
        return parent::compileNotIn($this->collate($attribute, $column), $values, $column);
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileRegex(string $attribute, array $values, ?string $column = null): string
    {
        $this->addBinding($values[0], $column);

        return $attribute.' REGEXP ?';
    }

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

    private function collate(string $attribute, ?string $column): string
    {
        return $column !== null && $this->isCollated($column) ? $attribute.self::COLLATE : $attribute;
    }

    private function isCollated(string $column): bool
    {
        $resolved = $this->resolveAttribute($column);
        $separator = \strrpos($resolved, '.');
        $name = $separator === false ? $resolved : \substr($resolved, $separator + 1);

        return \in_array($name, self::COLLATED_COLUMNS, true);
    }

    /**
     * @return list<array{string, string, string}>
     */
    private function joinComparisons(Query $query): array
    {
        if ($query->isNestedJoin()) {
            return $this->onComparisons($query->getJoinOnQueries());
        }

        return $this->comparison($query->getValues());
    }

    /**
     * @param  array<mixed>  $queries
     * @return list<array{string, string, string}>
     */
    private function onComparisons(array $queries): array
    {
        $comparisons = [];
        foreach ($queries as $query) {
            if (! $query instanceof Query) {
                continue;
            }

            $comparisons = match ($query->getMethod()) {
                Method::On => [...$comparisons, ...$this->comparison($query->getValues())],
                Method::And, Method::Or => [...$comparisons, ...$this->onComparisons($query->getValues())],
                default => $comparisons,
            };
        }

        return $comparisons;
    }

    /**
     * @param  array<mixed>  $values
     * @return list<array{string, string, string}>
     */
    private function comparison(array $values): array
    {
        [$left, $operator, $right] = [$values[0] ?? null, $values[1] ?? null, $values[2] ?? null];
        if (! \is_string($left) || ! \is_string($operator) || ! \is_string($right) || $left === '' || $right === '') {
            return [];
        }

        return [[$left, $operator, $right]];
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
