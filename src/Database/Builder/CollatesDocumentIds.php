<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * Document ids are unique in the collation of their unique indexes, and an engine only uses an index for a
 * comparison made in the index's collation, so equality on an id column, and a join ON an id column, compares in
 * that collation. The using builder names the collation in COLLATION and decides which columns, and which joins,
 * are collated.
 */
trait CollatesDocumentIds
{
    abstract private function isCollated(string $column): bool;

    abstract private function collatesJoin(string $left, string $right): bool;

    #[\Override]
    public function compileJoin(Query $query): string
    {
        return $this->collateJoin(parent::compileJoin($query), $this->onComparisons($query->getJoinOnQueries()));
    }

    #[\Override]
    protected function compileJoinWithBuilder(Query $query, JoinBuilder $joinBuilder): string
    {
        $comparisons = [];
        foreach ($joinBuilder->ons as $on) {
            $comparisons[] = [$on->left, $on->operator, $on->right];
        }

        return $this->collateJoin(parent::compileJoinWithBuilder($query, $joinBuilder), $comparisons);
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

    private function collate(string $attribute, ?string $column): string
    {
        return $column !== null && $this->isCollated($column) ? $attribute.' COLLATE '.self::COLLATION : $attribute;
    }

    /**
     * @param  list<array{string, string, string}>  $comparisons
     */
    private function collateJoin(string $sql, array $comparisons): string
    {
        foreach ($comparisons as [$left, $operator, $right]) {
            if (! \in_array($operator, ['=', '!=', '<>'], true) || ! $this->collatesJoin($left, $right)) {
                continue;
            }

            $comparison = ' '.$operator.' '.$this->resolveAndWrap($right);
            $wrappedLeft = $this->resolveAndWrap($left);
            $sql = \str_replace($wrappedLeft.$comparison, $wrappedLeft.' COLLATE '.self::COLLATION.$comparison, $sql);
        }

        return $sql;
    }

    /**
     * The last segment of a column, after the attribute hooks resolve it.
     */
    private function columnName(string $column): string
    {
        $resolved = $this->resolveAttribute($column);
        $separator = \strrpos($resolved, '.');

        return $separator === false ? $resolved : \substr($resolved, $separator + 1);
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
}
