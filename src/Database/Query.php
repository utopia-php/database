<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Query as QueryException;
use Utopia\Query\CursorDirection;
use Utopia\Query\Exception as BaseQueryException;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;

/**
 * Extends the base query library with database-specific query construction, parsing, and grouping.
 *
 * @phpstan-consistent-constructor
 */
class Query extends BaseQuery
{
    protected bool $isObjectAttribute = false;

    /**
     * Default table alias used in queries
     */
    public const DEFAULT_ALIAS = 'table_main';

    /**
     * Methods that compose child queries and contribute their inner
     * structure to a shape/fingerprint.
     *
     * @var array<Method>
     */
    private const LOGICAL_TYPES = [Method::And, Method::Or, Method::ElemMatch];

    public const TYPE_ELEM_MATCH = 'elemMatch';

    /**
     * @param  array<mixed>  $values
     */
    public function __construct(Method|string $method, string $attribute = '', array $values = [])
    {
        $methodEnum = $method instanceof Method ? $method : Method::from($method);

        if ($attribute === '' && \in_array($methodEnum, [Method::OrderAsc, Method::OrderDesc])) {
            $attribute = '$sequence';
        }

        parent::__construct($methodEnum, $attribute, $values);
    }

    /**
     * @throws QueryException
     */
    public static function parse(string $query): static
    {
        try {
            $parsed = parent::parse($query);

            return new static($parsed->getMethod(), $parsed->getAttribute(), $parsed->getValues());
        } catch (BaseQueryException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param  array<string, mixed>  $query
     *
     * @throws QueryException
     */
    public static function parseQuery(array $query): static
    {
        try {
            $parsed = parent::parseQuery($query);

            return new static($parsed->getMethod(), $parsed->getAttribute(), $parsed->getValues());
        } catch (BaseQueryException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param  Document  $value
     */
    public static function cursorAfter(mixed $value): static
    {
        return new static(Method::CursorAfter, values: [$value]);
    }

    /**
     * @param  Document  $value
     */
    public static function cursorBefore(mixed $value): static
    {
        return new static(Method::CursorBefore, values: [$value]);
    }

    /**
     * Check if method is supported. Accepts both string and Method enum.
     */
    public static function isMethod(Method|string $value): bool
    {
        if ($value instanceof Method) {
            return true;
        }

        return Method::tryFrom($value) !== null;
    }

    /**
     * Compute a shape-only fingerprint of an array of queries.
     *
     * The fingerprint captures the structure of the queries — method and
     * attribute — without values. Two query sets with the same shape but
     * different parameter values produce the same fingerprint, which is
     * useful for pattern-based counting and slow-query grouping.
     *
     * Logical queries (`and`, `or`, `elemMatch`) contribute their inner
     * structure to the hash via `Query::shape()` — two `and(...)` queries
     * with different child shapes produce different fingerprints.
     *
     * Accepts either raw query strings or parsed Query objects.
     *
     * @param array<mixed> $queries raw query strings or Query instances
     * @return string md5 hash of the canonical shape
     * @throws QueryException if an element is neither a string nor a Query
     */
    public static function fingerprint(array $queries): string
    {
        $shapes = [];

        foreach ($queries as $query) {
            if (\is_string($query)) {
                $query = self::parse($query);
            }

            if (!$query instanceof self) {
                throw new QueryException('Invalid query element for fingerprint: expected string or Query instance');
            }

            $shapes[] = $query->shape();
        }

        \sort($shapes);

        return \md5(\implode('|', $shapes));
    }

    /**
     * Canonical shape string for this Query — values excluded.
     *
     * Non-logical queries produce `method:attribute`. Logical queries
     * (`and`, `or`, `elemMatch`) produce `method:attribute(child1|child2|…)`
     * with children sorted so child order does not affect the shape.
     *
     * Implemented iteratively: walks the tree into a preorder list via a
     * stack, then processes the reversed list so each node's children are
     * always resolved before the node itself.
     *
     * @return string
     */
    public function shape(): string
    {
        // 1. Preorder flatten the tree.
        $nodes = [];
        $stack = [$this];
        while ($stack) {
            /** @var self $node */
            $node = \array_pop($stack);
            $nodes[] = $node;

            if (!\in_array($node->method, self::LOGICAL_TYPES, true)) {
                continue;
            }
            foreach ($node->values as $child) {
                if ($child instanceof self) {
                    $stack[] = $child;
                }
            }
        }

        // 2. Process reversed so children are always shaped before parents.
        $shapes = [];
        foreach (\array_reverse($nodes) as $node) {
            $id = \spl_object_id($node);

            if (!\in_array($node->method, self::LOGICAL_TYPES, true)) {
                $shapes[$id] = $node->method->value . ':' . $node->attribute;
                continue;
            }

            $childShapes = [];
            foreach ($node->values as $child) {
                if ($child instanceof self) {
                    $childShapes[] = $shapes[\spl_object_id($child)];
                }
            }
            \sort($childShapes);
            // Attribute is empty for and/or; meaningful for elemMatch (the field being matched).
            $shapes[$id] = $node->method->value . ':' . $node->attribute . '(' . \implode('|', $childShapes) . ')';
        }

        return $shapes[\spl_object_id($this)];
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $array = ['method' => $this->method->value];

        if (! empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if (\in_array($this->method, [Method::And, Method::Or, Method::ElemMatch])) {
            foreach ($this->values as $index => $value) {
                /** @var Query $value */
                $array['values'][$index] = $value->toArray();
            }
        } else {
            $array['values'] = [];
            foreach ($this->values as $value) {
                if ($value instanceof Document && in_array($this->method, [Method::CursorAfter, Method::CursorBefore])) {
                    $value = $value->getId();
                }
                $array['values'][] = $value;
            }
        }

        return $array;
    }

    /**
     * Iterates through queries and groups them by type,
     * returning the result in the Database-specific array format
     * with string order types and cursor directions.
     *
     * @param  array<Query>  $queries
     * @return array{
     *     filters: array<Query>,
     *     selections: array<Query>,
     *     aggregations: array<Query>,
     *     groupBy: array<string>,
     *     having: array<Query>,
     *     joins: array<Query>,
     *     distinct: bool,
     *     limit: int|null,
     *     offset: int|null,
     *     orderAttributes: array<string>,
     *     orderTypes: array<OrderDirection>,
     *     cursor: Document|null,
     *     cursorDirection: CursorDirection|null
     * }
     */
    public static function groupForDatabase(array $queries): array
    {
        $grouped = parent::groupByType($queries);

        /** @var array<Query> $filters */
        $filters = $grouped->filters;
        /** @var array<Query> $selections */
        $selections = $grouped->selections;
        /** @var array<Query> $aggregations */
        $aggregations = $grouped->aggregations;
        /** @var array<Query> $having */
        $having = $grouped->having;
        /** @var array<Query> $joins */
        $joins = $grouped->joins;
        /** @var Document|null $cursor */
        $cursor = $grouped->cursor;

        return [
            'filters' => $filters,
            'selections' => $selections,
            'aggregations' => $aggregations,
            'groupBy' => $grouped->groupBy,
            'having' => $having,
            'joins' => $joins,
            'distinct' => $grouped->distinct,
            'limit' => $grouped->limit,
            'offset' => $grouped->offset,
            'orderAttributes' => $grouped->orderAttributes,
            'orderTypes' => $grouped->orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $grouped->cursorDirection,
        ];
    }

    /**
     * Check whether this query targets a spatial attribute type (point, linestring, or polygon).
     *
     * @return bool True if the attribute type is spatial.
     */
    public function isSpatialAttribute(): bool
    {
        $type = ColumnType::tryFrom($this->attributeType);
        return in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true);
    }

    /**
     * Check whether this query targets an object (JSON/hashmap) attribute type.
     *
     * @return bool True if the attribute type is object.
     */
    public function isObjectAttribute(): bool
    {
        return ColumnType::tryFrom($this->attributeType) === ColumnType::Object;
    }
}
