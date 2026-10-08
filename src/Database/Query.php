<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Query as QueryException;
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
    public const string DEFAULT_ALIAS = 'table_main';

    /**
     * Methods that compose child queries and contribute their inner
     * structure to a shape/fingerprint. Widened from parent's protected
     * declaration so external validators (Queries.php) can reuse it
     * without redeclaring the list.
     *
     * @var list<Method>
     */
    public const array LOGICAL_TYPES = [Method::And, Method::Or, Method::ElemMatch];

    /**
     * Ceiling on the nodes shape() will walk.
     *
     * A query tree that reaches this is not one anybody wrote by hand: either a
     * child points back at an ancestor, which makes the walk unbounded, or the
     * same child object is shared often enough that the preorder walk visits it
     * exponentially. The walk deliberately does not deduplicate -- a node
     * reachable by two paths has to be listed under both, or the reversed pass
     * would shape a parent before its child -- so the count is what bounds it.
     */
    public const int SHAPE_MAX_NODES = 10000;

    /**
     * @param  array<mixed>  $values
     */
    public function __construct(Method|string $method, string $attribute = '', array $values = [], string $alias = '')
    {
        $methodEnum = $method instanceof Method ? $method : Method::from($method);

        if ($attribute === '' && \in_array($methodEnum, [Method::OrderAsc, Method::OrderDesc])) {
            $attribute = Document::SEQUENCE;
        }

        parent::__construct($methodEnum, $attribute, $values, $alias);
    }

    /**
     * @throws QueryException
     */
    #[\Override]
    public static function parse(string $query, bool $allowRaw = false): static
    {
        try {
            $parsed = parent::parse($query, $allowRaw);

            return new static($parsed->getMethod(), $parsed->getAttribute(), $parsed->getValues(), $parsed->getAlias());
        } catch (BaseQueryException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param  array<string, mixed>  $query
     *
     * @throws QueryException
     */
    #[\Override]
    public static function parseQuery(array $query, bool $allowRaw = false): static
    {
        try {
            $parsed = parent::parseQuery(self::decodeNestedValues($query, $allowRaw), $allowRaw);

            return new static($parsed->getMethod(), $parsed->getAttribute(), $parsed->getValues(), $parsed->getAlias());
        } catch (BaseQueryException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Decode a logical query's children to the array form the parser recurses into.
     *
     * Clients may serialise those children as query strings rather than nested
     * objects. The parser's recursion is typed for arrays and only documented as
     * such, so a string child reaches it and raises a TypeError, which is not a
     * QueryException and so escapes every caller that guards for one.
     *
     * @param  array<string, mixed>  $query
     * @return array<string, mixed>
     *
     * @throws QueryException
     */
    private static function decodeNestedValues(array $query, bool $allowRaw): array
    {
        $method = $query['method'] ?? null;

        if (! \is_string($method)) {
            return $query;
        }

        if (! (Method::tryFrom($method)?->isNested() ?? false)) {
            return $query;
        }

        $values = $query['values'] ?? [];

        if (! \is_array($values)) {
            return $query;
        }

        foreach ($values as $index => $value) {
            if (\is_array($value)) {
                continue;
            }

            if (! \is_string($value)) {
                throw new QueryException('Invalid nested query. Must be an array or string, got '.\gettype($value));
            }

            $values[$index] = self::parse($value, $allowRaw)->toArray();
        }

        $query['values'] = $values;

        return $query;
    }

    /**
     * @param  array<string, mixed>|object  $value  a Document; Validator\Query\Cursor also takes its id, and refuses an array
     */
    #[\Override]
    public static function cursorAfter(array|object $value): static
    {
        return new static(Method::CursorAfter, values: [$value]);
    }

    /**
     * @param  array<string, mixed>|object  $value  a Document; Validator\Query\Cursor also takes its id, and refuses an array
     */
    #[\Override]
    public static function cursorBefore(array|object $value): static
    {
        return new static(Method::CursorBefore, values: [$value]);
    }

    /**
     * Standard deviation over the **population**.
     *
     * Bare SQL `STDDEV` is population on MySQL and MariaDB and sample on
     * PostgreSQL. The adapters pin this method to `STDDEV_POP` so every engine
     * answers the same number; ask for stddevSamp() when you want the sample
     * statistic.
     */
    #[\Override]
    public static function stddev(string $attribute, string $alias = ''): static
    {
        return parent::stddev($attribute, $alias);
    }

    /**
     * Variance over the **population**.
     *
     * Bare SQL `VARIANCE` is population on MySQL and MariaDB and sample on
     * PostgreSQL. The adapters pin this method to `VAR_POP` so every engine
     * answers the same number; ask for varSamp() when you want the sample
     * statistic.
     */
    #[\Override]
    public static function variance(string $attribute, string $alias = ''): static
    {
        return parent::variance($attribute, $alias);
    }

    /**
     * Check if method is supported. Accepts both string and Method enum.
     */
    #[\Override]
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
    #[\Override]
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
     *
     * @throws QueryException if the tree exceeds self::SHAPE_MAX_NODES
     */
    #[\Override]
    public function shape(): string
    {
        // 1. Preorder flatten the tree.
        $nodes = [];
        $stack = [$this];
        while ($stack) {
            /** @var self $node */
            $node = \array_pop($stack);
            $nodes[] = $node;

            if (\count($nodes) > self::SHAPE_MAX_NODES) {
                throw new QueryException('Query is too deeply nested to fingerprint: exceeded '.self::SHAPE_MAX_NODES.' nodes, which means a cycle or a child shared across too many parents');
            }

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
    #[\Override]
    public function toArray(): array
    {
        $array = ['method' => $this->method->value];

        if (! empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if ($this->alias !== '') {
            $array['alias'] = $this->alias;
        }

        if (\in_array($this->method, self::LOGICAL_TYPES, true) || $this->method === Method::Having) {
            foreach ($this->values as $index => $value) {
                if (! $value instanceof self) {
                    throw new QueryException(
                        'Invalid child query in '.$this->method->value.' at index '.$index.': expected Query, got '.\get_debug_type($value)
                    );
                }
                $array['values'][$index] = $value->toArray();
            }
        } else {
            $array['values'] = [];
            foreach ($this->values as $value) {
                $array['values'][] = match (true) {
                    $value instanceof BaseQuery => $value->toArray(),
                    $value instanceof Document && \in_array($this->method, [Method::CursorAfter, Method::CursorBefore], true) => $value->getId(),
                    default => $value,
                };
            }
        }

        return $array;
    }

    /**
     * Group the queries by kind, with their orders in the order the queries give them.
     *
     * @param  array<mixed>  $queries  Database queries
     *
     * @throws QueryException When a cursor is not a document
     */
    #[\Override]
    public static function groupByType(array $queries): ParsedQuery
    {
        $grouped = parent::groupByType($queries);

        $cursor = $grouped->cursor;
        if ($cursor !== null && ! $cursor instanceof Document) {
            throw new QueryException('Invalid query: Invalid cursor: a cursor must be a document, '.\get_debug_type($cursor).' given');
        }

        $orderAttributes = [];
        $orderTypes = [];
        foreach ($queries as $query) {
            if (! $query instanceof BaseQuery) {
                continue;
            }

            $direction = match ($query->getMethod()) {
                Method::OrderAsc => OrderDirection::Asc,
                Method::OrderDesc => OrderDirection::Desc,
                Method::OrderRandom => OrderDirection::Random,
                default => null,
            };

            if ($direction === null) {
                continue;
            }

            $orderAttributes[] = $query->getAttribute();
            $orderTypes[] = $direction;
        }

        /** @var list<Query> $filters */
        $filters = $grouped->filters;
        /** @var list<Query> $selections */
        $selections = $grouped->selections;
        /** @var list<Query> $aggregations */
        $aggregations = $grouped->aggregations;
        /** @var list<Query> $having */
        $having = $grouped->having;
        /** @var list<Query> $joins */
        $joins = $grouped->joins;
        /** @var list<Query> $unions */
        $unions = $grouped->unions;

        return new ParsedQuery(
            filters: $filters,
            selections: $selections,
            aggregations: $aggregations,
            groupBy: $grouped->groupBy,
            having: $having,
            distinct: $grouped->distinct,
            joins: $joins,
            unions: $unions,
            limit: $grouped->limit,
            offset: $grouped->offset,
            cursor: $cursor,
            cursorDirection: $grouped->cursorDirection,
            timeBuckets: $grouped->timeBuckets,
            orderAttributes: $orderAttributes,
            orderTypes: $orderTypes,
        );
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
