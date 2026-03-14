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
            return parent::parse($query);
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
            return parent::parseQuery($query);
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
