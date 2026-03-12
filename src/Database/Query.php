<?php

namespace Utopia\Database;

use Utopia\Database\CursorDirection as DatabaseCursorDirection;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\OrderDirection as DatabaseOrderDirection;
use Utopia\Query\CursorDirection as QueryCursorDirection;
use Utopia\Query\Exception as BaseQueryException;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection as QueryOrderDirection;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;

/** @phpstan-consistent-constructor */
class Query extends BaseQuery
{
    protected bool $isObjectAttribute = false;

    // Backward compatibility constants mapping to Method enum values
    public const TYPE_EQUAL = Method::Equal;
    public const TYPE_NOT_EQUAL = Method::NotEqual;
    public const TYPE_LESSER = Method::LessThan;
    public const TYPE_LESSER_EQUAL = Method::LessThanEqual;
    public const TYPE_GREATER = Method::GreaterThan;
    public const TYPE_GREATER_EQUAL = Method::GreaterThanEqual;
    public const TYPE_CONTAINS = Method::Contains;
    public const TYPE_CONTAINS_ANY = Method::ContainsAny;
    public const TYPE_CONTAINS_ALL = Method::ContainsAll;
    public const TYPE_NOT_CONTAINS = Method::NotContains;
    public const TYPE_SEARCH = Method::Search;
    public const TYPE_NOT_SEARCH = Method::NotSearch;
    public const TYPE_IS_NULL = Method::IsNull;
    public const TYPE_IS_NOT_NULL = Method::IsNotNull;
    public const TYPE_BETWEEN = Method::Between;
    public const TYPE_NOT_BETWEEN = Method::NotBetween;
    public const TYPE_STARTS_WITH = Method::StartsWith;
    public const TYPE_NOT_STARTS_WITH = Method::NotStartsWith;
    public const TYPE_ENDS_WITH = Method::EndsWith;
    public const TYPE_NOT_ENDS_WITH = Method::NotEndsWith;
    public const TYPE_REGEX = Method::Regex;
    public const TYPE_EXISTS = Method::Exists;
    public const TYPE_NOT_EXISTS = Method::NotExists;

    // Spatial
    public const TYPE_CROSSES = Method::Crosses;
    public const TYPE_NOT_CROSSES = Method::NotCrosses;
    public const TYPE_DISTANCE_EQUAL = Method::DistanceEqual;
    public const TYPE_DISTANCE_NOT_EQUAL = Method::DistanceNotEqual;
    public const TYPE_DISTANCE_GREATER_THAN = Method::DistanceGreaterThan;
    public const TYPE_DISTANCE_LESS_THAN = Method::DistanceLessThan;
    public const TYPE_INTERSECTS = Method::Intersects;
    public const TYPE_NOT_INTERSECTS = Method::NotIntersects;
    public const TYPE_OVERLAPS = Method::Overlaps;
    public const TYPE_NOT_OVERLAPS = Method::NotOverlaps;
    public const TYPE_TOUCHES = Method::Touches;
    public const TYPE_NOT_TOUCHES = Method::NotTouches;
    public const TYPE_COVERS = Method::Covers;
    public const TYPE_NOT_COVERS = Method::NotCovers;
    public const TYPE_SPATIAL_EQUALS = Method::SpatialEquals;
    public const TYPE_NOT_SPATIAL_EQUALS = Method::NotSpatialEquals;

    // Vector
    public const TYPE_VECTOR_DOT = Method::VectorDot;
    public const TYPE_VECTOR_COSINE = Method::VectorCosine;
    public const TYPE_VECTOR_EUCLIDEAN = Method::VectorEuclidean;

    // Structure
    public const TYPE_SELECT = Method::Select;
    public const TYPE_ORDER_ASC = Method::OrderAsc;
    public const TYPE_ORDER_DESC = Method::OrderDesc;
    public const TYPE_ORDER_RANDOM = Method::OrderRandom;
    public const TYPE_LIMIT = Method::Limit;
    public const TYPE_OFFSET = Method::Offset;
    public const TYPE_CURSOR_AFTER = Method::CursorAfter;
    public const TYPE_CURSOR_BEFORE = Method::CursorBefore;

    // Logical
    public const TYPE_AND = Method::And;
    public const TYPE_OR = Method::Or;
    public const TYPE_ELEM_MATCH = Method::ElemMatch;

    /**
     * Backward compat: array of vector method enums
     * @var array<Method>
     */
    public const VECTOR_TYPES = [
        Method::VectorDot,
        Method::VectorCosine,
        Method::VectorEuclidean,
    ];

    /**
     * Backward compat: array of logical method enums
     * @var array<Method>
     */
    public const LOGICAL_TYPES = [
        Method::And,
        Method::Or,
        Method::ElemMatch,
    ];

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
     * @param array<string, mixed> $query
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
     * @param Document $value
     */
    public static function cursorAfter(mixed $value): static
    {
        return new static(Method::CursorAfter, values: [$value]);
    }

    /**
     * @param Document $value
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
     * Backward compat: array of all supported method enum values
     * @var array<Method>
     */
    public const TYPES = [
        Method::Equal,
        Method::NotEqual,
        Method::LessThan,
        Method::LessThanEqual,
        Method::GreaterThan,
        Method::GreaterThanEqual,
        Method::Contains,
        Method::ContainsAny,
        Method::ContainsAll,
        Method::NotContains,
        Method::Search,
        Method::NotSearch,
        Method::IsNull,
        Method::IsNotNull,
        Method::Between,
        Method::NotBetween,
        Method::StartsWith,
        Method::NotStartsWith,
        Method::EndsWith,
        Method::NotEndsWith,
        Method::Regex,
        Method::Exists,
        Method::NotExists,
        Method::Crosses,
        Method::NotCrosses,
        Method::DistanceEqual,
        Method::DistanceNotEqual,
        Method::DistanceGreaterThan,
        Method::DistanceLessThan,
        Method::Intersects,
        Method::NotIntersects,
        Method::Overlaps,
        Method::NotOverlaps,
        Method::Touches,
        Method::NotTouches,
        Method::Covers,
        Method::NotCovers,
        Method::SpatialEquals,
        Method::NotSpatialEquals,
        Method::VectorDot,
        Method::VectorCosine,
        Method::VectorEuclidean,
        Method::Select,
        Method::OrderAsc,
        Method::OrderDesc,
        Method::OrderRandom,
        Method::Limit,
        Method::Offset,
        Method::CursorAfter,
        Method::CursorBefore,
        Method::And,
        Method::Or,
        Method::ElemMatch,
    ];

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $array = ['method' => $this->method->value];

        if (!empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if (\in_array($this->method, static::LOGICAL_TYPES)) {
            foreach ($this->values as $index => $value) {
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
     * @param array<Query> $queries
     * @return array{
     *     filters: array<Query>,
     *     selections: array<Query>,
     *     limit: int|null,
     *     offset: int|null,
     *     orderAttributes: array<string>,
     *     orderTypes: array<string>,
     *     cursor: Document|null,
     *     cursorDirection: string|null
     * }
     */
    public static function groupForDatabase(array $queries): array
    {
        $grouped = parent::groupByType($queries);

        // Convert OrderDirection enums back to Database string constants
        $orderTypes = [];
        foreach ($grouped->orderTypes as $dir) {
            $orderTypes[] = match ($dir) {
                QueryOrderDirection::Asc => DatabaseOrderDirection::ASC->value,
                QueryOrderDirection::Desc => DatabaseOrderDirection::DESC->value,
                QueryOrderDirection::Random => DatabaseOrderDirection::RANDOM->value,
            };
        }

        // Convert CursorDirection enum back to string
        $cursorDirection = null;
        if ($grouped->cursorDirection !== null) {
            $cursorDirection = match ($grouped->cursorDirection) {
                QueryCursorDirection::After => DatabaseCursorDirection::After->value,
                QueryCursorDirection::Before => DatabaseCursorDirection::Before->value,
            };
        }

        /** @var array<Query> $filters */
        $filters = $grouped->filters;
        /** @var array<Query> $selections */
        $selections = $grouped->selections;

        return [
            'filters' => $filters,
            'selections' => $selections,
            'limit' => $grouped->limit,
            'offset' => $grouped->offset,
            'orderAttributes' => $grouped->orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $grouped->cursor,
            'cursorDirection' => $cursorDirection,
        ];
    }

    /**
     * @return bool
     */
    public function isSpatialAttribute(): bool
    {
        return in_array($this->attributeType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value]);
    }

    /**
     * @return bool
     */
    public function isObjectAttribute(): bool
    {
        return $this->attributeType === ColumnType::Object->value;
    }
}
