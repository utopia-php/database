<?php

namespace Utopia\Database;

use JsonException;
use Utopia\Database\Exception\Query as QueryException;

class Query
{
    // Filter methods
    public const TYPE_EQUAL = 'equal';
    public const TYPE_NOT_EQUAL = 'notEqual';
    public const TYPE_LESSER = 'lessThan';
    public const TYPE_LESSER_EQUAL = 'lessThanEqual';
    public const TYPE_GREATER = 'greaterThan';
    public const TYPE_GREATER_EQUAL = 'greaterThanEqual';
    public const TYPE_CONTAINS = 'contains';
    public const TYPE_NOT_CONTAINS = 'notContains';
    public const TYPE_SEARCH = 'search';
    public const TYPE_NOT_SEARCH = 'notSearch';
    public const TYPE_IS_NULL = 'isNull';
    public const TYPE_IS_NOT_NULL = 'isNotNull';
    public const TYPE_BETWEEN = 'between';
    public const TYPE_NOT_BETWEEN = 'notBetween';
    public const TYPE_STARTS_WITH = 'startsWith';
    public const TYPE_NOT_STARTS_WITH = 'notStartsWith';
    public const TYPE_ENDS_WITH = 'endsWith';
    public const TYPE_NOT_ENDS_WITH = 'notEndsWith';
    public const TYPE_REGEX = 'regex';
    public const TYPE_EXISTS = 'exists';
    public const TYPE_NOT_EXISTS = 'notExists';

    // Spatial methods
    public const TYPE_CROSSES = 'crosses';
    public const TYPE_NOT_CROSSES = 'notCrosses';
    public const TYPE_DISTANCE_EQUAL = 'distanceEqual';
    public const TYPE_DISTANCE_NOT_EQUAL = 'distanceNotEqual';
    public const TYPE_DISTANCE_GREATER_THAN = 'distanceGreaterThan';
    public const TYPE_DISTANCE_LESS_THAN = 'distanceLessThan';
    public const TYPE_INTERSECTS = 'intersects';
    public const TYPE_NOT_INTERSECTS = 'notIntersects';
    public const TYPE_OVERLAPS = 'overlaps';
    public const TYPE_NOT_OVERLAPS = 'notOverlaps';
    public const TYPE_TOUCHES = 'touches';
    public const TYPE_NOT_TOUCHES = 'notTouches';

    // Joins query methods
    public const TYPE_RELATION_EQUAL = 'relationEqual';

    // Vector query methods
    public const TYPE_VECTOR_DOT = 'vectorDot';
    public const TYPE_VECTOR_COSINE = 'vectorCosine';
    public const TYPE_VECTOR_EUCLIDEAN = 'vectorEuclidean';

    public const TYPE_SELECT = 'select';

    // Order methods
    public const TYPE_ORDER_DESC = 'orderDesc';
    public const TYPE_ORDER_ASC = 'orderAsc';
    public const TYPE_ORDER_RANDOM = 'orderRandom';

    // Pagination methods
    public const TYPE_LIMIT = 'limit';
    public const TYPE_OFFSET = 'offset';
    public const TYPE_CURSOR_AFTER = 'cursorAfter';
    public const TYPE_CURSOR_BEFORE = 'cursorBefore';

    // Logical methods
    public const TYPE_AND = 'and';
    public const TYPE_OR = 'or';
    public const TYPE_ELEM_MATCH = 'elemMatch';
    public const DEFAULT_ALIAS = 'main';

    // Join methods
    public const TYPE_INNER_JOIN = 'innerJoin';
    public const TYPE_LEFT_JOIN = 'leftJoin';
    public const TYPE_RIGHT_JOIN = 'rightJoin';

    public const TYPES = [
        self::TYPE_EQUAL,
        self::TYPE_NOT_EQUAL,
        self::TYPE_LESSER,
        self::TYPE_LESSER_EQUAL,
        self::TYPE_GREATER,
        self::TYPE_GREATER_EQUAL,
        self::TYPE_CONTAINS,
        self::TYPE_NOT_CONTAINS,
        self::TYPE_SEARCH,
        self::TYPE_NOT_SEARCH,
        self::TYPE_IS_NULL,
        self::TYPE_IS_NOT_NULL,
        self::TYPE_BETWEEN,
        self::TYPE_NOT_BETWEEN,
        self::TYPE_STARTS_WITH,
        self::TYPE_NOT_STARTS_WITH,
        self::TYPE_ENDS_WITH,
        self::TYPE_NOT_ENDS_WITH,
        self::TYPE_CROSSES,
        self::TYPE_NOT_CROSSES,
        self::TYPE_DISTANCE_EQUAL,
        self::TYPE_DISTANCE_NOT_EQUAL,
        self::TYPE_DISTANCE_GREATER_THAN,
        self::TYPE_DISTANCE_LESS_THAN,
        self::TYPE_INTERSECTS,
        self::TYPE_NOT_INTERSECTS,
        self::TYPE_OVERLAPS,
        self::TYPE_NOT_OVERLAPS,
        self::TYPE_TOUCHES,
        self::TYPE_NOT_TOUCHES,
        self::TYPE_VECTOR_DOT,
        self::TYPE_VECTOR_COSINE,
        self::TYPE_VECTOR_EUCLIDEAN,
        self::TYPE_EXISTS,
        self::TYPE_NOT_EXISTS,
        self::TYPE_SELECT,
        self::TYPE_ORDER_DESC,
        self::TYPE_ORDER_ASC,
        self::TYPE_ORDER_RANDOM,
        self::TYPE_LIMIT,
        self::TYPE_OFFSET,
        self::TYPE_CURSOR_AFTER,
        self::TYPE_CURSOR_BEFORE,
        self::TYPE_AND,
        self::TYPE_OR,
        self::TYPE_ELEM_MATCH,
        self::TYPE_REGEX
    ];

    public const VECTOR_TYPES = [
        self::TYPE_VECTOR_DOT,
        self::TYPE_VECTOR_COSINE,
        self::TYPE_VECTOR_EUCLIDEAN,
    ];

    protected const LOGICAL_TYPES = [
        self::TYPE_AND,
        self::TYPE_OR,
        self::TYPE_ELEM_MATCH,
    ];

    protected const JOINS_TYPES = [
        self::TYPE_INNER_JOIN,
        self::TYPE_LEFT_JOIN,
        self::TYPE_RIGHT_JOIN,
    ];

    protected const FILTER_TYPES = [
        self::TYPE_EQUAL,
        self::TYPE_NOT_EQUAL,
        self::TYPE_LESSER,
        self::TYPE_LESSER_EQUAL,
        self::TYPE_GREATER,
        self::TYPE_GREATER_EQUAL,
        self::TYPE_CONTAINS,
        self::TYPE_NOT_CONTAINS,
        self::TYPE_SEARCH,
        self::TYPE_NOT_SEARCH,
        self::TYPE_IS_NULL,
        self::TYPE_IS_NOT_NULL,
        self::TYPE_BETWEEN,
        self::TYPE_NOT_BETWEEN,
        self::TYPE_STARTS_WITH,
        self::TYPE_NOT_STARTS_WITH,
        self::TYPE_ENDS_WITH,
        self::TYPE_NOT_ENDS_WITH,
        self::TYPE_CROSSES,
        self::TYPE_NOT_CROSSES,
        self::TYPE_DISTANCE_EQUAL,
        self::TYPE_DISTANCE_NOT_EQUAL,
        self::TYPE_DISTANCE_GREATER_THAN,
        self::TYPE_DISTANCE_LESS_THAN,
        self::TYPE_INTERSECTS,
        self::TYPE_NOT_INTERSECTS,
        self::TYPE_OVERLAPS,
        self::TYPE_NOT_OVERLAPS,
        self::TYPE_TOUCHES,
        self::TYPE_NOT_TOUCHES,
        self::TYPE_AND,
        self::TYPE_OR,
        self::TYPE_RELATION_EQUAL,
        self::TYPE_REGEX,
        self::TYPE_EXISTS,
        self::TYPE_NOT_EXISTS,
        Query::TYPE_ELEM_MATCH,
    ];

    protected string $method = '';
    protected string $collection = '';
    protected string $alias = '';
    protected string $attribute = '';
    protected string $attributeType = '';
    protected string $aliasRight = '';
    protected string $attributeRight = '';
    protected string $as = '';
    protected bool $system = false;
    protected bool $onArray = false;

    /**
     * @var array<mixed>
     */
    protected array $values = [];

    /**
     * Construct a new query object
     *
     * @param string $method
     * @param string $attribute
     * @param array<mixed> $values
     */
    protected function __construct(
        string $method,
        string $attribute = '',
        array $values = [],
        string $alias = '',
        string $attributeRight = '',
        string $aliasRight = '',
        string $collection = '',
        string $as = '',
        bool $system = false,
    ) {
        if ($attribute === '' && \in_array($method, [Query::TYPE_ORDER_ASC, Query::TYPE_ORDER_DESC])) {
            $attribute = '$sequence';
        }

        /**
         * We can not make the fallback in the Query::static() calls , because parse method skips it
         */
        if (empty($alias)) {
            $alias = Query::DEFAULT_ALIAS;
        }

        if (empty($aliasRight)) {
            $aliasRight = Query::DEFAULT_ALIAS;
        }

        $this->method = $method;
        $this->alias = $alias;
        $this->attribute = $attribute;
        $this->values = $values;
        $this->aliasRight = $aliasRight;
        $this->attributeRight = $attributeRight;
        $this->collection = $collection;
        $this->as = $as;
        $this->system = $system;
    }

    public function __clone(): void
    {
        foreach ($this->values as $index => $value) {
            if ($value instanceof self) {
                $this->values[$index] = clone $value;
            }
        }
    }

    /**
     * @return string
     */
    public function getMethod(): string
    {
        return $this->method;
    }

    /**
     * @return string
     */
    public function getAttribute(): string
    {
        return $this->attribute;
    }

    /**
     * @return array<mixed>
     */
    public function getValues(): array
    {
        return $this->values;
    }

    /**
     * @param mixed $default
     * @return mixed
     */
    public function getValue(mixed $default = null): mixed
    {
        return $this->values[0] ?? $default;
    }

    public function getAlias(): string
    {
        return $this->alias;
    }

    public function getRightAlias(): string
    {
        return $this->aliasRight;
    }

    public function getAttributeRight(): string
    {
        return $this->attributeRight;
    }

    public function getAs(): string
    {
        return $this->as;
    }

    public function getCollection(): string
    {
        return $this->collection;
    }

    /**
     * Sets method
     *
     * @param string $method
     * @return self
     */
    public function setMethod(string $method): self
    {
        $this->method = $method;

        return $this;
    }

    /**
     * Sets attribute
     *
     * @param string $attribute
     * @return self
     */
    public function setAttribute(string $attribute): self
    {
        $this->attribute = $attribute;

        return $this;
    }

    /**
     * Sets right attribute
     */
    public function setAttributeRight(string $attribute): self
    {
        $this->attributeRight = $attribute;

        return $this;
    }

    public function getCursorDirection(): string
    {
        if ($this->method === self::TYPE_CURSOR_AFTER) {
            return Database::CURSOR_AFTER;
        }

        if ($this->method === self::TYPE_CURSOR_BEFORE) {
            return Database::CURSOR_BEFORE;
        }

        throw new \Exception('Invalid method: Get cursor direction on "'.$this->method.'" Query');
    }

    public function getOrderDirection(): string
    {
        if ($this->method === self::TYPE_ORDER_ASC) {
            return Database::ORDER_ASC;
        }

        if ($this->method === self::TYPE_ORDER_DESC) {
            return Database::ORDER_DESC;
        }

        if ($this->method === self::TYPE_ORDER_RANDOM) {
            return Database::ORDER_RANDOM;
        }

        throw new \Exception('Invalid method: Get order direction on "'.$this->method.'" Query');
    }
    /**
     * Sets values
     *
     * @param array<mixed> $values
     * @return self
     */
    public function setValues(array $values): self
    {
        $this->values = $values;

        return $this;
    }

    /**
     * Sets value
     * @param mixed $value
     * @return self
     */
    public function setValue(mixed $value): self
    {
        $this->values = [$value];

        return $this;
    }

    /**
     * Check if method is supported
     *
     * @param string $value
     * @return bool
     */
    public static function isMethod(string $value): bool
    {
        return match ($value) {
            self::TYPE_EQUAL,
            self::TYPE_NOT_EQUAL,
            self::TYPE_LESSER,
            self::TYPE_LESSER_EQUAL,
            self::TYPE_GREATER,
            self::TYPE_GREATER_EQUAL,
            self::TYPE_CONTAINS,
            self::TYPE_NOT_CONTAINS,
            self::TYPE_SEARCH,
            self::TYPE_NOT_SEARCH,
            self::TYPE_ORDER_ASC,
            self::TYPE_ORDER_DESC,
            self::TYPE_ORDER_RANDOM,
            self::TYPE_LIMIT,
            self::TYPE_OFFSET,
            self::TYPE_CURSOR_AFTER,
            self::TYPE_CURSOR_BEFORE,
            self::TYPE_IS_NULL,
            self::TYPE_IS_NOT_NULL,
            self::TYPE_BETWEEN,
            self::TYPE_NOT_BETWEEN,
            self::TYPE_STARTS_WITH,
            self::TYPE_NOT_STARTS_WITH,
            self::TYPE_ENDS_WITH,
            self::TYPE_NOT_ENDS_WITH,
            self::TYPE_CROSSES,
            self::TYPE_NOT_CROSSES,
            self::TYPE_DISTANCE_EQUAL,
            self::TYPE_DISTANCE_NOT_EQUAL,
            self::TYPE_DISTANCE_GREATER_THAN,
            self::TYPE_DISTANCE_LESS_THAN,
            self::TYPE_INTERSECTS,
            self::TYPE_NOT_INTERSECTS,
            self::TYPE_OVERLAPS,
            self::TYPE_NOT_OVERLAPS,
            self::TYPE_TOUCHES,
            self::TYPE_NOT_TOUCHES,
            self::TYPE_OR,
            self::TYPE_AND,
            self::TYPE_ELEM_MATCH,
            self::TYPE_SELECT,
            self::TYPE_RELATION_EQUAL,
            self::TYPE_INNER_JOIN,
            self::TYPE_LEFT_JOIN,
            self::TYPE_RIGHT_JOIN,
            self::TYPE_VECTOR_DOT,
            self::TYPE_VECTOR_COSINE,
            self::TYPE_VECTOR_EUCLIDEAN,
            self::TYPE_EXISTS,
            self::TYPE_NOT_EXISTS => true,
            default => false,
        };
    }

    /**
     * @param string $method
     * @return bool
     */
    public static function isSpatialQuery(string $method): bool
    {
        return match ($method) {
            self::TYPE_CROSSES,
            self::TYPE_NOT_CROSSES,
            self::TYPE_DISTANCE_EQUAL,
            self::TYPE_DISTANCE_NOT_EQUAL,
            self::TYPE_DISTANCE_GREATER_THAN,
            self::TYPE_DISTANCE_LESS_THAN,
            self::TYPE_INTERSECTS,
            self::TYPE_NOT_INTERSECTS,
            self::TYPE_OVERLAPS,
            self::TYPE_NOT_OVERLAPS,
            self::TYPE_TOUCHES,
            self::TYPE_NOT_TOUCHES => true,
            default => false,
        };
    }

    /**
     * @param string $method
     * @return bool
     */
    public static function isVectorQuery(string $method): bool
    {
        return \in_array($method, Query::VECTOR_TYPES);
    }

    /**
     * Parse query
     *
     * @param string $query
     * @return self
     * @throws QueryException
     */
    public static function parse(string $query): self
    {
        try {
            $query = \json_decode($query, true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new QueryException('Invalid query: ' . $e->getMessage());
        }

        if (!\is_array($query)) {
            throw new QueryException('Invalid query. Must be an array, got ' . \gettype($query));
        }

        return self::parseQuery($query);
    }

    /**
     * Parse query
     *
     * @param array<string, mixed> $query
     * @return self
     * @throws QueryException
     */
    public static function parseQuery(array $query): self
    {
        $method = $query['method'] ?? '';
        $attribute = $query['attribute'] ?? '';
        $attributeRight = $query['attributeRight'] ?? '';
        $values = $query['values'] ?? [];
        $alias = $query['alias'] ?? '';
        $aliasRight = $query['aliasRight'] ?? '';
        $as = $query['as'] ?? '';
        $collection = $query['collection'] ?? '';

        if (!\is_string($method)) {
            throw new QueryException('Invalid query method. Must be a string, got ' . \gettype($method));
        }

        if (!self::isMethod($method)) {
            throw new QueryException('Invalid query method: ' . $method);
        }

        if (!\is_string($attribute)) {
            throw new QueryException('Invalid query attribute. Must be a string, got ' . \gettype($attribute));
        }

        if (!\is_array($values)) {
            throw new QueryException('Invalid query values. Must be an array, got ' . \gettype($values));
        }

        if (\in_array($method, self::LOGICAL_TYPES) || \in_array($method, self::JOINS_TYPES)) {
            foreach ($values as $index => $value) {
                $values[$index] = self::parseQuery($value);
            }
        }

        return new self(
            $method,
            $attribute,
            $values,
            alias: $alias,
            attributeRight: $attributeRight,
            aliasRight: $aliasRight,
            collection: $collection,
            as: $as,
        );
    }

    /**
     * Parse an array of queries
     *
     * @param array<string> $queries
     *
     * @return array<Query>
     * @throws QueryException
     */
    public static function parseQueries(array $queries): array
    {
        $parsed = [];

        foreach ($queries as $query) {
            $parsed[] = Query::parse($query);
        }

        return $parsed;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $array = ['method' => $this->method];

        if (!empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if (!empty($this->attributeRight)) {
            $array['attributeRight'] = $this->attributeRight;
        }

        if (!empty($this->alias) && $this->alias != Query::DEFAULT_ALIAS) {
            $array['alias'] = $this->alias;
        }

        if (!empty($this->aliasRight) && $this->aliasRight != Query::DEFAULT_ALIAS) {
            $array['aliasRight'] = $this->aliasRight;
        }

        if (!empty($this->as)) {
            $array['as'] = $this->as;
        }

        if (!empty($this->collection)) {
            $array['collection'] = $this->collection;
        }

        if ($this->isNested() || $this->isJoin()) {
            foreach ($this->values as $index => $value) {
                $array['values'][$index] = $value->toArray();
            }
        } else {
            $array['values'] = [];
            foreach ($this->values as $value) {
                if ($value instanceof Document && in_array($this->method, [self::TYPE_CURSOR_AFTER, self::TYPE_CURSOR_BEFORE])) {
                    $value = $value->getId();
                }
                $array['values'][] = $value;
            }
        }

        return $array;
    }

    /**
     * @return string
     * @throws QueryException
     */
    public function toString(): string
    {
        try {
            return \json_encode($this->toArray(), flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new QueryException('Invalid Json: ' . $e->getMessage());
        }
    }

    /**
     * Helper method to create Query with equal method
     *
     * @param string $attribute
     * @param array<string|int|float|bool|array<mixed,mixed>> $values
     * @return Query
     */
    public static function equal(string $attribute, array $values, string $alias = ''): self
    {
        return new self(self::TYPE_EQUAL, $attribute, $values, alias: $alias);
    }

    /**
     * Helper method to create Query with notEqual method
     *
     * @param string $attribute
     * @param string|int|float|bool|array<mixed,mixed> $value
     * @return Query
     */
    public static function notEqual(string $attribute, string|int|float|bool|array $value, string $alias = ''): self
    {
        // maps or not an array
        if ((is_array($value) && !array_is_list($value)) || !is_array($value)) {
            $value = [$value];
        }
        return new self(self::TYPE_NOT_EQUAL, $attribute, $value, alias: $alias);
    }

    /**
     * Helper method to create Query with lessThan method
     *
     * @param string $attribute
     * @param string|int|float|bool $value
     * @return Query
     */
    public static function lessThan(string $attribute, string|int|float|bool $value, string $alias = ''): self
    {
        return new self(self::TYPE_LESSER, $attribute, [$value], alias: $alias);
    }

    /**
     * Helper method to create Query with lessThanEqual method
     *
     * @param string $attribute
     * @param string|int|float|bool $value
     * @return Query
     */
    public static function lessThanEqual(string $attribute, string|int|float|bool $value, string $alias = ''): self
    {
        return new self(self::TYPE_LESSER_EQUAL, $attribute, [$value], alias: $alias);
    }

    /**
     * Helper method to create Query with greaterThan method
     *
     * @param string $attribute
     * @param string|int|float|bool $value
     * @return Query
     */
    public static function greaterThan(string $attribute, string|int|float|bool $value, string $alias = ''): self
    {
        return new self(self::TYPE_GREATER, $attribute, [$value], alias: $alias);
    }

    /**
     * Helper method to create Query with greaterThanEqual method
     *
     * @param string $attribute
     * @param string|int|float|bool $value
     * @return Query
     */
    public static function greaterThanEqual(string $attribute, string|int|float|bool $value, string $alias = ''): self
    {
        return new self(self::TYPE_GREATER_EQUAL, $attribute, [$value], alias: $alias);
    }

    /**
     * Helper method to create Query with contains method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function contains(string $attribute, array $values): self
    {
        return new self(self::TYPE_CONTAINS, $attribute, $values);
    }

    /**
     * Helper method to create Query with notContains method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function notContains(string $attribute, array $values): self
    {
        return new self(self::TYPE_NOT_CONTAINS, $attribute, $values);
    }

    /**
     * Helper method to create Query with between method
     *
     * @param string $attribute
     * @param string|int|float|bool $start
     * @param string|int|float|bool $end
     * @return Query
     */
    public static function between(string $attribute, string|int|float|bool $start, string|int|float|bool $end, string $alias = ''): self
    {
        return new self(self::TYPE_BETWEEN, $attribute, [$start, $end], alias: $alias);
    }

    /**
     * Helper method to create Query with notBetween method
     *
     * @param string $attribute
     * @param string|int|float|bool $start
     * @param string|int|float|bool $end
     * @return Query
     */
    public static function notBetween(string $attribute, string|int|float|bool $start, string|int|float|bool $end): self
    {
        return new self(self::TYPE_NOT_BETWEEN, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with search method
     *
     * @param string $attribute
     * @param string $value
     * @return Query
     */
    public static function search(string $attribute, string $value): self
    {
        return new self(self::TYPE_SEARCH, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with notSearch method
     *
     * @param string $attribute
     * @param string $value
     * @return Query
     */
    public static function notSearch(string $attribute, string $value): self
    {
        return new self(self::TYPE_NOT_SEARCH, $attribute, [$value]);
    }

    public static function select(string $attribute, string $alias = '', string $as = '', bool $system = false): self
    {
        return new self(self::TYPE_SELECT, $attribute, [], alias: $alias, as: $as, system: $system);
    }

    /**
     * Helper method to create Query with orderDesc method
     *
     * @param string $attribute
     * @return Query
     */
    public static function orderDesc(string $attribute = '', string $alias = ''): self
    {
        return new self(self::TYPE_ORDER_DESC, $attribute, alias: $alias);
    }

    /**
     * Helper method to create Query with orderAsc method
     *
     * @param string $attribute
     * @return Query
     */
    public static function orderAsc(string $attribute = '', string $alias = ''): self
    {
        return new self(self::TYPE_ORDER_ASC, $attribute, alias: $alias);
    }

    /**
     * Helper method to create Query with orderRandom method
     *
     * @return Query
     */
    public static function orderRandom(): self
    {
        return new self(self::TYPE_ORDER_RANDOM);
    }

    /**
     * Helper method to create Query with limit method
     *
     * @param int $value
     * @return Query
     */
    public static function limit(int $value): self
    {
        return new self(self::TYPE_LIMIT, values: [$value]);
    }

    /**
     * Helper method to create Query with offset method
     *
     * @param int $value
     * @return Query
     */
    public static function offset(int $value): self
    {
        return new self(self::TYPE_OFFSET, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorAfter method
     *
     * @param Document $value
     * @return Query
     */
    public static function cursorAfter(Document $value): self
    {
        return new self(self::TYPE_CURSOR_AFTER, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorBefore method
     *
     * @param Document $value
     * @return Query
     */
    public static function cursorBefore(Document $value): self
    {
        return new self(self::TYPE_CURSOR_BEFORE, values: [$value]);
    }

    /**
     * Helper method to create Query with isNull method
     *
     * @param string $attribute
     * @return Query
     */
    public static function isNull(string $attribute): self
    {
        return new self(self::TYPE_IS_NULL, $attribute);
    }

    /**
     * Helper method to create Query with isNotNull method
     *
     * @param string $attribute
     * @return Query
     */
    public static function isNotNull(string $attribute): self
    {
        return new self(self::TYPE_IS_NOT_NULL, $attribute);
    }

    public static function startsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_STARTS_WITH, $attribute, [$value]);
    }

    public static function notStartsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_NOT_STARTS_WITH, $attribute, [$value]);
    }

    public static function endsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_ENDS_WITH, $attribute, [$value]);
    }

    public static function notEndsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_NOT_ENDS_WITH, $attribute, [$value]);
    }

    /**
     * Helper method to create Query for documents created before a specific date
     *
     * @param string $value
     * @return Query
     */
    public static function createdBefore(string $value): self
    {
        return self::lessThan('$createdAt', $value);
    }

    /**
     * Helper method to create Query for documents created after a specific date
     *
     * @param string $value
     * @return Query
     */
    public static function createdAfter(string $value): self
    {
        return self::greaterThan('$createdAt', $value);
    }

    /**
     * Helper method to create Query for documents updated before a specific date
     *
     * @param string $value
     * @return Query
     */
    public static function updatedBefore(string $value): self
    {
        return self::lessThan('$updatedAt', $value);
    }

    /**
     * Helper method to create Query for documents updated after a specific date
     *
     * @param string $value
     * @return Query
     */
    public static function updatedAfter(string $value): self
    {
        return self::greaterThan('$updatedAt', $value);
    }

    /**
     * Helper method to create Query for documents created between two dates
     *
     * @param string $start
     * @param string $end
     * @return Query
     */
    public static function createdBetween(string $start, string $end): self
    {
        return self::between('$createdAt', $start, $end);
    }

    /**
     * Helper method to create Query for documents updated between two dates
     *
     * @param string $start
     * @param string $end
     * @return Query
     */
    public static function updatedBetween(string $start, string $end): self
    {
        return self::between('$updatedAt', $start, $end);
    }

    /**
     * @param array<Query> $queries
     * @return Query
     */
    public static function or(array $queries): self
    {
        return new self(self::TYPE_OR, '', $queries);
    }

    /**
     * @param array<Query> $queries
     * @return Query
     */
    public static function and(array $queries): self
    {
        return new self(self::TYPE_AND, '', $queries);
    }

    /**
     * @param string $collection
     * @param string $alias
     * @param array<Query> $queries
     * @return self
     */
    public static function join(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_INNER_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    /**
     * @param string $collection
     * @param string $alias
     * @param array<Query> $queries
     * @return self
     */
    public static function innerJoin(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_INNER_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    /**
     * @param string $collection
     * @param string $alias
     * @param array<Query> $queries
     * @return self
     */
    public static function leftJoin(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_LEFT_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    /**
     * @param string $collection
     * @param string $alias
     * @param array<Query> $queries
     * @return self
     */
    public static function rightJoin(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_RIGHT_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    public static function relationEqual(string $leftAlias, string $leftColumn, string $rightAlias, string $rightColumn): self
    {
        return new self(self::TYPE_RELATION_EQUAL, $leftColumn, [], alias: $leftAlias, attributeRight: $rightColumn, aliasRight: $rightAlias);
    }

    /**
     * Filters $queries for $types
     *
     * @param array<Query> $queries
     * @param array<string> $types
     * @return array<Query>
     */
    protected static function getByType(array $queries, array $types): array
    {
        $filtered = [];

        foreach ($queries as $query) {
            if (\in_array($query->getMethod(), $types, true)) {
                $filtered[] = clone $query;
            }
        }

        return $filtered;
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getSelectQueries(array $queries): array
    {
        return self::getByType($queries, [
            Query::TYPE_SELECT,
        ]);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getJoinQueries(array $queries): array
    {
        return self::getByType($queries, self::JOINS_TYPES);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getLimitQueries(array $queries): array
    {
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_LIMIT) {
                return [clone $query];
            }
        }

        return [];
    }

    /**
     * @param array<Query> $queries
     * @param int|null $default
     * @return int|null
     */
    public static function getLimitQuery(array $queries, ?int $default = null): ?int
    {
        $queries = self::getLimitQueries($queries);

        if (empty($queries)) {
            return $default;
        }

        return $queries[0]->getValue();
    }

    /**
     * @param  array<Query> $queries
     * @return array<Query>
     */
    public static function getOffsetQueries(array $queries): array
    {
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_OFFSET) {
                return [clone $query];
            }
        }

        return [];
    }

    /**
     * @param array<Query> $queries
     * @param int|null $default
     * @return int|null
     */
    public static function getOffsetQuery(array $queries, ?int $default = null): ?int
    {
        $queries = self::getOffsetQueries($queries);

        if (empty($queries)) {
            return $default;
        }

        return $queries[0]->getValue();
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getOrderQueries(array $queries): array
    {
        return self::getByType($queries, [
            Query::TYPE_ORDER_ASC,
            Query::TYPE_ORDER_DESC,
            Query::TYPE_ORDER_RANDOM,
        ]);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getCursorQueries(array $queries): array
    {
        return self::getByType($queries, [
            Query::TYPE_CURSOR_AFTER,
            Query::TYPE_CURSOR_BEFORE,
        ]);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getFilterQueries(array $queries): array
    {
        return self::getByType($queries, self::FILTER_TYPES);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getVectorQueries(array $queries): array
    {
        return self::getByType($queries, self::VECTOR_TYPES);
    }

    /**
     * Iterates through queries are groups them by type
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
    public static function groupByType(array $queries): array
    {
        $filters = [];
        $selections = [];
        $limit = null;
        $offset = null;
        $orderAttributes = [];
        $orderTypes = [];
        $cursor = null;
        $cursorDirection = null;

        foreach ($queries as $query) {
            if (!$query instanceof Query) {
                continue;
            }

            $method = $query->getMethod();
            $attribute = $query->getAttribute();
            $values = $query->getValues();

            switch ($method) {
                case Query::TYPE_ORDER_ASC:
                case Query::TYPE_ORDER_DESC:
                case Query::TYPE_ORDER_RANDOM:
                    if (!empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = match ($method) {
                        Query::TYPE_ORDER_ASC => Database::ORDER_ASC,
                        Query::TYPE_ORDER_DESC => Database::ORDER_DESC,
                        Query::TYPE_ORDER_RANDOM => Database::ORDER_RANDOM,
                    };

                    break;
                case Query::TYPE_LIMIT:
                    // Keep the 1st limit encountered and ignore the rest
                    if ($limit !== null) {
                        break;
                    }

                    $limit = $values[0] ?? $limit;
                    break;
                case Query::TYPE_OFFSET:
                    // Keep the 1st offset encountered and ignore the rest
                    if ($offset !== null) {
                        break;
                    }

                    $offset = $values[0] ?? $limit;
                    break;
                case Query::TYPE_CURSOR_AFTER:
                case Query::TYPE_CURSOR_BEFORE:
                    // Keep the 1st cursor encountered and ignore the rest
                    if ($cursor !== null) {
                        break;
                    }

                    $cursor = $values[0] ?? $limit;
                    $cursorDirection = $method === Query::TYPE_CURSOR_AFTER ? Database::CURSOR_AFTER : Database::CURSOR_BEFORE;
                    break;

                case Query::TYPE_SELECT:
                    $selections[] = clone $query;
                    break;

                default:
                    $filters[] = clone $query;
                    break;
            }
        }

        return [
            'filters' => $filters,
            'selections' => $selections,
            'limit' => $limit,
            'offset' => $offset,
            'orderAttributes' => $orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $cursorDirection,
        ];
    }

    /**
     * Is this query able to contain other queries
     *
     * @return bool
     */
    public function isNested(): bool
    {
        if (in_array($this->getMethod(), self::LOGICAL_TYPES)) {
            return true;
        }

        return false;
    }

    /**
     * Is this query able to contain other queries
     */
    public function isJoin(): bool
    {
        return in_array($this->getMethod(), self::JOINS_TYPES);
    }

    public static function isFilter(string $method): bool
    {
        return in_array($method, self::FILTER_TYPES);
    }

    public function onArray(): bool
    {
        return $this->onArray;
    }

    /**
     * @param bool $bool
     * @return void
     */
    public function setOnArray(bool $bool): void
    {
        $this->onArray = $bool;
    }

    /**
     * @param string $type
     * @return void
     */
    public function setAttributeType(string $type): void
    {
        $this->attributeType = $type;
    }

    /**
     * @return string
     */
    public function getAttributeType(): string
    {
        return $this->attributeType;
    }
    /**
     * @return bool
     */
    public function isSpatialAttribute(): bool
    {
        return in_array($this->attributeType, Database::SPATIAL_TYPES);
    }

    /**
     * @return bool
     */
    public function isObjectAttribute(): bool
    {
        return $this->attributeType === Database::VAR_OBJECT;
    }

    // Spatial query methods

    /**
     * Helper method to create Query with distanceEqual method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @param int|float $distance
     * @param bool $meters
     * @return Query
     */
    public static function distanceEqual(string $attribute, array $values, int|float $distance, bool $meters = false): self
    {
        return new self(self::TYPE_DISTANCE_EQUAL, $attribute, [[$values,$distance,$meters]]);
    }

    /**
     * Helper method to create Query with distanceNotEqual method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @param int|float $distance
     * @param bool $meters
     * @return Query
     */
    public static function distanceNotEqual(string $attribute, array $values, int|float $distance, bool $meters = false): self
    {
        return new self(self::TYPE_DISTANCE_NOT_EQUAL, $attribute, [[$values,$distance,$meters]]);
    }

    /**
     * Helper method to create Query with distanceGreaterThan method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @param int|float $distance
     * @param bool $meters
     * @return Query
     */
    public static function distanceGreaterThan(string $attribute, array $values, int|float $distance, bool $meters = false): self
    {
        return new self(self::TYPE_DISTANCE_GREATER_THAN, $attribute, [[$values,$distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceLessThan method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @param int|float $distance
     * @param bool $meters
     * @return Query
     */
    public static function distanceLessThan(string $attribute, array $values, int|float $distance, bool $meters = false): self
    {
        return new self(self::TYPE_DISTANCE_LESS_THAN, $attribute, [[$values,$distance,$meters]]);
    }

    /**
     * Helper method to create Query with intersects method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function intersects(string $attribute, array $values): self
    {
        return new self(self::TYPE_INTERSECTS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notIntersects method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function notIntersects(string $attribute, array $values): self
    {
        return new self(self::TYPE_NOT_INTERSECTS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with crosses method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function crosses(string $attribute, array $values): self
    {
        return new self(self::TYPE_CROSSES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notCrosses method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function notCrosses(string $attribute, array $values): self
    {
        return new self(self::TYPE_NOT_CROSSES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with overlaps method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function overlaps(string $attribute, array $values): self
    {
        return new self(self::TYPE_OVERLAPS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notOverlaps method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function notOverlaps(string $attribute, array $values): self
    {
        return new self(self::TYPE_NOT_OVERLAPS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with touches method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function touches(string $attribute, array $values): self
    {
        return new self(self::TYPE_TOUCHES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notTouches method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function notTouches(string $attribute, array $values): self
    {
        return new self(self::TYPE_NOT_TOUCHES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with vectorDot method
     *
     * @param string $attribute
     * @param array<float> $vector
     * @return Query
     */
    public static function vectorDot(string $attribute, array $vector): self
    {
        return new self(self::TYPE_VECTOR_DOT, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with vectorCosine method
     *
     * @param string $attribute
     * @param array<float> $vector
     * @return Query
     */
    public static function vectorCosine(string $attribute, array $vector): self
    {
        return new self(self::TYPE_VECTOR_COSINE, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with vectorEuclidean method
     *
     * @param string $attribute
     * @param array<float> $vector
     * @return Query
     */
    public static function vectorEuclidean(string $attribute, array $vector): self
    {
        return new self(self::TYPE_VECTOR_EUCLIDEAN, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with regex method
     *
     * @param string $attribute
     * @param string $pattern
     * @return Query
     */
    public static function regex(string $attribute, string $pattern): self
    {
        return new self(self::TYPE_REGEX, $attribute, [$pattern]);
    }

    /**
     * Helper method to create Query with exists method
     *
     * @param array<string> $attributes
     * @return Query
     */
    public static function exists(array $attributes): self
    {
        return new self(self::TYPE_EXISTS, '', $attributes);
    }

    /**
     * Helper method to create Query with notExists method
     *
     * @param string|int|float|bool|array<mixed,mixed> $attribute
     * @return Query
     */
    public static function notExists(string|int|float|bool|array $attribute): self
    {
        return new self(self::TYPE_NOT_EXISTS, '', is_array($attribute) ? $attribute : [$attribute]);
    }

    /**
     * @param string $attribute
     * @param array<Query> $queries
     * @return Query
     */
    public static function elemMatch(string $attribute, array $queries): self
    {
        return new self(self::TYPE_ELEM_MATCH, $attribute, $queries);
    }
}
