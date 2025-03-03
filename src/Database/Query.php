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

    public const TYPE_SEARCH = 'search';

    public const TYPE_IS_NULL = 'isNull';

    public const TYPE_IS_NOT_NULL = 'isNotNull';

    public const TYPE_BETWEEN = 'between';

    public const TYPE_STARTS_WITH = 'startsWith';

    public const TYPE_ENDS_WITH = 'endsWith';

    public const TYPE_RELATION_EQUAL = 'relationEqual';

    public const TYPE_SELECT = 'select';

    public const TYPE_SELECTION = 'selection';

    // Order methods
    public const TYPE_ORDER_DESC = 'orderDesc';

    public const TYPE_ORDER_ASC = 'orderAsc';

    // Pagination methods
    public const TYPE_LIMIT = 'limit';

    public const TYPE_OFFSET = 'offset';

    public const TYPE_CURSOR_AFTER = 'cursorAfter';

    public const TYPE_CURSOR_BEFORE = 'cursorBefore';

    // Logical methods
    public const TYPE_AND = 'and';

    public const TYPE_OR = 'or';

    // Join methods
    public const TYPE_INNER_JOIN = 'innerJoin';

    public const TYPE_LEFT_JOIN = 'leftJoin';

    public const TYPE_RIGHT_JOIN = 'rightJoin';

    public const DEFAULT_ALIAS = 'A';

    public const TYPES = [
        self::TYPE_EQUAL,
        self::TYPE_NOT_EQUAL,
        self::TYPE_LESSER,
        self::TYPE_LESSER_EQUAL,
        self::TYPE_GREATER,
        self::TYPE_GREATER_EQUAL,
        self::TYPE_CONTAINS,
        self::TYPE_SEARCH,
        self::TYPE_IS_NULL,
        self::TYPE_IS_NOT_NULL,
        self::TYPE_BETWEEN,
        self::TYPE_STARTS_WITH,
        self::TYPE_ENDS_WITH,
        self::TYPE_SELECT,
        self::TYPE_ORDER_DESC,
        self::TYPE_ORDER_ASC,
        self::TYPE_LIMIT,
        self::TYPE_OFFSET,
        self::TYPE_CURSOR_AFTER,
        self::TYPE_CURSOR_BEFORE,
        self::TYPE_AND,
        self::TYPE_OR,
    ];

    protected const LOGICAL_TYPES = [
        self::TYPE_AND,
        self::TYPE_OR,
    ];

    protected string $method = '';

    protected string $collection = '';

    protected string $alias = '';

    protected string $attribute = '';

    protected string $aliasRight = '';

    protected string $attributeRight = '';

    protected string $as = '';

    protected bool $onArray = false;

    /**
     * @var array<mixed>
     */
    protected array $values = [];

    /**
     * Construct a new query object
     *
     * @param  array<mixed>  $values
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
    ) {
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
    }

    public function __clone(): void
    {
        foreach ($this->values as $index => $value) {
            if ($value instanceof self) {
                $this->values[$index] = clone $value;
            }
        }
    }

    public function getMethod(): string
    {
        return $this->method;
    }

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

    public function getCollection(): string
    {
        return $this->collection;
    }

    /**
     * Sets method
     */
    public function setMethod(string $method): self
    {
        $this->method = $method;

        return $this;
    }

    /**
     * Sets attribute
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
        } elseif ($this->method === self::TYPE_CURSOR_BEFORE) {
            return Database::CURSOR_BEFORE;
        }

        return '';
    }

    /**
     * Sets values
     *
     * @param  array<mixed>  $values
     */
    public function setValues(array $values): self
    {
        $this->values = $values;

        return $this;
    }

    /**
     * Sets value
     */
    public function setValue(mixed $value): self
    {
        $this->values = [$value];

        return $this;
    }

    /**
     * Check if method is supported
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
            self::TYPE_SEARCH,
            self::TYPE_ORDER_ASC,
            self::TYPE_ORDER_DESC,
            self::TYPE_LIMIT,
            self::TYPE_OFFSET,
            self::TYPE_CURSOR_AFTER,
            self::TYPE_CURSOR_BEFORE,
            self::TYPE_IS_NULL,
            self::TYPE_IS_NOT_NULL,
            self::TYPE_BETWEEN,
            self::TYPE_STARTS_WITH,
            self::TYPE_ENDS_WITH,
            self::TYPE_OR,
            self::TYPE_AND,
            self::TYPE_SELECT => true,
            default => false,
        };
    }

    /**
     * Parse query
     *
     * @throws QueryException
     */
    public static function parse(string $query): self
    {
        try {
            $query = \json_decode($query, true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new QueryException('Invalid query: '.$e->getMessage());
        }

        if (! \is_array($query)) {
            throw new QueryException('Invalid query. Must be an array, got '.\gettype($query));
        }

        return self::parseQuery($query);
    }

    /**
     * Parse query
     *
     * @param  array<string, mixed>  $query
     *
     * @throws QueryException
     */
    public static function parseQuery(array $query): self
    {
        $method = $query['method'] ?? '';
        $attribute = $query['attribute'] ?? '';
        $values = $query['values'] ?? [];

        if (! \is_string($method)) {
            throw new QueryException('Invalid query method. Must be a string, got '.\gettype($method));
        }

        if (! self::isMethod($method)) {
            throw new QueryException('Invalid query method: '.$method);
        }

        if (! \is_string($attribute)) {
            throw new QueryException('Invalid query attribute. Must be a string, got '.\gettype($attribute));
        }

        if (! \is_array($values)) {
            throw new QueryException('Invalid query values. Must be an array, got '.\gettype($values));
        }

        if (\in_array($method, self::LOGICAL_TYPES)) {
            foreach ($values as $index => $value) {
                $values[$index] = self::parseQuery($value);
            }
        }

        return new self($method, $attribute, $values);
    }

    /**
     * Parse an array of queries
     *
     * @param  array<string>  $queries
     * @return array<Query>
     *
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

        if (! empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if (\in_array($array['method'], self::LOGICAL_TYPES)) {
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
     * @throws QueryException
     */
    public function toString(): string
    {
        try {
            return \json_encode($this->toArray(), flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new QueryException('Invalid Json: '.$e->getMessage());
        }
    }

    /**
     * Helper method to create Query with equal method
     *
     * @param  array<string|int|float|bool>  $values
     */
    public static function equal(string $attribute, array $values, string $alias = Query::DEFAULT_ALIAS): self
    {
        return new self(self::TYPE_EQUAL, $attribute, $values, alias: $alias);
    }

    /**
     * Helper method to create Query with notEqual method
     */
    public static function notEqual(string $attribute, string|int|float|bool $value): self
    {
        return new self(self::TYPE_NOT_EQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with lessThan method
     */
    public static function lessThan(string $attribute, string|int|float|bool $value): self
    {
        return new self(self::TYPE_LESSER, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with lessThanEqual method
     */
    public static function lessThanEqual(string $attribute, string|int|float|bool $value): self
    {
        return new self(self::TYPE_LESSER_EQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThan method
     */
    public static function greaterThan(string $attribute, string|int|float|bool $value): self
    {
        return new self(self::TYPE_GREATER, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThanEqual method
     */
    public static function greaterThanEqual(string $attribute, string|int|float|bool $value): self
    {
        return new self(self::TYPE_GREATER_EQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with contains method
     *
     * @param  array<mixed>  $values
     */
    public static function contains(string $attribute, array $values): self
    {
        return new self(self::TYPE_CONTAINS, $attribute, $values);
    }

    /**
     * Helper method to create Query with between method
     */
    public static function between(string $attribute, string|int|float|bool $start, string|int|float|bool $end): self
    {
        return new self(self::TYPE_BETWEEN, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with search method
     */
    public static function search(string $attribute, string $value): self
    {
        return new self(self::TYPE_SEARCH, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with select method
     *
     * @param  array<string>  $attributes
     */
    public static function select(array $attributes): self
    {
        return new self(self::TYPE_SELECT, values: $attributes);
    }

    public static function selection(string $attribute, string $alias = '', string $as = '', string $function = ''): self
    {
        return new self(self::TYPE_SELECTION, $attribute, [], alias: $alias, as: $as);
    }

    /**
     * Helper method to create Query with orderDesc method
     */
    public static function orderDesc(string $attribute = '', string $alias = Query::DEFAULT_ALIAS): self
    {
        return new self(self::TYPE_ORDER_DESC, $attribute, alias: $alias);
    }

    /**
     * Helper method to create Query with orderAsc method
     */
    public static function orderAsc(string $attribute = ''): self
    {
        return new self(self::TYPE_ORDER_ASC, $attribute);
    }

    /**
     * Helper method to create Query with limit method
     */
    public static function limit(int $value): self
    {
        return new self(self::TYPE_LIMIT, values: [$value]);
    }

    /**
     * Helper method to create Query with offset method
     */
    public static function offset(int $value): self
    {
        return new self(self::TYPE_OFFSET, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorAfter method
     */
    public static function cursorAfter(Document $value): self
    {
        return new self(self::TYPE_CURSOR_AFTER, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorBefore method
     */
    public static function cursorBefore(Document $value): self
    {
        return new self(self::TYPE_CURSOR_BEFORE, values: [$value]);
    }

    /**
     * Helper method to create Query with isNull method
     */
    public static function isNull(string $attribute): self
    {
        return new self(self::TYPE_IS_NULL, $attribute);
    }

    /**
     * Helper method to create Query with isNotNull method
     */
    public static function isNotNull(string $attribute): self
    {
        return new self(self::TYPE_IS_NOT_NULL, $attribute);
    }

    public static function startsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_STARTS_WITH, $attribute, [$value]);
    }

    public static function endsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_ENDS_WITH, $attribute, [$value]);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function or(array $queries): self
    {
        return new self(self::TYPE_OR, '', $queries);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function and(array $queries): self
    {
        return new self(self::TYPE_AND, '', $queries);
    }

    public static function join(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_INNER_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    public static function innerJoin(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_INNER_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    /**
     * @param  array<Query>  $conditions
     */
    public static function leftJoin(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_LEFT_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    /**
     * @param  array<Query>  $conditions
     */
    public static function rightJoin(string $collection, string $alias, array $queries = []): self
    {
        return new self(self::TYPE_RIGHT_JOIN, values: $queries, alias: $alias, collection: $collection);
    }

    public static function relationEqual($leftAlias, string $leftColumn, string $rightAlias, string $rightColumn): self
    {
        return new self(self::TYPE_RELATION_EQUAL, $leftColumn, [], alias: $leftAlias, attributeRight: $rightColumn, aliasRight: $rightAlias);
    }

    /**
     * Filters $queries for $types
     *
     * @param  array<Query>  $queries
     * @param  array<string>  $types
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
        return self::getByType($queries, [
            Query::TYPE_INNER_JOIN,
            Query::TYPE_LEFT_JOIN,
            Query::TYPE_RIGHT_JOIN,
        ]);
    }

    /**
     * @param array<Query> $queries
     * @param int|null $default
     * @return int|null
     */
    public static function getLimitQueries(array $queries, ?int $default = null): ?int
    {
        $queries = self::getByType($queries, [
            Query::TYPE_LIMIT,
        ]);

        if (empty($queries)) {
            return $default;
        }

        return $queries[0]->getValue();
    }

    /**
     * @param array<Query> $queries
     * @param int|null $default
     * @return int|null
     */
    public static function getOffsetQueries(array $queries, ?int $default = null): ?int
    {
        $queries = self::getByType($queries, [
            Query::TYPE_OFFSET,
        ]);

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
        ]);
    }

    /**
     * @param array<Query> $queries
     * @return Query|null
     */
    public static function getCursorQueries(array $queries): ?Query
    {
        $queries = self::getByType($queries, [
            Query::TYPE_CURSOR_AFTER,
            Query::TYPE_CURSOR_BEFORE,
        ]);

        if (empty($queries)) {
            return null;
        }

        return $queries[0];
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    public static function getFilterQueries(array $queries): array
    {
        return self::getByType($queries, [
            self::TYPE_EQUAL,
            self::TYPE_NOT_EQUAL,
            self::TYPE_LESSER,
            self::TYPE_LESSER_EQUAL,
            self::TYPE_GREATER,
            self::TYPE_GREATER_EQUAL,
            self::TYPE_CONTAINS,
            self::TYPE_SEARCH,
            self::TYPE_IS_NULL,
            self::TYPE_IS_NOT_NULL,
            self::TYPE_BETWEEN,
            self::TYPE_STARTS_WITH,
            self::TYPE_ENDS_WITH,
            self::TYPE_AND,
            self::TYPE_OR,
        ]);
    }

    /**
     * Iterates through queries are groups them by type
     *
     * @param  array<Query>  $queries
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
        $joins = [];
        $selections = [];
        $limit = null;
        $offset = null;
        $orderAttributes = [];
        $orderTypes = [];
        $cursor = null;
        $cursorDirection = null;

        foreach ($queries as $query) {
            if (! $query instanceof Query) {
                continue;
            }

            $method = $query->getMethod();
            $attribute = $query->getAttribute();
            $values = $query->getValues();

            switch ($method) {
                case Query::TYPE_ORDER_ASC:
                case Query::TYPE_ORDER_DESC:
                    if (! empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = $method === Query::TYPE_ORDER_ASC
                        ? Database::ORDER_ASC
                        : Database::ORDER_DESC;

                    break;
                case Query::TYPE_LIMIT:
                    // keep the 1st limit encountered and ignore the rest
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

                case Query::TYPE_INNER_JOIN:
                case Query::TYPE_LEFT_JOIN:
                case Query::TYPE_RIGHT_JOIN:
                    $joins[] = clone $query;
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
            'join' => $joins,
        ];
    }

    /**
     * Is this query able to contain other queries
     */
    public function isNested(): bool
    {
        if (in_array($this->getMethod(), self::LOGICAL_TYPES)) {
            return true;
        }

        return false;
    }

    public function onArray(): bool
    {
        return $this->onArray;
    }

    public function setOnArray(bool $bool): void
    {
        $this->onArray = $bool;
    }
}
