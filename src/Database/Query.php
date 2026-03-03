<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Query as QueryException;
use Utopia\Query\Exception as BaseQueryException;
use Utopia\Query\Query as BaseQuery;

class Query extends BaseQuery
{
    protected bool $isObjectAttribute = false;

    /**
     * @param  array<mixed>  $values
     */
    public function __construct(string $method, string $attribute = '', array $values = [])
    {
        if ($attribute === '' && \in_array($method, [self::TYPE_ORDER_ASC, self::TYPE_ORDER_DESC])) {
            $attribute = '$sequence';
        }

        parent::__construct($method, $attribute, $values);
    }

    /**
     * @param string $query
     * @return self
     * @throws QueryException
     */
    public static function parse(string $query): self
    {
        try {
            return parent::parse($query);
        } catch (BaseQueryException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param array<string, mixed> $query
     * @return self
     * @throws QueryException
     */
    public static function parseQuery(array $query): self
    {
        try {
            return parent::parseQuery($query);
        } catch (BaseQueryException $e) {
            throw new QueryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Helper method to create Query with cursorAfter method
     *
     * @param Document $value
     * @return Query
     */
    public static function cursorAfter(mixed $value): self
    {
        return new self(self::TYPE_CURSOR_AFTER, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorBefore method
     *
     * @param Document $value
     * @return Query
     */
    public static function cursorBefore(mixed $value): self
    {
        return new self(self::TYPE_CURSOR_BEFORE, values: [$value]);
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

        if (\in_array($array['method'], static::LOGICAL_TYPES)) {
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
     * Iterates through queries and groups them by type
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
            if (!$query instanceof self) {
                continue;
            }

            $method = $query->getMethod();
            $attribute = $query->getAttribute();
            $values = $query->getValues();

            switch ($method) {
                case self::TYPE_ORDER_ASC:
                case self::TYPE_ORDER_DESC:
                case self::TYPE_ORDER_RANDOM:
                    if (!empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = match ($method) {
                        self::TYPE_ORDER_ASC => Database::ORDER_ASC,
                        self::TYPE_ORDER_DESC => Database::ORDER_DESC,
                        self::TYPE_ORDER_RANDOM => Database::ORDER_RANDOM,
                    };

                    break;
                case self::TYPE_LIMIT:
                    // Keep the 1st limit encountered and ignore the rest
                    if ($limit !== null) {
                        break;
                    }

                    $limit = $values[0] ?? $limit;
                    break;
                case self::TYPE_OFFSET:
                    // Keep the 1st offset encountered and ignore the rest
                    if ($offset !== null) {
                        break;
                    }

                    $offset = $values[0] ?? $limit;
                    break;
                case self::TYPE_CURSOR_AFTER:
                case self::TYPE_CURSOR_BEFORE:
                    // Keep the 1st cursor encountered and ignore the rest
                    if ($cursor !== null) {
                        break;
                    }

                    $cursor = $values[0] ?? $limit;
                    $cursorDirection = $method === self::TYPE_CURSOR_AFTER ? Database::CURSOR_AFTER : Database::CURSOR_BEFORE;
                    break;

                case self::TYPE_SELECT:
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
}
