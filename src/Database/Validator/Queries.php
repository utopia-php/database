<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Query as QueryValidator;
use Utopia\Database\Query;

class Queries extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Invalid queries';

    /**
     * @var QueryValidator
     */
    protected $validator;

    /**
     * @var Document[]
     */
    protected $attributes = [];

    /**
     * @var Document[]
     */
    protected $indexes = [];

    /**
     * @var bool
     */
    protected $strict;

    /**
     * Queries constructor
     *
     * @param QueryValidator $validator used to validate each query
     * @param Document[] $attributes allowed attributes to be queried
     * @param Document[] $indexes available for strict query matching
     * @param bool $strict
     */
    public function __construct(QueryValidator $validator, array $attributes, array $indexes, bool $strict = true)
    {
        $this->validator = $validator;
        $this->attributes = $attributes;

        $this->indexes[] = new Document([
            'type' => Database::INDEX_UNIQUE,
            'attributes' => ['$id']
        ]);

        $this->indexes[] = new Document([
            'type' => Database::INDEX_KEY,
            'attributes' => ['$createdAt']
        ]);

        $this->indexes[] = new Document([
            'type' => Database::INDEX_KEY,
            'attributes' => ['$updatedAt']
        ]);

        foreach ($indexes ?? [] as $index) {
            $this->indexes[] = $index;
        }

        $this->strict = $strict;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid.
     *
     * Returns false if:
     * 1. any query in $value is invalid based on $validator
     * 
     * In addition, if $strict is true, this returns false if:
     * 1. there is no index with an exact match of the filters
     * 2. there is no index with an exact match of the order attributes
     * 
     * Otherwise, returns true.
     * 
     * @param Query[] $value as array of Query objects
     * @return bool
     */
    public function isValid($value): bool
    {
        foreach ($value as $query) {

            if (!$this->validator->isValid($query)) {
                $this->message = 'Query not valid: ' . $this->validator->getDescription();
                return false;
            }
        }

        if (!$this->strict) {
            return true;
        }

        $queriesByMethod = self::groupByType($value);
        /** @var Query[] */ $filters = $queriesByMethod['filters'];
        /** @var string[] */ $orderAttributes = $queriesByMethod['orderAttributes'];

        // Check filter queries for exact index match
        if (count($filters) > 0) {
            $filtersByAttribute = [];
            foreach ($filters as $filter) {
                $filtersByAttribute[$filter->getAttribute()] = $filter->getMethod();
            }

            $found = null;

            foreach ($this->indexes as $index) {
                if ($this->arrayMatch($index->getAttribute('attributes'),  array_keys($filtersByAttribute))) {
                    $found = $index;
                }
            }

            if (!$found) {
                $this->message = 'Index not found: ' . implode(",", array_keys($filtersByAttribute));
                return false;
            }

            // search method requires fulltext index
            if (in_array(Query::TYPE_SEARCH, array_values($filtersByAttribute)) && $found['type'] !== Database::INDEX_FULLTEXT) {
                $this->message = 'Search method requires fulltext index: ' . implode(",", array_keys($filtersByAttribute));
                return false;
            }
        }

        // Check order attributes for exact index match
        $validator = new OrderAttributes($this->attributes, $this->indexes, true);
        if (count($orderAttributes) > 0 && !$validator->isValid($orderAttributes)) {
            $this->message = $validator->getDescription();
            return false;
        }

        return true;
    }
    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return true;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    /**
     * Is Strict
     *
     * Returns true if strict validation is set
     *
     * @return bool
     */
    public function isStrict(): bool
    {
        return $this->strict;
    }

    /**
     * Check if indexed array $indexes matches $queries
     *
     * @param array $indexes
     * @param array $queries
     *
     * @return bool
     */
    protected function arrayMatch(array $indexes, array $queries): bool
    {
        // Check the count of indexes first for performance
        if (count($queries) !== count($indexes)) {
            return false;
        }

        // Sort them for comparison, the order is not important here anymore.
        sort($indexes, SORT_STRING);
        sort($queries, SORT_STRING);

        // Only matching arrays will have equal diffs in both directions
        if (array_diff_assoc($indexes, $queries) !== array_diff_assoc($queries, $indexes)) {
            return false;
        }

        return true;
    }

    /**
     * Iterates through $queries and returns an array with:
     * - filters: array of filter queries
     * - limit: int
     * - offset: int
     * - orderAttributes: array of attribute keys
     * - orderTypes: array of Database::ORDER_ASC or Database::ORDER_DESC
     * - cursor: Document
     * - cursorDirection: Database::CURSOR_BEFORE or Database::CURSOR_AFTER
     * 
     * @param Query[] $queries
     * @param int $defaultLimit
     * @param int $defaultOffset
     * @param string $defaultCursorDirection
     * 
     * @return array
     */
    public static function groupByType(array $queries): array
    {
        $filters = [];
        $limit = null;
        $offset = null;
        $orderAttributes = [];
        $orderTypes = [];
        $cursor = null;
        $cursorDirection = null;
        foreach ($queries as $query) {
            if (!$query instanceof Query) continue;

            $method = $query->getMethod();
            $attribute = $query->getAttribute();
            $values = $query->getValues();
            switch ($method) {
                case Query::TYPE_ORDERASC:
                case Query::TYPE_ORDERDESC:
                    if (!empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = $method === Query::TYPE_ORDERASC ? Database::ORDER_ASC : Database::ORDER_DESC;
                    break;

                case Query::TYPE_LIMIT:
                    // keep the 1st limit encountered and ignore the rest
                    if ($limit !== null) break;

                    $limit = $values[0] ?? $limit;
                    break;

                case Query::TYPE_OFFSET:
                    // keep the 1st offset encountered and ignore the rest
                    if ($offset !== null) break;

                    $offset = $values[0] ?? $limit;
                    break;

                case Query::TYPE_CURSORAFTER:
                case Query::TYPE_CURSORBEFORE:
                    // keep the 1st cursor encountered and ignore the rest
                    if ($cursor !== null) break;

                    $cursor = $values[0] ?? $limit;
                    $cursorDirection = $method === Query::TYPE_CURSORAFTER ? Database::CURSOR_AFTER : Database::CURSOR_BEFORE;
                    break;

                default:
                    $filters[] = $query;
                    break;
            }
        }

        return [
            'filters' => $filters,
            'limit' => $limit,
            'offset' => $offset,
            'orderAttributes' => $orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $cursorDirection,
        ];
    }
}
