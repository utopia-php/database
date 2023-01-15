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
     * @param mixed $value
     * @return bool
     */
    public function isValid($value): bool
    {
        $queries = [];
        foreach ($value as $query) {
            if (!$query instanceof Query){
                try {
                    $query = Query::parse($query);
                    var_dump($query);
                } catch (\Throwable $th) {
                    $this->message = 'Invalid query: ${query}';
                    return false;
                }
            }

            if (!$this->validator->isValid($query)) {
                $this->message = 'Query not valid: ' . $this->validator->getDescription();
                return false;
            }

            $queries[] = $query;
        }

        if (!$this->strict) {
            return true;
        }

        $grouped = Query::groupByType($queries);
        /** @var Query[] */ $filters = $grouped['filters'];
        /** @var string[] */ $orderAttributes = $grouped['orderAttributes'];

        // Check filter queries for exact index match
        if (count($filters) > 0) {
            $filtersByAttribute = [];
            foreach ($filters as $filter) {
               // if($filter->getMethod() === Query::TYPE_SLEEP)return true; // todo: fix this
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
}
