<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\QueryValidator;
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
     * @var array
     */
    protected $indexes = [];

    /**
     * @var bool
     */
    protected $strict;

    /**
     * Queries constructor
     *
     * @param QueryValidator $validator
     * @param Document[] $indexes
     * @param bool $strict
     */
    public function __construct($validator, $indexes, $strict = true)
    {
        $this->validator = $validator;

        $this->indexes[] = [
            'type' => Database::INDEX_UNIQUE,
            'attributes' => ['$id']
        ];

        foreach ($indexes as $index) {
            $this->indexes[] = $index->getArrayCopy(['attributes', 'type']);
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
     * Returns true if all $queries are valid as a set.
     * @param mixed $value as array of Query objects
     * @return bool
     */
    public function isValid($value): bool
    {
        /**
         * Array of attributes from Query->getAttribute()
         *
         * @var string[]
         */
        $queries = [];

        foreach ($value as $query) {
            // [attribute => operator]
            $queries[$query->getAttribute()] = $query->getOperator(); 

            if (!$this->validator->isValid($query)) {
                $this->message = 'Query not valid: ' . $this->validator->getDescription();
                return false;
            }
        }

        $found = null;

        // Return false if attributes do not exactly match an index
        if ($this->strict) {
            // look for strict match among indexes
            foreach ($this->indexes as $index) {
                if ($this->arrayMatch($index['attributes'],  array_keys($queries))) {
                    $found = $index; 
                }
            }

            if (!$found) {
                $this->message = 'Index not found: ' . implode(",", array_keys($queries));
                return false;
            }

            // search operator requires fulltext index
            if (in_array(Query::TYPE_SEARCH, array_values($queries)) && $found['type'] !== Database::INDEX_FULLTEXT) {
                $this->message = 'Search operator requires fulltext index: ' . implode(",", array_keys($queries));
                return false;
            } 
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
    protected function arrayMatch($indexes, $queries): bool
    {
        // Check the count of indexes first for performance
        if (count($indexes) !== count($queries)) {
            return false;
        }

        // Only matching arrays will have equal diffs in both directions
        if (array_diff_assoc($indexes, $queries) !== array_diff_assoc($queries, $indexes)) {
            return false;
        }

        return true;
    }
}
