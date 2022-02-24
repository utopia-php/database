<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Database\Document;
use Utopia\Database\Query;

class OrderAttributes extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Invalid order attribute';

    /**
     * @var array
     */
    protected $schema = [];

    /**
     * @var array
     */
    protected $indexes = [];

    /**
     * @var bool
     */
    protected $strict;

    /**
     * Expression constructor
     *
     * @param Document[] $attributes
     * @param Document[] $indexes
     * @param bool $strict
     */
    public function __construct($attributes, $indexes, $strict = true)
    {
        $this->schema[] = [
            'key' => '$id',
            'array' => false,
            'type' => Database::VAR_STRING,
            'size' => 512
        ];

        foreach ($attributes as $attribute) {
            $this->schema[] = $attribute->getArrayCopy();
        }

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
     * Returns true if query typed according to schema.
     *
     * @param string[] $attributes
     *
     * @return bool
     */
    public function isValid($attributes): bool
    {
        foreach ($attributes as $attribute) {
            // Search for attribute in schema
            $attributeIndex = array_search($attribute, array_column($this->schema, 'key'));

            if ($attributeIndex === false) {
                $this->message = 'Order attribute not found in schema: ' . $attribute;
                return false;
            }
        }

        $found = null;

        // Return false if attributes do not exactly match an index
        if ($this->strict) {
            // look for strict match among indexes
            foreach ($this->indexes as $index) {
                if ($this->arrayMatch($index['attributes'],  $attributes)) {
                    $found = $index;
                }
            }

            if (!$found) {
                $this->message = 'Index not found: ' . implode(",", $attributes);
                return false;
            }

            // search operator requires fulltext index
            if (in_array(Query::TYPE_SEARCH, $attributes) && $found['type'] !== Database::INDEX_FULLTEXT) {
                $this->message = 'Search operator requires fulltext index: ' . implode(",", $attributes);
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
        return false;
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
