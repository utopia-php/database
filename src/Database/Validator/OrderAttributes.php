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
    protected string $message = 'Invalid order attribute';

    /**
     * @var array<string, mixed>
     */
    protected array $schema = [];

    /**
     * @var array<string, mixed>
     */
    protected array $indexes = [];

    /**
     * @var bool
     */
    protected bool $strict;

    /**
     * Expression constructor
     *
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @param bool $strict
     */
    public function __construct(array $attributes, array $indexes, bool $strict = true)
    {
        $this->schema[] = [
            'key' => '$id',
            'array' => false,
            'type' => Database::VAR_STRING,
            'size' => 512
        ];

        $this->schema[] = [
            'key' => '$createdAt',
            'array' => false,
            'type' => Database::VAR_INTEGER,
            'size' => 0
        ];

        $this->schema[] = [
            'key' => '$updatedAt',
            'array' => false,
            'type' => Database::VAR_INTEGER,
            'size' => 0
        ];

        foreach ($attributes as $attribute) {
            $this->schema[] = $attribute->getArrayCopy();
        }

        $this->indexes[] = [
            'type' => Database::INDEX_UNIQUE,
            'attributes' => ['$id']
        ];

        $this->indexes[] = [
            'type' => Database::INDEX_KEY,
            'attributes' => ['$createdAt']
        ];

        $this->indexes[] = [
            'type' => Database::INDEX_KEY,
            'attributes' => ['$updatedAt']
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
     * @param mixed $value
     *
     * @return bool
     */
    public function isValid($value): bool
    {
        foreach ($value as $attribute) {
            // Search for attribute in schema
            $attributeInSchema = \in_array($attribute, \array_column($this->schema, 'key'));

            if ($attributeInSchema === false) {
                $this->message = 'Order attribute not found in schema: ' . $attribute;
                return false;
            }
        }

        $found = null;

        // Return false if attributes do not exactly match an index
        if ($this->strict) {
            // look for strict match among indexes
            foreach ($this->indexes as $index) {
                if ($this->arrayMatch($index['attributes'], $value)) {
                    $found = $index;
                }
            }

            if (!$found) {
                $this->message = 'Index not found: ' . implode(",", $value);
                return false;
            }

            // search method requires fulltext index
            if (in_array(Query::TYPE_SEARCH, $value) && $found['type'] !== Database::INDEX_FULLTEXT) {
                $this->message = 'Search method requires fulltext index: ' . implode(",", $value);
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
     * @param array<string> $indexes
     * @param array<string> $queries
     *
     * @return bool
     */
    protected function arrayMatch(array $indexes, array $queries): bool
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
