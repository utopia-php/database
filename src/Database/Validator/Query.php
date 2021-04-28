<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Query;

class Query extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Invalid query';

    /**
     * @var array
     */
    protected $schema = [];

    /**
     * @var array
     */
    protected $operators = [
        'equal',
        'notEqual',
        'lesser',
        'lesserEqual',
        'greater',
        'greaterEqual',
        'contains',
        'notContains',
        'isNull',
        'isNotNull',
        'isEmpty',
        'isNotEmpty',
    ];

    /**
     * Expression constructor
     *
     * @param array $schema
     */
    public function __construct($schema)
    {
        $this->schema = $schema;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription()
    {
        return $this->message;
    }

    /**
     * Is valid.
     *
     * Returns true if query typed according to schema.
     *
     * @param Query $query
     *
     * @return bool
     */
    public function isValid(Query $query)
    {
        // Validate operator
        if (!in_array($query->getOperator(), $this->operators)) {
            $this->message = 'Query operator invalid';
            return false;
        }

        // Validate attribute by name and type
        $attributes = $this->schema['attributes'];

        if (!in_array($query->getAttribute(), $attributes)) {
            $this->message = 'Attribute not found in schema';
            return false;
        }

        // Extract the type of desired attribute from collection $schema
        $attributeType = $this->schema[array_search($query->getAttribute(), array_column($this->schema, '$id'))]['type'];

        if ($attributeType !== gettype($query->getValue())) {
            $this->message = 'Query type does not match schema';
            return false;
        }

        // Ensure values are array
        if (is_array($query->getValues())) {
            return true;
        }

        return false;
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
}
