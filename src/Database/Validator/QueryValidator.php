<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Query;

class QueryValidator extends Validator
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
     * @param $query
     *
     * @return bool
     */
    public function isValid($query)
    {
        // Validate operator
        if (!in_array($query->getOperator(), $this->operators)) {
            $this->message = 'Query operator invalid: ' . $query->getOperator();
            return false;
        }

        // Search for attribute in schema
        $attributeIndex = array_search($query->getAttribute(), array_column($this->schema, '$id'));

        if ($attributeIndex === false) {
            $this->message = 'Attribute not found in schema: ' . $query->getAttribute();
            return false;
        }

        // Extract the type of desired attribute from collection $schema
        $attributeType = $this->schema[array_search($query->getAttribute(), array_column($this->schema, '$id'))]['type'];

        foreach ($query->getValues() as $value) {
            if (gettype($value) !== $attributeType) {
                $this->message = 'Query type does not match expected: ' . $attributeType;
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
}
