<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Query;

class Query extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Query string of the format attribute.operator(value)';

    /**
     * @var array
     */
    protected $schema = [];

    //TODO@kodumbeats Validate operators against official list

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
        // Extract the type of desired attribute from collection $schema
        $attributeType = $this->schema[array_search($query->getAttribute(), array_column($this->schema, '$id'))]['type'];
        
        if ($attributeType === gettype($query->getValue())) {
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
}
