<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Database\Document;
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
        'search',
    ];

    /**
     * Expression constructor
     *
     * @param Document[] $attributes
     */
    public function __construct(array $attributes)
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
     * @param $query
     *
     * @return bool
     */
    public function isValid($query): bool
    {
        // Validate operator
        if (!in_array($query->getOperator(), $this->operators)) {
            $this->message = 'Query operator invalid: ' . $query->getOperator();
            return false;
        }

        // Search for attribute in schema
        $attributeIndex = array_search($query->getAttribute(), array_column($this->schema, 'key'));

        if ($attributeIndex === false) {
            $this->message = 'Attribute not found in schema: ' . $query->getAttribute();
            return false;
        }

        // Extract the type of desired attribute from collection $schema
        $attributeType = $this->schema[$attributeIndex]['type'];

        foreach ($query->getValues() as $value) {
            if (gettype($value) !== $attributeType) {
                $this->message = 'Query type does not match expected: ' . $attributeType;
                return false;
            }
        }

        // Contains operator only supports array attributes
        if (!$this->schema[$attributeIndex]['array'] && $query->getOperator() === Query::TYPE_CONTAINS) {
            $this->message = 'Query operator only supported on array attributes: ' . $query->getOperator();
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
