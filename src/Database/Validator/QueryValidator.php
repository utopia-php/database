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

    protected int $maxLimit;
    protected int $maxOffset;
    protected int $maxValuesCount;

    /**
     * Expression constructor
     *
     * @param Document[] $attributes
     */
    public function __construct(array $attributes, int $maxLimit = 100, int $maxOffset = 5000, int $maxValuesCount = 100)
    {
        $this->schema['$id'] = [
            'key' => '$id',
            'array' => false,
            'type' => Database::VAR_STRING,
            'size' => 512
        ];

        $this->schema['$createdAt'] = [
            'key' => '$createdAt',
            'array' => false,
            'type' => Database::VAR_DATETIME,
            'size' => 0
        ];

        $this->schema['$updatedAt'] = [
            'key' => '$updatedAt',
            'array' => false,
            'type' => Database::VAR_DATETIME,
            'size' => 0
        ];

        foreach ($attributes as $attribute) {
            $this->schema[$attribute->getAttribute('key')] = $attribute->getArrayCopy();
        }

        $this->maxLimit = $maxLimit;
        $this->maxOffset = $maxOffset;
        $this->maxValuesCount = $maxValuesCount;
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
     * 1. $query has an invalid method
     * 2. limit value is not a number, less than 0, or greater than $maxLimit
     * 3. offset value is not a number, less than 0, or greater than $maxOffset
     * 4. attribute does not exist
     * 5. count of values is greater than $maxValuesCount
     * 6. value type does not match attribute type
     * 6. contains method is used on non-array attribute
     * 
     * Otherwise, returns true.
     *
     * @param Query $query
     *
     * @return bool
     */
    public function isValid($query): bool
    {
        // Validate method
        $method = $query->getMethod();
        if (!Query::isMethod($method)) {
            $this->message = 'Query method invalid: ' . $method;
            return false;
        }

        if ($method === Query::TYPE_LIMIT) {
            $limit = $query->getValue();
            if ($limit === null || $limit < 0 || $limit > $this->maxLimit) {
                $this->message = 'Limit must be between 0 and ' . $this->maxLimit . '(inclusive)';
                return false;
            }
            return true;
        }

        if ($method === Query::TYPE_OFFSET) {
            $offset = $query->getValue();
            if ($offset === null || $offset < 0 || $offset > $this->maxOffset) {
                $this->message = 'Offset must be between 0 and ' . $this->maxOffset . '(inclusive)';
                return false;
            }
            return true;
        }

        if ($method === Query::TYPE_CURSORAFTER || $method === Query::TYPE_CURSORBEFORE) {
            $value = $query->getValue();
            if ($value === null) {
                $this->message = 'Cursor must not be null';
                return false;
            }
            return true;
        }

        // Allow empty string for order attribute so we can order by natural order
        $attribute = $query->getAttribute();
        if ($attribute === '' && ($method === DatabaseQuery::TYPE_ORDERASC || $method === DatabaseQuery::TYPE_ORDERDESC)) {
            return true;
        }

        // Search for attribute in schema
        if (!isset($this->schema[$attribute])) {
            $this->message = 'Attribute not found in schema: ' . $attribute;
            return false;
        }

        if ($method === Query::TYPE_ORDERASC || $method === Query::TYPE_ORDERDESC) {
            return true;
        }

        $attributeSchema = $this->schema[$attribute];

        $values = $query->getValues();
        if (count($values) > $this->maxValuesCount) {
            $this->message = 'Query on attribute has greater than ' . $this->maxValuesCount . ' values: ' . $attribute;
            return false;
        }

        // Extract the type of desired attribute from collection $schema
        $attributeType = $attributeSchema['type'];

        foreach ($values as $value) {
            $condition = match ($attributeType) {
                Database::VAR_DATETIME => gettype($value) === Database::VAR_STRING,
                default => gettype($value) === $attributeType
            };

            if (!$condition) {
                $this->message = 'Query type does not match expected: ' . $attributeType;
                return false;
            }
        }

        // Contains method only supports array attributes
        if (!$attributeSchema['array'] && $query->getMethod() === Query::TYPE_CONTAINS) {
            $this->message = 'Query method only supported on array attributes: ' . $query->getMethod();
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
