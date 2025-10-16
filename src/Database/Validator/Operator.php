<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator as DatabaseOperator;
use Utopia\Validator;

class Operator extends Validator
{
    protected Document $collection;

    /**
     * @var array<string, array<string, mixed>>
     */
    protected array $attributes;

    protected string $message = 'Invalid operator';

    /**
     * Constructor
     *
     * @param Document $collection
     */
    public function __construct(Document $collection)
    {
        $this->collection = $collection;

        foreach ($collection->getAttribute('attributes', []) as $attribute) {
            $this->attributes[$attribute->getAttribute('key', $attribute->getId())] = $attribute;
        }
    }

    /**
     * Get Description
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
     * Is valid
     *
     * Returns true if valid or false if not.
     *
     * @param $value
     *
     * @return bool
     */
    public function isValid($value): bool
    {
        if (!$value instanceof DatabaseOperator) {
            $this->message = 'Value must be an instance of Operator';
            return false;
        }

        $method = $value->getMethod();
        $attribute = $value->getAttribute();

        // Check if method is valid
        if (!DatabaseOperator::isMethod($method)) {
            $this->message = "Invalid operator method: {$method}";
            return false;
        }

        // Check if attribute exists in collection
        $attributeConfig = $this->attributes[$attribute] ?? null;
        if ($attributeConfig === null) {
            $this->message = "Attribute '{$attribute}' does not exist in collection";
            return false;
        }

        // Validate operator against attribute type
        return $this->validateOperatorForAttribute($value, $attributeConfig);
    }

    /**
     * Validate operator against attribute configuration
     *
     * @param DatabaseOperator $operator
     * @param array<string, mixed> $attribute
     * @return bool
     */
    private function validateOperatorForAttribute(
        DatabaseOperator $operator,
        array $attribute
    ): bool {
        $method = $operator->getMethod();
        $values = $operator->getValues();
        $type = $attribute['type'];
        $isArray = $attribute['array'] ?? false;

        switch ($method) {
            case DatabaseOperator::TYPE_INCREMENT:
            case DatabaseOperator::TYPE_DECREMENT:
            case DatabaseOperator::TYPE_MULTIPLY:
            case DatabaseOperator::TYPE_DIVIDE:
            case DatabaseOperator::TYPE_MODULO:
            case DatabaseOperator::TYPE_POWER:
                // Numeric operations only work on numeric types
                if (!\in_array($type, [Database::VAR_INTEGER, Database::VAR_FLOAT])) {
                    $this->message = "Cannot use {$method} operator on non-numeric attribute '{$operator->getAttribute()}' of type '{$type}'";
                    return false;
                }

                // Validate the numeric value and optional max/min
                if (empty($values) || !\is_numeric($values[0])) {
                    $this->message = "Numeric operator value must be numeric, got " . gettype($operator->getValue());
                    return false;
                }

                // Special validation for divide/modulo by zero
                if (($method === DatabaseOperator::TYPE_DIVIDE || $method === DatabaseOperator::TYPE_MODULO) && $values[0] == 0) {
                    $this->message = ($method === DatabaseOperator::TYPE_DIVIDE ? "Division" : "Modulo") . " by zero is not allowed";
                    return false;
                }

                // Validate max/min if provided
                if (\count($values) > 1 && $values[1] !== null && !\is_numeric($values[1])) {
                    $this->message = "Max/min limit must be numeric, got " . \gettype($values[1]);
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_APPEND:
            case DatabaseOperator::TYPE_ARRAY_PREPEND:
            case DatabaseOperator::TYPE_ARRAY_UNIQUE:
                if (!$isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_INSERT:
                if (!$isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";
                    return false;
                }

                if (\count($values) !== 2) {
                    $this->message = "Insert operator requires exactly 2 values: index and value";
                    return false;
                }

                $index = $values[0];
                if (!\is_int($index) || $index < 0) {
                    $this->message = "Insert index must be a non-negative integer";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_REMOVE:
                if (!$isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";
                    return false;
                }

                if (empty($values)) {
                    $this->message = "Array remove operator requires a value to remove";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_INTERSECT:
            case DatabaseOperator::TYPE_ARRAY_DIFF:
                if (!$isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";
                    return false;
                }

                if (empty($values) || !\is_array($values[0]) || \count($values[0]) === 0) {
                    $this->message = "{$method} operator requires an array of values";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_FILTER:
                if (!$isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";
                    return false;
                }

                if (\count($values) < 1 || \count($values) > 2) {
                    $this->message = "Array filter operator requires 1 or 2 values: condition and optional comparison value";
                    return false;
                }

                if (!\is_string($values[0])) {
                    $this->message = "Array filter condition must be a string";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_CONCAT:
                // Concat works on both strings and arrays
                if ($type !== Database::VAR_STRING && !$isArray) {
                    $this->message = "Cannot use concat operator on attribute '{$operator->getAttribute()}' of type '{$type}' (must be string or array)";
                    return false;
                }

                if (empty($values) || !\is_string($values[0])) {
                    $this->message = "String concatenation operator requires a string value";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_REPLACE:
                // Replace only works on string types
                if ($type !== Database::VAR_STRING) {
                    $this->message = "Cannot use replace operator on non-string attribute '{$operator->getAttribute()}' of type '{$type}'";
                    return false;
                }

                if (\count($values) !== 2 || !\is_string($values[0]) || !\is_string($values[1])) {
                    $this->message = "Replace operator requires exactly 2 string values: search and replace";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_TOGGLE:
                // Toggle only works on boolean types
                if ($type !== Database::VAR_BOOLEAN) {
                    $this->message = "Cannot use toggle operator on non-boolean attribute '{$operator->getAttribute()}' of type '{$type}'";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_DATE_ADD_DAYS:
            case DatabaseOperator::TYPE_DATE_SUB_DAYS:
                if ($type !== Database::VAR_DATETIME) {
                    $this->message = "Cannot use {$method} operator on non-datetime attribute '{$operator->getAttribute()}' of type '{$type}'";
                    return false;
                }

                if (empty($values) || !\is_int($values[0])) {
                    $this->message = "Date operator requires an integer number of days";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_DATE_SET_NOW:
                if ($type !== Database::VAR_DATETIME) {
                    $this->message = "Cannot use {$method} operator on non-datetime attribute '{$operator->getAttribute()}' of type '{$type}'";
                    return false;
                }

                break;
            default:
                $this->message = "Unsupported operator method: {$method}";
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
