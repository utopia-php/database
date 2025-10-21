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
     * @var array<string, Document|array<string, mixed>>
     */
    protected array $attributes = [];

    protected string $message = 'Invalid operator';

    protected ?Document $currentDocument = null;

    /**
     * Constructor
     *
     * @param Document $collection
     * @param Document|null $currentDocument Current document for runtime validation (e.g., array bounds checking)
     */
    public function __construct(Document $collection, ?Document $currentDocument = null)
    {
        $this->collection = $collection;
        $this->currentDocument = $currentDocument;

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
     * @param Document|array<string, mixed> $attribute
     * @return bool
     */
    private function validateOperatorForAttribute(
        DatabaseOperator $operator,
        Document|array $attribute
    ): bool {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        // Handle both Document objects and arrays
        $type = $attribute instanceof Document ? $attribute->getAttribute('type') : $attribute['type'];
        $isArray = $attribute instanceof Document ? ($attribute->getAttribute('array') ?? false) : ($attribute['array'] ?? false);

        switch ($method) {
            case DatabaseOperator::TYPE_INCREMENT:
            case DatabaseOperator::TYPE_DECREMENT:
            case DatabaseOperator::TYPE_MULTIPLY:
            case DatabaseOperator::TYPE_DIVIDE:
            case DatabaseOperator::TYPE_MODULO:
            case DatabaseOperator::TYPE_POWER:
                // Numeric operations only work on numeric types
                if (!\in_array($type, [Database::VAR_INTEGER, Database::VAR_FLOAT])) {
                    $this->message = "Cannot apply {$method} operator to non-numeric field '{$operator->getAttribute()}'";
                    return false;
                }

                // Validate the numeric value and optional max/min
                if (!isset($values[0]) || !\is_numeric($values[0])) {
                    $this->message = "Cannot apply {$method} operator: value must be numeric, got " . gettype($operator->getValue());
                    return false;
                }

                // Special validation for divide/modulo by zero
                if (($method === DatabaseOperator::TYPE_DIVIDE || $method === DatabaseOperator::TYPE_MODULO) && $values[0] == 0) {
                    $this->message = "Cannot apply {$method} operator: " . ($method === DatabaseOperator::TYPE_DIVIDE ? "division" : "modulo") . " by zero";
                    return false;
                }

                // Validate max/min if provided
                if (\count($values) > 1 && $values[1] !== null && !\is_numeric($values[1])) {
                    $this->message = "Cannot apply {$method} operator: max/min limit must be numeric, got " . \gettype($values[1]);
                    return false;
                }

                if ($this->currentDocument !== null && $type === Database::VAR_INTEGER && !isset($values[1])) {
                    $currentValue = $this->currentDocument->getAttribute($operator->getAttribute()) ?? 0;
                    $operatorValue = $values[0];

                    // Compute predicted result
                    $predictedResult = match ($method) {
                        DatabaseOperator::TYPE_INCREMENT => $currentValue + $operatorValue,
                        DatabaseOperator::TYPE_DECREMENT => $currentValue - $operatorValue,
                        DatabaseOperator::TYPE_MULTIPLY => $currentValue * $operatorValue,
                        DatabaseOperator::TYPE_DIVIDE => $operatorValue != 0 ? $currentValue / $operatorValue : $currentValue,
                        DatabaseOperator::TYPE_MODULO => $operatorValue != 0 ? $currentValue % $operatorValue : $currentValue,
                        DatabaseOperator::TYPE_POWER => $currentValue ** $operatorValue,
                    };

                    if ($predictedResult > Database::MAX_INT) {
                        $this->message = "Cannot apply {$method} operator: would overflow maximum value of " . Database::MAX_INT;
                        return false;
                    }

                    if ($predictedResult < Database::MIN_INT) {
                        $this->message = "Cannot apply {$method} operator: would underflow minimum value of " . Database::MIN_INT;
                        return false;
                    }
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_APPEND:
            case DatabaseOperator::TYPE_ARRAY_PREPEND:
                if (!$isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";
                    return false;
                }

                if (!empty($values) && $type === Database::VAR_INTEGER) {
                    $newItems = \is_array($values[0]) ? $values[0] : $values;
                    foreach ($newItems as $item) {
                        if (\is_numeric($item) && ($item > Database::MAX_INT || $item < Database::MIN_INT)) {
                            $this->message = "Cannot apply {$method} operator: array items must be between " . Database::MIN_INT . " and " . Database::MAX_INT;
                            return false;
                        }
                    }
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_UNIQUE:
                if (!$isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_INSERT:
                if (!$isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";
                    return false;
                }

                if (\count($values) !== 2) {
                    $this->message = "Cannot apply {$method} operator: requires exactly 2 values (index and value)";
                    return false;
                }

                $index = $values[0];
                if (!\is_int($index) || $index < 0) {
                    $this->message = "Cannot apply {$method} operator: index must be a non-negative integer";
                    return false;
                }

                $insertValue = $values[1];
                if ($type === Database::VAR_INTEGER && \is_numeric($insertValue)) {
                    if ($insertValue > Database::MAX_INT || $insertValue < Database::MIN_INT) {
                        $this->message = "Cannot apply {$method} operator: array items must be between " . Database::MIN_INT . " and " . Database::MAX_INT;
                        return false;
                    }
                }

                // Runtime validation: Check if index is within bounds
                if ($this->currentDocument !== null) {
                    $currentArray = $this->currentDocument->getAttribute($operator->getAttribute());
                    if (\is_array($currentArray)) {
                        $arrayLength = \count($currentArray);
                        // Valid indices are 0 to length (inclusive, as we can append)
                        if ($index > $arrayLength) {
                            $this->message = "Cannot apply {$method} operator: index {$index} is out of bounds for array of length {$arrayLength}";
                            return false;
                        }
                    }
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_REMOVE:
                if (!$isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";
                    return false;
                }

                if (empty($values)) {
                    $this->message = "Cannot apply {$method} operator: requires a value to remove";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_ARRAY_INTERSECT:
            case DatabaseOperator::TYPE_ARRAY_DIFF:
                if (!$isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";
                    return false;
                }


                break;
            case DatabaseOperator::TYPE_ARRAY_FILTER:
                if (!$isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";
                    return false;
                }

                if (\count($values) < 1 || \count($values) > 2) {
                    $this->message = "Cannot apply {$method} operator: requires 1 or 2 values (condition and optional comparison value)";
                    return false;
                }

                if (!\is_string($values[0])) {
                    $this->message = "Cannot apply {$method} operator: condition must be a string";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_CONCAT:
                if ($type !== Database::VAR_STRING || $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-string field '{$operator->getAttribute()}'";
                    return false;
                }

                if (empty($values) || !\is_string($values[0])) {
                    $this->message = "Cannot apply {$method} operator: requires a string value";
                    return false;
                }

                if ($this->currentDocument !== null && $type === Database::VAR_STRING) {
                    $currentString = $this->currentDocument->getAttribute($operator->getAttribute()) ?? '';
                    $concatValue = $values[0];
                    $predictedLength = strlen($currentString) + strlen($concatValue);

                    $maxSize = $attribute instanceof Document
                        ? $attribute->getAttribute('size', 0)
                        : ($attribute['size'] ?? 0);

                    if ($maxSize > 0 && $predictedLength > $maxSize) {
                        $this->message = "Cannot apply {$method} operator: result would exceed maximum length of {$maxSize} characters";
                        return false;
                    }
                }

                break;
            case DatabaseOperator::TYPE_REPLACE:
                // Replace only works on string types
                if ($type !== Database::VAR_STRING) {
                    $this->message = "Cannot apply {$method} operator to non-string field '{$operator->getAttribute()}'";
                    return false;
                }

                if (\count($values) !== 2 || !\is_string($values[0]) || !\is_string($values[1])) {
                    $this->message = "Cannot apply {$method} operator: requires exactly 2 string values (search and replace)";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_TOGGLE:
                // Toggle only works on boolean types
                if ($type !== Database::VAR_BOOLEAN) {
                    $this->message = "Cannot apply {$method} operator to non-boolean field '{$operator->getAttribute()}'";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_DATE_ADD_DAYS:
            case DatabaseOperator::TYPE_DATE_SUB_DAYS:
                if ($type !== Database::VAR_DATETIME) {
                    $this->message = "Cannot apply {$method} operator to non-datetime field '{$operator->getAttribute()}'";
                    return false;
                }

                if (empty($values) || !\is_int($values[0])) {
                    $this->message = "Cannot apply {$method} operator: requires an integer number of days";
                    return false;
                }

                break;
            case DatabaseOperator::TYPE_DATE_SET_NOW:
                if ($type !== Database::VAR_DATETIME) {
                    $this->message = "Cannot apply {$method} operator to non-datetime field '{$operator->getAttribute()}'";
                    return false;
                }

                break;
            default:
                $this->message = "Cannot apply {$method} operator: unsupported operator method";
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
