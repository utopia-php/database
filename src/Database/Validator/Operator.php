<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator as DatabaseOperator;
use Utopia\Database\OperatorType;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
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
     * @param  Document|null  $currentDocument  Current document for runtime validation (e.g., array bounds checking)
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
     * Check if a value is a valid relationship reference (string ID or Document)
     */
    private function isValidRelationshipValue(mixed $item): bool
    {
        return \is_string($item) || $item instanceof Document;
    }

    /**
     * Check if a relationship attribute represents a "many" side (returns array of documents)
     *
     * @param  Document|array<string, mixed>  $attribute
     */
    private function isRelationshipArray(Document|array $attribute): bool
    {
        $options = $attribute instanceof Document
            ? $attribute->getAttribute('options', [])
            : ($attribute['options'] ?? []);

        $relationType = $options['relationType'] ?? '';
        $side = $options['side'] ?? '';

        // Many-to-many is always an array on both sides
        if ($relationType === RelationType::ManyToMany->value) {
            return true;
        }

        // One-to-many: array on parent side, single on child side
        if ($relationType === RelationType::OneToMany->value && $side === RelationSide::Parent->value) {
            return true;
        }

        // Many-to-one: array on child side, single on parent side
        if ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Child->value) {
            return true;
        }

        return false;
    }

    /**
     * Get Description
     *
     * Returns validator description
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid
     *
     * Returns true if valid or false if not.
     */
    public function isValid($value): bool
    {
        if (! $value instanceof DatabaseOperator) {
            try {
                $value = DatabaseOperator::parse($value);
            } catch (\Throwable $e) {
                $this->message = 'Invalid operator: '.$e->getMessage();

                return false;
            }
        }

        $method = $value->getMethod();
        $attribute = $value->getAttribute();

        // Check if method is valid
        if (! DatabaseOperator::isMethod($method)) {
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
     * @param  Document|array<string, mixed>  $attribute
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
            case OperatorType::Increment->value:
            case OperatorType::Decrement->value:
            case OperatorType::Multiply->value:
            case OperatorType::Divide->value:
            case OperatorType::Modulo->value:
            case OperatorType::Power->value:
                // Numeric operations only work on numeric types
                if (! \in_array($type, [ColumnType::Integer->value, ColumnType::Double->value])) {
                    $this->message = "Cannot apply {$method} operator to non-numeric field '{$operator->getAttribute()}'";

                    return false;
                }

                // Validate the numeric value and optional max/min
                if (! isset($values[0]) || ! \is_numeric($values[0])) {
                    $this->message = "Cannot apply {$method} operator: value must be numeric, got ".gettype($operator->getValue());

                    return false;
                }

                // Special validation for divide/modulo by zero
                if (($method === OperatorType::Divide->value || $method === OperatorType::Modulo->value) && (float) $values[0] === 0.0) {
                    $this->message = "Cannot apply {$method} operator: ".($method === OperatorType::Divide->value ? 'division' : 'modulo').' by zero';

                    return false;
                }

                // Validate max/min if provided
                if (\count($values) > 1 && $values[1] !== null && ! \is_numeric($values[1])) {
                    $this->message = "Cannot apply {$method} operator: max/min limit must be numeric, got ".\gettype($values[1]);

                    return false;
                }

                if ($this->currentDocument !== null && $type === ColumnType::Integer->value && ! isset($values[1])) {
                    $currentValue = $this->currentDocument->getAttribute($operator->getAttribute()) ?? 0;
                    $operatorValue = $values[0];

                    // Compute predicted result
                    $predictedResult = match ($method) {
                        OperatorType::Increment->value => $currentValue + $operatorValue,
                        OperatorType::Decrement->value => $currentValue - $operatorValue,
                        OperatorType::Multiply->value => $currentValue * $operatorValue,
                        OperatorType::Divide->value => $currentValue / $operatorValue,
                        OperatorType::Modulo->value => $currentValue % $operatorValue,
                        OperatorType::Power->value => $currentValue ** $operatorValue,
                    };

                    if ($predictedResult > Database::MAX_INT) {
                        $this->message = "Cannot apply {$method} operator: would overflow maximum value of ".Database::MAX_INT;

                        return false;
                    }

                    if ($predictedResult < Database::MIN_INT) {
                        $this->message = "Cannot apply {$method} operator: would underflow minimum value of ".Database::MIN_INT;

                        return false;
                    }
                }

                break;
            case OperatorType::ArrayAppend->value:
            case OperatorType::ArrayPrepend->value:
                // For relationships, check if it's a "many" side
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                    foreach ($values as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$method} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (! empty($values) && $type === ColumnType::Integer->value) {
                    $newItems = \is_array($values[0]) ? $values[0] : $values;
                    foreach ($newItems as $item) {
                        if (\is_numeric($item) && ($item > Database::MAX_INT || $item < Database::MIN_INT)) {
                            $this->message = "Cannot apply {$method} operator: array items must be between ".Database::MIN_INT.' and '.Database::MAX_INT;

                            return false;
                        }
                    }
                }

                break;
            case OperatorType::ArrayUnique->value:
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                break;
            case OperatorType::ArrayInsert->value:
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (\count($values) !== 2) {
                    $this->message = "Cannot apply {$method} operator: requires exactly 2 values (index and value)";

                    return false;
                }

                $index = $values[0];
                if (! \is_int($index) || $index < 0) {
                    $this->message = "Cannot apply {$method} operator: index must be a non-negative integer";

                    return false;
                }

                $insertValue = $values[1];

                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isValidRelationshipValue($insertValue)) {
                        $this->message = "Cannot apply {$method} operator: relationship values must be document IDs (strings) or Document objects";

                        return false;
                    }
                }

                if ($type === ColumnType::Integer->value && \is_numeric($insertValue)) {
                    if ($insertValue > Database::MAX_INT || $insertValue < Database::MIN_INT) {
                        $this->message = "Cannot apply {$method} operator: array items must be between ".Database::MIN_INT.' and '.Database::MAX_INT;

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
            case OperatorType::ArrayRemove->value:
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                    $toValidate = \is_array($values[0]) ? $values[0] : $values;
                    foreach ($toValidate as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$method} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values)) {
                    $this->message = "Cannot apply {$method} operator: requires a value to remove";

                    return false;
                }

                break;
            case OperatorType::ArrayIntersect->value:
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values)) {
                    $this->message = "{$method} operator requires a non-empty array value";

                    return false;
                }

                if ($type === ColumnType::Relationship->value) {
                    foreach ($values as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$method} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                }

                break;
            case OperatorType::ArrayDiff->value:
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                    foreach ($values as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$method} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot use {$method} operator on non-array attribute '{$operator->getAttribute()}'";

                    return false;
                }

                break;
            case OperatorType::ArrayFilter->value:
                if ($type === ColumnType::Relationship->value) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$method} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (\count($values) < 1 || \count($values) > 2) {
                    $this->message = "Cannot apply {$method} operator: requires 1 or 2 values (condition and optional comparison value)";

                    return false;
                }

                if (! \is_string($values[0])) {
                    $this->message = "Cannot apply {$method} operator: condition must be a string";

                    return false;
                }

                $validConditions = [
                    'equal', 'notEqual',  // Comparison
                    'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual',  // Numeric
                    'isNull', 'isNotNull', // Null checks
                ];
                if (! \in_array($values[0], $validConditions, true)) {
                    $this->message = "Invalid array filter condition '{$values[0]}'. Must be one of: ".\implode(', ', $validConditions);

                    return false;
                }

                break;
            case OperatorType::StringConcat->value:
                if ($type !== ColumnType::String->value || $isArray) {
                    $this->message = "Cannot apply {$method} operator to non-string field '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values) || ! \is_string($values[0])) {
                    $this->message = "Cannot apply {$method} operator: requires a string value";

                    return false;
                }

                if ($this->currentDocument !== null && $type === ColumnType::String->value) {
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
            case OperatorType::StringReplace->value:
                // Replace only works on string types
                if ($type !== ColumnType::String->value) {
                    $this->message = "Cannot apply {$method} operator to non-string field '{$operator->getAttribute()}'";

                    return false;
                }

                if (\count($values) !== 2 || ! \is_string($values[0]) || ! \is_string($values[1])) {
                    $this->message = "Cannot apply {$method} operator: requires exactly 2 string values (search and replace)";

                    return false;
                }

                break;
            case OperatorType::Toggle->value:
                // Toggle only works on boolean types
                if ($type !== ColumnType::Boolean->value) {
                    $this->message = "Cannot apply {$method} operator to non-boolean field '{$operator->getAttribute()}'";

                    return false;
                }

                break;
            case OperatorType::DateAddDays->value:
            case OperatorType::DateSubDays->value:
                if ($type !== ColumnType::Datetime->value) {
                    $this->message = "Cannot apply {$method} operator to non-datetime field '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values) || ! \is_int($values[0])) {
                    $this->message = "Cannot apply {$method} operator: requires an integer number of days";

                    return false;
                }

                break;
            case OperatorType::DateSetNow->value:
                if ($type !== ColumnType::Datetime->value) {
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
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}
