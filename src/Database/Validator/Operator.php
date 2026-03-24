<?php

namespace Utopia\Database\Validator;

use Throwable;
use Utopia\Database\Attribute as AttributeVO;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator as DatabaseOperator;
use Utopia\Database\OperatorType;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;

/**
 * Validates update operators (increment, append, toggle, etc.) against collection attribute types and constraints.
 */
class Operator extends Validator
{
    protected Document $collection;

    /**
     * @var array<string, AttributeVO>
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

        /** @var array<Document> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        foreach ($collectionAttributes as $attribute) {
            $typed = AttributeVO::fromDocument($attribute);
            $this->attributes[$typed->key] = $typed;
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
     */
    private function isRelationshipArray(AttributeVO $attribute): bool
    {
        $options = $attribute->options ?? [];

        /** @var array<string, mixed> $options */

        $relationTypeRaw = $options['relationType'] ?? '';
        $sideRaw = $options['side'] ?? '';

        $relationType = $relationTypeRaw instanceof RelationType
            ? $relationTypeRaw
            : (\is_string($relationTypeRaw) && $relationTypeRaw !== '' ? RelationType::from($relationTypeRaw) : null);
        $side = $sideRaw instanceof RelationSide
            ? $sideRaw
            : (\is_string($sideRaw) && $sideRaw !== '' ? RelationSide::from($sideRaw) : null);

        // Many-to-many is always an array on both sides
        if ($relationType === RelationType::ManyToMany) {
            return true;
        }

        // One-to-many: array on parent side, single on child side
        if ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) {
            return true;
        }

        // Many-to-one: array on child side, single on parent side
        if ($relationType === RelationType::ManyToOne && $side === RelationSide::Child) {
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
                /** @var string $valueStr */
                $valueStr = $value;
                $value = DatabaseOperator::parse($valueStr);
            } catch (Throwable $e) {
                $this->message = 'Invalid operator: '.$e->getMessage();

                return false;
            }
        }

        $method = $value->getMethod();
        $attribute = $value->getAttribute();

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
     */
    private function validateOperatorForAttribute(
        DatabaseOperator $operator,
        AttributeVO $attribute
    ): bool {
        $method = $operator->getMethod();
        $methodName = $method->value;
        $values = $operator->getValues();

        $type = $attribute->type;
        $isArray = $attribute->array;

        switch ($method) {
            case OperatorType::Increment:
            case OperatorType::Decrement:
            case OperatorType::Multiply:
            case OperatorType::Divide:
            case OperatorType::Modulo:
            case OperatorType::Power:
                // Numeric operations only work on numeric types
                if (! \in_array($type, [ColumnType::Integer, ColumnType::Double])) {
                    $this->message = "Cannot apply {$methodName} operator to non-numeric field '{$operator->getAttribute()}'";

                    return false;
                }

                // Validate the numeric value and optional max/min
                if (! isset($values[0]) || ! \is_numeric($values[0])) {
                    $this->message = "Cannot apply {$methodName} operator: value must be numeric, got ".gettype($operator->getValue());

                    return false;
                }

                // Special validation for divide/modulo by zero
                if (($method === OperatorType::Divide || $method === OperatorType::Modulo) && (float) $values[0] === 0.0) {
                    $this->message = "Cannot apply {$methodName} operator: ".($method === OperatorType::Divide ? 'division' : 'modulo').' by zero';

                    return false;
                }

                // Validate max/min if provided
                if (\count($values) > 1 && $values[1] !== null && ! \is_numeric($values[1])) {
                    $this->message = "Cannot apply {$methodName} operator: max/min limit must be numeric, got ".\gettype($values[1]);

                    return false;
                }

                if ($this->currentDocument !== null && $type === ColumnType::Integer && ! isset($values[1])) {
                    /** @var int|float $currentValue */
                    $currentValue = $this->currentDocument->getAttribute($operator->getAttribute()) ?? 0;
                    /** @var int|float $operatorValue */
                    $operatorValue = $values[0];

                    // Compute predicted result
                    $predictedResult = match ($method) {
                        OperatorType::Increment => $currentValue + $operatorValue,
                        OperatorType::Decrement => $currentValue - $operatorValue,
                        OperatorType::Multiply => $currentValue * $operatorValue,
                        OperatorType::Divide => $currentValue / $operatorValue,
                        OperatorType::Modulo => (int) $currentValue % (int) $operatorValue,
                        OperatorType::Power => $currentValue ** $operatorValue,
                    };

                    if ($predictedResult > Database::MAX_INT) {
                        $this->message = "Cannot apply {$methodName} operator: would overflow maximum value of ".Database::MAX_INT;

                        return false;
                    }

                    if ($predictedResult < Database::MIN_INT) {
                        $this->message = "Cannot apply {$methodName} operator: would underflow minimum value of ".Database::MIN_INT;

                        return false;
                    }
                }

                break;
            case OperatorType::ArrayAppend:
            case OperatorType::ArrayPrepend:
                // For relationships, check if it's a "many" side
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                    foreach ($values as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$methodName} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$methodName} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (! empty($values) && $type === ColumnType::Integer) {
                    $newItems = \is_array($values[0]) ? $values[0] : $values;
                    foreach ($newItems as $item) {
                        if (\is_numeric($item) && ($item > Database::MAX_INT || $item < Database::MIN_INT)) {
                            $this->message = "Cannot apply {$methodName} operator: array items must be between ".Database::MIN_INT.' and '.Database::MAX_INT;

                            return false;
                        }
                    }
                }

                break;
            case OperatorType::ArrayUnique:
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$methodName} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                break;
            case OperatorType::ArrayInsert:
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$methodName} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (\count($values) !== 2) {
                    $this->message = "Cannot apply {$methodName} operator: requires exactly 2 values (index and value)";

                    return false;
                }

                $index = $values[0];
                if (! \is_int($index) || $index < 0) {
                    $this->message = "Cannot apply {$methodName} operator: index must be a non-negative integer";

                    return false;
                }

                $insertValue = $values[1];

                if ($type === ColumnType::Relationship) {
                    if (! $this->isValidRelationshipValue($insertValue)) {
                        $this->message = "Cannot apply {$methodName} operator: relationship values must be document IDs (strings) or Document objects";

                        return false;
                    }
                }

                if ($type === ColumnType::Integer && \is_numeric($insertValue)) {
                    if ($insertValue > Database::MAX_INT || $insertValue < Database::MIN_INT) {
                        $this->message = "Cannot apply {$methodName} operator: array items must be between ".Database::MIN_INT.' and '.Database::MAX_INT;

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
                            $this->message = "Cannot apply {$methodName} operator: index {$index} is out of bounds for array of length {$arrayLength}";

                            return false;
                        }
                    }
                }

                break;
            case OperatorType::ArrayRemove:
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                    $toValidate = \is_array($values[0]) ? $values[0] : $values;
                    foreach ($toValidate as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$methodName} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$methodName} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values)) {
                    $this->message = "Cannot apply {$methodName} operator: requires a value to remove";

                    return false;
                }

                break;
            case OperatorType::ArrayIntersect:
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot use {$methodName} operator on non-array attribute '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values)) {
                    $this->message = "{$methodName} operator requires a non-empty array value";

                    return false;
                }

                if ($type === ColumnType::Relationship) {
                    foreach ($values as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$methodName} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                }

                break;
            case OperatorType::ArrayDiff:
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                    foreach ($values as $item) {
                        if (! $this->isValidRelationshipValue($item)) {
                            $this->message = "Cannot apply {$methodName} operator: relationship values must be document IDs (strings) or Document objects";

                            return false;
                        }
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot use {$methodName} operator on non-array attribute '{$operator->getAttribute()}'";

                    return false;
                }

                break;
            case OperatorType::ArrayFilter:
                if ($type === ColumnType::Relationship) {
                    if (! $this->isRelationshipArray($attribute)) {
                        $this->message = "Cannot apply {$methodName} operator to single-value relationship '{$operator->getAttribute()}'";

                        return false;
                    }
                } elseif (! $isArray) {
                    $this->message = "Cannot apply {$methodName} operator to non-array field '{$operator->getAttribute()}'";

                    return false;
                }

                if (\count($values) < 1 || \count($values) > 2) {
                    $this->message = "Cannot apply {$methodName} operator: requires 1 or 2 values (condition and optional comparison value)";

                    return false;
                }

                if (! \is_string($values[0])) {
                    $this->message = "Cannot apply {$methodName} operator: condition must be a string";

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
            case OperatorType::StringConcat:
                if (! \in_array($type, [ColumnType::String, ColumnType::Varchar, ColumnType::Text, ColumnType::MediumText, ColumnType::LongText]) || $isArray) {
                    $this->message = "Cannot apply {$methodName} operator to non-string field '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values) || ! \is_string($values[0])) {
                    $this->message = "Cannot apply {$methodName} operator: requires a string value";

                    return false;
                }

                if ($this->currentDocument !== null && \in_array($type, [ColumnType::String, ColumnType::Varchar, ColumnType::Text, ColumnType::MediumText, ColumnType::LongText])) {
                    /** @var string $currentString */
                    $currentString = $this->currentDocument->getAttribute($operator->getAttribute()) ?? '';
                    $concatValue = $values[0];
                    $predictedLength = strlen($currentString) + strlen((string) $concatValue);

                    $maxSize = $attribute->size;

                    if ($maxSize > 0 && $predictedLength > $maxSize) {
                        $this->message = "Cannot apply {$methodName} operator: result would exceed maximum length of {$maxSize} characters";

                        return false;
                    }
                }

                break;
            case OperatorType::StringReplace:
                // Replace only works on string types
                if (! \in_array($type, [ColumnType::String, ColumnType::Varchar, ColumnType::Text, ColumnType::MediumText, ColumnType::LongText])) {
                    $this->message = "Cannot apply {$methodName} operator to non-string field '{$operator->getAttribute()}'";

                    return false;
                }

                if (\count($values) !== 2 || ! \is_string($values[0]) || ! \is_string($values[1])) {
                    $this->message = "Cannot apply {$methodName} operator: requires exactly 2 string values (search and replace)";

                    return false;
                }

                break;
            case OperatorType::Toggle:
                // Toggle only works on boolean types
                if ($type !== ColumnType::Boolean) {
                    $this->message = "Cannot apply {$methodName} operator to non-boolean field '{$operator->getAttribute()}'";

                    return false;
                }

                break;
            case OperatorType::DateAddDays:
            case OperatorType::DateSubDays:
                if ($type !== ColumnType::Datetime) {
                    $this->message = "Cannot apply {$methodName} operator to non-datetime field '{$operator->getAttribute()}'";

                    return false;
                }

                if (empty($values) || ! \is_int($values[0])) {
                    $this->message = "Cannot apply {$methodName} operator: requires an integer number of days";

                    return false;
                }

                break;
            case OperatorType::DateSetNow:
                if ($type !== ColumnType::Datetime) {
                    $this->message = "Cannot apply {$methodName} operator to non-datetime field '{$operator->getAttribute()}'";

                    return false;
                }

                break;
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
