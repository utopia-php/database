<?php

namespace Utopia\Database\Validator;

use Throwable;
use Utopia\Database\Attribute as AttributeVO;
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
    public function __construct(
        Document $collection,
        ?Document $currentDocument = null,
        private readonly bool $supportUnsignedBigInt = true,
    ) {
        $this->collection = $collection;
        $this->currentDocument = $currentDocument;

        /** @var array<AttributeVO|Document> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        foreach ($collectionAttributes as $attribute) {
            $typed = $attribute instanceof AttributeVO ? $attribute : AttributeVO::fromDocument($attribute);
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
     * @return array{min: int|float|string, max: int|float|string}|null
     */
    private function getNumericBounds(AttributeVO $attribute): ?array
    {
        return AttributeVO::getNumericBounds($attribute->type, $attribute->signed);
    }

    private function isNumericValueInBounds(mixed $value, AttributeVO $attribute): bool
    {
        $bounds = $this->getNumericBounds($attribute);
        if ($bounds === null) {
            return false;
        }

        if (AttributeVO::isIntegerType($attribute->type)) {
            $integer = $this->getIntegerValue($value);

            return $integer !== null
                && BigInt::compare($integer, $bounds['min']) >= 0
                && BigInt::compare($integer, $bounds['max']) <= 0;
        }

        $numeric = $this->getNumericValue($value);
        if ($numeric === null) {
            return false;
        }

        if (\is_float($numeric) && ! \is_finite($numeric)) {
            return false;
        }

        return $numeric >= $bounds['min'] && $numeric <= $bounds['max'];
    }

    private function getIntegerValue(mixed $value): int|string|null
    {
        if (\is_int($value)) {
            return $value;
        }
        if (\is_string($value) && BigInt::isIntegerString($value)) {
            return BigInt::toNative($value);
        }
        if (\is_float($value) && \is_finite($value) && \floor($value) === $value && $value >= \PHP_INT_MIN && $value <= \PHP_INT_MAX) {
            return (int) $value;
        }

        return null;
    }

    private function getNumericValue(mixed $value): int|float|null
    {
        if (\is_int($value) || \is_float($value)) {
            return $value;
        }

        if (! \is_string($value) || ! \is_numeric($value)) {
            return null;
        }

        return BigInt::fitsPhpInt($value) ? (int) $value : (float) $value;
    }

    private function setNumericRangeMessage(OperatorType $method, AttributeVO $attribute, int|float|string $result): bool
    {
        $bounds = $this->getNumericBounds($attribute);
        if ($bounds === null) {
            return false;
        }

        $aboveMaximum = AttributeVO::isIntegerType($attribute->type)
            ? BigInt::compare($result, $bounds['max']) > 0
            : $result > $bounds['max'];
        if ($aboveMaximum) {
            $this->message = "Cannot apply {$method->value} operator: would overflow maximum value of {$bounds['max']}";

            return false;
        }

        $belowMinimum = AttributeVO::isIntegerType($attribute->type)
            ? BigInt::compare($result, $bounds['min']) < 0
            : $result < $bounds['min'];
        if ($belowMinimum) {
            $this->message = "Cannot apply {$method->value} operator: would underflow minimum value of {$bounds['min']}";

            return false;
        }

        return true;
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

        // Array operators that carry a caller-supplied value list are capped to guard against
        // memory exhaustion. Enforced here so every adapter rejects an oversized list the same way.
        // The payload may be spread across $values or wrapped in $values[0] (the same shape the
        // operators normalize with), so measure whichever the operator will actually process.
        if (
            \in_array($method, [
                OperatorType::ArrayAppend,
                OperatorType::ArrayPrepend,
                OperatorType::ArrayIntersect,
                OperatorType::ArrayDiff,
                OperatorType::ArrayRemove,
            ], true)
        ) {
            $payload = (isset($values[0]) && \is_array($values[0])) ? $values[0] : $values;
            if (\count($payload) > DatabaseOperator::MAX_ARRAY_OPERATOR_SIZE) {
                $this->message = "Array size " . \count($payload) . " exceeds maximum allowed size of " . DatabaseOperator::MAX_ARRAY_OPERATOR_SIZE . " for array operations";
                return false;
            }
        }

        switch ($method) {
            case OperatorType::Increment:
            case OperatorType::Decrement:
            case OperatorType::Multiply:
            case OperatorType::Divide:
            case OperatorType::Modulo:
            case OperatorType::Power:
                // Numeric operations only work on numeric types
                if (! AttributeVO::isNumericType($type)) {
                    $this->message = "Cannot apply {$methodName} operator to non-numeric field '{$operator->getAttribute()}'";

                    return false;
                }

                if (! $attribute->signed
                    && \in_array($type, [ColumnType::BigInteger, ColumnType::BigSerial], true)
                    && ! $this->supportUnsignedBigInt) {
                    $this->message = "Cannot apply {$methodName} operator: unsigned 64-bit arithmetic is not supported by this adapter";

                    return false;
                }

                // Validate the numeric value and optional max/min
                if (! isset($values[0]) || ! $this->isNumericValueInBounds($values[0], $attribute)) {
                    $this->message = "Cannot apply {$methodName} operator: value must be numeric, got ".gettype($operator->getValue());

                    return false;
                }

                // Special validation for divide/modulo by zero
                $integerType = AttributeVO::isIntegerType($type);
                $operatorValue = $integerType
                    ? $this->getIntegerValue($values[0])
                    : $this->getNumericValue($values[0]);
                if ($operatorValue === null) {
                    return false;
                }

                if (($method === OperatorType::Divide || $method === OperatorType::Modulo) && ($operatorValue === 0 || $operatorValue === 0.0)) {
                    $this->message = "Cannot apply {$methodName} operator: ".($method === OperatorType::Divide ? 'division' : 'modulo').' by zero';

                    return false;
                }

                // Validate max/min if provided
                if (\count($values) > 1 && $values[1] !== null && ! $this->isNumericValueInBounds($values[1], $attribute)) {
                    $this->message = "Cannot apply {$methodName} operator: max/min limit must be numeric, got ".\gettype($values[1]);

                    return false;
                }

                if ($this->currentDocument !== null && $integerType && ! isset($values[1])) {
                    $currentValue = $this->getIntegerValue($this->currentDocument->getAttribute($operator->getAttribute()) ?? 0);

                    if ($currentValue === null || ! $this->isNumericValueInBounds($currentValue, $attribute)) {
                        $this->message = "Cannot apply {$methodName} operator: current value is outside the attribute range";

                        return false;
                    }

                    try {
                        $predictedResult = BigInt::calculate($method, $currentValue, $operatorValue);
                    } catch (\InvalidArgumentException) {
                        $this->message = "Cannot apply {$methodName} operator: result is outside the attribute range";

                        return false;
                    }

                    if (! $this->setNumericRangeMessage($method, $attribute, $predictedResult)) {
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

                if (! empty($values) && AttributeVO::isIntegerType($type)) {
                    $newItems = \is_array($values[0]) ? $values[0] : $values;
                    foreach ($newItems as $item) {
                        if (\is_numeric($item) && ! $this->isNumericValueInBounds($item, $attribute)) {
                            $bounds = $this->getNumericBounds($attribute);
                            if ($bounds === null) {
                                return false;
                            }
                            $this->message = "Cannot apply {$methodName} operator: array items must be between {$bounds['min']} and {$bounds['max']}";

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

                if (AttributeVO::isIntegerType($type) && \is_numeric($insertValue)) {
                    if (! $this->isNumericValueInBounds($insertValue, $attribute)) {
                        $bounds = $this->getNumericBounds($attribute);
                        if ($bounds === null) {
                            return false;
                        }
                        $this->message = "Cannot apply {$methodName} operator: array items must be between {$bounds['min']} and {$bounds['max']}";

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

                if (! \in_array($values[0], DatabaseOperator::ARRAY_FILTER_CONDITIONS, true)) {
                    $this->message = "Invalid array filter condition '{$values[0]}'. Must be one of: ".\implode(', ', DatabaseOperator::ARRAY_FILTER_CONDITIONS);

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
