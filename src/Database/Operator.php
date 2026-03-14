<?php

namespace Utopia\Database;

use JsonException;
use Utopia\Database\Exception\Operator as OperatorException;

/**
 * Operator class for atomic database operations
 *
 * This class provides a structured way to perform atomic operations
 * such as increment, decrement, and array manipulations on database documents.
 */
class Operator
{
    /**
     * Construct a new operator object
     *
     * @param  array<mixed>  $values
     */
    public function __construct(
        protected OperatorType $method,
        protected string $attribute = '',
        protected array $values = [],
    ) {
    }

    /**
     * Deep clone operator values that are themselves Operator instances.
     *
     * @return void
     */
    public function __clone(): void
    {
        foreach ($this->values as $index => $value) {
            if ($value instanceof self) {
                $this->values[$index] = clone $value;
            }
        }
    }

    /**
     * Get the operator method type.
     *
     * @return OperatorType
     */
    public function getMethod(): OperatorType
    {
        return $this->method;
    }

    /**
     * Get the target attribute name.
     *
     * @return string
     */
    public function getAttribute(): string
    {
        return $this->attribute;
    }

    /**
     * Get all operator values.
     *
     * @return array<mixed>
     */
    public function getValues(): array
    {
        return $this->values;
    }

    /**
     * Get the first value, or a default if none is set.
     *
     * @param mixed $default The fallback value
     * @return mixed
     */
    public function getValue(mixed $default = null): mixed
    {
        return $this->values[0] ?? $default;
    }

    /**
     * Sets method
     *
     * @param OperatorType $method The operator method type
     * @return self
     */
    public function setMethod(OperatorType $method): self
    {
        $this->method = $method;

        return $this;
    }

    /**
     * Sets attribute
     *
     * @param string $attribute The target attribute name
     * @return self
     */
    public function setAttribute(string $attribute): self
    {
        $this->attribute = $attribute;

        return $this;
    }

    /**
     * Sets values
     *
     * @param  array<mixed>  $values
     * @return self
     */
    public function setValues(array $values): self
    {
        $this->values = $values;

        return $this;
    }

    /**
     * Sets value
     *
     * @param mixed $value The value to set
     * @return self
     */
    public function setValue(mixed $value): self
    {
        $this->values = [$value];

        return $this;
    }

    /**
     * Check if method is supported
     *
     * @param OperatorType|string $value The method to check
     * @return bool
     */
    public static function isMethod(OperatorType|string $value): bool
    {
        if ($value instanceof OperatorType) {
            return true;
        }

        return OperatorType::tryFrom($value) !== null;
    }

    /**
     * Check if method is a numeric operation
     *
     * @return bool
     */
    public function isNumericOperation(): bool
    {
        return $this->method->isNumeric();
    }

    /**
     * Check if method is an array operation
     *
     * @return bool
     */
    public function isArrayOperation(): bool
    {
        return $this->method->isArray();
    }

    /**
     * Check if method is a string operation
     *
     * @return bool
     */
    public function isStringOperation(): bool
    {
        return $this->method->isString();
    }

    /**
     * Check if method is a boolean operation
     *
     * @return bool
     */
    public function isBooleanOperation(): bool
    {
        return $this->method->isBoolean();
    }

    /**
     * Check if method is a date operation
     *
     * @return bool
     */
    public function isDateOperation(): bool
    {
        return $this->method->isDate();
    }

    /**
     * Parse operator from string
     *
     * @param string $operator JSON-encoded operator string
     * @return self
     * @throws OperatorException
     */
    public static function parse(string $operator): self
    {
        try {
            $operator = \json_decode($operator, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new OperatorException('Invalid operator: '.$e->getMessage());
        }

        if (! \is_array($operator)) {
            throw new OperatorException('Invalid operator. Must be an array, got '.\gettype($operator));
        }

        /** @var array<string, mixed> $operator */
        return self::parseOperator($operator);
    }

    /**
     * Parse operator from array
     *
     * @param  array<string, mixed>  $operator
     * @return self
     * @throws OperatorException
     */
    public static function parseOperator(array $operator): self
    {
        $method = $operator['method'] ?? '';
        $attribute = $operator['attribute'] ?? '';
        $values = $operator['values'] ?? [];

        if (! \is_string($method)) {
            throw new OperatorException('Invalid operator method. Must be a string, got '.\gettype($method));
        }

        $operatorType = OperatorType::tryFrom($method);
        if ($operatorType === null) {
            throw new OperatorException('Invalid operator method: '.$method);
        }

        if (! \is_string($attribute)) {
            throw new OperatorException('Invalid operator attribute. Must be a string, got '.\gettype($attribute));
        }

        if (! \is_array($values)) {
            throw new OperatorException('Invalid operator values. Must be an array, got '.\gettype($values));
        }

        return new self($operatorType, $attribute, $values);
    }

    /**
     * Parse an array of operators
     *
     * @param  array<string>  $operators
     * @return array<Operator>
     *
     * @throws OperatorException
     */
    public static function parseOperators(array $operators): array
    {
        return \array_map(self::parse(...), $operators);
    }

    /**
     * Convert this operator to an associative array.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'method' => $this->method->value,
            'attribute' => $this->attribute,
            'values' => $this->values,
        ];
    }

    /**
     * Serialize this operator to a JSON string.
     *
     * @return string
     * @throws OperatorException
     */
    public function toString(): string
    {
        try {
            return \json_encode($this->toArray(), flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new OperatorException('Invalid Json: '.$e->getMessage());
        }
    }

    /**
     * Helper method to create increment operator
     *
     * @param int|float $value The amount to increment by
     * @param  int|float|null  $max  Maximum value (won't increment beyond this)
     * @return self
     */
    public static function increment(int|float $value = 1, int|float|null $max = null): self
    {
        $values = [$value];
        if ($max !== null) {
            $values[] = $max;
        }

        return new self(OperatorType::Increment, '', $values);
    }

    /**
     * Helper method to create decrement operator
     *
     * @param int|float $value The amount to decrement by
     * @param  int|float|null  $min  Minimum value (won't decrement below this)
     * @return self
     */
    public static function decrement(int|float $value = 1, int|float|null $min = null): self
    {
        $values = [$value];
        if ($min !== null) {
            $values[] = $min;
        }

        return new self(OperatorType::Decrement, '', $values);
    }

    /**
     * Helper method to create array append operator
     *
     * @param  array<mixed>  $values
     * @return self
     */
    public static function arrayAppend(array $values): self
    {
        return new self(OperatorType::ArrayAppend, '', $values);
    }

    /**
     * Helper method to create array prepend operator
     *
     * @param  array<mixed>  $values
     * @return self
     */
    public static function arrayPrepend(array $values): self
    {
        return new self(OperatorType::ArrayPrepend, '', $values);
    }

    /**
     * Helper method to create array insert operator
     *
     * @param int $index The position to insert at
     * @param mixed $value The value to insert
     * @return self
     */
    public static function arrayInsert(int $index, mixed $value): self
    {
        return new self(OperatorType::ArrayInsert, '', [$index, $value]);
    }

    /**
     * Helper method to create array remove operator
     *
     * @param mixed $value The value to remove
     * @return self
     */
    public static function arrayRemove(mixed $value): self
    {
        return new self(OperatorType::ArrayRemove, '', [$value]);
    }

    /**
     * Helper method to create concatenation operator
     *
     * @param  mixed  $value  Value to concatenate (string or array)
     * @return self
     */
    public static function stringConcat(mixed $value): self
    {
        return new self(OperatorType::StringConcat, '', [$value]);
    }

    /**
     * Helper method to create replace operator
     *
     * @param string $search The substring to search for
     * @param string $replace The replacement string
     * @return self
     */
    public static function stringReplace(string $search, string $replace): self
    {
        return new self(OperatorType::StringReplace, '', [$search, $replace]);
    }

    /**
     * Helper method to create multiply operator
     *
     * @param int|float $factor The factor to multiply by
     * @param  int|float|null  $max  Maximum value (won't multiply beyond this)
     * @return self
     */
    public static function multiply(int|float $factor, int|float|null $max = null): self
    {
        $values = [$factor];
        if ($max !== null) {
            $values[] = $max;
        }

        return new self(OperatorType::Multiply, '', $values);
    }

    /**
     * Helper method to create divide operator
     *
     * @param int|float $divisor The divisor
     * @param  int|float|null  $min  Minimum value (won't divide below this)
     * @return self
     * @throws OperatorException if divisor is zero
     */
    public static function divide(int|float $divisor, int|float|null $min = null): self
    {
        if ($divisor == 0) {
            throw new OperatorException('Division by zero is not allowed');
        }
        $values = [$divisor];
        if ($min !== null) {
            $values[] = $min;
        }

        return new self(OperatorType::Divide, '', $values);
    }

    /**
     * Helper method to create toggle operator
     *
     * @return self
     */
    public static function toggle(): self
    {
        return new self(OperatorType::Toggle, '', []);
    }

    /**
     * Helper method to create date add days operator
     *
     * @param  int  $days  Number of days to add (can be negative to subtract)
     * @return self
     */
    public static function dateAddDays(int $days): self
    {
        return new self(OperatorType::DateAddDays, '', [$days]);
    }

    /**
     * Helper method to create date subtract days operator
     *
     * @param  int  $days  Number of days to subtract
     * @return self
     */
    public static function dateSubDays(int $days): self
    {
        return new self(OperatorType::DateSubDays, '', [$days]);
    }

    /**
     * Helper method to create date set now operator
     *
     * @return self
     */
    public static function dateSetNow(): self
    {
        return new self(OperatorType::DateSetNow, '', []);
    }

    /**
     * Helper method to create modulo operator
     *
     * @param  int|float  $divisor  The divisor for modulo operation
     * @return self
     * @throws OperatorException if divisor is zero
     */
    public static function modulo(int|float $divisor): self
    {
        if ($divisor == 0) {
            throw new OperatorException('Modulo by zero is not allowed');
        }

        return new self(OperatorType::Modulo, '', [$divisor]);
    }

    /**
     * Helper method to create power operator
     *
     * @param  int|float  $exponent  The exponent to raise to
     * @param  int|float|null  $max  Maximum value (won't exceed this)
     * @return self
     */
    public static function power(int|float $exponent, int|float|null $max = null): self
    {
        $values = [$exponent];
        if ($max !== null) {
            $values[] = $max;
        }

        return new self(OperatorType::Power, '', $values);
    }

    /**
     * Helper method to create array unique operator
     *
     * @return self
     */
    public static function arrayUnique(): self
    {
        return new self(OperatorType::ArrayUnique, '', []);
    }

    /**
     * Helper method to create array intersect operator
     *
     * @param  array<mixed>  $values  Values to intersect with current array
     * @return self
     */
    public static function arrayIntersect(array $values): self
    {
        return new self(OperatorType::ArrayIntersect, '', $values);
    }

    /**
     * Helper method to create array diff operator
     *
     * @param  array<mixed>  $values  Values to remove from current array
     * @return self
     */
    public static function arrayDiff(array $values): self
    {
        return new self(OperatorType::ArrayDiff, '', $values);
    }

    /**
     * Helper method to create array filter operator
     *
     * @param  string  $condition  Filter condition ('equals', 'notEquals', 'greaterThan', 'lessThan', 'null', 'notNull')
     * @param  mixed  $value  Value to filter by (not used for 'null'/'notNull' conditions)
     * @return self
     */
    public static function arrayFilter(string $condition, mixed $value = null): self
    {
        return new self(OperatorType::ArrayFilter, '', [$condition, $value]);
    }

    /**
     * Check if a value is an operator instance
     *
     * @param mixed $value The value to check
     * @return bool
     */
    public static function isOperator(mixed $value): bool
    {
        return $value instanceof self;
    }

    /**
     * Extract operators from document data
     *
     * @param  array<string, mixed>  $data
     * @return array{operators: array<string, Operator>, updates: array<string, mixed>}
     */
    public static function extractOperators(array $data): array
    {
        /** @var array<string, Operator> $operators */
        $operators = [];
        $updates = [];

        foreach ($data as $key => $value) {
            if ($value instanceof self) {
                // Set the attribute from the document key if not already set
                if (empty($value->getAttribute())) {
                    $value->setAttribute($key);
                }
                $operators[$key] = $value;
            } else {
                $updates[$key] = $value;
            }
        }

        return [
            'operators' => $operators,
            'updates' => $updates,
        ];
    }
}
