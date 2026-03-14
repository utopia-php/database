<?php

namespace Utopia\Database;

/**
 * Defines the types of atomic operations that can be performed on document attributes.
 */
enum OperatorType: string
{
    // Numeric operations
    case Increment = 'increment';
    case Decrement = 'decrement';
    case Modulo = 'modulo';
    case Power = 'power';
    case Multiply = 'multiply';
    case Divide = 'divide';

    // Array operations
    case ArrayAppend = 'arrayAppend';
    case ArrayPrepend = 'arrayPrepend';
    case ArrayInsert = 'arrayInsert';
    case ArrayRemove = 'arrayRemove';
    case ArrayUnique = 'arrayUnique';
    case ArrayIntersect = 'arrayIntersect';
    case ArrayDiff = 'arrayDiff';
    case ArrayFilter = 'arrayFilter';

    // String operations
    case StringConcat = 'stringConcat';
    case StringReplace = 'stringReplace';

    // Boolean operations
    case Toggle = 'toggle';

    // Date operations
    case DateAddDays = 'dateAddDays';
    case DateSubDays = 'dateSubDays';
    case DateSetNow = 'dateSetNow';

    /**
     * Check if this operator type is a numeric operation.
     *
     * @return bool
     */
    public function isNumeric(): bool
    {
        return match ($this) {
            self::Increment,
            self::Decrement,
            self::Multiply,
            self::Divide,
            self::Modulo,
            self::Power => true,
            default => false,
        };
    }

    /**
     * Check if this operator type is an array operation.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return match ($this) {
            self::ArrayAppend,
            self::ArrayPrepend,
            self::ArrayInsert,
            self::ArrayRemove,
            self::ArrayUnique,
            self::ArrayIntersect,
            self::ArrayDiff,
            self::ArrayFilter => true,
            default => false,
        };
    }

    /**
     * Check if this operator type is a string operation.
     *
     * @return bool
     */
    public function isString(): bool
    {
        return match ($this) {
            self::StringConcat,
            self::StringReplace => true,
            default => false,
        };
    }

    /**
     * Check if this operator type is a boolean operation.
     *
     * @return bool
     */
    public function isBoolean(): bool
    {
        return match ($this) {
            self::Toggle => true,
            default => false,
        };
    }

    /**
     * Check if this operator type is a date operation.
     *
     * @return bool
     */
    public function isDate(): bool
    {
        return match ($this) {
            self::DateAddDays,
            self::DateSubDays,
            self::DateSetNow => true,
            default => false,
        };
    }
}
