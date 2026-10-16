<?php

namespace Utopia\Database\Validator;

use Utopia\Validator\FloatValidator;

/**
 * A float or integer that is neither infinite nor NaN, which no engine stores or compares as a double.
 */
class Finite extends FloatValidator
{
    #[\Override]
    public function getDescription(): string
    {
        return 'Value must be a finite float';
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! parent::isValid($value)) {
            return false;
        }

        return \is_numeric($value) && \is_finite((float) $value);
    }
}
