<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;

/**
 * Validates aggregate query methods such as count, sum, avg, min, max, stddev, and variance.
 */
class Aggregate extends Base
{
    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_AGGREGATE;
    }

    /**
     * Validate that the value is a valid aggregate query.
     *
     * @param mixed $value The query to validate
     * @return bool
     */
    public function isValid($value): bool
    {
        if (! $value instanceof Query) {
            $this->message = 'Value must be a Query';

            return false;
        }

        return true;
    }
}
