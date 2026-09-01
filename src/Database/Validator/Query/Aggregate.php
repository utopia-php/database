<?php

namespace Utopia\Database\Validator\Query;

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
}
