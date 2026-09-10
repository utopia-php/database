<?php

namespace Utopia\Database\Validator\Query;

/**
 * Validates distinct query methods for deduplicating result sets.
 */
class Distinct extends Base
{
    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_DISTINCT;
    }
}
