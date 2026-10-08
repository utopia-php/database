<?php

namespace Utopia\Database\Validator\Query;

/**
 * Validates distinct query methods for deduplicating result sets.
 */
class Distinct extends Base
{
    #[\Override]
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_DISTINCT;
    }
}
