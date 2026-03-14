<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Validator\Numeric;
use Utopia\Validator\Range;

/**
 * Validates offset query methods ensuring the value is a non-negative integer within the allowed range.
 */
class Offset extends Base
{
    protected int $maxOffset;

    /**
     * Create a new offset query validator.
     *
     * @param int $maxOffset Maximum allowed offset value
     */
    public function __construct(int $maxOffset = PHP_INT_MAX)
    {
        $this->maxOffset = $maxOffset;
    }

    /**
     * Validate that the value is a valid offset query within the allowed range.
     *
     * @param mixed $value The query to validate
     * @return bool
     */
    public function isValid($value): bool
    {
        if (! $value instanceof Query) {
            return false;
        }

        $method = $value->getMethod();

        if ($method !== Method::Offset) {
            $this->message = 'Query method invalid: '.$method->value;

            return false;
        }

        $offset = $value->getValue();

        $validator = new Numeric();
        if (! $validator->isValid($offset)) {
            $this->message = 'Invalid limit: '.$validator->getDescription();

            return false;
        }

        $validator = new Range(0, $this->maxOffset);
        if (! $validator->isValid($offset)) {
            $this->message = 'Invalid offset: '.$validator->getDescription();

            return false;
        }

        return true;
    }

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_OFFSET;
    }
}
