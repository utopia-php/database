<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Validator\Numeric;
use Utopia\Validator\Range;

/**
 * Validates limit query methods ensuring the value is a positive integer within the allowed range.
 */
class Limit extends Base
{
    protected int $maxLimit;

    /**
     * Query constructor
     */
    public function __construct(int $maxLimit = PHP_INT_MAX)
    {
        $this->maxLimit = $maxLimit;
    }

    /**
     * Is valid.
     *
     * Returns true if method is limit values are within range.
     *
     * @param  mixed  $value
     */
    public function isValid($value): bool
    {
        if (! $value instanceof Query) {
            return false;
        }

        if ($value->getMethod() !== Method::Limit) {
            $this->message = 'Invalid query method: '.$value->getMethod()->value;

            return false;
        }

        $limit = $value->getValue();

        $validator = new Numeric();
        if (! $validator->isValid($limit)) {
            $this->message = 'Invalid limit: '.$validator->getDescription();

            return false;
        }

        $validator = new Range(1, $this->maxLimit);
        if (! $validator->isValid($limit)) {
            $this->message = 'Invalid limit: '.$validator->getDescription();

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
        return self::METHOD_TYPE_LIMIT;
    }
}
