<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;

/**
 * Validates having query methods ensuring at least one condition is specified.
 */
class Having extends Base
{
    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_HAVING;
    }

    /**
     * Validate that the value is a valid having query with at least one condition.
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

        $conditions = $value->getValues();
        if (empty($conditions)) {
            $this->message = 'Having requires at least one condition';

            return false;
        }

        return true;
    }
}
