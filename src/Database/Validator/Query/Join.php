<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;

/**
 * Validates join query methods ensuring a target table name is specified.
 */
class Join extends Base
{
    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_JOIN;
    }

    /**
     * Validate that the value is a valid join query with a table name.
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

        $table = $value->getAttribute();
        if (empty($table)) {
            $this->message = 'Join requires a table name';

            return false;
        }

        return true;
    }
}
