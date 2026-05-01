<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;

/**
 * Validates groupBy query methods ensuring at least one grouping attribute is specified.
 */
class GroupBy extends Base
{
    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_GROUP_BY;
    }

    /**
     * Validate that the value is a valid groupBy query with at least one attribute.
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

        $columns = $value->getValues();
        if (empty($columns)) {
            $this->message = 'GroupBy requires at least one attribute';

            return false;
        }

        return true;
    }
}
