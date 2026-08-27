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
     * Validate a groupBy query has at least one non-empty attribute.
     */
    protected function isValidQuery(Query $query): bool
    {
        $columns = $query->getValues();
        if (empty($columns)) {
            $this->message = 'GroupBy requires at least one attribute';

            return false;
        }

        foreach ($columns as $column) {
            if (! \is_string($column) || $column === '') {
                $this->message = 'GroupBy attributes must be non-empty strings';

                return false;
            }
        }

        return true;
    }
}
