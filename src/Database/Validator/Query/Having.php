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
     * Validate a having query has at least one condition, each a Query.
     */
    protected function isValidQuery(Query $query): bool
    {
        $conditions = $query->getValues();
        if (empty($conditions)) {
            $this->message = 'Having requires at least one condition';

            return false;
        }

        foreach ($conditions as $condition) {
            if (! $condition instanceof Query) {
                $this->message = 'Having conditions must be Query instances';

                return false;
            }
        }

        return true;
    }
}
