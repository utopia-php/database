<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;
use Utopia\Query\Method;

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

        if ($value->getMethod() === Method::NaturalJoin) {
            $this->message = 'Natural joins are not supported';

            return false;
        }

        $table = $value->getAttribute();
        if (empty($table)) {
            $this->message = 'Join requires a table name';

            return false;
        }

        if (! $value->isNestedJoin()) {
            return true;
        }

        $onQueries = $value->getJoinOnQueries();
        if ($onQueries === []) {
            $this->message = 'Join ON requires at least one condition';

            return false;
        }

        $allowedOperators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        foreach ($onQueries as $onQuery) {
            if ($onQuery->getMethod() !== Method::On) {
                continue;
            }

            $values = $onQuery->getValues();
            $left = $values[0] ?? '';
            $operator = $values[1] ?? '=';
            $right = $values[2] ?? '';
            if (! \is_string($left) || $left === '' || ! \is_string($right) || $right === '') {
                $this->message = 'Join ON requires left and right columns';

                return false;
            }
            if (! \is_string($operator) || ! \in_array($operator, $allowedOperators, true)) {
                $this->message = 'Invalid join operator: '.(\is_string($operator) ? $operator : \gettype($operator));

                return false;
            }
        }

        return true;
    }
}
