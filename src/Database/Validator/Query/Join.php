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
     * Validate a join query names a table, and that any ON conditions are well formed.
     */
    protected function isValidQuery(Query $query): bool
    {
        if ($query->getMethod() === Method::NaturalJoin) {
            $this->message = 'Natural joins are not supported';

            return false;
        }

        $table = $query->getAttribute();
        if (empty($table)) {
            $this->message = 'Join requires a table name';

            return false;
        }

        if (! $query->isNestedJoin()) {
            return true;
        }

        $onQueries = $query->getJoinOnQueries();
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
