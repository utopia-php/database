<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;

class Join extends Base
{
    /**
     * @param Query $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (!$value instanceof Query) {
            return false;
        }

        $method = $value->getMethod();

        if (!in_array($method, [
            Query::TYPE_INNER_JOIN,
            Query::TYPE_LEFT_JOIN,
            Query::TYPE_RIGHT_JOIN,
            Query::TYPE_FULL_JOIN,
        ])) {
            return false;
        }

        if (empty($value->getAttribute())) {
            $this->message = 'Join query requires a collection ID';
            return false;
        }

        $values = $value->getValues();

        if (empty($values) || empty($values[0])) {
            $this->message = 'Join query requires an ON clause';
            return false;
        }

        return true;
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_FILTER; // Joins are treated similar to filters in grouping
    }
}
