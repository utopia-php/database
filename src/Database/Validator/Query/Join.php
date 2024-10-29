<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;

class Join extends Base
{
    protected array $types = [Query::TYPE_INNER_JOIN, Query::TYPE_LEFT_JOIN, Query::TYPE_RIGHT_JOIN];

    /**
     * Is valid.
     *
     * @param  Query  $value
     */
    public function isValid($value): bool
    {
        var_dump('Validating join');
        var_dump($value);

        if (! $value instanceof Query) {
            return false;
        }

        $method = $value->getMethod();

        if ($method === Query::TYPE_JOIN) {
            if (! in_array($value->getType(), $this->types)) {
                $this->message = 'Invalid join type';

                return false;
            }

            return true;
        }

        return false;
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_JOIN;
    }
}
