<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Validator;

abstract class Base extends Validator
{
    public const METHOD_TYPE_LIMIT = 'limit';
    public const METHOD_TYPE_OFFSET = 'offset';
    public const METHOD_TYPE_CURSOR = 'cursor';
    public const METHOD_TYPE_ORDER = 'order';
    public const METHOD_TYPE_FILTER = 'filter';
    public const METHOD_TYPE_SELECT = 'select';

    protected string $message = 'Invalid query';

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    /**
     * Returns what type of query this Validator is for
     */
    abstract public function getMethodType(): string;
}
