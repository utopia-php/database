<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;
use Utopia\Validator;

/**
 * Abstract base class for query method validators, providing shared constants and common methods.
 */
abstract class Base extends Validator
{
    public const METHOD_TYPE_LIMIT = 'limit';

    public const METHOD_TYPE_OFFSET = 'offset';

    public const METHOD_TYPE_CURSOR = 'cursor';

    public const METHOD_TYPE_ORDER = 'order';

    public const METHOD_TYPE_FILTER = 'filter';

    public const METHOD_TYPE_SELECT = 'select';

    public const METHOD_TYPE_JOIN = 'join';

    public const METHOD_TYPE_AGGREGATE = 'aggregate';

    public const METHOD_TYPE_GROUP_BY = 'groupBy';

    public const METHOD_TYPE_HAVING = 'having';

    public const METHOD_TYPE_DISTINCT = 'distinct';

    protected string $message = 'Invalid query';

    /**
     * Get Description.
     *
     * Returns validator description
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    /**
     * Rejects anything that is not a Query, then defers to the subclass rule.
     *
     * Subclasses that validate a Query further override isValidQuery(), not
     * this, so the not-a-Query message stays the same for every method that
     * uses it.
     *
     * @param  mixed  $value
     */
    public function isValid($value): bool
    {
        if (! $value instanceof Query) {
            $this->message = 'Value must be a Query';

            return false;
        }

        return $this->isValidQuery($value);
    }

    /**
     * Validate a Query beyond its type. A method with no further rule inherits this.
     */
    protected function isValidQuery(Query $query): bool
    {
        return true;
    }

    /**
     * The column half of an `alias.column` reference must be a single plain
     * identifier, so a dotted path cannot smuggle extra segments past the
     * alias check.
     */
    protected function isAllowedJoinColumn(string $column): bool
    {
        return $column !== '' && \preg_match('/^[A-Za-z_$][A-Za-z0-9_$]*$/', $column) === 1;
    }

    /**
     * Returns what type of query this Validator is for
     */
    abstract public function getMethodType(): string;
}
