<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Validator\QueryValidator;
use Utopia\Database\Query;

class Queries extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Invalid queries';

    /**
     * @var QueryValidator
     */
    protected $validator;

    /**
     * @var array
     */
    protected $indexes = [];

    /**
     * Queries constructor
     *
     * @param QueryValidator $validator
     * @param array $indexes
     */
    public function __construct($validator, $indexes)
    {
        $this->validator = $validator;
        $this->indexes = $indexes;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription()
    {
        return $this->message;
    }

    /**
     * Is valid.
     *
     * Returns true if all $queries are valid as a set.
     *
     * @param Query[] $queries
     * @param bool $strict
     *
     * @return bool
     */
    public function isValid($queries, $strict = true)
    {
        return true;
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
        return true;
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
}
