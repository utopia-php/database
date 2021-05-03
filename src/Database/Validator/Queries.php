<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Validator\QueryValidator;
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
     * @param mixed $value as array of Query objects
     * @param bool $strict
     *
     * @return bool
     */
    public function isValid($value, $strict = true)
    {
        /**
         * Array of attributes from $value
         *
         * @var string[]
         */
        $found = [];

        foreach ($value as $query) {
            // individually validate $query
            if (!$this->validator->isValid($query)) {
                $this->message = 'Query not valid: ' . $this->validator->getDescription();
                return false;
            }

            // $attribute = array_search($query->getAttribute(), $indexes);
            // TODO@kodumbeats add logic to check all attributes can be found in a single index

            // if ($strict && !$attribute) {
            //     $this->message = 'Index does not exist for ' . $query->getAttribute();
            //     return false;
            // }
        }

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
