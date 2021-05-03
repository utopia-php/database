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
     * @var bool
     */
    protected $strict;

    /**
     * Queries constructor
     *
     * @param QueryValidator $validator
     * @param array $indexes
     * @param bool $strict
     */
    public function __construct($validator, $indexes, $strict = true)
    {
        $this->validator = $validator;
        $this->indexes = $indexes;
        $this->strict = $strict;
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
     *
     * @return bool
     */
    public function isValid($value)
    {
        /**
         * Array of attributes from Query->getAttribute()
         *
         * @var string[]
         */
        $queries = [];

        foreach ($value as $query) {
            $queries[] = $query->getAttribute(); 

            if (!$this->validator->isValid($query)) {
                $this->message = 'Query not valid: ' . $this->validator->getDescription();
                return false;
            }
        }

        /**
         * @var string
         */
        $indexId = null;
        
        // Return false if attributes do not exactly match an index
        if ($this->strict) {
            // look for strict match among indexes
            foreach ($this->indexes as $index) {
                if ($index['attributes'] === $queries) {
                    $indexId = $index['$id']; 
                }
            }

            if (!$indexId) {
                $this->message = 'Index not found for ' . implode(",", $queries);
                return false;
            }
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

    /**
     * Is Strict
     *
     * Returns true if strict validation is set
     *
     * @return bool
     */
    public function isStrict(): bool
    {
        return $this->strict;
    }
}
