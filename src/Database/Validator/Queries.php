<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Database;
use Utopia\Database\Document;
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
     * @param Document[] $indexes
     * @param bool $strict
     */
    public function __construct($validator, $indexes, $strict = true)
    {
        $this->validator = $validator;

        $this->indexes[] = [
            'type' => Database::INDEX_UNIQUE,
            'attributes' => ['$id']
        ];

        foreach ($indexes as $index) {
            $this->indexes[] = $index->getArrayCopy(['attributes', 'type']);
        }

        $this->strict = $strict;
    }

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
     * Is valid.
     *
     * Returns true if all $queries are valid as a set.
     * @param mixed $value as array of Query objects
     * @return bool
     */
    public function isValid($value): bool
    {
        $queries = [];
        $queryAttributes = [];

        foreach ($value as $query) {
            /**
             * @var Query $query
             */
            // Single Query Validation
            if (!$this->validator->isValid($query)) {
                $this->message = 'Query not valid: ' . $this->validator->getDescription();
                return false;
            }

            $queryAttributes[] = $query->getAttribute();

            $queries[] = [
                'attribute' => $query->getAttribute(),
                'operator' => $query->getOperator(),
                'values' => $query->getValues()
            ];
        }

        $flag = false;
        $message = 'Index not found: ' . implode(',', $queryAttributes);

        if ($this->strict) {
           foreach ($this->indexes as $index) { // loop through all indexes
                $tmp =  $queries; // set attributes origin
                foreach ($index['attributes'] as $indexKey => $indexAttr){
                    foreach ($tmp as $query) {
                        if($query['attribute'] === $indexAttr){ // found match
                            if($query['operator'] === Query::TYPE_SEARCH){
                                if($index['type'] === Database::INDEX_FULLTEXT){
                                    return true;
                                }else{
                                    $message = 'Search operator requires fulltext index: '.$query['attribute'];
                                }
                            }
                            else {
                                unset($tmp[$indexKey]);
                            }
                        }
                        else {
                            break;
                        }
                    }
                }

                if(count($tmp) === 0){ // full index match!
                    $flag = true;
                    break;
                }
            }

            if($flag === false){
                $this->message = $message;
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
