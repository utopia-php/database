<?php

namespace Utopia\Database;

class Query
{
    const TYPE_EQUAL = 'equal';
    const TYPE_NOTEQUAL = 'notEqual';
    
    /**
     * @var string
     */
    protected $attribute = '';

    /**
     * @var string
     */
    protected $operator = '';

    /**
     * @var (mixed)[]
     */
    protected $values;

    /**
     * Construct.
     *
     * Construct a new query object
     *
     * @param string $attribute
     * @param string $operator
     * @param array $values
     */
    public function __construct(string $attribute, string $operator, array $values)
    {
        $this->attribute = $attribute;
        $this->operator = $operator;
        $this->values = $values;
    }

    /**
     * Get attribute
     *
     * @return string
     */
    public function getAttribute(): string
    {
        return $this->attribute;
    }

    /**
     * Get operator
     *
     * @return string
     */
    public function getOperator(): string
    {
        return $this->operator;
    }

    /**
     * Get operand
     *
     * @return mixed
     */
    public function getValues()
    {
        return $this->values;
    }

    /**
     * Get all query details as array
     *
     * @return array
     */
    public function getQuery(): array
    {
        return [
            'attribute' => $this->attribute,
            'operator' => $this->operator,
            'values' => $this->values,
        ];
    }

    /**
     * Parse query filter
     *
     * @param string $filter 
     *
     * @return Query
     * */
    public static function parse(string $filter): Query
    {
        // get index of open parentheses
        $end = mb_strpos($filter, '(');
        // count stanzas by only counting '.' that come before open parentheses
        $stanzas = mb_substr_count(mb_substr($filter, 0, $end), ".") + 1;

        // TODO@kodumbeats handle relations between collections, e.g. if($stanzas > 2)
        switch ($stanzas) {
            case 2:
                // use limit param to ignore '.' in $expression
                $input = explode('.', $filter, $stanzas);
                $attribute = $input[0];
                $expression = $input[1];
                [$operator, $values] = self::parseExpression($expression);
                break;
        }

        return new Query($attribute, $operator, $values);
    }

    /**
     * Get attribute key-value from query expression
     * $expression: string with format 'operator(value)'
     *
     * @param string $expression
     *
     * @return (string|array)[]
     */
    protected static function parseExpression(string $expression): array
    {
        //find location of parentheses in expression

        /** @var int */
        $start = mb_strpos($expression, '(');
        /** @var int */
        $end = mb_strrpos($expression, ')');

        //extract the query method

        /** @var string */
        $operator = mb_substr($expression, 0, $start);

        //grab everything inside parentheses

        /** @var mixed */
        $value = mb_substr($expression, 
            ($start + 1), /* exclude open paren*/ 
            ($end - $start - 1) /* exclude closed paren*/
        );

        // Explode comma-separated values

        $values = explode(',', $value);

        // Cast $value type

        $values = array_map(function ($value) {

            // Trim whitespace from around $value

            $value = trim($value);

            switch (true) {
                // type casted to int or float by "+" operator
                case is_numeric($value):
                    return $value + 0;

                // since (bool)"false" returns true, check bools manually
                case $value === 'true':
                    return true;

                case $value === 'false':
                    return false;

                // need special case to cast (null) as null, not string
                case $value === 'null':
                    return null;

                default:
                    // strip escape characters
                    $value = stripslashes($value);
                    // trim leading and tailing quotes
                    return trim($value, '\'"');
            }

        }, $values);

        return [$operator, $values];
    }
}
