<?php

namespace Utopia\Database;

class Query
{
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
        // TODO@kodumbeats handle '.' in expression value 
        $stanzas = mb_substr_count($filter, ".") + 1;

        // TODO@kodumbeats handle relations between collections, e.g. if($stanzas > 2)
        switch ($stanzas) {
            case 2:
                $input = explode('.', $filter);
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
        $end = mb_strpos($expression, ')');

        //extract the query method

        /** @var string */
        $operator = mb_substr($expression, 0, $start);

        //grab everything inside parentheses

        /** @var mixed */
        $value = mb_substr($expression, 
            ($start + 1), /* exclude open paren*/ 
            ($end - $start - 1) /* exclude closed paren*/
        );

        //strip quotes from queries of type string
        $value = str_replace('"', "", $value);
        $value = str_replace("'", "", $value);

        // if $value is not array, return array with single $value
        // TODO@kodumbeats appropriately cast type of ints, floats, bools
        if (gettype($value) !== 'array') {
            $value = [$value];
        }

        return [$operator, $value];
    }
}
