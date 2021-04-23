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
     * @var mixed
     */
    protected $value;

    /**
     * Construct.
     *
     * Construct a new query object
     *
     * @param string $attribute
     * @param string $operator
     * @param mixed $value
     */
    public function __construct(string $attribute, string $operator, $value)
    {
        $this->attribute = $attribute;
        $this->operator = $operator;
        $this->value = $value;
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
    public function getOperand()
    {
        return $this->operand;
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
            'operand' => $this->operand,
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
        switch ($stanzas):
            case 2:
                $input = explode('.', $filter);
                $attribute = $input[0];
                [$operator, $value] = Query::parseExpression($input[1]);
                break;
        endswitch;

        return new Query($attribute, $operator, $value);
    }

    /**
     * Get attribute key-value from query expression
     * $expression: string with format 'operator(operand)'
     *
     * @param string $expression
     *
     * @return (string|mixed)[]
     */
    public static function parseExpression(string $expression): array
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

        return [$operator, $value];
    }

    // /**
    //  * Validate query against collection schema
    //  *
    //  * @param array $schema Structured array of collection attributes
    //  *
    //  * @return bool
    //  */
    // protected function isValid($schema): bool
    // {
    //     $attributeType = $schema[array_search($attribute, array_column($schema, '$id'))]['type'];
    // }
}
