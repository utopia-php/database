<?php

namespace Utopia\Database;

class Query
{
    // Filter methods
    const TYPE_EQUAL = 'equal';
    const TYPE_NOTEQUAL = 'notEqual';
    const TYPE_LESSER = 'lessThan';
    const TYPE_LESSEREQUAL = 'lessThanEqual';
    const TYPE_GREATER = 'greaterThan';
    const TYPE_GREATEREQUAL = 'greaterThanEqual';
    const TYPE_CONTAINS = 'contains';
    const TYPE_SEARCH = 'search';

    // Order methods
    const TYPE_ORDERDESC = 'orderDesc';
    const TYPE_ORDERASC = 'orderAsc';

    // Pagination methods
    const TYPE_LIMIT = 'limit';
    const TYPE_OFFSET = 'offset';
    const TYPE_CURSORAFTER = 'cursorAfter';
    const TYPE_CURSORBEFORE = 'cursorBefore';

    public static mixed $TYPE_ALIASES; // Filled from constructor

    protected string $method = '';

    protected array $params = [];

    /**
     * Construct a new query object
     */
    public function __construct(string $method, array $params)
    {
        $this->method = $method;
        $this->params = $params;
    }

    public function getMethod(): string
    {
        return $this->method;
    }

    public function getParams(): array
    {
        return $this->params;
    }

    /**
     * Helper method returning first param. In first param we often store attribute
     */
    public function getFirstParam(): mixed
    {
        return $this->params[0];
    }

    /**
     * Helper method changing first param. In first param we often store attribute
     */
    public function setFirstParam(mixed $value): void
    {
        $this->params[0] = $value;
    }

    /**
     * Helper method. Returns param, but in form of array array 
     */
    public function getArrayParam(int $index): array
    {
        if(\is_array($this->params[$index])) {
            return $this->params[$index];
        }

        return [ $this->params[$index] ];
    }

    public function setMethod(string $method): self
    {
        $this->method = $method;

        return $this;
    }

    public function setParams(array $params): self
    {
        $this->params = $params;

        return $this;
    }

    /**
     * Check if method is supported
     */
    public static function isMethod(string $value): bool
    {
        switch ($value) {
            case self::TYPE_EQUAL:
            case self::TYPE_NOTEQUAL:
            case self::TYPE_LESSER:
            case self::TYPE_LESSEREQUAL:
            case self::TYPE_GREATER:
            case self::TYPE_GREATEREQUAL:
            case self::TYPE_CONTAINS:
            case self::TYPE_SEARCH:
            case self::TYPE_ORDERASC:
            case self::TYPE_ORDERDESC:
            case self::TYPE_LIMIT:
            case self::TYPE_OFFSET:
            case self::TYPE_CURSORAFTER:
            case self::TYPE_CURSORBEFORE:
                return true;
        }

        if(\array_key_exists($value, self::$TYPE_ALIASES)) {
            return true;
        }

        return false;
    }

    /**
     * Parse query filter
     * */
    public static function parse(string $filter): Query
    {
        // Init empty vars we fill later
        $method = '';
        $params = [];

        // Separate method from filter
        $paramsStart = mb_strpos($filter, '(');
        $method = mb_substr($filter, 0, $paramsStart);

        // Separate params from filter
        $paramsEnd = \strlen($filter) - 1; // -1 to ignore )
        $parametersStart = $paramsStart + 1; // +1 to ignore (

        // Check for deprecated query syntax
        if(\str_contains($method, '.')) {
            throw new \Exception("Invalid query method");
        }

        $currentParam = ""; // We build param here before pushing when it's ended
        $currentArrayParam = []; // We build array param here before pushing when it's ended
        $stack = []; // Stack of syntactical symbols

        // Utility method to know if we are inside string
        $isInStringStack = function() use (&$stack) {
            if(
                \count($stack) > 0 && // Stack is not empty
                ($stack[\count($stack) - 1] === '"' || $stack[\count($stack) - 1] === '\'')) // Stack ends with string symbol
            {
                return true;
            }

            return false;
        };

        // Utility method to know if we are inside array
        $isInArrayStack = function() use (&$stack) {
            if(
                \count($stack) > 0 && // Stack is not empty
                $stack[\count($stack) - 1] === '[') // Stack ends with array symbol
            {
                return true;
            }

            return false;
        };

        // Utility method to only add symbol is relevant
        $addSymbol = function (string $char, int $index) use (&$filter, &$currentParam, $isInStringStack) {
            $nextChar = $filter[$index + 1] ?? '';
            if(
                $char === '\\' && // Current char might be escaping
                ($nextChar === '"' || $nextChar === '\'') // Next char must be string syntax symbol
            ) {
                return;
            }

            // Ignore spaces and commas outside of string
            if($char === ' ' || $char === ',') {
                if(\call_user_func($isInStringStack)) {
                    $currentParam .= $char;
                }
            } else {
                $currentParam .= $char;
            }
        };

        // Loop thorough all characters
        for($i = $parametersStart; $i < $paramsEnd; $i++) {
            $char = $filter[$i];

            // String support + escaping support
            if(
                ($char === '"' || $char === '\'') && // Must be string indicator
                $filter[$i - 1] !== '\\') // Must not be escaped; first cant be
            {
                if(\call_user_func($isInStringStack)) {
                    // Dont mix-up string symbols. Only allow the same as on start
                    if($char === $stack[\count($stack) - 1]) {
                        // End of string
                        \array_pop($stack);
                    }

                    // Either way, add symbol to builder
                    \call_user_func($addSymbol, $char, $i);
                } else {
                    // Start of string
                    $stack[] = $char;
                    \call_user_func($addSymbol, $char, $i);
                }

                continue;
            }

            // Array support
            if(!(\call_user_func($isInStringStack))) {
                if($char === '[') {
                    // Start of array
                    $stack[] = $char;
                    continue;
                } else if($char === ']') {
                    // End of array
                    \array_pop($stack);

                    if(!empty($currentParam)) {
                        $currentArrayParam[] = $currentParam;
                    }

                    $params[] = $currentArrayParam;
                    $currentArrayParam = [];
                    $currentParam = "";

                    continue;
                }
            }

            // Params separation support
            if($char === ',') {
                // Only consider it end of param if stack doesn't end with string
                if(!(\call_user_func($isInStringStack))) {
                    // If in array stack, dont merge yet, just mark in array param builder
                    if(\call_user_func($isInArrayStack)) {
                        $currentArrayParam[] = $currentParam;
                        $currentParam = "";
                    } else {
                        // Append from parap builder. Either value, or array
                        if(!empty($currentArrayParam)) {
                            // Do nothing, it's done in ] check
                        } else {
                            if(!empty($currentParam)) {
                                $params[] = $currentParam;
                            }

                            $currentParam = "";
                        }
                    }

                }
            }

            // Value, not relevant to syntax
            \call_user_func($addSymbol, $char, $i);
        }

        if(!empty($currentParam)) {
            $params[] = $currentParam;
            $currentParam = "";
        }

        $parsedParams = [];

        foreach($params as $param) {
            // If array, parse each child separatelly
            if(\is_array($param)) {
                $arr = [];

                foreach($param as $element) {
                    $arr[] = self::parseParam($element);
                }

                $parsedParams[] = $arr;
            } else {
                $parsedParams[] = self::parseParam($param);
            }
        }


        return new Query($method, $parsedParams);
    }

    public static function parseParam(string $param) {
        $param = \trim($param);


        /*
        // Array param
        if(\str_starts_with($param, '[')) {
            $param = substr($param, 1, -1); // Remove [ and ]

            $array = [];

            foreach (\explode(',', $param) as $value) {
                $array[] = self::parseParam($value);
            }

            return $array;
        }
        */

        // Numeric param
        if(\is_numeric($param)) {
            // Cast to number
            return $param + 0;
        }

        // Boolean param
        if($param === 'false') {
            return false;
        } else if($param === 'true') {
            return true;
        }

        // Null param
        if($param === 'null') {
            return null;
        }

        // String param
        if(\str_starts_with($param, '"') || \str_starts_with($param, '\'')) {
            $param = substr($param, 1, -1); // Remove '' or ""
            return $param;
        }

        // Unknown format
        return $param;
    }
}

Query::$TYPE_ALIASES = [
    'lt' => fn(array $params) => [new Query(Query::TYPE_LESSER, $params)],
    'lte' => fn(array $params) => [new Query(Query::TYPE_LESSEREQUAL, $params)],
    'gt' => fn(array $params) => [new Query(Query::TYPE_GREATER, $params)],
    'gte' => fn(array $params) => [new Query(Query::TYPE_GREATEREQUAL, $params)],
    'eq' => fn(array $params) => [new Query(Query::TYPE_EQUAL, $params)],
    'page' => fn(array $params) => [new Query(Query::TYPE_LIMIT, [$params[1]]), new Query(Query::TYPE_OFFSET, [($params[0]-1)*$params[1]])],
];