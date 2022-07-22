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
        if (\is_array($this->params[$index])) {
            return $this->params[$index];
        }

        return [$this->params[$index]];
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
        switch (static::getMethodFromAlias($value)) {
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
        if (\str_contains($method, '.')) {
            throw new \Exception("Invalid query method");
        }

        $currentParam = ""; // We build param here before pushing when it's ended
        $currentArrayParam = []; // We build array param here before pushing when it's ended
        $stack = []; // Stack of syntactical symbols

        //TODO: make util methods part of the class

        // Loop thorough all characters
        for ($i = $parametersStart; $i < $paramsEnd; $i++) {
            $char = $filter[$i];

            // String support + escaping support
            if (
                (\in_array($char, ['"', '\''])) && // Must be string indicator
                $filter[$i - 1] !== '\\'
            ) // Must not be escaped; first cant be
            {
                if (static::isInStringStack($stack)) {
                    // Dont mix-up string symbols. Only allow the same as on start
                    if ($char === $stack[\count($stack) - 1]) {
                        // End of string
                        \array_pop($stack);
                    }

                    // Either way, add symbol to builder
                    static::appendSymbol($stack, $char, $i, $filter, $currentParam);
                } else {
                    // Start of string
                    $stack[] = $char;
                    static::appendSymbol($stack, $char, $i, $filter, $currentParam);
                }

                continue;
            }

            // Array support
            if (!(static::isInStringStack($stack))) {
                if ($char === '[') {
                    // Start of array
                    $stack[] = $char;
                    continue;
                } else if ($char === ']') {
                    // End of array
                    \array_pop($stack);

                    if (!empty($currentParam)) {
                        $currentArrayParam[] = $currentParam;
                    }

                    $params[] = $currentArrayParam;
                    $currentArrayParam = [];
                    $currentParam = "";

                    continue;
                }
            }

            // Params separation support
            if ($char === ',') {
                // Only consider it end of param if stack doesn't end with string
                if (!static::isInStringStack($stack)) {
                    // If in array stack, dont merge yet, just mark it in array param builder
                    if (static::isInArrayStack($stack)) {
                        $currentArrayParam[] = $currentParam;
                        $currentParam = "";
                    } else {
                        // Append from parap builder. Either value, or array
                        if (empty($currentArrayParam)) {
                            if (!empty($currentParam)) {
                                $params[] = $currentParam;
                            }

                            $currentParam = "";
                        }
                    }
                }
            }

            // Value, not relevant to syntax
            static::appendSymbol($stack, $char, $i, $filter, $currentParam);
        }

        if (!empty($currentParam)) {
            $params[] = $currentParam;
            $currentParam = "";
        }

        $parsedParams = [];

        foreach ($params as $param) {
            // If array, parse each child separatelly
            if (\is_array($param)) {
                foreach ($param as $element) {
                    $arr[] = self::parseParam($element);
                }

                $parsedParams[] = $arr ?? [];
            } else {
                $parsedParams[] = self::parseParam($param);
            }
        }
        $method = static::getMethodFromAlias($method);

        return new Query($method, $parsedParams);
    }

    /**
     * Utility method to know if we are inside String.
     *
     * @param array $stack
     * @return bool
     */
    protected static function isInStringStack(array $stack): bool
    {
        if (\count($stack) > 0 && \in_array($stack[\count($stack) - 1], ['"', '\''])) // Stack ends with string symbol ' or "
        {
            return true;
        }

        return false;
    }

    /**
     * Utility method to know if we are inside Array.
     *
     * @param array $stack
     * @return bool
     */
    protected static function isInArrayStack(array $stack): bool
    {
        if (
            \count($stack) > 0 && // Stack is not empty
            $stack[\count($stack) - 1] === '['
        ) // Stack ends with array symbol
        {
            return true;
        }

        return false;
    }

    /**
     * Utility method to only append symbol if relevant.
     *
     * @param array $stack
     * @param string $char
     * @param int $index
     * @param string $filter
     * @param string $currentParam
     * @return void
     */
    protected static function appendSymbol(array $stack, string $char, int $index, string $filter, string &$currentParam): void
    {
        $nextChar = $filter[$index + 1] ?? '';
        if (
            $char === '\\' && // Current char might be escaping
            (\in_array($nextChar, ['"', '\''])) // Next char must be string syntax symbol
        ) {
            return;
        }

        // Ignore spaces and commas outside of string
        if (\in_array($char, [' ', ','])) {
            if (static::isInStringStack($stack)) {
                $currentParam .= $char;
            }
        } else {
            $currentParam .= $char;
        }
    }

    /**
     * Parses param value.
     *
     * @param string $param
     * @return mixed
     */
    protected static function parseParam(string $param)
    {
        $param = \trim($param);

        // Numeric param
        if (\is_numeric($param)) {
            // Cast to number
            return $param + 0;
        }

        // Boolean param
        if ($param === 'false') {
            return false;
        } else if ($param === 'true') {
            return true;
        }

        // Null param
        if ($param === 'null') {
            return null;
        }

        // String param
        if (\str_starts_with($param, '"') || \str_starts_with($param, '\'')) {
            $param = substr($param, 1, -1); // Remove '' or ""

            return $param;
        }

        // Unknown format
        return $param;
    }

    /**
     * Returns Method from Alias.
     *
     * @param string $method
     * @return string
     */
    static protected function getMethodFromAlias(string $method): string
    {
        return match ($method) {
            'lt' => Query::TYPE_LESSER,
            'lte' => Query::TYPE_LESSEREQUAL,
            'gt' => Query::TYPE_GREATER,
            'gte' => Query::TYPE_GREATEREQUAL,
            'eq' => Query::TYPE_EQUAL,
            default => $method
        };
    }
}
