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

    protected const CHAR_SINGLE_QUOTE = '\'';
    protected const CHAR_DOUBLE_QUOTE = '"';
    protected const CHAR_COMMA = ',';
    protected const CHAR_SPACE = ' ';
    protected const CHAR_BRACKET_START = '[';
    protected const CHAR_BRACKET_END = ']';
    protected const CHAR_PARENTHESES_START = '(';
    protected const CHAR_PARENTHESES_END = ')';

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
     *
     * @return null|string
     */
    public function getFirstParam(): ?string
    {
        return $this->params[0] ?? null;
    }

    /**
     * Helper method changing first param. In first param we often store attribute
     *
     * @param string $value
     * @return self
     */
    public function setFirstParam(string $value): self
    {
        $this->params[0] = $value;

        return $this;
    }

    /**
     * Helper method. Returns param, but in form of array array.
     *
     * @param int $index
     * @return array
     */
    public function getArrayParam(int $index): array
    {
        if (\is_array($this->params[$index])) {
            return $this->params[$index];
        }

        return [$this->params[$index]];
    }

    /**
     * Sets Method.
     * @param string $method
     * @return self
     */
    public function setMethod(string $method): self
    {
        $this->method = $method;

        return $this;
    }

    /**
     * Sets Param.
     * @param array $params
     * @return self
     */
    public function setParams(array $params): self
    {
        $this->params = $params;

        return $this;
    }

    /**
     * Check if method is supported
     *
     * @param string $value
     * @return bool
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
     *
     * @param string $filter
     * @return self
     * @throws \Exception
     */
    public static function parse(string $filter): self
    {
        // Init empty vars we fill later
        $method = '';
        $params = [];

        // Separate method from filter
        $paramsStart = mb_strpos($filter, static::CHAR_PARENTHESES_START);
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

        // Loop thorough all characters
        for ($i = $parametersStart; $i < $paramsEnd; $i++) {
            $char = $filter[$i];

            $isStringStack = static::isInStringStack($stack);

            // If not special symbol, continue asap
            $isSpecialSymbol = self::isSpecialChar($char);
            if(!$isSpecialSymbol) {
                static::appendSymbol($isStringStack, $char, $i, $filter, $currentParam);
                continue;
            }

            // String support + escaping support
            if (
                (self::isQuote($char)) && // Must be string indicator
                $filter[$i - 1] !== '\\' // Must not be escaped; first cant be
            )
            {
                if ($isStringStack) {
                    // Dont mix-up string symbols. Only allow the same as on start
                    if ($char === $stack[\count($stack) - 1]) {
                        // End of string
                        \array_pop($stack);
                    }

                    // Either way, add symbol to builder
                    static::appendSymbol($isStringStack, $char, $i, $filter, $currentParam);
                } else {
                    // Start of string
                    $stack[] = $char;
                    static::appendSymbol($isStringStack, $char, $i, $filter, $currentParam);
                }

                continue;
            }

            // Array support
            if (!($isStringStack)) {
                if ($char === static::CHAR_BRACKET_START) {
                    // Start of array
                    $stack[] = $char;
                    continue;
                } else if ($char === static::CHAR_BRACKET_END) {
                    // End of array
                    \array_pop($stack);

                    if (!empty($currentParam)) {
                        $currentArrayParam[] = $currentParam;
                    }

                    $params[] = $currentArrayParam;
                    $currentArrayParam = [];
                    $currentParam = "";

                    continue;
                } else if ($char === static::CHAR_COMMA) { // Params separation support
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
                    continue;
                }
            }

            // Value, not relevant to syntax
            static::appendSymbol($isStringStack, $char, $i, $filter, $currentParam);
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
        $stackLength = \count($stack);

        if ($stackLength > 0 && self::isQuote($stack[$stackLength - 1])) // Stack ends with string symbol ' or "
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
        $stackLength = \count($stack);
        
        if ($stackLength > 0 && $stack[$stackLength - 1] === static::CHAR_BRACKET_START) // Stack ends with array symbol
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
    protected static function appendSymbol(bool $isStringStack, string $char, int $index, string $filter, string &$currentParam): void
    {
        $nextChar = $filter[$index + 1] ?? '';
        if (
            $char === '\\' && // Current char might be escaping
            self::isQuote($nextChar) // Next char must be string syntax symbol
        ) {
            return;
        }

        // Ignore spaces and commas outside of string
        $canBeIgnored = false;

        if($char === static::CHAR_SPACE) {
            $canBeIgnored = true;
        } else if($char === static::CHAR_COMMA) {
            $canBeIgnored = true;
        }

        if ($canBeIgnored) {
            if ($isStringStack) {
                $currentParam .= $char;
            }
        } else {
            $currentParam .= $char;
        }
    }

    protected static function isQuote(string $char) {
        if($char === self::CHAR_SINGLE_QUOTE) {
            return true;
        } else if($char === self::CHAR_DOUBLE_QUOTE) {
            return true;
        }

        return false;
    }

    protected static function isSpecialChar(string $char) {
        if($char === static::CHAR_COMMA) {
            return true;
        } else if($char === static::CHAR_BRACKET_END) {
            return true;
        } else if($char === static::CHAR_BRACKET_START) {
            return true;
        } else if($char === static::CHAR_DOUBLE_QUOTE) {
            return true;
        } else if($char === static::CHAR_SINGLE_QUOTE) {
            return true;
        }

        return false;
    }

    /**
     * Parses param value.
     *
     * @param string $param
     * @return mixed
     */
    protected static function parseParam(string $param): mixed
    {
        $param = \trim($param);
        
        if ($param === 'false') { // Boolean param
            return false;
        } else if ($param === 'true') {
            return true;
        } else if ($param === 'null') { // Null param
            return null;
        }  else if (\is_numeric($param)) { // Numeric param
            // Cast to number
            return $param + 0;
        } else if (\str_starts_with($param, static::CHAR_DOUBLE_QUOTE) || \str_starts_with($param, static::CHAR_SINGLE_QUOTE)) { // String param
            $param = \substr($param, 1, -1); // Remove '' or ""
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
