<?php

namespace Utopia\Database;

use Error;

class Query
{
    // Filter methods
    const TYPE_EQUAL = 'equal';
    const TYPE_NOTEQUAL = 'notEqual';
    const TYPE_LESSER = 'lessThan';
    const TYPE_LESSEREQUAL = 'greaterEqualThan';
    const TYPE_GREATER = 'greaterThan';
    const TYPE_GREATEREQUAL = 'greaterEqualThan';
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
        // TODO: Support for array [] (there are commas and spaces inside)
        $method = '';
        $params = [];

        // Separate method and params
        $paramsStart = mb_strpos($filter, '(');
        $method = mb_substr($filter, 0, $paramsStart);

        // Remove everything after end of query
        $paramsEnd = mb_strpos($filter, ')');
        $overflowChars = \strlen($filter) - 1 - $paramsEnd;
        if($overflowChars > 0) {
            $filter = substr($filter, 0, -1 * $overflowChars);
        }

        // Check for deprecated query syntax
        if(\str_contains($method, '.')) {
            throw new Error("Invalid query method");
        }

        // Keep track of what hasn't been processed yet
        $unprocessedFilter = substr($filter, $paramsStart + 1);

        // While ends when we only have ')'
        while(\strlen($unprocessedFilter) > 1 ) {
            $paramEnd = mb_strpos($unprocessedFilter, ',');

            if($paramEnd === false) {
                $paramEnd = \strlen($unprocessedFilter) - 1;
            }

            $param = mb_substr($unprocessedFilter, 0, $paramEnd);
            $params[] = self::parseParam($param);

            $unprocessedFilter = substr($unprocessedFilter, $paramEnd + 1);
        }

        \var_dump($method);
        \var_dump($params);

        return new Query($method, $params);
    }

    public static function parseParam(string $param) {
        $param = \trim($param);
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