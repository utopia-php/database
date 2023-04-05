<?php

namespace Utopia\Database;

use Exception;

class Query
{
    // Filter methods
    public const TYPE_EQUAL = 'equal';
    public const TYPE_NOTEQUAL = 'notEqual';
    public const TYPE_LESSER = 'lessThan';
    public const TYPE_LESSEREQUAL = 'lessThanEqual';
    public const TYPE_GREATER = 'greaterThan';
    public const TYPE_GREATEREQUAL = 'greaterThanEqual';
    public const TYPE_CONTAINS = 'contains';
    public const TYPE_SEARCH = 'search';
    public const TYPE_IS_NULL = 'isNull';
    public const TYPE_IS_NOT_NULL = 'isNotNull';
    public const TYPE_BETWEEN = 'between';
    public const TYPE_STARTS_WITH = 'startsWith';
    public const TYPE_ENDS_WITH = 'endsWith';

    public const TYPE_SELECT = 'select';

    // Order methods
    public const TYPE_ORDERDESC = 'orderDesc';
    public const TYPE_ORDERASC = 'orderAsc';

    // Pagination methods
    public const TYPE_LIMIT = 'limit';
    public const TYPE_OFFSET = 'offset';
    public const TYPE_CURSORAFTER = 'cursorAfter';
    public const TYPE_CURSORBEFORE = 'cursorBefore';

    protected const CHAR_SINGLE_QUOTE = '\'';
    protected const CHAR_DOUBLE_QUOTE = '"';
    protected const CHAR_COMMA = ',';
    protected const CHAR_SPACE = ' ';
    protected const CHAR_BRACKET_START = '[';
    protected const CHAR_BRACKET_END = ']';
    protected const CHAR_PARENTHESES_START = '(';
    protected const CHAR_PARENTHESES_END = ')';
    protected const CHAR_BACKSLASH = '\\';

    protected string $method = '';
    protected string $attribute = '';

    /**
     * @var array<mixed>
     */
    protected array $values = [];


    /**
     * Construct a new query object
     *
     * @param string $method
     * @param string $attribute
     * @param array<mixed> $values
     */
    public function __construct(string $method, string $attribute = '', array $values = [])
    {
        $this->method = $method;
        $this->attribute = $attribute;
        $this->values = $values;
    }

    /**
     * @return string
     */
    public function getMethod(): string
    {
        return $this->method;
    }

    /**
     * @return string
     */
    public function getAttribute(): string
    {
        return $this->attribute;
    }

    /**
     * @return array<mixed>
     */
    public function getValues(): array
    {
        return $this->values;
    }

    /**
     * @param mixed $default
     * @return mixed
     */
    public function getValue(mixed $default = null): mixed
    {
        return $this->values[0] ?? $default;
    }

    /**
     * Sets method
     *
     * @param string $method
     * @return self
     */
    public function setMethod(string $method): self
    {
        $this->method = $method;

        return $this;
    }

    /**
     * Sets attribute
     *
     * @param string $attribute
     * @return self
     */
    public function setAttribute(string $attribute): self
    {
        $this->attribute = $attribute;

        return $this;
    }

    /**
     * Sets values
     *
     * @param array<mixed> $values
     * @return self
     */
    public function setValues(array $values): self
    {
        $this->values = $values;

        return $this;
    }

    /**
     * Sets value
     * @param mixed $value
     * @return self
     */
    public function setValue(mixed $value): self
    {
        $this->values = [$value];

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
        return match (static::getMethodFromAlias($value)) {
            self::TYPE_EQUAL,
            self::TYPE_NOTEQUAL,
            self::TYPE_LESSER,
            self::TYPE_LESSEREQUAL,
            self::TYPE_GREATER,
            self::TYPE_GREATEREQUAL,
            self::TYPE_CONTAINS,
            self::TYPE_SEARCH,
            self::TYPE_ORDERASC,
            self::TYPE_ORDERDESC,
            self::TYPE_LIMIT,
            self::TYPE_OFFSET,
            self::TYPE_CURSORAFTER,
            self::TYPE_CURSORBEFORE,
            self::TYPE_IS_NULL,
            self::TYPE_IS_NOT_NULL,
            self::TYPE_BETWEEN,
            self::TYPE_STARTS_WITH,
            self::TYPE_ENDS_WITH,
            self::TYPE_SELECT => true,
            default => false,
        };
    }

    /**
     * Parse query filter
     *
     * @param string $filter
     * @return self
     * @throws Exception
     */
    public static function parse(string $filter): self
    {
        // Init empty vars we fill later
        $method = '';
        $params = [];

        // Separate method from filter
        $paramsStart = mb_strpos($filter, static::CHAR_PARENTHESES_START);

        if ($paramsStart === false) {
            throw new Exception("Invalid query");
        }

        $method = mb_substr($filter, 0, $paramsStart);

        // Separate params from filter
        $paramsEnd = \strlen($filter) - 1; // -1 to ignore )
        $parametersStart = $paramsStart + 1; // +1 to ignore (

        // Check for deprecated query syntax
        if (\str_contains($method, '.')) {
            throw new Exception("Invalid query method");
        }

        $currentParam = ""; // We build param here before pushing when it's ended
        $currentArrayParam = []; // We build array param here before pushing when it's ended

        $stack = []; // State for stack of parentheses
        $stackCount = 0; // Length of stack array. Kept as variable to improve performance
        $stringStackState = null; // State for string support

        // Loop thorough all characters
        for ($i = $parametersStart; $i < $paramsEnd; $i++) {
            $char = $filter[$i];

            $isStringStack = $stringStackState !== null;
            $isArrayStack = !$isStringStack && $stackCount > 0;

            if ($char === static::CHAR_BACKSLASH) {
                if (!(static::isSpecialChar($filter[$i + 1]))) {
                    static::appendSymbol($isStringStack, $filter[$i], $i, $filter, $currentParam);
                }

                static::appendSymbol($isStringStack, $filter[$i + 1], $i, $filter, $currentParam);
                $i++;

                continue;
            }

            // String support + escaping support
            if (
                (self::isQuote($char)) && // Must be string indicator
                ($filter[$i - 1] !== static::CHAR_BACKSLASH || $filter[$i - 2] === static::CHAR_BACKSLASH) // Must not be escaped;
            ) {
                if ($isStringStack) {
                    // Dont mix-up string symbols. Only allow the same as on start
                    if ($char === $stringStackState) {
                        // End of string
                        $stringStackState = null;
                    }

                    // Either way, add symbol to builder
                    static::appendSymbol($isStringStack, $char, $i, $filter, $currentParam);
                } else {
                    // Start of string
                    $stringStackState = $char;
                    static::appendSymbol($isStringStack, $char, $i, $filter, $currentParam);
                }

                continue;
            }

            // Array support
            if (!($isStringStack)) {
                if ($char === static::CHAR_BRACKET_START) {
                    // Start of array
                    $stack[] = $char;
                    $stackCount++;
                    continue;
                } elseif ($char === static::CHAR_BRACKET_END) {
                    // End of array
                    \array_pop($stack);
                    $stackCount--;

                    if (strlen($currentParam)) {
                        $currentArrayParam[] = $currentParam;
                    }

                    $params[] = $currentArrayParam;
                    $currentArrayParam = [];
                    $currentParam = "";

                    continue;
                } elseif ($char === static::CHAR_COMMA) { // Params separation support
                    // If in array stack, dont merge yet, just mark it in array param builder
                    if ($isArrayStack) {
                        $currentArrayParam[] = $currentParam;
                        $currentParam = "";
                    } else {
                        // Append from parap builder. Either value, or array
                        if (empty($currentArrayParam)) {
                            if (strlen($currentParam)) {
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

        if (strlen($currentParam)) {
            $params[] = $currentParam;
            $currentParam = "";
        }

        $parsedParams = [];

        foreach ($params as $param) {
            // If array, parse each child separatelly
            if (\is_array($param)) {
                foreach ($param as $element) {
                    $arr[] = self::parseValue($element);
                }

                $parsedParams[] = $arr ?? [];
            } else {
                $parsedParams[] = self::parseValue($param);
            }
        }

        $method = static::getMethodFromAlias($method);
        switch ($method) {
            case self::TYPE_EQUAL:
            case self::TYPE_NOTEQUAL:
            case self::TYPE_LESSER:
            case self::TYPE_LESSEREQUAL:
            case self::TYPE_GREATER:
            case self::TYPE_GREATEREQUAL:
            case self::TYPE_CONTAINS:
            case self::TYPE_SEARCH:
            case self::TYPE_IS_NULL:
            case self::TYPE_IS_NOT_NULL:
            case self::TYPE_STARTS_WITH:
            case self::TYPE_ENDS_WITH:
                $attribute = $parsedParams[0] ?? '';
                if (count($parsedParams) < 2) {
                    return new self($method, $attribute);
                }
                return new self($method, $attribute, \is_array($parsedParams[1]) ? $parsedParams[1] : [$parsedParams[1]]);

            case self::TYPE_BETWEEN:
                return new self($method, $parsedParams[0], [$parsedParams[1], $parsedParams[2]]);
            case self::TYPE_SELECT:
                return new self($method, values: $parsedParams[0]);
            case self::TYPE_ORDERASC:
            case self::TYPE_ORDERDESC:
                return new self($method, $parsedParams[0] ?? '');

            case self::TYPE_LIMIT:
            case self::TYPE_OFFSET:
            case self::TYPE_CURSORAFTER:
            case self::TYPE_CURSORBEFORE:
                if (count($parsedParams) > 0) {
                    return new self($method, values: [$parsedParams[0]]);
                }
                return new self($method);

            default:
                return new self($method);
        }
    }

    /**
     * Utility method to only append symbol if relevant.
     *
     * @param bool $isStringStack
     * @param string $char
     * @param int $index
     * @param string $filter
     * @param string $currentParam
     * @return void
     */
    protected static function appendSymbol(bool $isStringStack, string $char, int $index, string $filter, string &$currentParam): void
    {
        // Ignore spaces and commas outside of string
        $canBeIgnored = false;

        if ($char === static::CHAR_SPACE) {
            $canBeIgnored = true;
        } elseif ($char === static::CHAR_COMMA) {
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

    protected static function isQuote(string $char): bool
    {
        if ($char === self::CHAR_SINGLE_QUOTE) {
            return true;
        } elseif ($char === self::CHAR_DOUBLE_QUOTE) {
            return true;
        }

        return false;
    }

    protected static function isSpecialChar(string $char): bool
    {
        if ($char === static::CHAR_COMMA) {
            return true;
        } elseif ($char === static::CHAR_BRACKET_END) {
            return true;
        } elseif ($char === static::CHAR_BRACKET_START) {
            return true;
        } elseif ($char === static::CHAR_DOUBLE_QUOTE) {
            return true;
        } elseif ($char === static::CHAR_SINGLE_QUOTE) {
            return true;
        }

        return false;
    }

    /**
     * Parses value.
     *
     * @param string $value
     * @return mixed
     */
    protected static function parseValue(string $value): mixed
    {
        $value = \trim($value);

        if ($value === 'false') { // Boolean value
            return false;
        } elseif ($value === 'true') {
            return true;
        } elseif ($value === 'null') { // Null value
            return null;
        } elseif (\is_numeric($value)) { // Numeric value
            // Cast to number
            return $value + 0;
        } elseif (\str_starts_with($value, static::CHAR_DOUBLE_QUOTE) || \str_starts_with($value, static::CHAR_SINGLE_QUOTE)) { // String param
            $value = \substr($value, 1, -1); // Remove '' or ""
            return $value;
        }

        // Unknown format
        return $value;
    }

    /**
     * Returns Method from Alias.
     *
     * @param string $method
     * @return string
     */
    protected static function getMethodFromAlias(string $method): string
    {
        return $method;
        /*
        Commented out as we didn't consider this important at the moment, since IDE autocomplete should do the job.
        return match ($method) {
            'lt' => self::TYPE_LESSER,
            'lte' => self::TYPE_LESSEREQUAL,
            'gt' => self::TYPE_GREATER,
            'gte' => self::TYPE_GREATEREQUAL,
            'eq' => self::TYPE_EQUAL,
            default => $method
        };
        */
    }

    /**
     * Helper method to create Query with equal method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function equal(string $attribute, array $values): self
    {
        return new self(self::TYPE_EQUAL, $attribute, $values);
    }

    /**
     * Helper method to create Query with notEqual method
     *
     * @param string $attribute
     * @param mixed $value
     * @return Query
     */
    public static function notEqual(string $attribute, mixed $value): self
    {
        return new self(self::TYPE_NOTEQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with lessThan method
     *
     * @param string $attribute
     * @param mixed $value
     * @return Query
     */
    public static function lessThan(string $attribute, mixed $value): self
    {
        return new self(self::TYPE_LESSER, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with lessThanEqual method
     *
     * @param string $attribute
     * @param mixed $value
     * @return Query
     */
    public static function lessThanEqual(string $attribute, mixed $value): self
    {
        return new self(self::TYPE_LESSEREQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThan method
     *
     * @param string $attribute
     * @param mixed $value
     * @return Query
     */
    public static function greaterThan(string $attribute, mixed $value): self
    {
        return new self(self::TYPE_GREATER, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThanEqual method
     *
     * @param string $attribute
     * @param mixed $value
     * @return Query
     */
    public static function greaterThanEqual(string $attribute, mixed$value): self
    {
        return new self(self::TYPE_GREATEREQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with contains method
     *
     * @param string $attribute
     * @param array<mixed> $values
     * @return Query
     */
    public static function contains(string $attribute, array $values): self
    {
        return new self(self::TYPE_CONTAINS, $attribute, $values);
    }

    /**
     * Helper method to create Query with between method
     *
     * @param string $attribute
     * @param mixed $start
     * @param mixed $end
     * @return Query
     */
    public static function between(string $attribute, mixed $start, mixed $end): self
    {
        return new self(self::TYPE_BETWEEN, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with search method
     *
     * @param string $attribute
     * @param mixed $value
     * @return Query
     */
    public static function search(string $attribute, mixed $value): self
    {
        return new self(self::TYPE_SEARCH, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with select method
     *
     * @param array<string> $attributes
     * @return Query
     */
    public static function select(array $attributes): self
    {
        return new self(self::TYPE_SELECT, values: $attributes);
    }

    /**
     * Helper method to create Query with orderDesc method
     *
     * @param string $attribute
     * @return Query
     */
    public static function orderDesc(string $attribute): self
    {
        return new self(self::TYPE_ORDERDESC, $attribute);
    }

    /**
     * Helper method to create Query with orderAsc method
     *
     * @param string $attribute
     * @return Query
     */
    public static function orderAsc(string $attribute): self
    {
        return new self(self::TYPE_ORDERASC, $attribute);
    }

    /**
     * Helper method to create Query with limit method
     *
     * @param int $value
     * @return Query
     */
    public static function limit(int $value): self
    {
        return new self(self::TYPE_LIMIT, values: [$value]);
    }

    /**
     * Helper method to create Query with offset method
     *
     * @param int $value
     * @return Query
     */
    public static function offset(int $value): self
    {
        return new self(self::TYPE_OFFSET, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorAfter method
     *
     * @param Document $value
     * @return Query
     */
    public static function cursorAfter(Document $value): self
    {
        return new self(self::TYPE_CURSORAFTER, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorBefore method
     *
     * @param Document $value
     * @return Query
     */
    public static function cursorBefore(Document $value): self
    {
        return new self(self::TYPE_CURSORBEFORE, values: [$value]);
    }

    /**
     * Helper method to create Query with isNull method
     *
     * @param string $attribute
     * @return Query
     */
    public static function isNull(string $attribute): self
    {
        return new self(self::TYPE_IS_NULL, $attribute);
    }

    /**
     * Helper method to create Query with isNotNull method
     *
     * @param string $attribute
     * @return Query
     */
    public static function isNotNull(string $attribute): self
    {
        return new self(self::TYPE_IS_NOT_NULL, $attribute);
    }

    public static function startsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_STARTS_WITH, $attribute, [$value]);
    }

    public static function endsWith(string $attribute, string $value): self
    {
        return new self(self::TYPE_ENDS_WITH, $attribute, [$value]);
    }

    /**
     * Filters $queries for $types
     *
     * @param array<Query> $queries
     * @param string ...$types
     *
     * @return array<Query>
     */
    public static function getByType(array $queries, string ...$types): array
    {
        $filtered = [];
        foreach ($queries as $query) {
            if (in_array($query->getMethod(), $types, true)) {
                $filtered[] = $query;
            }
        }

        return $filtered;
    }

    /**
     * Iterates through queries are groups them by type
     *
     * @param array<Query> $queries
     * @return array{
     *     filters: array<Query>,
     *     selections: array<Query>,
     *     limit: int|null,
     *     offset: int|null,
     *     orderAttributes: array<string>,
     *     orderTypes: array<string>,
     *     cursor: Document|null,
     *     cursorDirection: string|null
     * }
     */
    public static function groupByType(array $queries): array
    {
        $filters = [];
        $selections = [];
        $limit = null;
        $offset = null;
        $orderAttributes = [];
        $orderTypes = [];
        $cursor = null;
        $cursorDirection = null;
        foreach ($queries as $query) {
            if (!$query instanceof Query) {
                continue;
            }

            $method = $query->getMethod();
            $attribute = $query->getAttribute();
            $values = $query->getValues();
            switch ($method) {
                case Query::TYPE_ORDERASC:
                case Query::TYPE_ORDERDESC:
                    if (!empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = $method === Query::TYPE_ORDERASC ? Database::ORDER_ASC : Database::ORDER_DESC;
                    break;

                case Query::TYPE_LIMIT:
                    // keep the 1st limit encountered and ignore the rest
                    if ($limit !== null) {
                        break;
                    }

                    $limit = $values[0] ?? $limit;
                    break;

                case Query::TYPE_OFFSET:
                    // keep the 1st offset encountered and ignore the rest
                    if ($offset !== null) {
                        break;
                    }

                    $offset = $values[0] ?? $limit;
                    break;

                case Query::TYPE_CURSORAFTER:
                case Query::TYPE_CURSORBEFORE:
                    // keep the 1st cursor encountered and ignore the rest
                    if ($cursor !== null) {
                        break;
                    }

                    $cursor = $values[0] ?? $limit;
                    $cursorDirection = $method === Query::TYPE_CURSORAFTER ? Database::CURSOR_AFTER : Database::CURSOR_BEFORE;
                    break;

                case Query::TYPE_SELECT:
                    $selections[] = $query;
                    break;

                default:
                    $filters[] = $query;
                    break;
            }
        }

        return [
            'filters' => $filters,
            'selections' => $selections,
            'limit' => $limit,
            'offset' => $offset,
            'orderAttributes' => $orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $cursorDirection,
        ];
    }

    /**
     * Iterate over $queries attempting to parse each
     *
     * @param array<string> $queries
     *
     * @return array<Query>
     * @throws Exception
     */
    public static function parseQueries(array $queries): array
    {
        $parsed = [];
        foreach ($queries as $query) {
            try {
                $parsed[] = Query::parse($query);
            } catch (\Throwable $th) {
                throw new Exception("Invalid query: ${query}", previous: $th);
            }
        }

        return $parsed;
    }
}
