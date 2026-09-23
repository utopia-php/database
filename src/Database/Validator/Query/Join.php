<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;
use Utopia\Query\Method;

/**
 * Validates join query methods ensuring a target table name is specified.
 */
class Join extends Base
{
    public const string ALIAS_PATTERN = '/^[A-Za-z_][A-Za-z0-9_]*$/';

    /**
     * Why a join alias cannot be used, or null when it can. The reserved alias is matched without
     * case: SQLite, and MySQL on case-insensitive file systems, treat `x` and `X` as one alias.
     */
    public static function describeInvalidAlias(string $alias): ?string
    {
        if (\preg_match(self::ALIAS_PATTERN, $alias) !== 1) {
            return 'Join alias must start with a letter or an underscore and contain only letters, digits and underscores';
        }

        if (\strcasecmp($alias, Query::DEFAULT_ALIAS) === 0) {
            return "Join alias \"{$alias}\" is reserved for the main collection";
        }

        return null;
    }

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_JOIN;
    }

    public const int MAX_PER_QUERY = 8;

    /**
     * Validate how many joins one query set declares.
     */
    public function isValidCount(int $count): bool
    {
        if ($count > self::MAX_PER_QUERY) {
            $this->message = 'Too many joins: at most '.self::MAX_PER_QUERY.' are allowed';

            return false;
        }

        return true;
    }

    /**
     * Validate a join query names a table, and that any ON conditions are well formed.
     */
    protected function isValidQuery(Query $query): bool
    {
        if ($query->getMethod() === Method::NaturalJoin) {
            $this->message = 'Natural joins are not supported';

            return false;
        }

        $table = $query->getAttribute();
        if (empty($table)) {
            $this->message = 'Join requires a table name';

            return false;
        }

        $alias = $query->getJoinAlias();
        $invalidAlias = $alias === '' ? null : self::describeInvalidAlias($alias);
        if ($invalidAlias !== null) {
            $this->message = $invalidAlias;

            return false;
        }

        if (! $query->isNestedJoin()) {
            return true;
        }

        $onQueries = $query->getJoinOnQueries();
        if ($onQueries === []) {
            $this->message = 'Join ON requires at least one condition';

            return false;
        }

        $allowedOperators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        foreach ($onQueries as $onQuery) {
            if ($onQuery->getMethod() !== Method::On) {
                continue;
            }

            $values = $onQuery->getValues();
            $left = $values[0] ?? '';
            $operator = $values[1] ?? '=';
            $right = $values[2] ?? '';
            if (! \is_string($left) || $left === '' || ! \is_string($right) || $right === '') {
                $this->message = 'Join ON requires left and right columns';

                return false;
            }
            if (! \is_string($operator) || ! \in_array($operator, $allowedOperators, true)) {
                $this->message = 'Invalid join operator: '.(\is_string($operator) ? $operator : \gettype($operator));

                return false;
            }
        }

        return true;
    }
}
