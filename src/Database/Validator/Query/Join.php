<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;

/**
 * Validates join query methods: a join names a table, and each of its conditions compares a column of
 * the main collection or of a join declared before it with a column of the collection it joins.
 */
class Join extends Base
{
    public const string ALIAS_PATTERN = '/^[A-Za-z_][A-Za-z0-9_]*$/';

    private const array OPERATORS = ['=', '!=', '<', '>', '<=', '>=', '<>'];

    /**
     * The internal attributes every table holds a column for that a join condition can compare, as a
     * filter compares them.
     */
    private const array INTERNAL_COLUMNS = [Document::ID, Document::SEQUENCE, Document::CREATED_AT, Document::UPDATED_AT];

    /**
     * The main collection's attributes and whether each holds a column, or null when they are not known.
     *
     * @var array<string, bool>|null
     */
    private readonly ?array $columns;

    /**
     * The joins of the query set whose collection is known.
     *
     * @var list<JoinedCollection>
     */
    private array $joins = [];

    /**
     * The aliases the joins validated so far in this query set declared, each with the collection it
     * joins when that is known.
     *
     * @var array<string, JoinedCollection|null>
     */
    private array $declared = [];

    /**
     * @param  array<Document>|null  $attributes  The main collection's attributes, or null when they are not known
     */
    public function __construct(?array $attributes = null, private readonly bool $supportForAttributes = true)
    {
        $this->columns = $attributes === null ? null : JoinedCollection::columns($attributes);
    }

    /**
     * Declare the joins of the query set whose collection is known, so the columns of their
     * conditions can be checked against it.
     *
     * @param  list<JoinedCollection>  $joins
     */
    public function allowJoins(array $joins): void
    {
        $this->joins = $joins;
    }

    /**
     * Start a new query set: its joins are checked in query order, each against the joins before it.
     */
    public function resetJoinAliases(): void
    {
        $this->joins = [];
        $this->declared = [];
    }

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
     * Validate a join query names a table, and that its conditions compare columns the tables have.
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

        $join = $this->joinOf($query);

        if ($query->getMethod() !== Method::CrossJoin && ! $this->isValidConditions($query, $alias, $join)) {
            return false;
        }

        if ($alias !== '') {
            $this->declared[$alias] = $join;
        }

        return true;
    }

    private function isValidConditions(Query $query, string $alias, ?JoinedCollection $join): bool
    {
        if (! $query->isNestedJoin()) {
            $values = $query->getValues();

            return $this->isValidCondition($values[0] ?? null, $values[1] ?? null, $values[2] ?? null, $alias, $join);
        }

        $onQueries = $query->getJoinOnQueries();
        if ($onQueries === []) {
            $this->message = 'Join ON requires at least one condition';

            return false;
        }

        foreach ($onQueries as $onQuery) {
            if ($onQuery->getMethod() !== Method::On) {
                continue;
            }

            $values = $onQuery->getValues();
            if (! $this->isValidCondition($values[0] ?? null, $values[1] ?? '=', $values[2] ?? null, $alias, $join)) {
                return false;
            }
        }

        return true;
    }

    private function isValidCondition(mixed $left, mixed $operator, mixed $right, string $alias, ?JoinedCollection $join): bool
    {
        if (! \is_string($left) || $left === '' || ! \is_string($right) || $right === '') {
            $this->message = 'Join ON requires left and right columns';

            return false;
        }

        if (! \is_string($operator) || ! \in_array($operator, self::OPERATORS, true)) {
            $this->message = 'Invalid join operator: '.(\is_string($operator) ? $operator : \gettype($operator));

            return false;
        }

        return $this->isValidLeftColumn($left) && $this->isValidRightColumn($right, $alias, $join);
    }

    /**
     * The left column belongs to the main collection, or under its alias to a join declared before
     * this one.
     */
    private function isValidLeftColumn(string $column): bool
    {
        $dot = \strpos($column, '.');
        if ($dot === false) {
            return $this->isColumn($this->columns, $column, $column);
        }

        $alias = \substr($column, 0, $dot);
        if (! \array_key_exists($alias, $this->declared)) {
            $this->message = 'The left column of a join condition must belong to the main collection or to a join declared before it: '.$column;

            return false;
        }

        return $this->isColumn($this->columnsOf($this->declared[$alias]), \substr($column, $dot + 1), $column);
    }

    /**
     * The right column belongs to the collection the join reads, bare or under the join's alias.
     */
    private function isValidRightColumn(string $column, string $alias, ?JoinedCollection $join): bool
    {
        $name = $column;
        $dot = \strpos($column, '.');
        if ($dot !== false) {
            if ($alias === '' || \substr($column, 0, $dot) !== $alias) {
                $this->message = 'The right column of a join condition must belong to the joined collection: '.$column;

                return false;
            }

            $name = \substr($column, $dot + 1);
        }

        return $this->isColumn($this->columnsOf($join), $name, $column);
    }

    /**
     * @param  array<string, bool>|null  $columns  A collection's attributes and whether each holds a column, or null when they are not known
     */
    private function isColumn(?array $columns, string $name, string $column): bool
    {
        if ($name !== '' && (! $this->supportForAttributes || $columns === null || \in_array($name, self::INTERNAL_COLUMNS, true))) {
            return true;
        }

        $holdsColumn = $columns[$name] ?? null;

        if ($holdsColumn === false) {
            $this->message = 'Cannot join on virtual relationship attribute: '.$column;

            return false;
        }

        if ($holdsColumn === null) {
            $this->message = 'Attribute not found in schema: '.$column;

            return false;
        }

        return true;
    }

    /**
     * @return array<string, bool>|null
     */
    private function columnsOf(?JoinedCollection $join): ?array
    {
        if ($join === null) {
            return null;
        }

        return \array_fill_keys(\array_keys($join->attributes), true) + $join->columns;
    }

    private function joinOf(Query $query): ?JoinedCollection
    {
        foreach ($this->joins as $join) {
            if ($join->collection === $query->getAttribute() && $join->alias === $query->getJoinAlias()) {
                return $join;
            }
        }

        return null;
    }
}
