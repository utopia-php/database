<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;

/**
 * Resolution of attributes this collection does not declare, for validators that may legitimately
 * name an attribute of a joined collection.
 */
trait JoinedAttributes
{
    /**
     * The joins of the query set whose collection is known, in query order.
     *
     * @var list<JoinedCollection>
     */
    protected array $joins = [];

    /**
     * The known join of each alias.
     *
     * @var array<string, JoinedCollection>
     */
    protected array $joinsByAlias = [];

    /**
     * @var array<string, true>
     */
    protected array $joinAliases = [];

    /**
     * Declare the joins of the query set whose collection is known: an `alias.column` under one of
     * their aliases has to name a column of that collection, and a bare name may resolve to one.
     *
     * @param  list<JoinedCollection>  $joins
     */
    public function allowJoins(array $joins): void
    {
        $this->joins = $joins;
        $this->joinsByAlias = [];

        foreach ($joins as $join) {
            if ($join->alias !== '') {
                $this->joinsByAlias[$join->alias] ??= $join;
                $this->joinAliases[$join->alias] = true;
            }
        }
    }

    /**
     * Declare join aliases of the query set. Under an alias whose collection is not known, any
     * plain column is accepted.
     *
     * @param  array<string>  $aliases
     */
    public function allowJoinAliases(array $aliases): void
    {
        foreach ($aliases as $alias) {
            if ($alias !== '') {
                $this->joinAliases[$alias] = true;
            }
        }
    }

    public function resetJoinAliases(): void
    {
        $this->joins = [];
        $this->joinsByAlias = [];
        $this->joinAliases = [];
    }

    /**
     * Whether `alias.column` refers to a join: the alias is one the query set declared, and the
     * column a plain identifier.
     */
    protected function isJoinColumnReference(string $alias, string $column): bool
    {
        return isset($this->joinAliases[$alias]) && $this->isAllowedJoinColumn($column);
    }

    /**
     * A column under a join alias is valid exactly when it would be valid unaliased on the
     * collection the alias joins: one of its attributes, or an internal attribute this validator
     * accepts on the main collection. Sets the message when the column is not valid.
     */
    protected function isJoinedColumn(string $alias, string $column): bool
    {
        $join = $this->joinsByAlias[$alias] ?? null;

        if ($join === null || isset($join->attributes[$column]) || $this->isJoinedInternalAttribute($column)) {
            return true;
        }

        $this->message = 'Attribute not found in schema: '.$alias.'.'.$column;

        return false;
    }

    /**
     * An `alias.column` reference has to name a join alias the query set declared and a column of
     * the collection it joins, so a typo in either is still rejected. A bare name has to be
     * declared by exactly one join: by none it is not found, and by several it would silently
     * pick one of them. Sets the message when the attribute does not resolve.
     */
    protected function isJoinedAttribute(string $attribute): bool
    {
        $dot = \strpos($attribute, '.');

        if ($dot === false) {
            $joins = 0;
            foreach ($this->joins as $join) {
                if (isset($join->attributes[$attribute])) {
                    $joins++;
                }
            }

            if ($joins === 1) {
                return true;
            }

            if ($joins > 1) {
                $this->message = 'Attribute "'.$attribute.'" is ambiguous across joins; qualify it with a join alias';

                return false;
            }
        } else {
            $alias = \substr($attribute, 0, $dot);
            $column = \substr($attribute, $dot + 1);

            if ($this->isJoinColumnReference($alias, $column)) {
                return $this->isJoinedColumn($alias, $column);
            }
        }

        $this->message = 'Attribute not found in schema: '.$attribute;

        return false;
    }

    /**
     * The known join an attribute this collection does not declare resolves to: the join of its
     * alias, or the join that declares a bare name.
     */
    protected function joinOf(string $attribute): ?JoinedCollection
    {
        $dot = \strpos($attribute, '.');

        if ($dot !== false) {
            return $this->joinsByAlias[\substr($attribute, 0, $dot)] ?? null;
        }

        foreach ($this->joins as $join) {
            if (isset($join->attributes[$attribute])) {
                return $join;
            }
        }

        return null;
    }

    /**
     * Internal attributes are the same on every collection, except `$collection`: a read derives
     * it from the collection it reads, and a joined row has no column for it.
     */
    protected function isJoinedInternalAttribute(string $column): bool
    {
        return \str_starts_with($column, '$')
            && $column !== Document::COLLECTION
            && $this->acceptsMainAttribute($column);
    }

    /**
     * The internal attributes a table holds a column for: every one but `$collection`, which a read
     * derives from the collection it reads, and `$tenant` only under shared tables.
     *
     * @return array<string, true>
     */
    protected static function internalColumns(bool $sharedTables): array
    {
        $columns = [];
        foreach (Database::internalAttributes() as $attribute) {
            if ($attribute->key !== Document::COLLECTION && ($sharedTables || $attribute->key !== Document::TENANT)) {
                $columns[$attribute->key] = true;
            }
        }

        return $columns;
    }

    abstract protected function isAllowedJoinColumn(string $column): bool;

    /**
     * The column an attribute names: a bare name the collection does not declare is the column of
     * the one join that declares it, any other name is its own.
     */
    protected function column(string $attribute): string
    {
        if (\str_contains($attribute, '.') || $this->acceptsMainAttribute($attribute)) {
            return $attribute;
        }

        $declaring = \array_values(\array_filter(
            $this->joins,
            static fn (JoinedCollection $join): bool => isset($join->attributes[$attribute]),
        ));

        return \count($declaring) === 1 ? $declaring[0]->alias.'.'.$attribute : $attribute;
    }

    /**
     * Whether this validator accepts the attribute unaliased on the main collection.
     */
    abstract protected function acceptsMainAttribute(string $attribute): bool;
}
