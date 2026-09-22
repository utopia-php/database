<?php

namespace Utopia\Database\Validator\Query;

/**
 * Schema tolerance for validators that may legitimately name an attribute of a
 * joined collection, whose schema is not available to the validator.
 */
trait JoinedAttributes
{
    protected bool $joinedAttributes = false;

    /**
     * @var array<string, true>
     */
    protected array $joinAliases = [];

    /**
     * Stand the schema check down for a query set that joins: the joined collection's
     * attributes are legitimate operands here and are not in this collection's schema.
     */
    public function allowJoinedAttributes(): void
    {
        $this->joinedAttributes = true;
    }

    public function resetJoinedAttributes(): void
    {
        $this->joinedAttributes = false;
        $this->joinAliases = [];
    }

    /**
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

    /**
     * An `alias.column` reference has to name a join alias the query set actually
     * declared, so a typo in the alias is still rejected. A bare name can only be
     * taken on trust: the joined collection's schema never reaches this validator.
     */
    protected function isJoinedAttribute(string $attribute): bool
    {
        if (! $this->joinedAttributes) {
            return false;
        }

        $dot = \strpos($attribute, '.');

        if ($dot === false) {
            return true;
        }

        return isset($this->joinAliases[\substr($attribute, 0, $dot)])
            && $this->isAllowedJoinColumn(\substr($attribute, $dot + 1));
    }

    abstract protected function isAllowedJoinColumn(string $column): bool;
}
