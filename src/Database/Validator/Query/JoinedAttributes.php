<?php

namespace Utopia\Database\Validator\Query;

/**
 * Resolution of attributes this collection does not declare, for validators that may legitimately
 * name an attribute of a joined collection.
 */
trait JoinedAttributes
{
    /**
     * The attributes each join of the query set declares, one set per join.
     *
     * @var list<array<string, true>>
     */
    protected array $joinedAttributes = [];

    /**
     * @var array<string, true>
     */
    protected array $joinAliases = [];

    /**
     * @param  list<array<string, true>>  $joins  The attributes each join of the query set declares
     */
    public function allowJoinedAttributes(array $joins): void
    {
        $this->joinedAttributes = $joins;
    }

    public function resetJoinedAttributes(): void
    {
        $this->joinedAttributes = [];
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
     * An `alias.column` reference has to name a join alias the query set declared, so a typo in
     * the alias is still rejected. A bare name has to be declared by exactly one join: by none it
     * is not found, and by several it would silently pick one of them. Sets the message when the
     * attribute does not resolve.
     */
    protected function isJoinedAttribute(string $attribute): bool
    {
        $dot = \strpos($attribute, '.');

        if ($dot === false) {
            $joins = 0;
            foreach ($this->joinedAttributes as $attributes) {
                if (isset($attributes[$attribute])) {
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
        } elseif (
            isset($this->joinAliases[\substr($attribute, 0, $dot)])
            && $this->isAllowedJoinColumn(\substr($attribute, $dot + 1))
        ) {
            return true;
        }

        $this->message = 'Attribute not found in schema: '.$attribute;

        return false;
    }

    abstract protected function isAllowedJoinColumn(string $column): bool;
}
