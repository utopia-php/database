<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;

/**
 * Validates select query methods ensuring referenced attributes exist in the schema, are not duplicated and, in an
 * aggregation query, are grouped.
 */
class Select extends Base
{
    use JoinedAttributes;

    /**
     * @var array<int|string, true>
     */
    protected array $schema = [];

    /**
     * The relationship attributes of the collection.
     *
     * @var array<int|string, true>
     */
    private array $relationships = [];

    /**
     * Whether the query set holds an aggregate.
     */
    private bool $aggregates = false;

    /**
     * The attributes the query set groups by.
     *
     * @var list<string>
     */
    private array $groupBy = [];

    /**
     * @param  array<Document>  $attributes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true, protected bool $sharedTables = false)
    {
        foreach ($attributes as $attribute) {
            /** @var string $attrKey */
            $attrKey = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));
            $this->schema[$attrKey] = true;

            if (Attribute::isRelationship($attribute)) {
                $this->relationships[$attrKey] = true;
            }
        }
    }

    /**
     * The aggregates of the query set. With one, or with a groupBy, the query returns a row per
     * group, so a select can name only an attribute the query groups by.
     *
     * @param  array<BaseQuery>  $aggregations
     */
    public function setAggregations(array $aggregations): void
    {
        $this->aggregates = $aggregations !== [];
    }

    /**
     * @param  array<mixed>  $attributes  the groupBy attributes of the query set
     */
    public function setGroupBy(array $attributes): void
    {
        $this->groupBy = [];

        foreach ($attributes as $attribute) {
            if (\is_string($attribute) && $attribute !== '') {
                $this->groupBy[] = $attribute;
            }
        }
    }

    /**
     * Is valid.
     *
     * Returns true if method is TYPE_SELECT selections are valid
     *
     * Otherwise, returns false
     *
     * @param  mixed  $value
     */
    public function isValid($value): bool
    {
        if (! $value instanceof Query) {
            return false;
        }

        if ($value->getMethod() !== Method::Select) {
            return false;
        }

        $internalKeys = $this->internalKeys();

        if (\count($value->getValues()) === 0) {
            $this->message = 'No attributes selected';

            return false;
        }

        // Before the duplicate check: array_unique() stringifies every array element
        // to "Array", so two nested values collapse into one and report a misleading
        // duplicate instead of the type error that is actually there.
        $attributes = [];
        foreach ($value->getValues() as $attribute) {
            if (!\is_string($attribute)) {
                $this->message = 'Attribute selection must be a string, got ' . \get_debug_type($attribute);
                return false;
            }
            $attributes[] = $attribute;
        }

        if (\count($attributes) !== \count(\array_unique($attributes))) {
            $this->message = 'Duplicate attributes selected';

            return false;

        }
        foreach ($value->getValues() as $attributeValue) {
            /** @var string $attribute */
            $attribute = $attributeValue;
            $dot = \strpos($attribute, '.');
            if ($dot !== false) {
                // special symbols with `dots`
                if (isset($this->schema[$attribute])) {
                    continue;
                }

                $alias = \substr($attribute, 0, $dot);
                $column = \substr($attribute, $dot + 1);

                if ($this->isJoinColumnReference($alias, $column)) {
                    if ($this->supportForAttributes && ! $this->isJoinedColumn($alias, $column)) {
                        return false;
                    }

                    continue;
                }

                if ($this->isAggregation() && isset($this->joinAliases[$alias])) {
                    return $this->rejectUngrouped($attribute);
                }

                // For relationships, just validate the top level.
                // Will validate each nested level during the recursive calls.
                $attribute = $alias;
            }

            // Skip internal attributes
            if (\in_array($attribute, $internalKeys)) {
                continue;
            }

            if ($this->supportForAttributes && ! isset($this->schema[$attribute]) && $attribute !== '*') {
                $this->message = 'Attribute not found in schema: '.$attribute;

                return false;
            }
        }

        return ! $this->isAggregation() || $this->isGroupedSelection($attributes);
    }

    /**
     * Whether the query set aggregates: with an aggregate or a groupBy it returns a row per group.
     */
    private function isAggregation(): bool
    {
        return $this->aggregates || $this->groupBy !== [];
    }

    /**
     * An aggregation query returns only its groups and aggregates, so every selected attribute has
     * to be one it groups by. `*` and relationship wildcards at any depth add nothing to those rows,
     * and are accepted. Sets the message when an attribute is not grouped.
     *
     * @param  list<string>  $attributes
     */
    private function isGroupedSelection(array $attributes): bool
    {
        $groups = [];
        foreach ($this->groupBy as $group) {
            $groups[$this->column($group)] = true;
        }

        foreach ($attributes as $attribute) {
            if ($attribute !== '*' && ! $this->isRelationshipWildcard($attribute) && ! isset($groups[$this->column($attribute)])) {
                return $this->rejectUngrouped($attribute);
            }
        }

        return true;
    }

    private function rejectUngrouped(string $attribute): false
    {
        $this->message = 'Cannot select "'.$attribute.'": an aggregation query can only select the attributes it groups by';

        return false;
    }

    /**
     * A wildcard under a relationship of the collection: `key.*`, or a nested `key.related.*`.
     * Under a join alias a wildcard names the joined collection's columns instead.
     */
    private function isRelationshipWildcard(string $attribute): bool
    {
        $key = \strstr($attribute, '.', true);

        return $key !== false
            && \str_ends_with($attribute, '.*')
            && isset($this->relationships[$key])
            && ! isset($this->joinAliases[$key]);
    }

    /**
     * The column an attribute names: a bare name the collection does not declare is the column of
     * the one join that declares it, any other name is its own.
     */
    private function column(string $attribute): string
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

    protected function acceptsMainAttribute(string $attribute): bool
    {
        return isset($this->schema[$attribute]) || \in_array($attribute, $this->internalKeys(), true);
    }

    /**
     * The internal attributes a read can select: every one but `$tenant`, which only shared tables
     * hold.
     *
     * @return array<string>
     */
    private function internalKeys(): array
    {
        $keys = [];
        foreach (Database::internalAttributes() as $attribute) {
            if ($this->sharedTables || $attribute->key !== Document::TENANT) {
                $keys[] = $attribute->key;
            }
        }

        return $keys;
    }

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_SELECT;
    }
}
