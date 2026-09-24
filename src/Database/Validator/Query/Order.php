<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;

/**
 * Validates order query methods ensuring referenced attributes exist in the schema.
 */
class Order extends Base
{
    use JoinedAttributes;

    /**
     * @var array<int|string, true>
     */
    protected array $schema = [];

    /**
     * Transient aggregation aliases registered by Queries::isValid for the
     * current validation pass. Kept separate from $schema so it can be reset
     * per-call without clobbering the real attribute schema — prior versions
     * mutated $schema directly, leaking aliases across requests in long-lived
     * processes (Swoole) and pooled validator instances.
     *
     * @var array<string, true>
     */
    protected array $aggregationAliases = [];

    private bool $aggregates = false;

    /**
     * @var list<string>
     */
    private array $groupBy = [];

    /**
     * @param  array<Document>  $attributes
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true)
    {
        foreach ($attributes as $attribute) {
            /** @var string $attrKey */
            $attrKey = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));
            $this->schema[$attrKey] = true;
        }
    }

    protected function isValidAttribute(string $attribute): bool
    {
        $dot = \strpos($attribute, '.');
        if ($dot !== false) {
            // Check for special symbol `.`
            if (isset($this->schema[$attribute])) {
                return true;
            }

            $alias = \substr($attribute, 0, $dot);
            $column = \substr($attribute, $dot + 1);

            if ($this->isJoinColumnReference($alias, $column)) {
                return ! $this->supportForAttributes || $this->isJoinedColumn($alias, $column);
            }

            // For relationships, just validate the top level.
            // Will validate each nested level during the recursive calls.
            $attribute = $alias;

            if (isset($this->schema[$attribute])) {
                $this->message = 'Cannot order by nested attribute: '.$attribute;

                return false;
            }
        }

        // Accept transient aggregation aliases registered by Queries::isValid
        if (isset($this->aggregationAliases[$attribute])) {
            return true;
        }

        // Search for attribute in schema
        if ($this->supportForAttributes && ! isset($this->schema[$attribute])) {
            $this->message = 'Attribute not found in schema: '.$attribute;

            return false;
        }

        return true;
    }

    /**
     * Is valid.
     *
     * Returns true if method is ORDER_ASC or ORDER_DESC and attributes are valid
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

        $method = $value->getMethod();
        $attribute = $value->getAttribute();

        if ($method === Method::OrderAsc || $method === Method::OrderDesc) {
            return $this->isValidAttribute($attribute) && $this->isGroupedOrder($attribute);
        }

        if ($method === Method::OrderRandom) {
            return true; // orderRandom doesn't need an attribute
        }

        return false;
    }

    protected function acceptsMainAttribute(string $attribute): bool
    {
        return isset($this->schema[$attribute]);
    }

    /**
     * Register aggregation aliases that become valid order targets for the
     * current validation pass. Callers (see Queries::isValid) must invoke
     * resetAggregationAliases() before the pass to avoid cross-call leakage.
     *
     * @param array<string> $aliases
     */
    public function addAggregationAliases(array $aliases): void
    {
        foreach ($aliases as $alias) {
            $this->aggregationAliases[$alias] = true;
        }
    }

    /**
     * Clear any aggregation aliases added by a previous validation pass.
     */
    public function resetAggregationAliases(): void
    {
        $this->aggregationAliases = [];
    }

    /**
     * The aggregates of the query set. With one, or with a groupBy, the query returns a row per
     * group, so an order can name only an aggregate alias or an attribute the query groups by.
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

    private function isGroupedOrder(string $attribute): bool
    {
        if ((! $this->aggregates && $this->groupBy === []) || isset($this->aggregationAliases[$attribute])) {
            return true;
        }

        foreach ($this->groupBy as $group) {
            if ($this->column($group) === $this->column($attribute)) {
                return true;
            }
        }

        $this->message = 'Cannot order by "'.$attribute.'": an aggregation query can only order by its groups and aggregates';

        return false;
    }

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_ORDER;
    }
}
