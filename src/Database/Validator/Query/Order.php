<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;

/**
 * Validates order query methods ensuring referenced attributes exist in the schema.
 */
class Order extends Base
{
    /**
     * @var array<int|string, mixed>
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

    /**
     * @param  array<Document>  $attributes
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true)
    {
        foreach ($attributes as $attribute) {
            /** @var string $attrKey */
            $attrKey = $attribute->getAttribute('key', $attribute->getAttribute('$id'));
            $this->schema[$attrKey] = $attribute->getArrayCopy();
        }
    }

    protected function isValidAttribute(string $attribute): bool
    {
        if (\str_contains($attribute, '.')) {
            // Check for special symbol `.`
            if (isset($this->schema[$attribute])) {
                return true;
            }

            // For relationships, just validate the top level.
            // Will validate each nested level during the recursive calls.
            $attribute = \explode('.', $attribute)[0];

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
            return $this->isValidAttribute($attribute);
        }

        if ($method === Method::OrderRandom) {
            return true; // orderRandom doesn't need an attribute
        }

        return false;
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
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_ORDER;
    }
}
