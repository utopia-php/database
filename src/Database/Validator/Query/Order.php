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
     * @param array<string> $aliases
     */
    public function addAggregationAliases(array $aliases): void
    {
        foreach ($aliases as $alias) {
            $this->schema[$alias] = ['$id' => $alias, 'key' => $alias];
        }
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
