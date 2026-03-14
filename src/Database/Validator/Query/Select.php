<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;

/**
 * Validates select query methods ensuring referenced attributes exist in the schema and are not duplicated.
 */
class Select extends Base
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

        $internalKeys = \array_map(
            fn (Attribute $attr): string => $attr->key,
            Database::internalAttributes()
        );

        if (\count($value->getValues()) === 0) {
            $this->message = 'No attributes selected';

            return false;
        }

        if (\count($value->getValues()) !== \count(\array_unique($value->getValues()))) {
            $this->message = 'Duplicate attributes selected';

            return false;

        }
        foreach ($value->getValues() as $attributeValue) {
            /** @var string $attribute */
            $attribute = $attributeValue;
            if (\str_contains($attribute, '.')) {
                // special symbols with `dots`
                if (isset($this->schema[$attribute])) {
                    continue;
                }

                // For relationships, just validate the top level.
                // Will validate each nested level during the recursive calls.
                $attribute = \explode('.', $attribute)[0];
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

        return true;
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
