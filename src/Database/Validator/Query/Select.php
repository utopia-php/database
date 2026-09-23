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
    use JoinedAttributes;

    /**
     * @var array<int|string, true>
     */
    protected array $schema = [];

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

        $internalKeys = self::internalKeys();

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

        return true;
    }

    protected function acceptsMainAttribute(string $attribute): bool
    {
        return isset($this->schema[$attribute]) || \in_array($attribute, self::internalKeys(), true);
    }

    /**
     * @return array<string>
     */
    private static function internalKeys(): array
    {
        return \array_map(
            fn (Attribute $attribute): string => $attribute->key,
            Database::internalAttributes()
        );
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
