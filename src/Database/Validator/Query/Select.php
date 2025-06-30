<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class Select extends Base
{
    /**
     * @var array<int|string, mixed>
     */
    protected array $schema = [];

    /**
     * List of internal attributes
     *
     * @var array<string>
     */
    protected const INTERNAL_ATTRIBUTES = [
        '$id',
        '$sequence',
        '$createdAt',
        '$updatedAt',
        '$permissions',
        '$collection',
    ];

    /**
     * @param array<Document> $attributes
     */
    public function __construct(array $attributes = [])
    {
        foreach ($attributes as $attribute) {
            $this->schema[$attribute->getAttribute('key', $attribute->getAttribute('$id'))] = $attribute->getArrayCopy();
        }
    }

    /**
     * Is valid.
     *
     * Returns true if method is TYPE_SELECT selections are valid
     *
     * Otherwise, returns false
     *
     * @param Query $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (!$value instanceof Query) {
            return false;
        }

        if ($value->getMethod() !== Query::TYPE_SELECT) {
            return false;
        }

        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
        );

        if (\count($value->getValues()) === 0) {
            $this->message = 'No attributes selected';
            return false;
        }

        if (\count($value->getValues()) !== \count(\array_unique($value->getValues()))) {
            $this->message = 'Duplicate attributes selected';
            return false;

        }
        foreach ($value->getValues() as $attribute) {
            if (\str_contains($attribute, '.')) {
                //special symbols with `dots`
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

            if (!isset($this->schema[$attribute]) && $attribute !== '*') {
                $this->message = 'Attribute not found in schema: ' . $attribute;
                return false;
            }
        }
        return true;
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_SELECT;
    }
}
