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
     * @param bool $supportForAttributes
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true)
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

        $method = $value->getMethod();
        $allowedMethods = [
            Query::TYPE_SELECT,
            Query::TYPE_SELECT_DISTINCT,
            Query::TYPE_COUNT,
            Query::TYPE_SUM,
            Query::TYPE_AVG,
            Query::TYPE_MIN,
            Query::TYPE_MAX,
        ];

        if (!\in_array($method, $allowedMethods)) {
            return false;
        }

        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
        );

        $attributes = $value->getValues();

        // For aggregates, the attribute is stored in the attribute property, not values
        if (\in_array($method, [Query::TYPE_COUNT, Query::TYPE_SUM, Query::TYPE_AVG, Query::TYPE_MIN, Query::TYPE_MAX])) {
            $attributes = [$value->getAttribute()];
        }

        if (\count($attributes) === 0 && $method === Query::TYPE_SELECT) {
            $this->message = 'No attributes selected';
            return false;
        }

        if (\count($attributes) !== \count(\array_unique($attributes))) {
            $this->message = 'Duplicate attributes selected';
            return false;
        }

        foreach ($attributes as $attribute) {
            if ($attribute === '*') {
                continue;
            }

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

            if ($this->supportForAttributes && !isset($this->schema[$attribute])) {
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
