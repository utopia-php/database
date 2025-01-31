<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class Count extends Base
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
        '$internalId',
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
     * Returns true if method is TYPE_SUM selections are valid
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

        if ($value->getMethod() !== Query::TYPE_COUNT) {
            return false;
        }

        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
        );

        if (!isset($this->schema[$value->getAttribute()])) {
            $this->message = 'Attribute not found in schema: ' . $value->getAttribute();
            return false;
        }
        return true;
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_COUNT;
    }
}
