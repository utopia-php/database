<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Document;
use Utopia\Database\Query;

class Select extends Base
{
    /**
     * @var array<int|string, mixed>
     */
    protected array $schema = [];

    /**
     * @param array<Document> $attributes
     */
    public function __construct(array $attributes = [])
    {
        foreach ($attributes as $attribute) {
            $this->schema[$attribute->getAttribute('key')] = $attribute->getArrayCopy();
        }
    }

    /**
     * Is valid.
     *
     * Returns true if method is TYPE_SELECT selections are valid
     *
     * Otherwise, returns false
     *
     * @param $query
     * @return bool
     */
    public function isValid($query): bool
    {
        /* @var $query Query */

        if ($query->getMethod() !== Query::TYPE_SELECT) {
            return false;
        }

        foreach ($query->getValues() as $attribute) {
            if (\str_contains($attribute, '.')) {
                // For relationships, just validate the top level.
                // Utopia will validate each nested level during the recursive calls.
                $attribute = \explode('.', $attribute)[0];
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
