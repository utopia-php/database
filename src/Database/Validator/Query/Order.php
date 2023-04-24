<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Document;
use Utopia\Database\Query;

class Order extends Base
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
     * @param string $attribute
     * @return bool
     */
    protected function isValidAttribute(string $attribute): bool
    {
        // Search for attribute in schema
        if (!isset($this->schema[$attribute])) {
            $this->message = 'Attribute not found in schema: ' . $attribute;
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
     * @param Query $query
     * @return bool
     */
    public function isValid($query): bool
    {
        $method = $query->getMethod();
        $attribute = $query->getAttribute();

        if ($method === Query::TYPE_ORDERASC || $method === Query::TYPE_ORDERDESC) {
            if ($attribute === '') {
                return true;
            }
            return $this->isValidAttribute($attribute);
        }

        return false;
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_ORDER;
    }
}
