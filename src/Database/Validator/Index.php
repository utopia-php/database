<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Validator;
use Utopia\Database\Document;

class Index extends Validator
{
    protected string $message = 'Invalid index';
    protected int $maxLength;

    /**
     * @var array<Document> $attributes
     */
    protected array $attributes;

    /**
     * @param array<Document> $attributes
     * @param int $maxLength
     * @throws DatabaseException
     */
    public function __construct(array $attributes, int $maxLength)
    {
        $this->maxLength = $maxLength;

        foreach ($attributes as $attribute) {
            $key = \strtolower($attribute->getAttribute('key', $attribute->getAttribute('$id')));
            $this->attributes[$key] = $attribute;
        }
        foreach (Database::INTERNAL_ATTRIBUTES as $attribute) {
            $key = \strtolower($attribute['$id']);
            $this->attributes[$key] = new Document($attribute);
        }
    }

    /**
     * Returns validator description
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkAttributesNotFound(Document $index): bool
    {
        foreach ($index->getAttribute('attributes', []) as $attribute) {
            if (!isset($this->attributes[\strtolower($attribute)])) {
                $this->message = 'Invalid index attribute "' . $attribute . '" not found';
                return false;
            }
        }
        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkEmptyIndexAttributes(Document $index): bool
    {
        if (empty($index->getAttribute('attributes', []))) {
            $this->message = 'No attributes provided for index';
            return false;
        }
        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkDuplicatedAttributes(Document $index): bool
    {
        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);
        $stack = [];
        foreach ($attributes as $key => $attribute) {
            $direction = $orders[$key] ?? 'ASC';
            $value = \strtolower($attribute . '|' . $direction);
            if (\in_array($value, $stack)) {
                $this->message = 'Duplicate attributes provided';
                return false;
            }
            $stack[] = $value;
        }
        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkFulltextIndexNonString(Document $index): bool
    {
        if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
            foreach ($index->getAttribute('attributes', []) as $attribute) {
                $attribute = $this->attributes[\strtolower($attribute)] ?? new Document();
                if ($attribute->getAttribute('type', '') !== Database::VAR_STRING) {
                    $this->message = 'Attribute "' . $attribute->getAttribute('key', $attribute->getAttribute('$id')) . '" cannot be part of a FULLTEXT index, must be of type string';
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkArrayIndex(Document $index): bool
    {
        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);
        $lengths = $index->getAttribute('lengths', []);

        $arrayAttributes = [];
        foreach ($attributes as $attributePosition => $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document();

            if($attribute->getAttribute('array', false)) {
                // Database::INDEX_UNIQUE Is not allowed! since mariaDB VS MySQL makes the unique Different on values
                if($index->getAttribute('type') != Database::INDEX_KEY) {
                    $this->message = '"' . ucfirst($index->getAttribute('type')) . '" index is forbidden on array attributes';
                    return false;
                }

                if(empty($lengths[$attributePosition])) {
                    $this->message = 'Index length for array not specified';
                    return false;
                }

                $arrayAttributes[] = $attribute->getAttribute('key', '');
                if(count($arrayAttributes) > 1) {
                    $this->message = 'An index may only contain one array attribute';
                    return false;
                }

                $direction = $orders[$attributePosition] ?? '';
                if(!empty($direction)) {
                    $this->message = 'Invalid index order "' . $direction . '" on array attribute "'. $attribute->getAttribute('key', '') .'"';
                    return false;
                }
            } elseif($attribute->getAttribute('type') !== Database::VAR_STRING && !empty($lengths[$attributePosition])) {
                $this->message = 'Cannot set a length on "'. $attribute->getAttribute('type') . '" attributes';
                return false;
            }
        }
        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkIndexLength(Document $index): bool
    {
        if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
            return true;
        }

        $total = 0;
        $lengths = $index->getAttribute('lengths', []);

        foreach ($index->getAttribute('attributes', []) as $attributePosition => $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)];

            switch ($attribute->getAttribute('type')) {
                case Database::VAR_STRING:
                    $attributeSize = $attribute->getAttribute('size', 0);
                    $indexLength = $lengths[$attributePosition] ?? $attributeSize;
                    break;
                case Database::VAR_FLOAT:
                    $attributeSize = 2; // 8 bytes / 4 mb4
                    $indexLength = 2;
                    break;
                default:
                    $attributeSize = 1; // 4 bytes / 4 mb4
                    $indexLength = 1;
                    break;
            }

            if($attribute->getAttribute('array', false)) {
                $attributeSize = Database::ARRAY_INDEX_LENGTH;
                $indexLength = Database::ARRAY_INDEX_LENGTH;
            }

            if ($indexLength > $attributeSize) {
                $this->message = 'Index length ' . $indexLength . ' is larger than the size for ' . $attributeName . ': ' . $attributeSize . '"';
                return false;
            }

            $total += $indexLength;
        }

        if ($total > $this->maxLength && $this->maxLength > 0) {
            $this->message = 'Index length is longer than the maximum: ' . $this->maxLength;
            return false;
        }

        return true;
    }

    /**
     * Is valid.
     *
     * Returns true index if valid.
     * @param Document $value
     * @return bool
     * @throws DatabaseException
     */
    public function isValid($value): bool
    {
        if (!$this->checkAttributesNotFound($value)) {
            return false;
        }

        if (!$this->checkEmptyIndexAttributes($value)) {
            return false;
        }

        if (!$this->checkDuplicatedAttributes($value)) {
            return false;
        }

        if (!$this->checkFulltextIndexNonString($value)) {
            return false;
        }

        if (!$this->checkArrayIndex($value)) {
            return false;
        }

        if (!$this->checkIndexLength($value)) {
            return false;
        }

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}
