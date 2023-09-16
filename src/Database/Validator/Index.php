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
    protected array $attributes = [];

    /**
     * @param int $maxLength
     */
    public function __construct(int $maxLength)
    {
        $this->maxLength = $maxLength;
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
     * @param Document $collection
     * @return bool
     */
    public function checkEmptyIndexAttributes(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if (empty($index->getAttribute('attributes', []))) {
                $this->message = 'No attributes provided for index';
                return false;
            }
        }

        return true;
    }

    /**
     * @param Document $collection
     * @return bool
     */
    public function checkDuplicatedAttributes(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index) {
            $attributes = $index->getAttribute('attributes', []);
            $orders = $index->getAttribute('orders', []);
            $stack = [];
            foreach ($attributes as $key => $attribute) {
                $direction = $orders[$key] ?? 'asc';
                $value = strtolower($attribute . '|' . $direction);
                if (in_array($value, $stack)) {
                    $this->message = 'Duplicate attributes provided';
                    return false;
                }
                $stack[] = $value;
            }
        }

        return true;
    }

    /**
     * @param Document $collection
     * @return bool
     * @throws DatabaseException
     */
    public function checkFulltextIndexNonString(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
                foreach ($index->getAttribute('attributes', []) as $attributeName) {
                    $attribute = $this->attributes[$attributeName] ?? new Document([]);
                    if ($attribute->getAttribute('type', '') !== Database::VAR_STRING) {
                        $this->message = 'Attribute "'.$attribute->getAttribute('key', $attribute->getAttribute('$id')).'" cannot be part of a FULLTEXT index, must be of type string';
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * @param Document $collection
     * @return bool
     */
    public function checkIndexLength(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
                continue;
            }

            $total = 0;
            $lengths = $index->getAttribute('lengths', []);

            foreach ($index->getAttribute('attributes', []) as $attributePosition => $attributeName) {
                $attribute = $this->attributes[$attributeName];

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

                if ($indexLength > $attributeSize) {
                    $this->message = 'Index length '.$indexLength.' is larger than the size for '.$attributeName.': '.$attributeSize.'"';
                    return false;
                }

                $total += $indexLength;
            }

            if ($total > $this->maxLength && $this->maxLength > 0) {
                $this->message = 'Index length is longer than the maximum: ' . $this->maxLength;
                return false;
            }
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
        foreach ($value->getAttribute('attributes', []) as $attribute) {
            $this->attributes[$attribute->getAttribute('key', $attribute->getAttribute('$id'))] = $attribute;
        }

        foreach (Database::getInternalAttributes() as $attribute) {
            $this->attributes[$attribute->getAttribute('$id')] = $attribute;
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
