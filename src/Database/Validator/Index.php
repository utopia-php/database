<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Database;
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
     * @throws Exception
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
     * @throws Exception
     */
    public function checkFulltextIndexNonString(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
                foreach ($index->getAttribute('attributes', []) as $attributeName) {
                    $attribute = $this->attributes[$attributeName] ?? new Document([]);
                    if ($attribute->getAttribute('type', '') !== Database::VAR_STRING) {
                        $this->message = 'Attribute "'.$attribute->getAttribute('key', $attribute->getAttribute('$id')).'" cannot be part of a FULLTEXT index';
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
                return true;
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
                    $this->message = 'Index length("'.$indexLength.'") is longer than the key part for "'.$attributeKey.'("'.$attributeSize.'")"';
                    return false;
                }

                $total += $indexLength;
            }

            if ($total > $this->maxLength) {
                $this->message = 'Index Length is longer than max (' . $this->maxLength . '))';
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
     * @throws Exception
     */
    public function isValid($value): bool
    {
        foreach ($value->getAttribute('attributes', []) as $attribute) {
            $this->attributes[$attribute->getAttribute('$id', $value->getAttribute('key'))] = $attribute;
        }

        $this->attributes['$id'] = new Document([
            'type' => Database::VAR_STRING,
            'size' => Database::LENGTH_KEY,
        ]);

        $this->attributes['$createdAt'] = new Document([
            'type' => Database::VAR_DATETIME,
            'size' => Database::LENGTH_KEY,
        ]);

        $this->attributes['$updatedAt'] = new Document([
            'type' => Database::VAR_DATETIME,
            'size' => Database::LENGTH_KEY,
        ]);

        if (!$this->checkEmptyIndexAttributes($value)) {
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
