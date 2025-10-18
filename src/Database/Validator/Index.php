<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Validator;

class Index extends Validator
{
    protected string $message = 'Invalid index';

    /**
     * @var array<Document> $attributes
     */
    protected array $attributes;

    /**
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @param int $maxLength
     * @param array<string> $reservedKeys
     * @param bool $supportForArrayIndexes
     * @param bool $supportForSpatialIndexNull
     * @param bool $supportForSpatialIndexOrder
     * @param bool $supportForVectorIndexes
     * @param bool $supportForAttributes
     * @param bool $supportForMultipleFulltextIndexes
     * @param bool $supportForIdenticalIndexes
     * @throws DatabaseException
     */
    public function __construct(
        array $attributes,
        protected array $indexes,
        protected int $maxLength,
        protected array $reservedKeys = [],
        protected bool $supportForArrayIndexes = false,
        protected bool $supportForSpatialIndexNull = false,
        protected bool $supportForSpatialIndexOrder = false,
        protected bool $supportForVectorIndexes = false,
        protected bool $supportForAttributes = true,
        protected bool $supportForMultipleFulltextIndexes = true,
        protected bool $supportForIdenticalIndexes = true,
    ) {
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
            if ($this->supportForAttributes && !isset($this->attributes[\strtolower($attribute)])) {
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
        $stack = [];
        foreach ($attributes as $attribute) {
            $value = \strtolower($attribute);

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
        if (!$this->supportForAttributes) {
            return true;
        }
        if ($index->getAttribute('type') === Database::INDEX_FULLTEXT) {
            foreach ($index->getAttribute('attributes', []) as $attribute) {
                $attribute = $this->attributes[\strtolower($attribute)] ?? new Document();
                if ($attribute->getAttribute('type', '') !== Database::VAR_STRING) {
                    $this->message = 'Attribute "' . $attribute->getAttribute('key', $attribute->getAttribute('$id')) . '" cannot be part of a fulltext index, must be of type string';
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
        if (!$this->supportForAttributes) {
            return true;
        }
        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);
        $lengths = $index->getAttribute('lengths', []);

        $arrayAttributes = [];
        foreach ($attributes as $attributePosition => $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document();

            if ($attribute->getAttribute('array', false)) {
                // Database::INDEX_UNIQUE Is not allowed! since mariaDB VS MySQL makes the unique Different on values
                if ($index->getAttribute('type') != Database::INDEX_KEY) {
                    $this->message = '"' . ucfirst($index->getAttribute('type')) . '" index is forbidden on array attributes';
                    return false;
                }

                if (empty($lengths[$attributePosition])) {
                    $this->message = 'Index length for array not specified';
                    return false;
                }

                $arrayAttributes[] = $attribute->getAttribute('key', '');
                if (count($arrayAttributes) > 1) {
                    $this->message = 'An index may only contain one array attribute';
                    return false;
                }

                $direction = $orders[$attributePosition] ?? '';
                if (!empty($direction)) {
                    $this->message = 'Invalid index order "' . $direction . '" on array attribute "' . $attribute->getAttribute('key', '') . '"';
                    return false;
                }

                if ($this->supportForArrayIndexes === false) {
                    $this->message = 'Indexing an array attribute is not supported';
                    return false;
                }
            } elseif ($attribute->getAttribute('type') !== Database::VAR_STRING && !empty($lengths[$attributePosition])) {
                $this->message = 'Cannot set a length on "' . $attribute->getAttribute('type') . '" attributes';
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

        if (!$this->supportForAttributes) {
            return true;
        }

        $total = 0;
        $lengths = $index->getAttribute('lengths', []);
        $attributes = $index->getAttribute('attributes', []);
        if (count($lengths) > count($attributes)) {
            $this->message = 'Invalid index lengths. Count of lengths must be equal or less than the number of attributes.';
            return false;
        }
        foreach ($attributes as $attributePosition => $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)];

            switch ($attribute->getAttribute('type')) {
                case Database::VAR_STRING:
                    $attributeSize = $attribute->getAttribute('size', 0);
                    $indexLength = !empty($lengths[$attributePosition]) ? $lengths[$attributePosition] : $attributeSize;
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
            if ($indexLength < 0) {
                $this->message = 'Negative index length provided for ' . $attributeName;
                return false;
            }

            if ($attribute->getAttribute('array', false)) {
                $attributeSize = Database::MAX_ARRAY_INDEX_LENGTH;
                $indexLength = Database::MAX_ARRAY_INDEX_LENGTH;
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
     * @param Document $index
     * @return bool
     */
    public function checkReservedNames(Document $index): bool
    {
        $key = $index->getAttribute('key', $index->getAttribute('$id'));

        foreach ($this->reservedKeys as $reserved) {
            if (\strtolower($key) === \strtolower($reserved)) {
                $this->message = 'Index key name is reserved';
                return false;
            }
        }

        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkSpatialIndex(Document $index): bool
    {
        $type = $index->getAttribute('type');

        if ($type !== Database::INDEX_SPATIAL) {
            return true;
        }

        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);

        if (\count($attributes) !== 1) {
            $this->message = 'Spatial index must have exactly one attribute';
            return false;
        }

        foreach ($attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document();
            $attributeType = $attribute->getAttribute('type', '');

            if (!\in_array($attributeType, Database::SPATIAL_TYPES, true)) {
                $this->message = 'Spatial index can only be created on spatial attributes (point, linestring, polygon). Attribute "' . $attributeName . '" is of type "' . $attributeType . '"';
                return false;
            }

            $required = (bool)$attribute->getAttribute('required', false);
            if (!$required && !$this->supportForSpatialIndexNull) {
                $this->message = 'Spatial indexes do not allow null values. Mark the attribute "' . $attributeName . '" as required or create the index on a column with no null values.';
                return false;
            }
        }

        if (!empty($orders) && !$this->supportForSpatialIndexOrder) {
            $this->message = 'Spatial indexes with explicit orders are not supported. Remove the orders to create this index.';
            return false;
        }

        return true;
    }

    /**
     * @param Document $index
     * @return bool
     */
    public function checkNonSpatialIndexOnSpatialAttribute(Document $index): bool
    {
        $type = $index->getAttribute('type');

        // Skip check for spatial indexes
        if ($type === Database::INDEX_SPATIAL) {
            return true;
        }

        $attributes = $index->getAttribute('attributes', []);

        foreach ($attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document();
            $attributeType = $attribute->getAttribute('type', '');

            if (\in_array($attributeType, Database::SPATIAL_TYPES, true)) {
                $this->message = 'Cannot create ' . $type . ' index on spatial attribute "' . $attributeName . '". Spatial attributes require spatial indexes.';
                return false;
            }
        }

        return true;
    }

    /**
     * @param Document $index
     * @return bool
     * @throws DatabaseException
     */
    public function checkVectorIndex(Document $index): bool
    {
        $type = $index->getAttribute('type');

        if (
            $type !== Database::INDEX_HNSW_DOT &&
            $type !== Database::INDEX_HNSW_COSINE &&
            $type !== Database::INDEX_HNSW_EUCLIDEAN
        ) {
            return true;
        }

        if ($this->supportForVectorIndexes === false) {
            $this->message = 'Vector indexes are not supported';
            return false;
        }

        $attributes = $index->getAttribute('attributes', []);

        if (\count($attributes) !== 1) {
            $this->message = 'Vector index must have exactly one attribute';
            return false;
        }

        $attribute = $this->attributes[\strtolower($attributes[0])] ?? new Document();
        if ($attribute->getAttribute('type') !== Database::VAR_VECTOR) {
            $this->message = 'Vector index can only be created on vector attributes';
            return false;
        }

        $orders = $index->getAttribute('orders', []);
        $lengths = $index->getAttribute('lengths', []);
        if (!empty($orders) || \count(\array_filter($lengths)) > 0) {
            $this->message = 'Vector indexes do not support orders or lengths';
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
        if (!$this->checkReservedNames($value)) {
            return false;
        }
        if (!$this->checkSpatialIndex($value)) {
            return false;
        }
        if (!$this->checkNonSpatialIndexOnSpatialAttribute($value)) {
            return false;
        }
        if (!$this->checkVectorIndex($value)) {
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
