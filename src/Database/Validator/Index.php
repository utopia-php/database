<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Attribute as AttributeVO;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index as IndexVO;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Validator;

/**
 * Validates database index definitions including type support, attribute references, lengths, and constraints.
 */
class Index extends Validator
{
    protected string $message = 'Invalid index';

    /**
     * @var array<string, AttributeVO>
     */
    protected array $attributes;

    /**
     * @var array<IndexVO>
     */
    protected array $indexes;

    /**
     * @param  array<AttributeVO|Document>  $attributes
     * @param  array<IndexVO|Document>  $indexes
     * @param  array<string>  $reservedKeys
     *
     * @throws DatabaseException
     */
    public function __construct(
        array $attributes,
        array $indexes,
        protected int $maxLength,
        protected array $reservedKeys = [],
        protected bool $supportForArrayIndexes = false,
        protected bool $supportForSpatialIndexNull = false,
        protected bool $supportForSpatialIndexOrder = false,
        protected bool $supportForVectorIndexes = false,
        protected bool $supportForAttributes = true,
        protected bool $supportForMultipleFulltextIndexes = true,
        protected bool $supportForIdenticalIndexes = true,
        protected bool $supportForObjectIndexes = false,
        protected bool $supportForTrigramIndexes = false,
        protected bool $supportForSpatialIndexes = false,
        protected bool $supportForKeyIndexes = true,
        protected bool $supportForUniqueIndexes = true,
        protected bool $supportForFulltextIndexes = true,
        protected bool $supportForTTLIndexes = false,
        protected bool $supportForObjects = false
    ) {
        $this->attributes = [];
        foreach ($attributes as $attribute) {
            $typed = $attribute instanceof AttributeVO ? $attribute : AttributeVO::fromDocument($attribute);
            $this->attributes[\strtolower($typed->key)] = $typed;
        }
        foreach (Database::internalAttributes() as $attribute) {
            $key = \strtolower($attribute->key);
            $this->attributes[$key] = $attribute;
        }

        $this->indexes = [];
        foreach ($indexes as $index) {
            $this->indexes[] = $index instanceof IndexVO ? $index : IndexVO::fromDocument($index);
        }
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    /**
     * Returns validator description
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Is valid.
     *
     * Returns true index if valid.
     *
     * @param  IndexVO|Document  $value
     *
     * @throws DatabaseException
     */
    public function isValid($value): bool
    {
        $index = $value instanceof IndexVO ? $value : IndexVO::fromDocument($value);

        if (! $this->checkValidIndex($index)) {
            return false;
        }
        if (! $this->checkValidAttributes($index)) {
            return false;
        }
        if (! $this->checkEmptyIndexAttributes($index)) {
            return false;
        }
        if (! $this->checkDuplicatedAttributes($index)) {
            return false;
        }
        if (! $this->checkMultipleFulltextIndexes($index)) {
            return false;
        }
        if (! $this->checkFulltextIndexNonString($index)) {
            return false;
        }
        if (! $this->checkArrayIndexes($index)) {
            return false;
        }
        if (! $this->checkIndexLengths($index)) {
            return false;
        }
        if (! $this->checkReservedNames($index)) {
            return false;
        }
        if (! $this->checkSpatialIndexes($index)) {
            return false;
        }
        if (! $this->checkNonSpatialIndexOnSpatialAttributes($index)) {
            return false;
        }
        if (! $this->checkVectorIndexes($index)) {
            return false;
        }
        if (! $this->checkIdenticalIndexes($index)) {
            return false;
        }
        if (! $this->checkObjectIndexes($index)) {
            return false;
        }
        if (! $this->checkTrigramIndexes($index)) {
            return false;
        }
        if (! $this->checkKeyUniqueFulltextSupport($index)) {
            return false;
        }
        if (! $this->checkTTLIndexes($index)) {
            return false;
        }

        return true;
    }

    /**
     * Check that the index type is supported by the current adapter.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkValidIndex(IndexVO $index): bool
    {
        $type = $index->type;
        if ($this->supportForObjects) {
            // getting dotted attributes not present in schema
            $dottedAttributes = array_filter($index->attributes, fn (string $attr) => ! isset($this->attributes[\strtolower($attr)]) && $this->isDottedAttribute($attr));
            if (\count($dottedAttributes)) {
                foreach ($dottedAttributes as $attribute) {
                    $baseAttribute = $this->getBaseAttributeFromDottedAttribute($attribute);
                    if (isset($this->attributes[\strtolower($baseAttribute)])) {
                        $baseType = $this->attributes[\strtolower($baseAttribute)]->type;
                        if ($baseType !== ColumnType::Object) {
                            $this->message = 'Index attribute "'.$attribute.'" is only supported on object attributes';

                            return false;
                        }
                    }
                }
            }
        }

        switch ($type) {
            case IndexType::Key:
                if (! $this->supportForKeyIndexes) {
                    $this->message = 'Key index is not supported';

                    return false;
                }
                break;

            case IndexType::Unique:
                if (! $this->supportForUniqueIndexes) {
                    $this->message = 'Unique index is not supported';

                    return false;
                }
                break;

            case IndexType::Fulltext:
                if (! $this->supportForFulltextIndexes) {
                    $this->message = 'Fulltext index is not supported';

                    return false;
                }
                break;

            case IndexType::Spatial:
                if (! $this->supportForSpatialIndexes) {
                    $this->message = 'Spatial indexes are not supported';

                    return false;
                }
                if (! empty($index->orders) && ! $this->supportForSpatialIndexOrder) {
                    $this->message = 'Spatial indexes with explicit orders are not supported. Remove the orders to create this index.';

                    return false;
                }
                break;

            case IndexType::HnswEuclidean:
            case IndexType::HnswCosine:
            case IndexType::HnswDot:
                if (! $this->supportForVectorIndexes) {
                    $this->message = 'Vector indexes are not supported';

                    return false;
                }
                break;

            case IndexType::Object:
                if (! $this->supportForObjectIndexes) {
                    $this->message = 'Object indexes are not supported';

                    return false;
                }
                break;

            case IndexType::Trigram:
                if (! $this->supportForTrigramIndexes) {
                    $this->message = 'Trigram indexes are not supported';

                    return false;
                }
                break;

            case IndexType::Ttl:
                if (! $this->supportForTTLIndexes) {
                    $this->message = 'TTL indexes are not supported';

                    return false;
                }
                break;

            default:
                $this->message = 'Unknown index type: '.$type->value.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value.', '.IndexType::Spatial->value.', '.IndexType::Object->value.', '.IndexType::HnswEuclidean->value.', '.IndexType::HnswCosine->value.', '.IndexType::HnswDot->value.', '.IndexType::Trigram->value.', '.IndexType::Ttl->value;

                return false;
        }

        return true;
    }

    /**
     * Check that all index attributes exist in the collection schema.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkValidAttributes(IndexVO $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        foreach ($index->attributes as $attribute) {
            // attribute is part of the attributes
            // or object indexes supported and its a dotted attribute with base present in the attributes
            if (! isset($this->attributes[\strtolower($attribute)])) {
                if ($this->supportForObjects) {
                    $baseAttribute = $this->getBaseAttributeFromDottedAttribute($attribute);
                    if (isset($this->attributes[\strtolower($baseAttribute)])) {
                        continue;
                    }
                }
                $this->message = 'Invalid index attribute "'.$attribute.'" not found';

                return false;
            }
        }

        return true;
    }

    /**
     * Check that the index has at least one attribute.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkEmptyIndexAttributes(IndexVO $index): bool
    {
        if (empty($index->attributes)) {
            $this->message = 'No attributes provided for index';

            return false;
        }

        return true;
    }

    /**
     * Check that the index does not contain duplicate attributes.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkDuplicatedAttributes(IndexVO $index): bool
    {
        $stack = [];
        foreach ($index->attributes as $attribute) {
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
     * Check that fulltext indexes only reference string-type attributes.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkFulltextIndexNonString(IndexVO $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        if ($index->type === IndexType::Fulltext) {
            foreach ($index->attributes as $attributeName) {
                $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();
                $attributeType = $attribute->type;
                $validFulltextTypes = [
                    ColumnType::String,
                    ColumnType::Varchar,
                    ColumnType::Text,
                    ColumnType::MediumText,
                    ColumnType::LongText,
                ];
                if (! in_array($attributeType, $validFulltextTypes)) {
                    $this->message = 'Attribute "'.$attribute->key.'" cannot be part of a fulltext index, must be of type string';

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Check constraints for indexes on array attributes including type, length, and count limits.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkArrayIndexes(IndexVO $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }

        $arrayAttributes = [];
        foreach ($index->attributes as $attributePosition => $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();

            if ($attribute->array) {
                // Database::INDEX_UNIQUE Is not allowed! since mariaDB VS MySQL makes the unique Different on values
                if ($index->type !== IndexType::Key) {
                    $this->message = '"'.ucfirst($index->type->value).'" index is forbidden on array attributes';

                    return false;
                }

                if (empty($index->lengths[$attributePosition])) {
                    $this->message = 'Index length for array not specified';

                    return false;
                }

                $arrayAttributes[] = $attribute->key;
                if (count($arrayAttributes) > 1) {
                    $this->message = 'An index may only contain one array attribute';

                    return false;
                }

                $direction = $index->orders[$attributePosition] ?? '';
                if (! empty($direction)) {
                    $this->message = 'Invalid index order "'.$direction.'" on array attribute "'.$attribute->key.'"';

                    return false;
                }

                if ($this->supportForArrayIndexes === false) {
                    $this->message = 'Indexing an array attribute is not supported';

                    return false;
                }
            } elseif (! in_array($attribute->type, [
                ColumnType::String,
                ColumnType::Varchar,
                ColumnType::Text,
                ColumnType::MediumText,
                ColumnType::LongText,
            ]) && ! empty($index->lengths[$attributePosition])) {
                $this->message = 'Cannot set a length on "'.$attribute->type->value.'" attributes';

                return false;
            }
        }

        return true;
    }

    /**
     * Check that index lengths are valid and do not exceed the maximum allowed total.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkIndexLengths(IndexVO $index): bool
    {
        if ($index->type === IndexType::Fulltext) {
            return true;
        }

        if (! $this->supportForAttributes) {
            return true;
        }

        $total = 0;
        if (count($index->lengths) > count($index->attributes)) {
            $this->message = 'Invalid index lengths. Count of lengths must be equal or less than the number of attributes.';

            return false;
        }
        foreach ($index->attributes as $attributePosition => $attributeName) {
            if ($this->supportForObjects && ! isset($this->attributes[\strtolower($attributeName)])) {
                $attributeName = $this->getBaseAttributeFromDottedAttribute($attributeName);
            }
            $attribute = $this->attributes[\strtolower($attributeName)];

            $attrType = $attribute->type;
            $attrSize = $attribute->size;
            [$attributeSize, $indexLength] = match ($attrType) {
                ColumnType::String,
                ColumnType::Varchar,
                ColumnType::Text,
                ColumnType::MediumText,
                ColumnType::LongText => [
                    $attrSize,
                    ! empty($index->lengths[$attributePosition]) ? $index->lengths[$attributePosition] : $attrSize,
                ],
                ColumnType::Double => [2, 2],
                default => [1, 1],
            };
            if ($indexLength < 0) {
                $this->message = 'Negative index length provided for '.$attributeName;

                return false;
            }

            if ($attribute->array) {
                $attributeSize = Database::MAX_ARRAY_INDEX_LENGTH;
                $indexLength = Database::MAX_ARRAY_INDEX_LENGTH;
            }

            if ($indexLength > $attributeSize) {
                $this->message = 'Index length '.$indexLength.' is larger than the size for '.$attributeName.': '.$attributeSize.'"';

                return false;
            }

            $total += $indexLength;
        }

        if ($total > $this->maxLength && $this->maxLength > 0) {
            $this->message = 'Index length is longer than the maximum: '.$this->maxLength;

            return false;
        }

        return true;
    }

    /**
     * Check that the index key name is not a reserved name.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkReservedNames(IndexVO $index): bool
    {
        $key = $index->key;

        foreach ($this->reservedKeys as $reserved) {
            if (\strtolower($key) === \strtolower($reserved)) {
                $this->message = 'Index key name is reserved';

                return false;
            }
        }

        return true;
    }

    /**
     * Check spatial index constraints including attribute type and nullability.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkSpatialIndexes(IndexVO $index): bool
    {
        $type = $index->type;

        if ($type !== IndexType::Spatial) {
            return true;
        }

        if ($this->supportForSpatialIndexes === false) {
            $this->message = 'Spatial indexes are not supported';

            return false;
        }

        if (\count($index->attributes) !== 1) {
            $this->message = 'Spatial index must have exactly one attribute';

            return false;
        }

        foreach ($index->attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();
            $attributeType = $attribute->type;

            if (! \in_array($attributeType, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
                $this->message = 'Spatial index can only be created on spatial attributes (point, linestring, polygon). Attribute "'.$attributeName.'" is of type "'.$attributeType->value.'"';

                return false;
            }

            if (! $attribute->required && ! $this->supportForSpatialIndexNull) {
                $this->message = 'Spatial indexes do not allow null values. Mark the attribute "'.$attributeName.'" as required or create the index on a column with no null values.';

                return false;
            }
        }

        if (! empty($index->orders) && ! $this->supportForSpatialIndexOrder) {
            $this->message = 'Spatial indexes with explicit orders are not supported. Remove the orders to create this index.';

            return false;
        }

        return true;
    }

    /**
     * Check that non-spatial index types are not applied to spatial attributes.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkNonSpatialIndexOnSpatialAttributes(IndexVO $index): bool
    {
        $type = $index->type;

        // Skip check for spatial indexes
        if ($type === IndexType::Spatial) {
            return true;
        }

        foreach ($index->attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();
            $attributeType = $attribute->type;

            if (\in_array($attributeType, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
                $this->message = 'Cannot create '.$type->value.' index on spatial attribute "'.$attributeName.'". Spatial attributes require spatial indexes.';

                return false;
            }
        }

        return true;
    }

    /**
     * @throws DatabaseException
     */
    public function checkVectorIndexes(IndexVO $index): bool
    {
        $type = $index->type;

        if (
            $type !== IndexType::HnswDot &&
            $type !== IndexType::HnswCosine &&
            $type !== IndexType::HnswEuclidean
        ) {
            return true;
        }

        if ($this->supportForVectorIndexes === false) {
            $this->message = 'Vector indexes are not supported';

            return false;
        }

        if (\count($index->attributes) !== 1) {
            $this->message = 'Vector index must have exactly one attribute';

            return false;
        }

        $attribute = $this->attributes[\strtolower($index->attributes[0])] ?? new AttributeVO();
        if ($attribute->type !== ColumnType::Vector) {
            $this->message = 'Vector index can only be created on vector attributes';

            return false;
        }

        if (! empty($index->orders) || \count(\array_filter($index->lengths)) > 0) {
            $this->message = 'Vector indexes do not support orders or lengths';

            return false;
        }

        return true;
    }

    /**
     * @throws DatabaseException
     */
    public function checkTrigramIndexes(IndexVO $index): bool
    {
        $type = $index->type;

        if ($type !== IndexType::Trigram) {
            return true;
        }

        if ($this->supportForTrigramIndexes === false) {
            $this->message = 'Trigram indexes are not supported';

            return false;
        }

        $validStringTypes = [
            ColumnType::String,
            ColumnType::Varchar,
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText,
        ];

        foreach ($index->attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();
            if (! in_array($attribute->type, $validStringTypes)) {
                $this->message = 'Trigram index can only be created on string type attributes';

                return false;
            }
        }

        if (! empty($index->orders) || \count(\array_filter($index->lengths)) > 0) {
            $this->message = 'Trigram indexes do not support orders or lengths';

            return false;
        }

        return true;
    }

    /**
     * Check that key and unique index types are supported by the current adapter.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkKeyUniqueFulltextSupport(IndexVO $index): bool
    {
        $type = $index->type;

        if ($type === IndexType::Key && $this->supportForKeyIndexes === false) {
            $this->message = 'Key index is not supported';

            return false;
        }

        if ($type === IndexType::Unique && $this->supportForUniqueIndexes === false) {
            $this->message = 'Unique index is not supported';

            return false;
        }

        return true;
    }

    /**
     * Check that multiple fulltext indexes are not created when unsupported.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkMultipleFulltextIndexes(IndexVO $index): bool
    {
        if ($this->supportForMultipleFulltextIndexes) {
            return true;
        }

        if ($index->type === IndexType::Fulltext) {
            foreach ($this->indexes as $existingIndex) {
                if ($existingIndex->key === $index->key) {
                    continue;
                }
                if ($existingIndex->type === IndexType::Fulltext) {
                    $this->message = 'There is already a fulltext index in the collection';

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Check that identical indexes (same attributes and orders) are not created when unsupported.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkIdenticalIndexes(IndexVO $index): bool
    {
        if ($this->supportForIdenticalIndexes) {
            return true;
        }

        foreach ($this->indexes as $existingIndex) {
            $attributesMatch = false;
            if (empty(\array_diff($existingIndex->attributes, $index->attributes)) &&
                empty(\array_diff($index->attributes, $existingIndex->attributes))) {
                $attributesMatch = true;
            }

            $ordersMatch = false;
            if (empty(\array_diff($existingIndex->orders, $index->orders)) &&
                empty(\array_diff($index->orders, $existingIndex->orders))) {
                $ordersMatch = true;
            }

            if ($attributesMatch && $ordersMatch) {
                // Allow fulltext + key/unique combinations (different purposes)
                $regularTypes = [IndexType::Key, IndexType::Unique];
                $isRegularIndex = \in_array($index->type, $regularTypes);
                $isRegularExisting = \in_array($existingIndex->type, $regularTypes);

                // Only reject if both are regular index types (key or unique)
                if ($isRegularIndex && $isRegularExisting) {
                    $this->message = 'There is already an index with the same attributes and orders';

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Check object index constraints including single-attribute and top-level requirements.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkObjectIndexes(IndexVO $index): bool
    {
        $type = $index->type;

        if ($type !== IndexType::Object) {
            return true;
        }

        if (! $this->supportForObjectIndexes) {
            $this->message = 'Object indexes are not supported';

            return false;
        }

        if (count($index->attributes) !== 1) {
            $this->message = 'Object index can be created on a single object attribute';

            return false;
        }

        if (! empty($index->orders)) {
            $this->message = 'Object index do not support explicit orders. Remove the orders to create this index.';

            return false;
        }

        $attributeName = (string) ($index->attributes[0] ?? '');

        // Object indexes are only allowed on the top-level object attribute,
        // not on nested paths like "data.key.nestedKey".
        if (\strpos($attributeName, '.') !== false) {
            $this->message = 'Object index can only be created on a top-level object attribute';

            return false;
        }

        $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();
        $attributeType = $attribute->type;

        if ($attributeType !== ColumnType::Object) {
            $this->message = 'Object index can only be created on object attributes. Attribute "'.$attributeName.'" is of type "'.$attributeType->value.'"';

            return false;
        }

        return true;
    }

    /**
     * Check TTL index constraints including single-attribute, datetime type, and uniqueness requirements.
     *
     * @param IndexVO $index The index to validate
     * @return bool
     */
    public function checkTTLIndexes(IndexVO $index): bool
    {
        $type = $index->type;

        if ($type !== IndexType::Ttl) {
            return true;
        }

        if (count($index->attributes) !== 1) {
            $this->message = 'TTL indexes must be created on a single datetime attribute.';

            return false;
        }

        $attributeName = (string) ($index->attributes[0] ?? '');
        $attribute = $this->attributes[\strtolower($attributeName)] ?? new AttributeVO();
        $attributeType = $attribute->type;

        if ($this->supportForAttributes && $attributeType !== ColumnType::Datetime) {
            $this->message = 'TTL index can only be created on datetime attributes. Attribute "'.$attributeName.'" is of type "'.$attributeType->value.'"';

            return false;
        }

        if ($index->ttl < 1) {
            $this->message = 'TTL must be at least 1 second';

            return false;
        }

        // Check if there's already a TTL index in this collection
        foreach ($this->indexes as $existingIndex) {
            if ($existingIndex->key === $index->key) {
                continue;
            }

            // Check if existing index is also a TTL index
            if ($existingIndex->type === IndexType::Ttl) {
                $this->message = 'There can be only one TTL index in a collection';

                return false;
            }
        }

        return true;
    }

    private function isDottedAttribute(string $attribute): bool
    {
        return \str_contains($attribute, '.');
    }

    private function getBaseAttributeFromDottedAttribute(string $attribute): string
    {
        return $this->isDottedAttribute($attribute) ? \explode('.', $attribute, 2)[0] : $attribute;
    }
}
