<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Validator;

class Index extends Validator
{
    protected string $message = 'Invalid index';

    /**
     * @var array<Document>
     */
    protected array $attributes;

    /**
     * @param  array<Document>  $attributes
     * @param  array<Document>  $indexes
     * @param  array<string>  $reservedKeys
     *
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
        protected bool $supportForObjectIndexes = false,
        protected bool $supportForTrigramIndexes = false,
        protected bool $supportForSpatialIndexes = false,
        protected bool $supportForKeyIndexes = true,
        protected bool $supportForUniqueIndexes = true,
        protected bool $supportForFulltextIndexes = true,
        protected bool $supportForTTLIndexes = false,
        protected bool $supportForObjects = false
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
     * @param  Document  $value
     *
     * @throws DatabaseException
     */
    public function isValid($value): bool
    {
        if (! $this->checkValidIndex($value)) {
            return false;
        }
        if (! $this->checkValidAttributes($value)) {
            return false;
        }
        if (! $this->checkEmptyIndexAttributes($value)) {
            return false;
        }
        if (! $this->checkDuplicatedAttributes($value)) {
            return false;
        }
        if (! $this->checkMultipleFulltextIndexes($value)) {
            return false;
        }
        if (! $this->checkFulltextIndexNonString($value)) {
            return false;
        }
        if (! $this->checkArrayIndexes($value)) {
            return false;
        }
        if (! $this->checkIndexLengths($value)) {
            return false;
        }
        if (! $this->checkReservedNames($value)) {
            return false;
        }
        if (! $this->checkSpatialIndexes($value)) {
            return false;
        }
        if (! $this->checkNonSpatialIndexOnSpatialAttributes($value)) {
            return false;
        }
        if (! $this->checkVectorIndexes($value)) {
            return false;
        }
        if (! $this->checkIdenticalIndexes($value)) {
            return false;
        }
        if (! $this->checkObjectIndexes($value)) {
            return false;
        }
        if (! $this->checkTrigramIndexes($value)) {
            return false;
        }
        if (! $this->checkKeyUniqueFulltextSupport($value)) {
            return false;
        }
        if (! $this->checkTTLIndexes($value)) {
            return false;
        }

        return true;
    }

    public function checkValidIndex(Document $index): bool
    {
        $type = $index->getAttribute('type');
        if ($this->supportForObjects) {
            // getting dotted attributes not present in schema
            $dottedAttributes = array_filter($index->getAttribute('attributes'), fn ($attr) => ! isset($this->attributes[\strtolower($attr)]) && $this->isDottedAttribute($attr));
            if (\count($dottedAttributes)) {
                foreach ($dottedAttributes as $attribute) {
                    $baseAttribute = $this->getBaseAttributeFromDottedAttribute($attribute);
                    if (isset($this->attributes[\strtolower($baseAttribute)]) && $this->attributes[\strtolower($baseAttribute)]->getAttribute('type') != ColumnType::Object->value) {
                        $this->message = 'Index attribute "'.$attribute.'" is only supported on object attributes';

                        return false;
                    }
                }
            }
        }

        switch ($type) {
            case IndexType::Key->value:
                if (! $this->supportForKeyIndexes) {
                    $this->message = 'Key index is not supported';

                    return false;
                }
                break;

            case IndexType::Unique->value:
                if (! $this->supportForUniqueIndexes) {
                    $this->message = 'Unique index is not supported';

                    return false;
                }
                break;

            case IndexType::Fulltext->value:
                if (! $this->supportForFulltextIndexes) {
                    $this->message = 'Fulltext index is not supported';

                    return false;
                }
                break;

            case IndexType::Spatial->value:
                if (! $this->supportForSpatialIndexes) {
                    $this->message = 'Spatial indexes are not supported';

                    return false;
                }
                if (! empty($index->getAttribute('orders')) && ! $this->supportForSpatialIndexOrder) {
                    $this->message = 'Spatial indexes with explicit orders are not supported. Remove the orders to create this index.';

                    return false;
                }
                break;

            case IndexType::HnswEuclidean->value:
            case IndexType::HnswCosine->value:
            case IndexType::HnswDot->value:
                if (! $this->supportForVectorIndexes) {
                    $this->message = 'Vector indexes are not supported';

                    return false;
                }
                break;

            case IndexType::Object->value:
                if (! $this->supportForObjectIndexes) {
                    $this->message = 'Object indexes are not supported';

                    return false;
                }
                break;

            case IndexType::Trigram->value:
                if (! $this->supportForTrigramIndexes) {
                    $this->message = 'Trigram indexes are not supported';

                    return false;
                }
                break;

            case IndexType::Ttl->value:
                if (! $this->supportForTTLIndexes) {
                    $this->message = 'TTL indexes are not supported';

                    return false;
                }
                break;

            default:
                $this->message = 'Unknown index type: '.$type.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value.', '.IndexType::Spatial->value.', '.IndexType::Object->value.', '.IndexType::HnswEuclidean->value.', '.IndexType::HnswCosine->value.', '.IndexType::HnswDot->value.', '.IndexType::Trigram->value.', '.IndexType::Ttl->value;

                return false;
        }

        return true;
    }

    public function checkValidAttributes(Document $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        foreach ($index->getAttribute('attributes', []) as $attribute) {
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

    public function checkEmptyIndexAttributes(Document $index): bool
    {
        if (empty($index->getAttribute('attributes', []))) {
            $this->message = 'No attributes provided for index';

            return false;
        }

        return true;
    }

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

    public function checkFulltextIndexNonString(Document $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        if ($index->getAttribute('type') === IndexType::Fulltext->value) {
            foreach ($index->getAttribute('attributes', []) as $attribute) {
                $attribute = $this->attributes[\strtolower($attribute)] ?? new Document;
                $attributeType = $attribute->getAttribute('type', '');
                $validFulltextTypes = [
                    ColumnType::String->value,
                    ColumnType::Varchar->value,
                    ColumnType::Text->value,
                    ColumnType::MediumText->value,
                    ColumnType::LongText->value,
                ];
                if (! in_array($attributeType, $validFulltextTypes)) {
                    $this->message = 'Attribute "'.$attribute->getAttribute('key', $attribute->getAttribute('$id')).'" cannot be part of a fulltext index, must be of type string';

                    return false;
                }
            }
        }

        return true;
    }

    public function checkArrayIndexes(Document $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);
        $lengths = $index->getAttribute('lengths', []);

        $arrayAttributes = [];
        foreach ($attributes as $attributePosition => $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document;

            if ($attribute->getAttribute('array', false)) {
                // Database::INDEX_UNIQUE Is not allowed! since mariaDB VS MySQL makes the unique Different on values
                if ($index->getAttribute('type') != IndexType::Key->value) {
                    $this->message = '"'.ucfirst($index->getAttribute('type')).'" index is forbidden on array attributes';

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
                if (! empty($direction)) {
                    $this->message = 'Invalid index order "'.$direction.'" on array attribute "'.$attribute->getAttribute('key', '').'"';

                    return false;
                }

                if ($this->supportForArrayIndexes === false) {
                    $this->message = 'Indexing an array attribute is not supported';

                    return false;
                }
            } elseif (! in_array($attribute->getAttribute('type'), [
                ColumnType::String->value,
                ColumnType::Varchar->value,
                ColumnType::Text->value,
                ColumnType::MediumText->value,
                ColumnType::LongText->value,
            ]) && ! empty($lengths[$attributePosition])) {
                $this->message = 'Cannot set a length on "'.$attribute->getAttribute('type').'" attributes';

                return false;
            }
        }

        return true;
    }

    public function checkIndexLengths(Document $index): bool
    {
        if ($index->getAttribute('type') === IndexType::Fulltext->value) {
            return true;
        }

        if (! $this->supportForAttributes) {
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
            if ($this->supportForObjects && ! isset($this->attributes[\strtolower($attributeName)])) {
                $attributeName = $this->getBaseAttributeFromDottedAttribute($attributeName);
            }
            $attribute = $this->attributes[\strtolower($attributeName)];

            [$attributeSize, $indexLength] = match ($attribute->getAttribute('type')) {
                ColumnType::String->value,
                ColumnType::Varchar->value,
                ColumnType::Text->value,
                ColumnType::MediumText->value,
                ColumnType::LongText->value => [
                    $attribute->getAttribute('size', 0),
                    ! empty($lengths[$attributePosition]) ? $lengths[$attributePosition] : $attribute->getAttribute('size', 0),
                ],
                ColumnType::Double->value => [2, 2],
                default => [1, 1],
            };
            if ($indexLength < 0) {
                $this->message = 'Negative index length provided for '.$attributeName;

                return false;
            }

            if ($attribute->getAttribute('array', false)) {
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

    public function checkSpatialIndexes(Document $index): bool
    {
        $type = $index->getAttribute('type');

        if ($type !== IndexType::Spatial->value) {
            return true;
        }

        if ($this->supportForSpatialIndexes === false) {
            $this->message = 'Spatial indexes are not supported';

            return false;
        }

        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);

        if (\count($attributes) !== 1) {
            $this->message = 'Spatial index must have exactly one attribute';

            return false;
        }

        foreach ($attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document;
            $attributeType = $attribute->getAttribute('type', '');

            if (! \in_array($attributeType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value], true)) {
                $this->message = 'Spatial index can only be created on spatial attributes (point, linestring, polygon). Attribute "'.$attributeName.'" is of type "'.$attributeType.'"';

                return false;
            }

            $required = (bool) $attribute->getAttribute('required', false);
            if (! $required && ! $this->supportForSpatialIndexNull) {
                $this->message = 'Spatial indexes do not allow null values. Mark the attribute "'.$attributeName.'" as required or create the index on a column with no null values.';

                return false;
            }
        }

        if (! empty($orders) && ! $this->supportForSpatialIndexOrder) {
            $this->message = 'Spatial indexes with explicit orders are not supported. Remove the orders to create this index.';

            return false;
        }

        return true;
    }

    public function checkNonSpatialIndexOnSpatialAttributes(Document $index): bool
    {
        $type = $index->getAttribute('type');

        // Skip check for spatial indexes
        if ($type === IndexType::Spatial->value) {
            return true;
        }

        $attributes = $index->getAttribute('attributes', []);

        foreach ($attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document;
            $attributeType = $attribute->getAttribute('type', '');

            if (\in_array($attributeType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value], true)) {
                $this->message = 'Cannot create '.$type.' index on spatial attribute "'.$attributeName.'". Spatial attributes require spatial indexes.';

                return false;
            }
        }

        return true;
    }

    /**
     * @throws DatabaseException
     */
    public function checkVectorIndexes(Document $index): bool
    {
        $type = $index->getAttribute('type');

        if (
            $type !== IndexType::HnswDot->value &&
            $type !== IndexType::HnswCosine->value &&
            $type !== IndexType::HnswEuclidean->value
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

        $attribute = $this->attributes[\strtolower($attributes[0])] ?? new Document;
        if ($attribute->getAttribute('type') !== ColumnType::Vector->value) {
            $this->message = 'Vector index can only be created on vector attributes';

            return false;
        }

        $orders = $index->getAttribute('orders', []);
        $lengths = $index->getAttribute('lengths', []);
        if (! empty($orders) || \count(\array_filter($lengths)) > 0) {
            $this->message = 'Vector indexes do not support orders or lengths';

            return false;
        }

        return true;
    }

    /**
     * @throws DatabaseException
     */
    public function checkTrigramIndexes(Document $index): bool
    {
        $type = $index->getAttribute('type');

        if ($type !== IndexType::Trigram->value) {
            return true;
        }

        if ($this->supportForTrigramIndexes === false) {
            $this->message = 'Trigram indexes are not supported';

            return false;
        }

        $attributes = $index->getAttribute('attributes', []);

        $validStringTypes = [
            ColumnType::String->value,
            ColumnType::Varchar->value,
            ColumnType::Text->value,
            ColumnType::MediumText->value,
            ColumnType::LongText->value,
        ];

        foreach ($attributes as $attributeName) {
            $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document;
            if (! in_array($attribute->getAttribute('type', ''), $validStringTypes)) {
                $this->message = 'Trigram index can only be created on string type attributes';

                return false;
            }
        }

        $orders = $index->getAttribute('orders', []);
        $lengths = $index->getAttribute('lengths', []);
        if (! empty($orders) || \count(\array_filter($lengths)) > 0) {
            $this->message = 'Trigram indexes do not support orders or lengths';

            return false;
        }

        return true;
    }

    public function checkKeyUniqueFulltextSupport(Document $index): bool
    {
        $type = $index->getAttribute('type');

        if ($type === IndexType::Key->value && $this->supportForKeyIndexes === false) {
            $this->message = 'Key index is not supported';

            return false;
        }

        if ($type === IndexType::Unique->value && $this->supportForUniqueIndexes === false) {
            $this->message = 'Unique index is not supported';

            return false;
        }

        return true;
    }

    public function checkMultipleFulltextIndexes(Document $index): bool
    {
        if ($this->supportForMultipleFulltextIndexes) {
            return true;
        }

        if ($index->getAttribute('type') === IndexType::Fulltext->value) {
            foreach ($this->indexes as $existingIndex) {
                if ($existingIndex->getId() === $index->getId()) {
                    continue;
                }
                if ($existingIndex->getAttribute('type') === IndexType::Fulltext->value) {
                    $this->message = 'There is already a fulltext index in the collection';

                    return false;
                }
            }
        }

        return true;
    }

    public function checkIdenticalIndexes(Document $index): bool
    {
        if ($this->supportForIdenticalIndexes) {
            return true;
        }

        $indexAttributes = $index->getAttribute('attributes', []);
        $indexOrders = $index->getAttribute('orders', []);
        $indexType = $index->getAttribute('type', '');

        foreach ($this->indexes as $existingIndex) {
            $existingAttributes = $existingIndex->getAttribute('attributes', []);
            $existingOrders = $existingIndex->getAttribute('orders', []);
            $existingType = $existingIndex->getAttribute('type', '');

            $attributesMatch = false;
            if (empty(\array_diff($existingAttributes, $indexAttributes)) &&
                empty(\array_diff($indexAttributes, $existingAttributes))) {
                $attributesMatch = true;
            }

            $ordersMatch = false;
            if (empty(\array_diff($existingOrders, $indexOrders)) &&
                empty(\array_diff($indexOrders, $existingOrders))) {
                $ordersMatch = true;
            }

            if ($attributesMatch && $ordersMatch) {
                // Allow fulltext + key/unique combinations (different purposes)
                $regularTypes = [IndexType::Key->value, IndexType::Unique->value];
                $isRegularIndex = \in_array($indexType, $regularTypes);
                $isRegularExisting = \in_array($existingType, $regularTypes);

                // Only reject if both are regular index types (key or unique)
                if ($isRegularIndex && $isRegularExisting) {
                    $this->message = 'There is already an index with the same attributes and orders';

                    return false;
                }
            }
        }

        return true;
    }

    public function checkObjectIndexes(Document $index): bool
    {
        $type = $index->getAttribute('type');

        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);

        if ($type !== IndexType::Object->value) {
            return true;
        }

        if (! $this->supportForObjectIndexes) {
            $this->message = 'Object indexes are not supported';

            return false;
        }

        if (count($attributes) !== 1) {
            $this->message = 'Object index can be created on a single object attribute';

            return false;
        }

        if (! empty($orders)) {
            $this->message = 'Object index do not support explicit orders. Remove the orders to create this index.';

            return false;
        }

        $attributeName = $attributes[0] ?? '';

        // Object indexes are only allowed on the top-level object attribute,
        // not on nested paths like "data.key.nestedKey".
        if (\strpos($attributeName, '.') !== false) {
            $this->message = 'Object index can only be created on a top-level object attribute';

            return false;
        }

        $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document;
        $attributeType = $attribute->getAttribute('type', '');

        if ($attributeType !== ColumnType::Object->value) {
            $this->message = 'Object index can only be created on object attributes. Attribute "'.$attributeName.'" is of type "'.$attributeType.'"';

            return false;
        }

        return true;
    }

    public function checkTTLIndexes(Document $index): bool
    {
        $type = $index->getAttribute('type');

        $attributes = $index->getAttribute('attributes', []);
        $orders = $index->getAttribute('orders', []);
        $ttl = $index->getAttribute('ttl', 0);
        if ($type !== IndexType::Ttl->value) {
            return true;
        }

        if (count($attributes) !== 1) {
            $this->message = 'TTL indexes must be created on a single datetime attribute.';

            return false;
        }

        $attributeName = $attributes[0] ?? '';
        $attribute = $this->attributes[\strtolower($attributeName)] ?? new Document;
        $attributeType = $attribute->getAttribute('type', '');

        if ($this->supportForAttributes && $attributeType !== ColumnType::Datetime->value) {
            $this->message = 'TTL index can only be created on datetime attributes. Attribute "'.$attributeName.'" is of type "'.$attributeType.'"';

            return false;
        }

        if ($ttl < 1) {
            $this->message = 'TTL must be at least 1 second';

            return false;
        }

        // Check if there's already a TTL index in this collection
        foreach ($this->indexes as $existingIndex) {
            if ($existingIndex->getId() === $index->getId()) {
                continue;
            }

            // Check if existing index is also a TTL index
            if ($existingIndex->getAttribute('type') === IndexType::Ttl->value) {
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
        return $this->isDottedAttribute($attribute) ? \explode('.', $attribute, 2)[0] ?? '' : $attribute;
    }
}
