<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use Utopia\Validator;

/**
 * Validates database index definitions including type support, attribute references, lengths, and constraints.
 */
class IndexDefinition extends Validator
{
    private const array STRING_TYPES = [
        ColumnType::String,
        ColumnType::Varchar,
        ColumnType::Text,
        ColumnType::MediumText,
        ColumnType::LongText,
    ];

    private const int BIG_INTEGER_SIZE = 8;

    protected string $message = 'Invalid index';

    /**
     * @var array<string, Attribute>
     */
    protected array $attributes;

    /**
     * @var array<Index>
     */
    protected array $indexes;

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  array<Index|Document>  $indexes
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
            $typed = $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute);
            $this->attributes[\strtolower($typed->key)] = $typed;
        }
        foreach (Database::internalAttributesFor(true) as $attribute) {
            $key = \strtolower($attribute->key);
            $this->attributes[$key] = $attribute;
        }

        $this->indexes = [];
        foreach ($indexes as $index) {
            $this->indexes[] = $index instanceof Index ? $index : Index::fromDocument($index);
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
     * Stored index documents are checked for a known type and a TTL before they are hydrated, since
     * Index::fromDocument() reads stored metadata leniently.
     *
     * @param  mixed  $value
     *
     * @throws DatabaseException
     */
    public function isValid($value): bool
    {
        if ($value instanceof Document) {
            if (! $this->checkStoredDefinition($value)) {
                return false;
            }
            $value = Index::fromDocument($value);
        }

        if (! $value instanceof Index) {
            $this->message = 'Value must be an index';

            return false;
        }

        $index = $value;

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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkValidIndex(Index $index): bool
    {
        $type = $index->type;
        if ($this->supportForObjects) {
            $dottedAttributes = array_filter($index->attributes, fn (string $name) => ! isset($this->attributes[\strtolower($name)]) && $this->isDottedAttribute($name));
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
                $this->message = self::unknownTypeMessage($type->value);

                return false;
        }

        return true;
    }

    private function checkStoredDefinition(Document $index): bool
    {
        $type = $index->getAttribute('type');
        if ($type instanceof IndexType) {
            $type = $type->value;
        }

        if (! \is_string($type) || IndexType::tryFrom($type) === null) {
            $this->message = self::unknownTypeMessage(\is_string($type) ? $type : '');

            return false;
        }

        if ($type === IndexType::Ttl->value && $index->getAttribute('ttl') === null) {
            $this->message = 'TTL must be at least 1 second';

            return false;
        }

        return true;
    }

    private static function unknownTypeMessage(string $type): string
    {
        return 'Unknown index type: '.$type.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value.', '.IndexType::Spatial->value.', '.IndexType::Object->value.', '.IndexType::HnswEuclidean->value.', '.IndexType::HnswCosine->value.', '.IndexType::HnswDot->value.', '.IndexType::Trigram->value.', '.IndexType::Ttl->value;
    }

    /**
     * Check that all index attributes exist in the collection schema.
     *
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkValidAttributes(Index $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        foreach ($index->attributes as $attribute) {
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkEmptyIndexAttributes(Index $index): bool
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkDuplicatedAttributes(Index $index): bool
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkFulltextIndexNonString(Index $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        if ($index->type === IndexType::Fulltext) {
            foreach ($index->attributes as $attributeName) {
                $attribute = $this->findAttribute($attributeName);
                if (! $this->isStringAttribute($attribute)) {
                    $key = $attribute === null ? $attributeName : $attribute->key;
                    $this->message = 'Attribute "'.$key.'" cannot be part of a fulltext index, must be of type string';

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Check constraints for indexes on array attributes including type, length, and count limits.
     *
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkArrayIndexes(Index $index): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }

        $arrayAttributes = [];
        $indexType = $index->type;
        $lengths = $index->lengths;
        $orders = $index->orders;
        foreach ($index->attributes as $attributePosition => $attributeName) {
            $attribute = $this->findAttribute($attributeName);

            if ($attribute !== null && $attribute->array) {
                // Database::INDEX_UNIQUE Is not allowed! since mariaDB VS MySQL makes the unique Different on values
                if ($indexType !== IndexType::Key) {
                    $this->message = '"'.ucfirst($indexType->value).'" index is forbidden on array attributes';

                    return false;
                }

                if (empty($lengths[$attributePosition])) {
                    $this->message = 'Index length for array not specified';

                    return false;
                }

                $arrayAttributes[] = $attribute->key;
                if (count($arrayAttributes) > 1) {
                    $this->message = 'An index may only contain one array attribute';

                    return false;
                }

                $direction = $orders[$attributePosition] ?? null;
                if ($direction !== null) {
                    $this->message = 'Invalid index order "'.$direction->value.'" on array attribute "'.$attribute->key.'"';

                    return false;
                }

                if ($this->supportForArrayIndexes === false) {
                    $this->message = 'Indexing an array attribute is not supported';

                    return false;
                }
            } elseif (! $this->isStringAttribute($attribute) && ! empty($lengths[$attributePosition])) {
                $type = $attribute === null ? '' : $attribute->type->value;
                $this->message = 'Cannot set a length on "'.$type.'" attributes';

                return false;
            }
        }

        return true;
    }

    /**
     * Check that index lengths are valid and do not exceed the maximum allowed total.
     *
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkIndexLengths(Index $index): bool
    {
        if ($index->type === IndexType::Fulltext) {
            return true;
        }

        if (! $this->supportForAttributes) {
            return true;
        }

        $total = 0;
        $lengths = $index->lengths;
        $indexedAttributes = $index->attributes;
        if (count($lengths) > count($indexedAttributes)) {
            $this->message = 'Invalid index lengths. Count of lengths must be equal or less than the number of attributes.';

            return false;
        }
        foreach ($indexedAttributes as $attributePosition => $attributeName) {
            if ($this->supportForObjects && ! isset($this->attributes[\strtolower($attributeName)])) {
                $attributeName = $this->getBaseAttributeFromDottedAttribute($attributeName);
            }
            $attribute = $this->attributes[\strtolower($attributeName)];

            $attributeType = $attribute->type;
            $resolvedSize = $attribute->resolvedSize();
            [$attributeSize, $indexLength] = match ($attributeType) {
                ColumnType::String,
                ColumnType::Varchar,
                ColumnType::Text,
                ColumnType::MediumText,
                ColumnType::LongText => [
                    $resolvedSize,
                    ! empty($lengths[$attributePosition]) ? $lengths[$attributePosition] : $resolvedSize,
                ],
                ColumnType::Float,
                ColumnType::Double,
                ColumnType::BigInteger,
                ColumnType::Id => [2, 2],
                ColumnType::Integer => $resolvedSize >= self::BIG_INTEGER_SIZE ? [2, 2] : [1, 1],
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkReservedNames(Index $index): bool
    {
        $key = \strtolower($index->key);

        foreach ($this->reservedKeys as $reserved) {
            if ($key === \strtolower($reserved)) {
                $this->message = 'Index key name is reserved';

                return false;
            }
        }

        return true;
    }

    /**
     * Check spatial index constraints including attribute type and nullability.
     *
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkSpatialIndexes(Index $index): bool
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
            $attribute = $this->findAttribute($attributeName);
            $attributeType = $attribute->type ?? ColumnType::String;

            if (! \in_array($attributeType, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
                $this->message = 'Spatial index can only be created on spatial attributes (point, linestring, polygon). Attribute "'.$attributeName.'" is of type "'.$attributeType->value.'"';

                return false;
            }

            if (! ($attribute->required ?? false) && ! $this->supportForSpatialIndexNull) {
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkNonSpatialIndexOnSpatialAttributes(Index $index): bool
    {
        $type = $index->type;

        if ($type === IndexType::Spatial) {
            return true;
        }

        foreach ($index->attributes as $attributeName) {
            $attribute = $this->findAttribute($attributeName);
            $attributeType = $attribute->type ?? ColumnType::String;

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
    public function checkVectorIndexes(Index $index): bool
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

        if ($this->findAttribute($index->attributes[0])?->type !== ColumnType::Vector) {
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
    public function checkTrigramIndexes(Index $index): bool
    {
        $type = $index->type;

        if ($type !== IndexType::Trigram) {
            return true;
        }

        if ($this->supportForTrigramIndexes === false) {
            $this->message = 'Trigram indexes are not supported';

            return false;
        }

        foreach ($index->attributes as $attributeName) {
            if (! $this->isStringAttribute($this->findAttribute($attributeName))) {
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkKeyUniqueFulltextSupport(Index $index): bool
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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkMultipleFulltextIndexes(Index $index): bool
    {
        if ($this->supportForMultipleFulltextIndexes) {
            return true;
        }

        if ($index->type === IndexType::Fulltext) {
            $key = $index->key;
            foreach ($this->indexes as $existingIndex) {
                if ($existingIndex->key === $key) {
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
     * The index itself is skipped, so revalidating an existing index does not compare it with itself.
     *
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkIdenticalIndexes(Index $index): bool
    {
        if ($this->supportForIdenticalIndexes) {
            return true;
        }

        $key = \strtolower($index->key);
        $indexedAttributes = $index->attributes;
        $incomingOrders = self::orderValues($index);
        $regularTypes = [IndexType::Key, IndexType::Unique];
        $isRegularIndex = \in_array($index->type, $regularTypes);

        foreach ($this->indexes as $existingIndex) {
            if (\strtolower($existingIndex->key) === $key) {
                continue;
            }

            $existingAttributes = $existingIndex->attributes;
            $attributesMatch = false;
            if (empty(\array_diff($existingAttributes, $indexedAttributes)) &&
                empty(\array_diff($indexedAttributes, $existingAttributes))) {
                $attributesMatch = true;
            }

            $ordersMatch = false;
            $existingOrders = self::orderValues($existingIndex);
            if (empty(\array_diff($existingOrders, $incomingOrders)) &&
                empty(\array_diff($incomingOrders, $existingOrders))) {
                $ordersMatch = true;
            }

            if ($attributesMatch && $ordersMatch) {
                // Allow fulltext + key/unique combinations (different purposes)
                $isRegularExisting = \in_array($existingIndex->type, $regularTypes);

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
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkObjectIndexes(Index $index): bool
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

        $attributeName = $index->attributes[0];

        // Object indexes are only allowed on the top-level object attribute,
        // not on nested paths like "data.key.nestedKey".
        if (\strpos($attributeName, '.') !== false) {
            $this->message = 'Object index can only be created on a top-level object attribute';

            return false;
        }

        $attribute = $this->findAttribute($attributeName);
        $attributeType = $attribute->type ?? ColumnType::String;

        if ($attributeType !== ColumnType::Object) {
            $this->message = 'Object index can only be created on object attributes. Attribute "'.$attributeName.'" is of type "'.$attributeType->value.'"';

            return false;
        }

        return true;
    }

    /**
     * Check TTL index constraints including single-attribute, datetime type, and uniqueness requirements.
     *
     * @param Index $index The index to validate
     * @return bool
     */
    public function checkTTLIndexes(Index $index): bool
    {
        $type = $index->type;

        if ($type !== IndexType::Ttl) {
            return true;
        }

        if (count($index->attributes) !== 1) {
            $this->message = 'TTL indexes must be created on a single datetime attribute.';

            return false;
        }

        $attributeName = $index->attributes[0];
        $attribute = $this->findAttribute($attributeName);
        $attributeType = $attribute->type ?? ColumnType::String;

        if ($this->supportForAttributes && $attributeType !== ColumnType::Datetime) {
            $this->message = 'TTL index can only be created on datetime attributes. Attribute "'.$attributeName.'" is of type "'.$attributeType->value.'"';

            return false;
        }

        if (($index->ttl ?? 0) < 1) {
            $this->message = 'TTL must be at least 1 second';

            return false;
        }

        $key = $index->key;
        foreach ($this->indexes as $existingIndex) {
            if ($existingIndex->key === $key) {
                continue;
            }

            if ($existingIndex->type === IndexType::Ttl) {
                $this->message = 'There can be only one TTL index in a collection';

                return false;
            }
        }

        return true;
    }

    /**
     * Returns null for names outside the schema, such as a dotted path into an object attribute,
     * so guards that only accept declared types reject them instead of treating them as a blank
     * attribute of the default type.
     */
    private function findAttribute(string $name): ?Attribute
    {
        return $this->attributes[\strtolower($name)] ?? null;
    }

    private function isStringAttribute(?Attribute $attribute): bool
    {
        return $attribute !== null && \in_array($attribute->type, self::STRING_TYPES, true);
    }

    private function isDottedAttribute(string $attribute): bool
    {
        return \str_contains($attribute, '.');
    }

    private function getBaseAttributeFromDottedAttribute(string $attribute): string
    {
        return $this->isDottedAttribute($attribute) ? \explode('.', $attribute, 2)[0] : $attribute;
    }

    /**
     * @return list<string|null>
     */
    private static function orderValues(Index $index): array
    {
        return \array_map(
            static fn (?OrderDirection $order): ?string => $order?->value,
            $index->orders,
        );
    }
}
