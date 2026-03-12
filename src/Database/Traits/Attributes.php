<?php

namespace Utopia\Database\Traits;

use Exception;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\SetType;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Attribute;
use Utopia\Database\Validator\Attribute as AttributeValidator;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Database\Validator\IndexDependency as IndexDependencyValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait Attributes
{
    /**
     * Create Attribute
     *
     * @param string $collection
     * @param Attribute $attribute
     * @return bool
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws Exception
     */
    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        $id = $attribute->key;
        $type = $attribute->type->value;
        $size = $attribute->size;
        $required = $attribute->required;
        $default = $attribute->default;
        $signed = $attribute->signed;
        $array = $attribute->array;
        $format = $attribute->format;
        $formatOptions = $attribute->formatOptions;
        $filters = $attribute->filters;

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        if (in_array($type, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value, ColumnType::Vector->value, ColumnType::Object->value], true)) {
            $filters[] = $type;
            $filters = array_unique($filters);
            $attribute->filters = $filters;
        }

        $existsInSchema = false;

        $schemaAttributes = $this->adapter->supports(Capability::SchemaAttributes)
            ? $this->getSchemaAttributes($collection->getId())
            : [];

        try {
            $attributeDoc = $this->validateAttribute(
                $collection,
                $id,
                $type,
                $size,
                $required,
                $default,
                $signed,
                $array,
                $format,
                $formatOptions,
                $filters,
                $schemaAttributes
            );
        } catch (DuplicateException $e) {
            // If the column exists in the physical schema but not in collection
            // metadata, this is recovery from a partial failure where the column
            // was created but metadata wasn't updated. Allow re-creation by
            // skipping physical column creation and proceeding to metadata update.
            // checkDuplicateId (metadata) runs before checkDuplicateInSchema, so
            // if the attribute is absent from metadata the duplicate is in the
            // physical schema only — a recoverable partial-failure state.
            $existsInMetadata = false;
            foreach ($collection->getAttribute('attributes', []) as $attr) {
                if (\strtolower($attr->getAttribute('key', $attr->getId())) === \strtolower($id)) {
                    $existsInMetadata = true;
                    break;
                }
            }

            if ($existsInMetadata) {
                throw $e;
            }

            // Check if the existing schema column matches the requested type.
            // If it matches we can skip column creation. If not, drop the
            // orphaned column so it gets recreated with the correct type.
            $typesMatch = true;
            $expectedColumnType = $this->adapter->getColumnType($type, $size, $signed, $array, $required);
            if ($expectedColumnType !== '') {
                $filteredId = $this->adapter->filter($id);
                foreach ($schemaAttributes as $schemaAttr) {
                    $schemaId = $schemaAttr->getId();
                    if (\strtolower($schemaId) === \strtolower($filteredId)) {
                        $actualColumnType = \strtoupper($schemaAttr->getAttribute('columnType', ''));
                        if ($actualColumnType !== \strtoupper($expectedColumnType)) {
                            $typesMatch = false;
                        }
                        break;
                    }
                }
            }

            if (!$typesMatch) {
                // Column exists with wrong type and is not tracked in metadata,
                // so no indexes or relationships reference it. Drop and recreate.
                $this->adapter->deleteAttribute($collection->getId(), $id);
            } else {
                $existsInSchema = true;
            }

            $attributeDoc = $attribute->toDocument();
        }

        $created = false;

        if (!$existsInSchema) {
            try {
                $created = $this->adapter->createAttribute($collection->getId(), $attribute);

                if (!$created) {
                    throw new DatabaseException('Failed to create attribute');
                }
            } catch (DuplicateException) {
                // Attribute not in metadata (orphan detection above confirmed this).
                // A DuplicateException from the adapter means the column exists only
                // in physical schema — suppress and proceed to metadata update.
            }
        }

        $collection->setAttribute('attributes', $attributeDoc, SetType::Append);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->cleanupAttribute($collection->getId(), $id),
            shouldRollback: $created,
            operationDescription: "attribute creation '{$id}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedDocumentInternal(self::METADATA, $collection->getId()));

        try {
            $this->trigger(self::EVENT_DOCUMENT_PURGE, new Document([
                '$id' => $collection->getId(),
                '$collection' => self::METADATA
            ]));
        } catch (\Throwable $e) {
            // Ignore
        }

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_CREATE, $attributeDoc);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    /**
     * Create Attributes
     *
     * @param string $collection
     * @param array<Attribute> $attributes
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     * @throws Exception
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        if (empty($attributes)) {
            throw new DatabaseException('No attributes to create');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $schemaAttributes = $this->adapter->supports(Capability::SchemaAttributes)
            ? $this->getSchemaAttributes($collection->getId())
            : [];

        $attributeDocuments = [];
        $attributesToCreate = [];
        foreach ($attributes as $attribute) {
            if (empty($attribute->key)) {
                throw new DatabaseException('Missing attribute key');
            }
            if (empty($attribute->type)) {
                throw new DatabaseException('Missing attribute type');
            }

            $existsInSchema = false;

            try {
                $attributeDocument = $this->validateAttribute(
                    $collection,
                    $attribute->key,
                    $attribute->type->value,
                    $attribute->size,
                    $attribute->required,
                    $attribute->default,
                    $attribute->signed,
                    $attribute->array,
                    $attribute->format,
                    $attribute->formatOptions,
                    $attribute->filters,
                    $schemaAttributes
                );
            } catch (DuplicateException $e) {
                // Check if the duplicate is in metadata or only in schema
                $existsInMetadata = false;
                foreach ($collection->getAttribute('attributes', []) as $attr) {
                    if (\strtolower($attr->getAttribute('key', $attr->getId())) === \strtolower($attribute->key)) {
                        $existsInMetadata = true;
                        break;
                    }
                }

                if ($existsInMetadata) {
                    throw $e;
                }

                // Schema-only orphan — check type match
                $expectedColumnType = $this->adapter->getColumnType(
                    $attribute->type->value,
                    $attribute->size,
                    $attribute->signed,
                    $attribute->array,
                    $attribute->required
                );
                if ($expectedColumnType !== '') {
                    $filteredId = $this->adapter->filter($attribute->key);
                    foreach ($schemaAttributes as $schemaAttr) {
                        if (\strtolower($schemaAttr->getId()) === \strtolower($filteredId)) {
                            $actualColumnType = \strtoupper($schemaAttr->getAttribute('columnType', ''));
                            if ($actualColumnType !== \strtoupper($expectedColumnType)) {
                                // Type mismatch — drop orphaned column so it gets recreated
                                $this->adapter->deleteAttribute($collection->getId(), $attribute->key);
                            } else {
                                $existsInSchema = true;
                            }
                            break;
                        }
                    }
                }

                $attributeDocument = $attribute->toDocument();
            }

            $attributeDocuments[] = $attributeDocument;
            if (!$existsInSchema) {
                $attributesToCreate[] = $attribute;
            }
        }

        $created = false;

        if (!empty($attributesToCreate)) {
            try {
                $created = $this->adapter->createAttributes($collection->getId(), $attributesToCreate);

                if (!$created) {
                    throw new DatabaseException('Failed to create attributes');
                }
            } catch (DuplicateException) {
                // Batch failed because at least one column already exists.
                // Fallback to per-attribute creation so non-duplicates still land in schema.
                foreach ($attributesToCreate as $attr) {
                    try {
                        $this->adapter->createAttribute(
                            $collection->getId(),
                            $attr
                        );
                        $created = true;
                    } catch (DuplicateException) {
                        // Column already exists in schema — skip
                    }
                }
            }
        }

        foreach ($attributeDocuments as $attributeDocument) {
            $collection->setAttribute('attributes', $attributeDocument, SetType::Append);
        }

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->cleanupAttributes($collection->getId(), $attributeDocuments),
            shouldRollback: $created,
            operationDescription: 'attributes creation',
            rollbackReturnsErrors: true
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedDocumentInternal(self::METADATA, $collection->getId()));

        try {
            $this->trigger(self::EVENT_DOCUMENT_PURGE, new Document([
                '$id' => $collection->getId(),
                '$collection' => self::METADATA
            ]));
        } catch (\Throwable $e) {
            // Ignore
        }

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_CREATE, $attributeDocuments);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    /**
     * @param Document $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $required
     * @param mixed $default
     * @param bool $signed
     * @param bool $array
     * @param string $format
     * @param array<string, mixed> $formatOptions
     * @param array<string> $filters
     * @param array<Document>|null $schemaAttributes Pre-fetched schema attributes, or null to fetch internally
     * @return Document
     * @throws DuplicateException
     * @throws LimitException
     * @throws Exception
     */
    private function validateAttribute(
        Document $collection,
        string $id,
        string $type,
        int $size,
        bool $required,
        mixed $default,
        bool $signed,
        bool $array,
        ?string $format,
        array $formatOptions,
        array $filters,
        ?array $schemaAttributes = null
    ): Document {
        $attribute = new Document([
            '$id' => ID::custom($id),
            'key' => $id,
            'type' => $type,
            'size' => $size,
            'required' => $required,
            'default' => $default,
            'signed' => $signed,
            'array' => $array,
            'format' => $format,
            'formatOptions' => $formatOptions,
            'filters' => $filters,
        ]);

        $collectionClone = clone $collection;
        $collectionClone->setAttribute('attributes', $attribute, SetType::Append);

        $validator = new AttributeValidator(
            attributes: $collection->getAttribute('attributes', []),
            schemaAttributes: $schemaAttributes ?? ($this->adapter->supports(Capability::SchemaAttributes)
                ? $this->getSchemaAttributes($collection->getId())
                : []),
            maxAttributes: $this->adapter->getLimitForAttributes(),
            maxWidth: $this->adapter->getDocumentSizeLimit(),
            maxStringLength: $this->adapter->getLimitForString(),
            maxVarcharLength: $this->adapter->getMaxVarcharLength(),
            maxIntLength: $this->adapter->getLimitForInt(),
            supportForSchemaAttributes: $this->adapter->supports(Capability::SchemaAttributes),
            supportForVectors: $this->adapter->supports(Capability::Vectors),
            supportForSpatialAttributes: $this->adapter->supports(Capability::Spatial),
            supportForObject: $this->adapter->supports(Capability::Objects),
            attributeCountCallback: fn () => $this->adapter->getCountOfAttributes($collectionClone),
            attributeWidthCallback: fn () => $this->adapter->getAttributeWidth($collectionClone),
            filterCallback: fn ($id) => $this->adapter->filter($id),
            isMigrating: $this->isMigrating(),
            sharedTables: $this->getSharedTables(),
        );

        $validator->isValid($attribute);

        return $attribute;
    }

    /**
     * Get the list of required filters for each data type
     *
     * @param string|null $type Type of the attribute
     *
     * @return array<string>
     */
    protected function getRequiredFilters(?string $type): array
    {
        return match ($type) {
            ColumnType::Datetime->value => ['datetime'],
            default => [],
        };
    }

    /**
     * Function to validate if the default value of an attribute matches its attribute type
     *
     * @param string $type Type of the attribute
     * @param mixed $default Default value of the attribute
     *
     * @return void
     * @throws DatabaseException
     */
    protected function validateDefaultTypes(string $type, mixed $default): void
    {
        $defaultType = \gettype($default);

        if ($defaultType === 'NULL') {
            // Disable null. No validation required
            return;
        }

        if ($defaultType === 'array') {
            // Spatial types require the array itself
            if (!in_array($type, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value]) && $type != ColumnType::Object->value) {
                foreach ($default as $value) {
                    $this->validateDefaultTypes($type, $value);
                }
            }
            return;
        }

        switch ($type) {
            case ColumnType::String->value:
            case ColumnType::Varchar->value:
            case ColumnType::Text->value:
            case ColumnType::MediumText->value:
            case ColumnType::LongText->value:
                if ($defaultType !== 'string') {
                    throw new DatabaseException('Default value ' . $default . ' does not match given type ' . $type);
                }
                break;
            case ColumnType::Integer->value:
            case ColumnType::Double->value:
            case ColumnType::Boolean->value:
                if ($type !== $defaultType) {
                    throw new DatabaseException('Default value ' . $default . ' does not match given type ' . $type);
                }
                break;
            case ColumnType::Datetime->value:
                if ($defaultType !== ColumnType::String->value) {
                    throw new DatabaseException('Default value ' . $default . ' does not match given type ' . $type);
                }
                break;
            case ColumnType::Vector->value:
                // When validating individual vector components (from recursion), they should be numeric
                if ($defaultType !== 'double' && $defaultType !== 'integer') {
                    throw new DatabaseException('Vector components must be numeric values (float or integer)');
                }
                break;
            default:
                $supportedTypes = [
                    ColumnType::String->value,
                    ColumnType::Varchar->value,
                    ColumnType::Text->value,
                    ColumnType::MediumText->value,
                    ColumnType::LongText->value,
                    ColumnType::Integer->value,
                    ColumnType::Double->value,
                    ColumnType::Boolean->value,
                    ColumnType::Datetime->value,
                    ColumnType::Relationship->value
                ];
                if ($this->adapter->supports(Capability::Vectors)) {
                    $supportedTypes[] = ColumnType::Vector->value;
                }
                if ($this->adapter->supports(Capability::Spatial)) {
                    \array_push($supportedTypes, ...[ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value]);
                }
                throw new DatabaseException('Unknown attribute type: ' . $type . '. Must be one of ' . implode(', ', $supportedTypes));
        }
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     *
     * @param string $collection
     * @param string $id
     * @param callable(Document, Document, int|string): void $updateCallback method that receives document, and returns it with changes applied
     *
     * @return Document
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected function updateAttributeMeta(string $collection, string $id, callable $updateCallback): Document
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata attributes');
        }

        $attributes = $collection->getAttribute('attributes', []);
        $index = \array_search($id, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($index === false) {
            throw new NotFoundException('Attribute not found');
        }

        // Execute update from callback
        $updateCallback($attributes[$index], $collection, $index);

        $collection->setAttribute('attributes', $attributes);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "attribute metadata update '{$id}'"
        );

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_UPDATE, $attributes[$index]);
        } catch (\Throwable $e) {
            // Ignore
        }

        return $attributes[$index];
    }

    /**
     * Update required status of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param bool $required
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeRequired(string $collection, string $id, bool $required): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($required) {
            $attribute->setAttribute('required', $required);
        });
    }

    /**
     * Update format of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param string $format validation format of attribute
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeFormat(string $collection, string $id, string $format): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($format) {
            if (!Structure::hasFormat($format, $attribute->getAttribute('type'))) {
                throw new DatabaseException('Format "' . $format . '" not available for attribute type "' . $attribute->getAttribute('type') . '"');
            }

            $attribute->setAttribute('format', $format);
        });
    }

    /**
     * Update format options of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param array<string, mixed> $formatOptions assoc array with custom options that can be passed for the format validation
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeFormatOptions(string $collection, string $id, array $formatOptions): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($formatOptions) {
            $attribute->setAttribute('formatOptions', $formatOptions);
        });
    }

    /**
     * Update filters of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param array<string> $filters
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeFilters(string $collection, string $id, array $filters): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($filters) {
            $attribute->setAttribute('filters', $filters);
        });
    }

    /**
     * Update default value of attribute
     *
     * @param string $collection
     * @param string $id
     * @param mixed $default
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeDefault(string $collection, string $id, mixed $default = null): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($default) {
            if ($attribute->getAttribute('required') === true) {
                throw new DatabaseException('Cannot set a default value on a required attribute');
            }

            $this->validateDefaultTypes($attribute->getAttribute('type'), $default);

            $attribute->setAttribute('default', $default);
        });
    }

    /**
     * Update Attribute. This method is for updating data that causes underlying structure to change. Check out other updateAttribute methods if you are looking for metadata adjustments.
     *
     * @param string $collection
     * @param string $id
     * @param ColumnType|string|null $type
     * @param int|null $size utf8mb4 chars length
     * @param bool|null $required
     * @param mixed $default
     * @param bool $signed
     * @param bool $array
     * @param string|null $format
     * @param array<string, mixed>|null $formatOptions
     * @param array<string>|null $filters
     * @param string|null $newKey
     * @return Document
     * @throws Exception
     */
    public function updateAttribute(string $collection, string $id, ColumnType|string|null $type = null, ?int $size = null, ?bool $required = null, mixed $default = null, ?bool $signed = null, ?bool $array = null, ?string $format = null, ?array $formatOptions = null, ?array $filters = null, ?string $newKey = null): Document
    {
        if ($type instanceof ColumnType) {
            $type = $type->value;
        }
        $collectionDoc = $this->silent(fn () => $this->getCollection($collection));

        if ($collectionDoc->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata attributes');
        }

        $attributes = $collectionDoc->getAttribute('attributes', []);
        $attributeIndex = \array_search($id, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attributeIndex === false) {
            throw new NotFoundException('Attribute not found');
        }

        $attribute = $attributes[$attributeIndex];

        $originalType = $attribute->getAttribute('type');
        $originalSize = $attribute->getAttribute('size');
        $originalSigned = $attribute->getAttribute('signed');
        $originalArray = $attribute->getAttribute('array');
        $originalRequired = $attribute->getAttribute('required');
        $originalKey = $attribute->getAttribute('key');

        $originalIndexes = [];
        foreach ($collectionDoc->getAttribute('indexes', []) as $index) {
            $originalIndexes[] = clone $index;
        }

        $altering = !\is_null($type)
            || !\is_null($size)
            || !\is_null($signed)
            || !\is_null($array)
            || !\is_null($newKey);
        $type ??= $attribute->getAttribute('type');
        $size ??= $attribute->getAttribute('size');
        $signed ??= $attribute->getAttribute('signed');
        $required ??= $attribute->getAttribute('required');
        $default ??= $attribute->getAttribute('default');
        $array ??= $attribute->getAttribute('array');
        $format ??= $attribute->getAttribute('format');
        $formatOptions ??= $attribute->getAttribute('formatOptions');
        $filters ??= $attribute->getAttribute('filters');

        if ($required === true && !\is_null($default)) {
            $default = null;
        }

        // we need to alter table attribute type to NOT NULL/NULL for change in required
        if (!$this->adapter->supports(Capability::SpatialIndexNull) && in_array($type, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value])) {
            $altering = true;
        }

        switch ($type) {
            case ColumnType::String->value:
                if (empty($size)) {
                    throw new DatabaseException('Size length is required');
                }

                if ($size > $this->adapter->getLimitForString()) {
                    throw new DatabaseException('Max size allowed for string is: ' . number_format($this->adapter->getLimitForString()));
                }
                break;

            case ColumnType::Varchar->value:
                if (empty($size)) {
                    throw new DatabaseException('Size length is required');
                }

                if ($size > $this->adapter->getMaxVarcharLength()) {
                    throw new DatabaseException('Max size allowed for varchar is: ' . number_format($this->adapter->getMaxVarcharLength()));
                }
                break;

            case ColumnType::Text->value:
            case ColumnType::MediumText->value:
            case ColumnType::LongText->value:
                // Text types don't require size validation as they have fixed max sizes
                break;

            case ColumnType::Integer->value:
                $limit = ($signed) ? $this->adapter->getLimitForInt() / 2 : $this->adapter->getLimitForInt();
                if ($size > $limit) {
                    throw new DatabaseException('Max size allowed for int is: ' . number_format($limit));
                }
                break;
            case ColumnType::Double->value:
            case ColumnType::Boolean->value:
            case ColumnType::Datetime->value:
                if (!empty($size)) {
                    throw new DatabaseException('Size must be empty');
                }
                break;
            case ColumnType::Object->value:
                if (!$this->adapter->supports(Capability::Objects)) {
                    throw new DatabaseException('Object attributes are not supported');
                }
                if (!empty($size)) {
                    throw new DatabaseException('Size must be empty for object attributes');
                }
                if (!empty($array)) {
                    throw new DatabaseException('Object attributes cannot be arrays');
                }
                break;
            case ColumnType::Point->value:
            case ColumnType::Linestring->value:
            case ColumnType::Polygon->value:
                if (!$this->adapter->supports(Capability::Spatial)) {
                    throw new DatabaseException('Spatial attributes are not supported');
                }
                if (!empty($size)) {
                    throw new DatabaseException('Size must be empty for spatial attributes');
                }
                if (!empty($array)) {
                    throw new DatabaseException('Spatial attributes cannot be arrays');
                }
                break;
            case ColumnType::Vector->value:
                if (!$this->adapter->supports(Capability::Vectors)) {
                    throw new DatabaseException('Vector types are not supported by the current database');
                }
                if ($array) {
                    throw new DatabaseException('Vector type cannot be an array');
                }
                if ($size <= 0) {
                    throw new DatabaseException('Vector dimensions must be a positive integer');
                }
                if ($size > self::MAX_VECTOR_DIMENSIONS) {
                    throw new DatabaseException('Vector dimensions cannot exceed ' . self::MAX_VECTOR_DIMENSIONS);
                }
                if ($default !== null) {
                    if (!\is_array($default)) {
                        throw new DatabaseException('Vector default value must be an array');
                    }
                    if (\count($default) !== $size) {
                        throw new DatabaseException('Vector default value must have exactly ' . $size . ' elements');
                    }
                    foreach ($default as $component) {
                        if (!\is_int($component) && !\is_float($component)) {
                            throw new DatabaseException('Vector default value must contain only numeric elements');
                        }
                    }
                }
                break;
            default:
                $supportedTypes = [
                    ColumnType::String->value,
                    ColumnType::Varchar->value,
                    ColumnType::Text->value,
                    ColumnType::MediumText->value,
                    ColumnType::LongText->value,
                    ColumnType::Integer->value,
                    ColumnType::Double->value,
                    ColumnType::Boolean->value,
                    ColumnType::Datetime->value,
                    ColumnType::Relationship->value
                ];
                if ($this->adapter->supports(Capability::Vectors)) {
                    $supportedTypes[] = ColumnType::Vector->value;
                }
                if ($this->adapter->supports(Capability::Spatial)) {
                    \array_push($supportedTypes, ...[ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value]);
                }
                throw new DatabaseException('Unknown attribute type: ' . $type . '. Must be one of ' . implode(', ', $supportedTypes));
        }

        /** Ensure required filters for the attribute are passed */
        $requiredFilters = $this->getRequiredFilters($type);
        if (!empty(array_diff($requiredFilters, $filters))) {
            throw new DatabaseException("Attribute of type: $type requires the following filters: " . implode(",", $requiredFilters));
        }

        if ($format) {
            if (!Structure::hasFormat($format, $type)) {
                throw new DatabaseException('Format ("' . $format . '") not available for this attribute type ("' . $type . '")');
            }
        }

        if (!\is_null($default)) {
            if ($required) {
                throw new DatabaseException('Cannot set a default value on a required attribute');
            }

            $this->validateDefaultTypes($type, $default);
        }

        $attribute
            ->setAttribute('$id', $newKey ?? $id)
            ->setattribute('key', $newKey ?? $id)
            ->setAttribute('type', $type)
            ->setAttribute('size', $size)
            ->setAttribute('signed', $signed)
            ->setAttribute('array', $array)
            ->setAttribute('format', $format)
            ->setAttribute('formatOptions', $formatOptions)
            ->setAttribute('filters', $filters)
            ->setAttribute('required', $required)
            ->setAttribute('default', $default);

        $attributes = $collectionDoc->getAttribute('attributes');
        $attributes[$attributeIndex] = $attribute;
        $collectionDoc->setAttribute('attributes', $attributes, SetType::Assign);

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            $this->adapter->getAttributeWidth($collectionDoc) >= $this->adapter->getDocumentSizeLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot update attribute.');
        }

        if (in_array($type, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value], true) && !$this->adapter->supports(Capability::SpatialIndexNull)) {
            $attributeMap = [];
            foreach ($attributes as $attrDoc) {
                $key = \strtolower($attrDoc->getAttribute('key', $attrDoc->getAttribute('$id')));
                $attributeMap[$key] = $attrDoc;
            }

            $indexes = $collectionDoc->getAttribute('indexes', []);
            foreach ($indexes as $index) {
                if ($index->getAttribute('type') !== IndexType::Spatial->value) {
                    continue;
                }
                $indexAttributes = $index->getAttribute('attributes', []);
                foreach ($indexAttributes as $attributeName) {
                    $lookup = \strtolower($attributeName);
                    if (!isset($attributeMap[$lookup])) {
                        continue;
                    }
                    $attrDoc = $attributeMap[$lookup];
                    $attrType = $attrDoc->getAttribute('type');
                    $attrRequired = (bool)$attrDoc->getAttribute('required', false);

                    if (in_array($attrType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value], true) && !$attrRequired) {
                        throw new IndexException('Spatial indexes do not allow null values. Mark the attribute "' . $attributeName . '" as required or create the index on a column with no null values.');
                    }
                }
            }
        }

        $updated = false;

        if ($altering) {
            $indexes = $collectionDoc->getAttribute('indexes');

            if (!\is_null($newKey) && $id !== $newKey) {
                foreach ($indexes as $index) {
                    if (in_array($id, $index['attributes'])) {
                        $index['attributes'] = array_map(function ($attribute) use ($id, $newKey) {
                            return $attribute === $id ? $newKey : $attribute;
                        }, $index['attributes']);
                    }
                }

                /**
                 * Check index dependency if we are changing the key
                 */
                $validator = new IndexDependencyValidator(
                    $collectionDoc->getAttribute('indexes', []),
                    $this->adapter->supports(Capability::CastIndexArray),
                );

                if (!$validator->isValid($attribute)) {
                    throw new DependencyException($validator->getDescription());
                }
            }

            /**
             * Since we allow changing type & size we need to validate index length
             */
            if ($this->validate) {
                $validator = new IndexValidator(
                    $attributes,
                    $originalIndexes,
                    $this->adapter->getMaxIndexLength(),
                    $this->adapter->getInternalIndexesKeys(),
                    $this->adapter->supports(Capability::IndexArray),
                    $this->adapter->supports(Capability::SpatialIndexNull),
                    $this->adapter->supports(Capability::SpatialIndexOrder),
                    $this->adapter->supports(Capability::Vectors),
                    $this->adapter->supports(Capability::DefinedAttributes),
                    $this->adapter->supports(Capability::MultipleFulltextIndexes),
                    $this->adapter->supports(Capability::IdenticalIndexes),
                    $this->adapter->supports(Capability::ObjectIndexes),
                    $this->adapter->supports(Capability::TrigramIndex),
                    $this->adapter->supports(Capability::Spatial),
                    $this->adapter->supports(Capability::Index),
                    $this->adapter->supports(Capability::UniqueIndex),
                    $this->adapter->supports(Capability::Fulltext),
                    $this->adapter->supports(Capability::TTLIndexes),
                    $this->adapter->supports(Capability::Objects)
                );

                foreach ($indexes as $index) {
                    if (!$validator->isValid($index)) {
                        throw new IndexException($validator->getDescription());
                    }
                }
            }

            $updateAttrModel = new Attribute(
                key: $id,
                type: ColumnType::from($type),
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
            );
            $updated = $this->adapter->updateAttribute($collection, $updateAttrModel, $newKey);

            if (!$updated) {
                throw new DatabaseException('Failed to update attribute');
            }
        }

        $collectionDoc->setAttribute('attributes', $attributes);

        $rollbackAttrModel = new Attribute(
            key: $newKey ?? $id,
            type: ColumnType::from($originalType),
            size: $originalSize,
            required: $originalRequired,
            signed: $originalSigned,
            array: $originalArray,
        );
        $this->updateMetadata(
            collection: $collectionDoc,
            rollbackOperation: fn () => $this->adapter->updateAttribute(
                $collection,
                $rollbackAttrModel,
                $originalKey
            ),
            shouldRollback: $updated,
            operationDescription: "attribute update '{$id}'",
            silentRollback: true
        );

        if ($altering) {
            $this->withRetries(fn () => $this->purgeCachedCollection($collection));
        }
        $this->withRetries(fn () => $this->purgeCachedDocumentInternal(self::METADATA, $collection));

        try {
            $this->trigger(self::EVENT_DOCUMENT_PURGE, new Document([
                '$id' => $collection,
                '$collection' => self::METADATA
            ]));
        } catch (\Throwable $e) {
            // Ignore
        }

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_UPDATE, $attribute);
        } catch (\Throwable $e) {
            // Ignore
        }

        return $attribute;
    }

    /**
     * Checks if attribute can be added to collection.
     * Used to check attribute limits without asking the database
     * Returns true if attribute can be added to collection, throws exception otherwise
     *
     * @param Document $collection
     * @param Document $attribute
     *
     * @return bool
     * @throws LimitException
     */
    public function checkAttribute(Document $collection, Document $attribute): bool
    {
        $collection = clone $collection;

        $collection->setAttribute('attributes', $attribute, SetType::Append);

        if (
            $this->adapter->getLimitForAttributes() > 0 &&
            $this->adapter->getCountOfAttributes($collection) > $this->adapter->getLimitForAttributes()
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute. Current attribute count is ' . $this->adapter->getCountOfAttributes($collection) . ' but the maximum is ' . $this->adapter->getLimitForAttributes() . '. Remove some attributes to free up space.');
        }

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getDocumentSizeLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute. Current row width is ' . $this->adapter->getAttributeWidth($collection) . ' bytes but the maximum is ' . $this->adapter->getDocumentSizeLimit() . ' bytes. Reduce the size of existing attributes or remove some attributes to free up space.');
        }

        return true;
    }

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws ConflictException
     * @throws DatabaseException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $attribute = null;

        foreach ($attributes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $id) {
                $attribute = $value;
                unset($attributes[$key]);
                break;
            }
        }

        if (\is_null($attribute)) {
            throw new NotFoundException('Attribute not found');
        }

        if ($attribute['type'] === ColumnType::Relationship->value) {
            throw new DatabaseException('Cannot delete relationship as an attribute');
        }

        if ($this->validate) {
            $validator = new IndexDependencyValidator(
                $collection->getAttribute('indexes', []),
                $this->adapter->supports(Capability::CastIndexArray),
            );

            if (!$validator->isValid($attribute)) {
                throw new DependencyException($validator->getDescription());
            }
        }

        foreach ($indexes as $indexKey => $index) {
            $indexAttributes = $index->getAttribute('attributes', []);

            $indexAttributes = \array_filter($indexAttributes, fn ($attribute) => $attribute !== $id);

            if (empty($indexAttributes)) {
                unset($indexes[$indexKey]);
            } else {
                $index->setAttribute('attributes', \array_values($indexAttributes));
            }
        }

        $collection->setAttribute('attributes', \array_values($attributes));
        $collection->setAttribute('indexes', \array_values($indexes));

        $shouldRollback = false;
        try {
            if (!$this->adapter->deleteAttribute($collection->getId(), $id)) {
                throw new DatabaseException('Failed to delete attribute');
            }
            $shouldRollback = true;
        } catch (NotFoundException) {
            // Ignore
        }

        $rollbackAttr = new Attribute(
            key: $id,
            type: ColumnType::from($attribute['type']),
            size: $attribute['size'],
            required: $attribute['required'] ?? false,
            signed: $attribute['signed'] ?? true,
            array: $attribute['array'] ?? false,
        );
        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->adapter->createAttribute(
                $collection->getId(),
                $rollbackAttr
            ),
            shouldRollback: $shouldRollback,
            operationDescription: "attribute deletion '{$id}'",
            silentRollback: true
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));
        $this->withRetries(fn () => $this->purgeCachedDocumentInternal(self::METADATA, $collection->getId()));

        try {
            $this->trigger(self::EVENT_DOCUMENT_PURGE, new Document([
                '$id' => $collection->getId(),
                '$collection' => self::METADATA
            ]));
        } catch (\Throwable $e) {
            // Ignore
        }

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_DELETE, $attribute);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old Current attribute ID
     * @param string $new
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        /**
         * @var array<Document> $attributes
         */
        $attributes = $collection->getAttribute('attributes', []);

        /**
         * @var array<Document> $indexes
         */
        $indexes = $collection->getAttribute('indexes', []);

        $attribute = new Document();

        foreach ($attributes as $value) {
            if ($value->getId() === $old) {
                $attribute = $value;
            }

            if ($value->getId() === $new) {
                throw new DuplicateException('Attribute name already used');
            }
        }

        if ($attribute->isEmpty()) {
            throw new NotFoundException('Attribute not found');
        }

        if ($this->validate) {
            $validator = new IndexDependencyValidator(
                $collection->getAttribute('indexes', []),
                $this->adapter->supports(Capability::CastIndexArray),
            );

            if (!$validator->isValid($attribute)) {
                throw new DependencyException($validator->getDescription());
            }
        }

        $attribute->setAttribute('$id', $new);
        $attribute->setAttribute('key', $new);

        foreach ($indexes as $index) {
            $indexAttributes = $index->getAttribute('attributes', []);

            $indexAttributes = \array_map(fn ($attr) => ($attr === $old) ? $new : $attr, $indexAttributes);

            $index->setAttribute('attributes', $indexAttributes);
        }

        $renamed = false;
        try {
            $renamed = $this->adapter->renameAttribute($collection->getId(), $old, $new);
            if (!$renamed) {
                throw new DatabaseException('Failed to rename attribute');
            }
        } catch (\Throwable $e) {
            // Check if the rename already happened in schema (orphan from prior
            // partial failure where rename succeeded but metadata update failed).
            // We verified $new doesn't exist in metadata (above), so if $new
            // exists in schema, it must be from a prior rename.
            if ($this->adapter->supports(Capability::SchemaAttributes)) {
                $schemaAttributes = $this->getSchemaAttributes($collection->getId());
                $filteredNew = $this->adapter->filter($new);
                $newExistsInSchema = false;
                foreach ($schemaAttributes as $schemaAttr) {
                    if (\strtolower($schemaAttr->getId()) === \strtolower($filteredNew)) {
                        $newExistsInSchema = true;
                        break;
                    }
                }
                if ($newExistsInSchema) {
                    $renamed = true;
                } else {
                    throw new DatabaseException("Failed to rename attribute '{$old}' to '{$new}': " . $e->getMessage(), previous: $e);
                }
            } else {
                throw new DatabaseException("Failed to rename attribute '{$old}' to '{$new}': " . $e->getMessage(), previous: $e);
            }
        }

        $collection->setAttribute('attributes', $attributes);
        $collection->setAttribute('indexes', $indexes);

        $this->updateMetadata(
            collection: $collection,
            rollbackOperation: fn () => $this->adapter->renameAttribute($collection->getId(), $new, $old),
            shouldRollback: $renamed,
            operationDescription: "attribute rename '{$old}' to '{$new}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($collection->getId()));

        try {
            $this->trigger(self::EVENT_ATTRIBUTE_UPDATE, $attribute);
        } catch (\Throwable $e) {
            // Ignore
        }

        return $renamed;
    }

    /**
     * Cleanup (delete) a single attribute with retry logic
     *
     * @param string $collectionId The collection ID
     * @param string $attributeId The attribute ID
     * @param int $maxAttempts Maximum retry attempts
     * @return void
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupAttribute(
        string $collectionId,
        string $attributeId,
        int $maxAttempts = 3
    ): void {
        $this->cleanup(
            fn () => $this->adapter->deleteAttribute($collectionId, $attributeId),
            'attribute',
            $attributeId,
            $maxAttempts
        );
    }

    /**
     * Cleanup (delete) multiple attributes with retry logic
     *
     * @param string $collectionId The collection ID
     * @param array<Document> $attributeDocuments The attribute documents to cleanup
     * @param int $maxAttempts Maximum retry attempts per attribute
     * @return array<string> Array of error messages for failed cleanups (empty if all succeeded)
     */
    private function cleanupAttributes(
        string $collectionId,
        array $attributeDocuments,
        int $maxAttempts = 3
    ): array {
        $errors = [];

        foreach ($attributeDocuments as $attributeDocument) {
            try {
                $this->cleanupAttribute($collectionId, $attributeDocument->getId(), $maxAttempts);
            } catch (DatabaseException $e) {
                // Continue cleaning up other attributes even if one fails
                $errors[] = $e->getMessage();
            }
        }

        return $errors;
    }

    /**
     * Rollback metadata state by removing specified attributes from collection
     *
     * @param Document $collection The collection document
     * @param array<string> $attributeIds Attribute IDs to remove
     * @return void
     */
    private function rollbackAttributeMetadata(Document $collection, array $attributeIds): void
    {
        $attributes = $collection->getAttribute('attributes', []);
        $filteredAttributes = \array_filter(
            $attributes,
            fn ($attr) => !\in_array($attr->getId(), $attributeIds)
        );
        $collection->setAttribute('attributes', \array_values($filteredAttributes));
    }
}
