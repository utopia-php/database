<?php

namespace Utopia\Database\Traits;

use Closure;
use Exception;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Mismatch as MismatchException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Index;
use Utopia\Database\SetType;
use Utopia\Database\Validator\Attribute as AttributeValidator;
use Utopia\Database\Validator\BigInt;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Database\Validator\IndexDependency as IndexDependencyValidator;
use Utopia\Database\Validator\Spatial as SpatialValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Provides CRUD operations for collection attributes including creation, update, rename, and deletion.
 */
trait Attributes
{
    /**
     * @var array<string, string>
     */
    private const array COLUMN_TYPE_SPELLINGS = [
        '/\s+/' => ' ',
        '/ (NOT )?NULL$/' => '',
        '/^(POINT|LINESTRING|POLYGON)\b.*$/' => '$1',
        '/\b(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)\(\d+\)/' => '$1',
    ];

    /**
     * @return Attribute The attribute as stored
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws NotFoundException
     * @throws Exception
     */
    public function createAttribute(string $collection, Attribute $attribute): Attribute
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $attribute = self::normalise($attribute);

        $schemaAttributes = $this->adapter->hasFeature(Feature\SchemaAttributes::class)
            ? $this->getSchemaAttributes($definition->getId())
            : [];

        $existsInSchema = false;

        try {
            $this->validateAttribute($definition, $attribute, $schemaAttributes);
        } catch (DuplicateException $error) {
            $existsInSchema = $this->reconcileSchemaOnlyColumn($definition, $attribute, $schemaAttributes, $error);
        }

        $created = false;

        if (! $existsInSchema) {
            try {
                $created = $this->adapter->createAttribute($definition->getId(), $attribute);

                if (! $created) {
                    throw new DatabaseException('Failed to create attribute');
                }
            } catch (MismatchException $error) {
                throw $error;
            } catch (DuplicateException) {
                // The column exists only in the physical schema (the metadata check above passed),
                // so the metadata is written for it.
            }
        }

        $definition->setAttribute(self::COLLECTION_ATTRIBUTES, $attribute->toDocument(), SetType::Append);

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->cleanupAttribute($definition->getId(), $attribute->key),
            shouldRollback: $created,
            operationDescription: "attribute creation '{$attribute->key}'"
        );

        $this->purgeCollectionCaches($definition->getId());

        $this->triggerHooks(
            Event::AttributeCreate,
            $attribute->toDocument()->setAttribute(Document::COLLECTION, $definition->getId()),
        );

        return $attribute;
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<Attribute> The attributes as stored
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws NotFoundException
     * @throws StructureException
     * @throws Exception
     */
    public function createAttributes(string $collection, array $attributes): array
    {
        if ($attributes === []) {
            throw new DatabaseException('No attributes to create');
        }

        $definition = $this->silent(fn () => $this->getCollection($collection));

        $schemaAttributes = $this->adapter->hasFeature(Feature\SchemaAttributes::class)
            ? $this->getSchemaAttributes($definition->getId())
            : [];

        $stored = [];
        $toCreate = [];
        foreach ($attributes as $attribute) {
            if ($attribute->key === '') {
                throw new DatabaseException('Missing attribute key');
            }

            $attribute = self::normalise($attribute);
            $existsInSchema = false;

            try {
                $this->validateAttribute($definition, $attribute, $schemaAttributes);
            } catch (DuplicateException $error) {
                $existsInSchema = $this->reconcileSchemaOnlyColumn($definition, $attribute, $schemaAttributes, $error);
            }

            $stored[] = $attribute;
            if (! $existsInSchema) {
                $toCreate[] = $attribute;
            }
        }

        $created = [];

        if ($toCreate !== []) {
            try {
                if (! $this->adapter->createAttributes($definition->getId(), $toCreate)) {
                    throw new DatabaseException('Failed to create attributes');
                }
                $created = $toCreate;
            } catch (MismatchException $error) {
                throw $error;
            } catch (DuplicateException) {
                // At least one column already exists, so each is created on its own and the
                // duplicates are skipped.
                foreach ($toCreate as $attribute) {
                    try {
                        $this->adapter->createAttribute($definition->getId(), $attribute);
                        $created[] = $attribute;
                    } catch (MismatchException $error) {
                        throw $error;
                    } catch (DuplicateException) {
                        // Already in the schema.
                    }
                }
            }
        }

        foreach ($stored as $attribute) {
            $definition->setAttribute(self::COLLECTION_ATTRIBUTES, $attribute->toDocument(), SetType::Append);
        }

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->cleanupAttributes($definition->getId(), $created),
            shouldRollback: $created !== [],
            operationDescription: 'attributes creation',
            rollbackReturnsErrors: true
        );

        $this->purgeCollectionCaches($definition->getId());

        $documents = \array_map(
            static fn (Attribute $attribute): Document => $attribute->toDocument()
                ->setAttribute(Document::COLLECTION, $definition->getId()),
            $stored,
        );

        foreach ($documents as $document) {
            $this->triggerHooks(Event::AttributeCreate, $document);
        }

        $this->triggerHooks(Event::AttributesCreate, $documents);

        return $stored;
    }

    /**
     * Applies a sparse update: a null field keeps its value and `default: null` clears the default. An explicit
     * `required: true` clears the default; a default on a required attribute is refused. `required: false`
     * relaxes the column's NOT NULL.
     *
     * @return Attribute The attribute as stored
     *
     * @throws DatabaseException
     * @throws DependencyException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     * @throws NotFoundException
     * @throws StructureException
     * @throws Exception
     */
    public function updateAttribute(string $collection, string $key, AttributeUpdate $update): Attribute
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));

        if ($definition->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata attributes');
        }

        $attributes = $definition->attributes();
        $position = self::attributePosition($attributes, $key);

        if ($position === null) {
            throw new NotFoundException('Attribute not found');
        }

        $stored = $attributes[$position];

        if ($stored->type === ColumnType::Relationship) {
            throw new DatabaseException('Cannot update relationship as an attribute');
        }

        if ($update->isEmpty()) {
            return $stored;
        }

        $required = $update->required ?? $stored->required;
        if ($update->required !== true && $required && $update->changesDefault() && $update->default !== null) {
            throw new DatabaseException('Cannot set a default value on a required attribute');
        }

        $updated = $stored->apply($update);
        if ($update->required === true && $updated->default !== null) {
            $updated = $updated->apply(new AttributeUpdate(default: null));
        }

        $newKey = $updated->key;
        $renaming = $newKey !== $key;
        if ($renaming && self::attributePosition($attributes, $newKey) !== null) {
            throw new DuplicateException('Attribute name already used');
        }

        $this->validateAttributeUpdate($updated, $update->size ?? $stored->size ?? 0, $update->array ?? $stored->array);

        $altering = $update->type !== null
            || $update->size !== null
            || $update->signed !== null
            || $update->array !== null
            || $update->key !== null
            || ($updated->isSpatial() && ! $this->adapter->supports(Capability::SpatialIndexNull));

        $originalIndexes = $definition->indexes();
        $attributes = self::replacing($attributes, $key, $updated);
        $indexes = $renaming ? self::renameIndexedAttribute($originalIndexes, $key, $newKey) : $originalIndexes;

        $this->writeAttributes($definition, $attributes);
        if ($renaming) {
            $this->writeIndexes($definition, $indexes);
        }

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            $this->adapter->getAttributeWidth($definition) >= $this->adapter->getDocumentSizeLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot update attribute.');
        }

        if ($updated->isSpatial() && ! $this->adapter->supports(Capability::SpatialIndexNull)) {
            $this->assertSpatialIndexesRequired($attributes, $indexes);
        }

        $updatedInSchema = false;

        if ($altering) {
            if ($renaming) {
                $validator = new IndexDependencyValidator(
                    $indexes,
                    $this->adapter->supports(Capability::CastIndexArray),
                );

                if (! $validator->isValid($updated)) {
                    throw new DependencyException($validator->getDescription());
                }
            }

            if ($this->validation()->get()) {
                $validator = $this->indexValidator($attributes, $originalIndexes);

                foreach ($indexes as $index) {
                    if (! $validator->isValid($index)) {
                        throw new IndexException($validator->getDescription());
                    }
                }
            }

            $updatedInSchema = $this->adapter->updateAttribute($definition->getId(), $key, $updated);

            if (! $updatedInSchema) {
                throw new DatabaseException('Failed to update attribute');
            }
        } elseif ($stored->required && ! $updated->required) {
            // The alter path applies nullability itself. A required-only change relaxes the column on its own,
            // because the column rewrite re-casts datetime columns on Postgres.
            if (! $this->adapter->relaxAttributeRequired($definition->getId(), $key)) {
                throw new DatabaseException('Failed to update attribute');
            }
        }

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->adapter->updateAttribute($definition->getId(), $newKey, $stored),
            shouldRollback: $updatedInSchema,
            operationDescription: "attribute update '{$key}'",
            silentRollback: true
        );

        if ($altering) {
            $this->withRetries(fn () => $this->purgeCachedCollection($definition->getId()));
        }
        $this->withRetries(fn () => $this->purgeCachedDocumentInternal(self::METADATA, $definition->getId()));

        $this->triggerHooks(Event::DocumentPurge, new Document([
            Document::ID => $definition->getId(),
            Document::COLLECTION => self::METADATA,
        ]));

        $this->triggerHooks(
            Event::AttributeUpdate,
            $updated->toDocument()->setAttribute(Document::COLLECTION, $definition->getId()),
        );

        return $updated;
    }

    /**
     * Checks that the attribute can be added to the collection without exceeding its limits.
     *
     * @throws LimitException
     * @throws NotFoundException
     */
    public function checkAttribute(string $collection, Attribute $attribute): bool
    {
        $definition = clone ($this->silent(fn () => $this->getCollection($collection)));

        $definition->setAttribute(self::COLLECTION_ATTRIBUTES, $attribute->toDocument(), SetType::Append);

        if (
            $this->adapter->getLimitForAttributes() > 0 &&
            $this->adapter->getCountOfAttributes($definition) > $this->adapter->getLimitForAttributes()
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute. Current attribute count is '.$this->adapter->getCountOfAttributes($definition).' but the maximum is '.$this->adapter->getLimitForAttributes().'. Remove some attributes to free up space.');
        }

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            $this->adapter->getAttributeWidth($definition) >= $this->adapter->getDocumentSizeLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute. Current row width is '.$this->adapter->getAttributeWidth($definition).' bytes but the maximum is '.$this->adapter->getDocumentSizeLimit().' bytes. Reduce the size of existing attributes or remove some attributes to free up space.');
        }

        return true;
    }

    /**
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function deleteAttribute(string $collection, string $key): void
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $definition->attributes();
        $position = self::attributePosition($attributes, $key);

        if ($position === null) {
            throw new NotFoundException('Attribute not found');
        }

        $attribute = $attributes[$position];

        if ($attribute->type === ColumnType::Relationship) {
            throw new DatabaseException('Cannot delete relationship as an attribute');
        }

        $indexes = $definition->indexes();

        if ($this->validation()->get()) {
            $validator = new IndexDependencyValidator(
                $indexes,
                $this->adapter->supports(Capability::CastIndexArray),
            );

            if (! $validator->isValid($attribute)) {
                throw new DependencyException($validator->getDescription());
            }
        }

        unset($attributes[$position]);
        $this->writeAttributes($definition, \array_values($attributes));
        $this->writeIndexes($definition, self::withoutIndexedAttribute($indexes, $key));

        $deletedInSchema = false;
        try {
            if (! $this->adapter->deleteAttribute($definition->getId(), $key)) {
                throw new DatabaseException('Failed to delete attribute');
            }
            $deletedInSchema = true;
        } catch (NotFoundException) {
            // Already absent from the schema; the metadata is still removed below.
        }

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->adapter->createAttribute($definition->getId(), $attribute),
            shouldRollback: $deletedInSchema,
            operationDescription: "attribute deletion '{$key}'",
            silentRollback: true
        );

        $this->purgeCollectionCaches($definition->getId());

        $this->triggerHooks(
            Event::AttributeDelete,
            $attribute->toDocument()->setAttribute(Document::COLLECTION, $definition->getId()),
        );
    }

    /**
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DependencyException
     * @throws DuplicateException
     * @throws NotFoundException
     * @throws StructureException
     */
    public function renameAttribute(string $collection, string $old, string $new): void
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $definition->attributes();
        $position = self::attributePosition($attributes, $old);

        if (self::attributePosition($attributes, $new) !== null) {
            throw new DuplicateException('Attribute name already used');
        }

        if ($position === null) {
            throw new NotFoundException('Attribute not found');
        }

        $indexes = $definition->indexes();

        if ($this->validation()->get()) {
            $validator = new IndexDependencyValidator(
                $indexes,
                $this->adapter->supports(Capability::CastIndexArray),
            );

            if (! $validator->isValid($attributes[$position])) {
                throw new DependencyException($validator->getDescription());
            }
        }

        $renamed = $attributes[$position]->apply(new AttributeUpdate(key: $new));
        $attributes = self::replacing($attributes, $old, $renamed);

        try {
            if (! $this->adapter->renameAttribute($definition->getId(), $old, $new)) {
                throw new DatabaseException('Failed to rename attribute');
            }
        } catch (DuplicateException $error) {
            throw $error;
        } catch (Throwable $error) {
            throw new DatabaseException("Failed to rename attribute '{$old}' to '{$new}': ".$error->getMessage(), previous: $error);
        }

        $this->writeAttributes($definition, $attributes);
        $this->writeIndexes($definition, self::renameIndexedAttribute($indexes, $old, $new));

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->adapter->renameAttribute($definition->getId(), $new, $old),
            shouldRollback: true,
            operationDescription: "attribute rename '{$old}' to '{$new}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($definition->getId()));

        $this->triggerHooks(
            Event::AttributeUpdate,
            $renamed->toDocument()->setAttribute(Document::COLLECTION, $definition->getId()),
        );
    }

    /**
     * Rewrites one attribute's metadata without touching the schema; relationships keep their stored
     * definitions in step through it.
     *
     * @param  Closure(Attribute): Attribute  $update
     *
     * @throws ConflictException
     * @throws DatabaseException
     * @throws NotFoundException
     */
    private function updateAttributeMeta(string $collection, string $key, Closure $update, bool $triggerEvent = true): Attribute
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));

        if ($definition->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata attributes');
        }

        $attributes = $definition->attributes();
        $position = self::attributePosition($attributes, $key);

        if ($position === null) {
            throw new NotFoundException('Attribute not found');
        }

        $attribute = $update($attributes[$position]);

        $this->writeAttributes($definition, self::replacing($attributes, $key, $attribute));

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: null,
            shouldRollback: false,
            operationDescription: "attribute metadata update '{$key}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($definition->getId()));

        if ($triggerEvent) {
            $this->triggerHooks(
                Event::AttributeUpdate,
                $attribute->toDocument()->setAttribute(Document::COLLECTION, $definition->getId()),
            );
        }

        return $attribute;
    }

    /**
     * A column in the schema but not in this collection's metadata is reused when its type
     * matches the request, and dropped to be recreated otherwise. Under shared tables it
     * belongs to another tenant's collection, so a mismatch is refused instead.
     *
     * @param  array<Document>  $schemaAttributes
     * @return bool True when the existing column is reused
     *
     * @throws DuplicateException
     */
    private function reconcileSchemaOnlyColumn(
        Collection $definition,
        Attribute $attribute,
        array $schemaAttributes,
        DuplicateException $duplicate,
    ): bool {
        $key = \strtolower($attribute->key);
        foreach ($definition->attributes() as $existing) {
            if (\strtolower($existing->key) === $key) {
                throw $duplicate;
            }
        }

        if (! $this->adapterHasFeature(Feature\ColumnTypes::class)) {
            return true;
        }

        $expected = $this->adapter->getColumnType(
            $attribute->type->value,
            $attribute->size ?? 0,
            $attribute->signed,
            $attribute->array,
            $attribute->required,
        );
        if ($expected === '') {
            return true;
        }

        $filteredId = \strtolower($this->adapter->filter($attribute->key));
        foreach ($schemaAttributes as $column) {
            if (\strtolower($column->getId()) !== $filteredId) {
                continue;
            }

            $columnType = $column->getAttribute('columnType', '');
            if (self::canonicalColumnType(\is_string($columnType) ? $columnType : '') === self::canonicalColumnType($expected)) {
                return true;
            }

            if ($this->getSharedTables()) {
                throw new DuplicateException('Attribute exists in the shared table with another type', previous: $duplicate);
            }

            $this->adapter->deleteAttribute($definition->getId(), $attribute->key);

            return false;
        }

        return true;
    }

    /**
     * Engines report integer display widths (int(11)), spatial types without their SRID or
     * nullability, and MariaDB's JSON as LONGTEXT.
     */
    private static function canonicalColumnType(string $columnType): string
    {
        $canonical = \preg_replace(
            \array_keys(self::COLUMN_TYPE_SPELLINGS),
            \array_values(self::COLUMN_TYPE_SPELLINGS),
            \strtoupper(\trim($columnType)),
        ) ?? $columnType;

        return $canonical === 'JSON' ? 'LONGTEXT' : $canonical;
    }

    /**
     * @param  array<Document>  $schemaAttributes
     *
     * @throws DuplicateException
     * @throws LimitException
     * @throws Exception
     */
    private function validateAttribute(Collection $definition, Attribute $attribute, array $schemaAttributes): void
    {
        $withAttribute = clone $definition;
        $withAttribute->setAttribute(self::COLLECTION_ATTRIBUTES, $attribute->toDocument(), SetType::Append);

        $validator = new AttributeValidator(
            attributes: $definition->attributes(),
            schemaAttributes: $schemaAttributes,
            maxAttributes: $this->adapter->getLimitForAttributes(),
            maxWidth: $this->adapter->getDocumentSizeLimit(),
            maxStringLength: $this->adapter->getLimitForString(),
            maxVarcharLength: $this->adapter->getMaxVarcharLength(),
            maxIntLength: $this->adapter->getLimitForInt(),
            maxBigIntLength: $this->adapter->getLimitForBigInt(),
            supportForSchemaAttributes: $this->adapter->hasFeature(Feature\SchemaAttributes::class),
            supportForVectors: $this->adapter->supports(Capability::Vectors),
            supportForSpatialAttributes: $this->adapter->hasFeature(Feature\Spatial::class),
            supportForObject: $this->adapter->supports(Capability::Objects),
            supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt),
            attributeCountCallback: fn (): int => $this->adapter->getCountOfAttributes($withAttribute),
            attributeWidthCallback: fn (): int => $this->adapter->getAttributeWidth($withAttribute),
            filterCallback: fn (string $key): string => $this->adapter->filter($key),
            isMigrating: $this->isMigrating(),
            sharedTables: $this->getSharedTables(),
        );

        $validator->isValid($attribute);
    }

    /**
     * Checks the updated attribute against the adapter. Size and array are the requested values, since
     * the model normalises them away for types that take neither.
     *
     * @throws DatabaseException
     */
    private function validateAttributeUpdate(Attribute $attribute, int $size, bool $array): void
    {
        switch ($attribute->type) {
            case ColumnType::String:
                if ($size === 0) {
                    throw new DatabaseException('Size length is required');
                }
                if ($size > $this->adapter->getLimitForString()) {
                    throw new DatabaseException('Max size allowed for string is: '.\number_format($this->adapter->getLimitForString()));
                }
                break;

            case ColumnType::Varchar:
                if ($size === 0) {
                    throw new DatabaseException('Size length is required');
                }
                if ($size > $this->adapter->getMaxVarcharLength()) {
                    throw new DatabaseException('Max size allowed for varchar is: '.\number_format($this->adapter->getMaxVarcharLength()));
                }
                break;

            case ColumnType::Integer:
                $limit = $attribute->signed ? $this->adapter->getLimitForInt() / 2 : $this->adapter->getLimitForInt();
                if ($size > $limit) {
                    throw new DatabaseException('Max size allowed for int is: '.\number_format($limit));
                }
                break;

            case ColumnType::Float:
            case ColumnType::Double:
            case ColumnType::Boolean:
            case ColumnType::Datetime:
                if ($size !== 0) {
                    throw new DatabaseException('Size must be empty');
                }
                break;

            case ColumnType::Object:
                if (! $this->adapter->supports(Capability::Objects)) {
                    throw new DatabaseException('Object attributes are not supported');
                }
                if ($size !== 0) {
                    throw new DatabaseException('Size must be empty for object attributes');
                }
                if ($array) {
                    throw new DatabaseException('Object attributes cannot be arrays');
                }
                break;

            case ColumnType::Point:
            case ColumnType::Linestring:
            case ColumnType::Polygon:
                if (! $this->adapter->hasFeature(Feature\Spatial::class)) {
                    throw new DatabaseException('Spatial attributes are not supported');
                }
                if ($size !== 0) {
                    throw new DatabaseException('Size must be empty for spatial attributes');
                }
                if ($array) {
                    throw new DatabaseException('Spatial attributes cannot be arrays');
                }
                break;

            case ColumnType::Vector:
                if (! $this->adapter->supports(Capability::Vectors)) {
                    throw new DatabaseException('Vector types are not supported by the current database');
                }
                if ($array) {
                    throw new DatabaseException('Vector type cannot be an array');
                }
                if ($size <= 0) {
                    throw new DatabaseException('Vector dimensions must be a positive integer');
                }
                if ($size > self::MAX_VECTOR_DIMENSIONS) {
                    throw new DatabaseException('Vector dimensions cannot exceed '.self::MAX_VECTOR_DIMENSIONS);
                }
                if ($attribute->default !== null) {
                    if (! \is_array($attribute->default)) {
                        throw new DatabaseException('Vector default value must be an array');
                    }
                    if (\count($attribute->default) !== $size) {
                        throw new DatabaseException('Vector default value must have exactly '.$size.' elements');
                    }
                    foreach ($attribute->default as $component) {
                        if (! \is_int($component) && ! \is_float($component)) {
                            throw new DatabaseException('Vector default value must contain only numeric elements');
                        }
                    }
                }
                break;

            default:
                break;
        }

        if ($attribute->format !== null && ! Structure::hasFormat($attribute->format->name, $attribute->type)) {
            throw new DatabaseException('Format ("'.$attribute->format->name.'") not available for this attribute type ("'.$attribute->type->value.'")');
        }

        if ($attribute->default !== null) {
            $this->validateDefaultTypes($attribute->type, $attribute->default, $attribute->signed);
        }
    }

    /**
     * Function to validate if the default value of an attribute matches its attribute type
     *
     * @throws DatabaseException
     */
    protected function validateDefaultTypes(ColumnType $type, mixed $default, bool $signed = true): void
    {
        if ($default === null) {
            return;
        }

        if (\is_array($default)) {
            if ($type === ColumnType::Point || $type === ColumnType::Linestring || $type === ColumnType::Polygon) {
                $spatial = new SpatialValidator($type->value);
                if (! $spatial->isValid($default)) {
                    throw new DatabaseException('Invalid default value: '.$spatial->getDescription());
                }

                return;
            }

            if ($type !== ColumnType::Object) {
                foreach ($default as $value) {
                    $this->validateDefaultTypes($type, $value, $signed);
                }
            }

            return;
        }

        $matches = match ($type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Text, ColumnType::MediumText, ColumnType::LongText, ColumnType::Datetime => \is_string($default),
            ColumnType::Integer => \is_int($default),
            ColumnType::Boolean => \is_bool($default),
            ColumnType::BigInteger => (new BigInt($signed, $this->adapter->supports(Capability::UnsignedBigInt)))->isValid($default),
            ColumnType::Float, ColumnType::Double => \is_float($default),
            ColumnType::Vector => \is_int($default) || \is_float($default),
            default => false,
        };

        if ($matches) {
            return;
        }

        if ($type === ColumnType::Vector) {
            throw new DatabaseException('Vector components must be numeric values (float or integer)');
        }

        $value = \is_scalar($default) ? (string) $default : '[non-scalar]';

        throw new DatabaseException('Default value '.$value.' does not match given type '.Attribute::storedType($type));
    }

    private function typeValidator(): AttributeValidator
    {
        return new AttributeValidator(
            attributes: [],
            maxStringLength: $this->adapter->getLimitForString(),
            maxVarcharLength: $this->adapter->getMaxVarcharLength(),
            maxIntLength: $this->adapter->getLimitForInt(),
            maxBigIntLength: $this->adapter->getLimitForBigInt(),
            supportForVectors: $this->adapter->supports(Capability::Vectors),
            supportForSpatialAttributes: $this->adapter->hasFeature(Feature\Spatial::class),
            supportForObject: $this->adapter->supports(Capability::Objects),
            supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt),
        );
    }

    /**
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     */
    private function indexValidator(array $attributes, array $indexes): IndexValidator
    {
        return new IndexValidator(
            $attributes,
            $indexes,
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
            $this->adapter->hasFeature(Feature\Spatial::class),
            $this->adapter->supports(Capability::Index),
            $this->adapter->supports(Capability::UniqueIndex),
            $this->adapter->supports(Capability::Fulltext),
            $this->adapter->supports(Capability::TTLIndexes),
            $this->adapter->supports(Capability::Objects)
        );
    }

    /**
     * Engines without nullable spatial indexes need every attribute a spatial index covers to be required.
     *
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     *
     * @throws IndexException
     */
    private function assertSpatialIndexesRequired(array $attributes, array $indexes): void
    {
        $byKey = [];
        foreach ($attributes as $attribute) {
            $byKey[\strtolower($attribute->key)] = $attribute;
        }

        foreach ($indexes as $index) {
            if ($index->type !== IndexType::Spatial) {
                continue;
            }

            foreach ($index->attributes as $key) {
                $attribute = $byKey[\strtolower($key)] ?? null;
                if ($attribute !== null && $attribute->isSpatial() && ! $attribute->required) {
                    throw new IndexException('Spatial indexes do not allow null values. Mark the attribute "'.$key.'" as required or create the index on a column with no null values.');
                }
            }
        }
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private static function attributePosition(array $attributes, string $key): ?int
    {
        foreach ($attributes as $position => $attribute) {
            if ($attribute->key === $key) {
                return $position;
            }
        }

        return null;
    }

    /**
     * The attribute with every field normalised for its type, as it is stored.
     *
     * @throws StructureException
     */
    private static function normalise(Attribute $attribute): Attribute
    {
        return $attribute->apply(new AttributeUpdate());
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<Attribute>
     */
    private static function replacing(array $attributes, string $key, Attribute $replacement): array
    {
        return \array_map(
            static fn (Attribute $attribute): Attribute => $attribute->key === $key ? $replacement : $attribute,
            $attributes,
        );
    }

    /**
     * @param  list<Index>  $indexes
     * @return list<Index>
     *
     * @throws IndexException
     */
    private static function renameIndexedAttribute(array $indexes, string $old, string $new): array
    {
        $renamed = [];
        foreach ($indexes as $index) {
            $renamed[] = \in_array($old, $index->attributes, true)
                ? self::withIndexedAttributes($index, \array_map(
                    static fn (string $attribute): string => $attribute === $old ? $new : $attribute,
                    $index->attributes,
                ))
                : $index;
        }

        return $renamed;
    }

    /**
     * Drops the attribute from every index on it, and every index left on no attribute.
     *
     * @param  list<Index>  $indexes
     * @return list<Index>
     *
     * @throws IndexException
     */
    private static function withoutIndexedAttribute(array $indexes, string $key): array
    {
        $remaining = [];
        foreach ($indexes as $index) {
            if (! \in_array($key, $index->attributes, true)) {
                $remaining[] = $index;
                continue;
            }

            $attributes = \array_values(\array_filter(
                $index->attributes,
                static fn (string $attribute): bool => $attribute !== $key,
            ));

            if ($attributes !== []) {
                $remaining[] = self::withIndexedAttributes($index, $attributes);
            }
        }

        return $remaining;
    }

    /**
     * @param  list<string>  $attributes
     *
     * @throws IndexException
     */
    private static function withIndexedAttributes(Index $index, array $attributes): Index
    {
        return Index::fromDocument($index->toDocument()->setAttribute(self::INDEX_ATTRIBUTES, $attributes));
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function writeAttributes(Collection $definition, array $attributes): void
    {
        $definition->setAttribute(
            self::COLLECTION_ATTRIBUTES,
            \array_map(static fn (Attribute $attribute): Document => $attribute->toDocument(), $attributes),
        );
    }

    /**
     * @param  list<Index>  $indexes
     */
    private function writeIndexes(Collection $definition, array $indexes): void
    {
        $definition->setAttribute(
            self::COLLECTION_INDEXES,
            \array_map(static fn (Index $index): Document => $index->toDocument(), $indexes),
        );
    }

    private function purgeCollectionCaches(string $collection): void
    {
        $this->withRetries(fn () => $this->purgeCachedCollection($collection));
        $this->withRetries(fn () => $this->purgeCachedDocumentInternal(self::METADATA, $collection));

        $this->triggerHooks(Event::DocumentPurge, new Document([
            Document::ID => $collection,
            Document::COLLECTION => self::METADATA,
        ]));
    }

    /**
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanupAttribute(string $collection, string $key, int $maxAttempts = 3): void
    {
        $this->cleanup(
            fn () => $this->adapter->deleteAttribute($collection, $key),
            'attribute',
            $key,
            $maxAttempts
        );
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<string> The errors of the cleanups that failed
     */
    private function cleanupAttributes(string $collection, array $attributes, int $maxAttempts = 3): array
    {
        $errors = [];

        foreach ($attributes as $attribute) {
            try {
                $this->cleanupAttribute($collection, $attribute->key, $maxAttempts);
            } catch (Exception $error) {
                $errors[] = $error->getMessage();
            }
        }

        return $errors;
    }

    /**
     * @param  list<string>  $keys
     */
    private function rollbackAttributeMetadata(Collection $definition, array $keys): void
    {
        $this->writeAttributes($definition, \array_values(\array_filter(
            $definition->attributes(),
            static fn (Attribute $attribute): bool => ! \in_array($attribute->key, $keys, true),
        )));
    }
}
