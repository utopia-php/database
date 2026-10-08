<?php

namespace Utopia\Database\Trait;

use Exception;
use Throwable;
use Utopia\Console;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Index;
use Utopia\Database\Schema;
use Utopia\Database\Storage;

trait Indexes
{
    /**
     * @return Index The index as stored
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     * @throws NotFoundException
     * @throws StructureException
     * @throws Exception
     */
    public function createIndex(string $collection, Index $index): Index
    {
        return $this->storeIndexes($collection, [$index])[0];
    }

    /**
     * Every index is validated before any is created, and the metadata is written once. When the engine fails
     * part way, the indexes this call already created are dropped again.
     *
     * @param  list<Index>  $indexes
     * @return list<Index> The indexes as stored
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     * @throws NotFoundException
     * @throws StructureException
     * @throws Exception
     */
    public function createIndexes(string $collection, array $indexes): array
    {
        if ($indexes === []) {
            return [];
        }

        $stored = $this->storeIndexes($collection, $indexes);

        $listeners = $this->listens(Event::IndexesCreate);
        if ($listeners !== []) {
            $this->dispatch(new Event\Index\BatchCreated($collection, $stored), $listeners);
        }

        return $stored;
    }

    /**
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws NotFoundException
     * @throws StructureException
     */
    public function renameIndex(string $collection, string $old, string $new): void
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $indexes = $definition->indexes();

        $position = self::indexPosition($indexes, $old);
        if ($position === null) {
            throw new NotFoundException('Index not found');
        }

        if (self::indexPosition($indexes, $new) !== null) {
            throw new DuplicateException('Index name already used');
        }

        $renamed = $indexes[$position]->withKey($new);
        \array_splice($indexes, $position, 1, [$renamed]);
        $this->writeIndexList($definition, $indexes);

        $renamedInSchema = false;
        try {
            $renamedInSchema = $this->adapter->renameIndex($definition->getId(), $old, $new);
            if (! $renamedInSchema) {
                throw new DatabaseException('Failed to rename index');
            }
        } catch (Throwable $error) {
            $renamedInSchema = $this->completePriorIndexRename($definition->getId(), $old, $new, $error);
        }

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->adapter->renameIndex($definition->getId(), $new, $old),
            shouldRollback: $renamedInSchema,
            operationDescription: "index rename '{$old}' to '{$new}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($definition->getId()));

        $listeners = $this->listens(Event::IndexRename);
        if ($listeners !== []) {
            $this->dispatch(new Event\Index\Renamed($definition->getId(), $old, $renamed), $listeners);
        }
    }

    /**
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws NotFoundException
     * @throws StructureException
     */
    public function deleteIndex(string $collection, string $key): void
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $indexes = $definition->indexes();

        $position = self::indexPosition($indexes, $key);
        if ($position === null) {
            throw new NotFoundException('Index not found');
        }

        $deleted = $indexes[$position];

        $deletedInSchema = false;
        try {
            if (! $this->adapter->deleteIndex($definition->getId(), $key)) {
                throw new DatabaseException('Failed to delete index');
            }
            $deletedInSchema = true;
        } catch (NotFoundException) {
            // Already absent from the schema; the metadata is still removed below.
        }

        unset($indexes[$position]);
        $this->writeIndexList($definition, \array_values($indexes));

        $attributeTypes = self::indexAttributeTypes($deleted, $definition->attributes());

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->adapter->createIndex($definition->getId(), $deleted, $attributeTypes),
            shouldRollback: $deletedInSchema,
            operationDescription: "index deletion '{$key}'",
            silentRollback: true
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($definition->getId()));

        $listeners = $this->listens(Event::IndexDelete);
        if ($listeners !== []) {
            $this->dispatch(new Event\Index\Deleted($definition->getId(), $deleted), $listeners);
        }
    }

    /**
     * @param  non-empty-list<Index>  $indexes
     * @return non-empty-list<Index>
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     * @throws Exception
     */
    private function storeIndexes(string $collection, array $indexes): array
    {
        $definition = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $definition->attributes();
        $existing = $definition->indexes();

        $prepared = $this->prepareIndexes($definition, $attributes, $existing, $indexes);

        $created = [];
        try {
            foreach ($prepared as $index) {
                if ($this->createIndexInSchema($definition->getId(), $index, $attributes)) {
                    $created[] = $index->key;
                }
            }
        } catch (Throwable $error) {
            try {
                $this->cleanupIndexes($definition->getId(), $created);
            } catch (Throwable $cleanupError) {
                Console::error('Failed to roll back indexes created before the failure: '.$cleanupError->getMessage());
            }

            throw $error;
        }

        $this->writeIndexList($definition, [...$existing, ...$prepared]);

        $keys = \implode("', '", \array_map(static fn (Index $index): string => $index->key, $prepared));

        $this->updateMetadata(
            collection: $definition,
            rollbackOperation: fn () => $this->cleanupIndexes($definition->getId(), $created),
            shouldRollback: $created !== [],
            operationDescription: "index creation '{$keys}'"
        );

        $this->withRetries(fn () => $this->purgeCachedCollection($definition->getId()));

        $listeners = $this->listens(Event::IndexCreate);
        if ($listeners !== []) {
            foreach ($prepared as $index) {
                $this->dispatch(new Event\Index\Created($definition->getId(), $index), $listeners);
            }
        }

        return $prepared;
    }

    /**
     * Fits each index to its attributes and validates it against the stored indexes and the ones before it.
     *
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $existing
     * @param  non-empty-list<Index>  $indexes
     * @return non-empty-list<Index>
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws IndexException
     * @throws LimitException
     */
    private function prepareIndexes(Collection $definition, array $attributes, array $existing, array $indexes): array
    {
        $keys = [];
        foreach ($existing as $index) {
            $keys[\strtolower($index->key)] = true;
        }

        foreach ($indexes as $index) {
            if ($index->attributes === []) {
                throw new DatabaseException('Missing attributes');
            }

            $key = \strtolower($index->key);
            if (isset($keys[$key])) {
                throw new DuplicateException('Index already exists');
            }
            $keys[$key] = true;
        }

        if ($this->adapter->getCountOfIndexes($definition) + \count($indexes) > $this->adapter->limits()->indexes) {
            throw new LimitException('Index limit reached. Cannot create new index.');
        }

        $prepared = [];
        foreach ($indexes as $index) {
            $index = $this->fitIndexToAttributes($index, $attributes);

            if ($this->validation()->get()) {
                $validator = $this->indexValidator($attributes, [...$existing, ...$prepared]);
                if (! $validator->isValid($index)) {
                    throw new IndexException($validator->getDescription());
                }
            }

            $prepared[] = $index;
        }

        return $prepared;
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return bool True when this call created the index, false when the schema already held it
     *
     * @throws DatabaseException
     * @throws DuplicateException
     */
    private function createIndexInSchema(string $collection, Index $index, array $attributes): bool
    {
        if ($this->reconcileSchemaOnlyIndex($collection, $index)) {
            return false;
        }

        try {
            if (! $this->adapter->createIndex($collection, $index, self::indexAttributeTypes($index, $attributes))) {
                throw new DatabaseException('Failed to create index');
            }
        } catch (DuplicateException) {
            // The metadata holds no index under this key, so the schema's copy is an orphan of a
            // partial failure: it is kept and the metadata written for it.
            return false;
        }

        return true;
    }

    /**
     * Index attributes by the type of the attribute they index; a dotted path into an object attribute
     * takes the object attribute's type.
     *
     * @param  list<Attribute>  $attributes
     * @return array<string, string>
     */
    private static function indexAttributeTypes(Index $index, array $attributes): array
    {
        $types = [];
        foreach ($index->attributes as $indexed) {
            $base = \explode('.', $indexed, 2)[0];
            foreach ($attributes as $attribute) {
                if ($attribute->key === $base) {
                    $types[$indexed] = $attribute->type->value;
                    break;
                }
            }
        }

        return $types;
    }

    /**
     * @param  list<Index>  $indexes
     */
    private static function indexPosition(array $indexes, string $key): ?int
    {
        foreach ($indexes as $position => $index) {
            if ($index->key === $key) {
                return $position;
            }
        }

        return null;
    }

    /**
     * @param  list<Index>  $indexes
     */
    private function writeIndexList(Collection $definition, array $indexes): void
    {
        $definition->setAttribute(
            self::COLLECTION_INDEXES,
            \array_map(static fn (Index $index): Document => $index->toDocument(), $indexes),
        );
    }

    /**
     * A failed rename may follow a prior partial failure whose rename reached the schema while its metadata
     * update and rollback failed. Renaming back and forth again proves the schema holds the index under the
     * new name and completes the rename.
     *
     * @throws DatabaseException
     */
    private function completePriorIndexRename(string $collection, string $old, string $new, Throwable $error): bool
    {
        try {
            if (! $this->adapter->renameIndex($collection, $new, $old)) {
                throw new DatabaseException('Failed to rename index');
            }
            if (! $this->adapter->renameIndex($collection, $old, $new)) {
                throw new DatabaseException('Failed to rename index');
            }
        } catch (Throwable) {
            throw new DatabaseException("Failed to rename index '{$old}' to '{$new}': ".$error->getMessage(), previous: $error);
        }

        return true;
    }

    /**
     * An index in the schema but not in this collection's metadata is reused when its
     * definition matches the request, and dropped to be recreated otherwise. Under shared
     * tables it serves another tenant's collection, so a mismatch is refused instead.
     *
     * @return bool True when the existing index is reused
     *
     * @throws DuplicateException
     */
    private function reconcileSchemaOnlyIndex(string $collection, Index $index): bool
    {
        if (! $this->adapter->supports(Capability::SchemaIntrospection)
            || ($this->hasSharedTables() && $this->isMigrating())) {
            return false;
        }

        $id = \strtolower($this->adapter->filter($index->key));
        foreach ($this->adapter->limits()->internalIndexKeys as $internal) {
            if (\strtolower($this->adapter->filter($internal)) === $id) {
                return false;
            }
        }

        foreach ($this->getSchemaIndexes($collection) as $schemaIndex) {
            if (\strtolower($schemaIndex->name) !== $id) {
                continue;
            }

            if ($this->schemaIndexMatches($schemaIndex, $index)) {
                return true;
            }

            if ($this->hasSharedTables()) {
                throw new DuplicateException('Index exists in the shared table with another definition');
            }

            try {
                $this->adapter->deleteIndex($collection, $index->key);
            } catch (NotFoundException) {
                // Already absent from the schema
            }

            return false;
        }

        return false;
    }

    private function schemaIndexMatches(Schema\Index $schemaIndex, Index $index): bool
    {
        $columns = \array_map(\strtolower(...), $schemaIndex->columns);
        $lengths = $schemaIndex->lengths;

        if ($this->hasSharedTables() && ($columns[0] ?? '') === Storage::TENANT) {
            \array_shift($columns);
            \array_shift($lengths);
        }

        if (\count($columns) !== \count($index->attributes)) {
            return false;
        }

        foreach ($index->attributes as $position => $attribute) {
            if ($columns[$position] === '') {
                continue;
            }
            if ($columns[$position] !== \strtolower($this->adapter->filter(Storage::column($attribute)))) {
                return false;
            }
            $length = $lengths[$position] ?? null;
            if ($length !== null && $length !== ($index->lengths[$position] ?? 0)) {
                return false;
            }
        }

        return $schemaIndex->type === $this->adapter->getSchemaIndexType($index->type);
    }

    /**
     * Drops indexes created in the adapter whose metadata could not be stored.
     *
     * @param  list<string>  $keys
     *
     * @throws DatabaseException If a cleanup fails after all retries
     */
    private function cleanupIndexes(string $collection, array $keys, int $maxAttempts = 3): void
    {
        $failure = null;
        foreach ($keys as $key) {
            try {
                $this->cleanup(
                    fn () => $this->adapter->deleteIndex($collection, $key),
                    'index',
                    $key,
                    $maxAttempts
                );
            } catch (Throwable $error) {
                $failure ??= $error;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }
}
