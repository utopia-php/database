<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;
use Utopia\Database\Query;

/**
 * A simple adapter that keeps all data in-process in PHP arrays.
 *
 * Intended for ephemeral use cases like tests, fixtures, and development.
 * Only basic operations are implemented - relationships, spatial, vectors,
 * operators, fulltext search and regex throw a DatabaseException.
 */
class Memory extends Adapter
{
    /**
     * Map of database name to the set of collection storage keys it owns.
     *
     * @var array<string, array<string, true>>
     */
    protected array $databases = [];

    /**
     * @var array<string, array{attributes: array<string, array<string, mixed>>, indexes: array<string, array<string, mixed>>, documents: array<string, array<string, mixed>>, sequence: int}>
     */
    protected array $data = [];

    /**
     * @var array<string, array<int, array{document: string, type: string, permission: string, tenant: int|string|null}>>
     */
    protected array $permissions = [];

    /**
     * Transaction savepoint stack. Each entry is a [data, permissions] tuple.
     *
     * @var array<int, array{data: array<string, mixed>, permissions: array<string, mixed>}>
     */
    protected array $snapshots = [];

    protected bool $supportForAttributes = true;

    public function __construct()
    {
        // No external resources to initialise
    }

    public function getDriver(): mixed
    {
        return 'memory';
    }

    protected function key(string $collection): string
    {
        // Schema scoping: prefix the storage key with the current database
        // so two databases can hold collections under the same namespace
        // without colliding (mirrors MariaDB's separate `CREATE DATABASE`).
        return $this->getDatabase().'.'.$this->getNamespace().'_'.$this->filter($collection);
    }

    protected function documentKey(string $id, int|string|null $tenant = null): string
    {
        // Mirror MariaDB/Postgres default collation — document ids collide
        // case-insensitively. Lower-casing here keeps collisions consistent
        // across read/write paths.
        $id = \strtolower($id);
        if (! $this->sharedTables) {
            return $id;
        }

        return ($tenant ?? $this->getTenant()).'|'.$id;
    }

    /**
     * Locate a stored row in $key by uid honouring the metadata-collection
     * fallback to a NULL-tenant copy under shared tables.
     *
     * @return array{0: string, 1: array<string, mixed>}|null [storageKey, row] or null if not found
     */
    protected function locateDocument(string $key, string $collectionId, string $id): ?array
    {
        $primary = $this->documentKey($id);
        if (isset($this->data[$key]['documents'][$primary])) {
            return [$primary, $this->data[$key]['documents'][$primary]];
        }
        if ($this->sharedTables && $collectionId === Database::METADATA) {
            $lower = \strtolower($id);
            foreach ($this->data[$key]['documents'] as $storageKey => $candidate) {
                if (
                    \strtolower((string) ($candidate['_uid'] ?? '')) === $lower
                    && ($candidate['_tenant'] ?? null) === null
                ) {
                    return [$storageKey, $candidate];
                }
            }
        }

        return null;
    }

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        // No-op: nothing to time out in-memory
    }

    public function ping(): bool
    {
        return true;
    }

    public function reconnect(): void
    {
        // No-op
    }

    public function startTransaction(): bool
    {
        $this->snapshots[] = [
            'data' => $this->deepCopy($this->data),
            'permissions' => $this->deepCopy($this->permissions),
        ];
        $this->inTransaction++;

        return true;
    }

    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        \array_pop($this->snapshots);
        $this->inTransaction--;

        return true;
    }

    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        $snapshot = \array_pop($this->snapshots);
        if ($snapshot !== null) {
            $this->data = $snapshot['data'];
            $this->permissions = $snapshot['permissions'];
        }
        $this->inTransaction--;

        return true;
    }

    public function create(string $name): bool
    {
        if (! isset($this->databases[$name])) {
            $this->databases[$name] = [];
        }

        return true;
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        if ($collection === null) {
            return isset($this->databases[$database]);
        }

        if (! isset($this->databases[$database])) {
            return false;
        }

        return isset($this->databases[$database][$this->key($collection)]);
    }

    public function list(): array
    {
        $databases = [];
        foreach (\array_keys($this->databases) as $name) {
            $databases[] = new Document(['name' => $name]);
        }

        return $databases;
    }

    public function delete(string $name): bool
    {
        if (! isset($this->databases[$name])) {
            return true;
        }

        foreach (\array_keys($this->databases[$name]) as $collectionKey) {
            unset($this->data[$collectionKey]);
            unset($this->permissions[$collectionKey]);
        }
        unset($this->databases[$name]);

        return true;
    }

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $key = $this->key($name);
        if (isset($this->data[$key])) {
            throw new DuplicateException('Collection already exists');
        }

        $this->data[$key] = [
            'attributes' => [],
            'indexes' => [],
            'documents' => [],
            'sequence' => 0,
        ];
        $this->permissions[$key] = [];

        $database = $this->getDatabase();
        if ($database !== '') {
            if (! isset($this->databases[$database])) {
                $this->databases[$database] = [];
            }
            $this->databases[$database][$key] = true;
        }

        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->getId());
            $this->data[$key]['attributes'][$attrId] = [
                'type' => $attribute->getAttribute('type'),
                'size' => $attribute->getAttribute('size', 0),
                'signed' => $attribute->getAttribute('signed', true),
                'array' => $attribute->getAttribute('array', false),
                'required' => $attribute->getAttribute('required', false),
            ];
        }

        foreach ($indexes as $index) {
            $indexId = $this->filter($index->getId());
            $this->data[$key]['indexes'][$indexId] = [
                'type' => $index->getAttribute('type'),
                'attributes' => $index->getAttribute('attributes', []),
                'lengths' => $index->getAttribute('lengths', []),
                'orders' => $index->getAttribute('orders', []),
            ];
        }

        return true;
    }

    public function deleteCollection(string $id): bool
    {
        $key = $this->key($id);
        unset($this->data[$key]);
        unset($this->permissions[$key]);
        foreach ($this->databases as $name => $collections) {
            if (isset($collections[$key])) {
                unset($this->databases[$name][$key]);
            }
        }

        return true;
    }

    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $id = $this->filter($id);
        $this->data[$key]['attributes'][$id] = [
            'type' => $type,
            'size' => $size,
            'signed' => $signed,
            'array' => $array,
            'required' => $required,
        ];

        return true;
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        foreach ($attributes as $attribute) {
            $this->createAttribute(
                $collection,
                (string) $attribute['$id'],
                (string) $attribute['type'],
                (int) ($attribute['size'] ?? 0),
                (bool) ($attribute['signed'] ?? true),
                (bool) ($attribute['array'] ?? false),
                (bool) ($attribute['required'] ?? false),
            );
        }

        return true;
    }

    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $id = $this->filter($id);
        if (! empty($newKey) && $newKey !== $id) {
            $this->renameAttribute($collection, $id, $newKey);
            $id = $this->filter($newKey);
        }

        $this->data[$key]['attributes'][$id] = [
            'type' => $type,
            'size' => $size,
            'signed' => $signed,
            'array' => $array,
            'required' => $required,
        ];

        return true;
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return true;
        }

        $id = $this->filter($id);
        unset($this->data[$key]['attributes'][$id]);
        foreach ($this->data[$key]['documents'] as &$document) {
            unset($document[$id]);
        }
        unset($document);

        foreach ($this->data[$key]['indexes'] as &$index) {
            $attributes = $index['attributes'] ?? [];
            $filtered = [];
            $lengths = [];
            $orders = [];
            foreach ($attributes as $i => $attribute) {
                if ($this->filter($attribute) === $id) {
                    continue;
                }
                $filtered[] = $attribute;
                if (isset($index['lengths'][$i])) {
                    $lengths[] = $index['lengths'][$i];
                }
                if (isset($index['orders'][$i])) {
                    $orders[] = $index['orders'][$i];
                }
            }
            $index['attributes'] = $filtered;
            $index['lengths'] = $lengths;
            $index['orders'] = $orders;
        }
        unset($index);

        return true;
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $old = $this->filter($old);
        $new = $this->filter($new);

        if (! isset($this->data[$key]['attributes'][$old])) {
            return true;
        }

        $this->data[$key]['attributes'][$new] = $this->data[$key]['attributes'][$old];
        unset($this->data[$key]['attributes'][$old]);

        foreach ($this->data[$key]['documents'] as &$document) {
            if (\array_key_exists($old, $document)) {
                $document[$new] = $document[$old];
                unset($document[$old]);
            }
        }
        unset($document);

        foreach ($this->data[$key]['indexes'] as &$index) {
            $attributes = $index['attributes'] ?? [];
            foreach ($attributes as $i => $attribute) {
                if ($this->filter($attribute) === $old) {
                    $attributes[$i] = $new;
                }
            }
            $index['attributes'] = $attributes;
        }
        unset($index);

        return true;
    }

    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        // Memory stores documents as flexible maps, so the relationship "column"
        // is registered on the attribute list rather than added as a physical
        // schema column. The registration ensures that reads always surface
        // the relationship key (as null when unpopulated) — matching MariaDB,
        // which selects the column even when no rows have a value.
        // The M2M junction collection itself is created by the wrapper through
        // the standard createCollection path.
        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                $this->registerRelationshipField($collection, $id);
                if ($twoWay) {
                    $this->registerRelationshipField($relatedCollection, $twoWayKey);
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                $this->registerRelationshipField($relatedCollection, $twoWayKey);
                break;
            case Database::RELATION_MANY_TO_ONE:
                $this->registerRelationshipField($collection, $id);
                break;
            case Database::RELATION_MANY_TO_MANY:
                // Junction columns live on the junction collection, which is
                // created with explicit attributes by the wrapper.
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        $key = $this->filter($key);
        $twoWayKey = $this->filter($twoWayKey);
        $newKey = $newKey !== null ? $this->filter($newKey) : null;
        $newTwoWayKey = $newTwoWayKey !== null ? $this->filter($newTwoWayKey) : null;

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                if ($newKey !== null && $newKey !== $key) {
                    $this->renameDocumentField($collection, $key, $newKey);
                }
                if ($twoWay && $newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                    $this->renameDocumentField($relatedCollection, $twoWayKey, $newTwoWayKey);
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameDocumentField($relatedCollection, $twoWayKey, $newTwoWayKey);
                    }
                } else {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameDocumentField($collection, $key, $newKey);
                    }
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameDocumentField($relatedCollection, $twoWayKey, $newTwoWayKey);
                    }
                } else {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameDocumentField($collection, $key, $newKey);
                    }
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                $junction = $this->resolveJunctionCollection($collection, $relatedCollection, $side);
                if ($junction !== null) {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameDocumentField($junction, $key, $newKey);
                    }
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameDocumentField($junction, $twoWayKey, $newTwoWayKey);
                    }
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        $key = $this->filter($key);
        $twoWayKey = $this->filter($twoWayKey);

        switch ($type) {
            case Database::RELATION_ONE_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $this->dropDocumentField($collection, $key);
                    if ($twoWay) {
                        $this->dropDocumentField($relatedCollection, $twoWayKey);
                    }
                } else {
                    $this->dropDocumentField($relatedCollection, $twoWayKey);
                    if ($twoWay) {
                        $this->dropDocumentField($collection, $key);
                    }
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $this->dropDocumentField($relatedCollection, $twoWayKey);
                } else {
                    $this->dropDocumentField($collection, $key);
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $this->dropDocumentField($collection, $key);
                } else {
                    $this->dropDocumentField($relatedCollection, $twoWayKey);
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                // Junction collection is dropped by the wrapper via cleanupCollection.
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    /**
     * Register a relationship field on the collection's attribute list so
     * subsequent reads materialise the field (as null) even when no document
     * has been written to it. Mirrors MariaDB's `ADD COLUMN ... DEFAULT NULL`.
     */
    protected function registerRelationshipField(string $collection, string $field): void
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return;
        }
        $field = $this->filter($field);
        $this->data[$key]['attributes'][$field] = [
            'type' => Database::VAR_RELATIONSHIP,
            'size' => 0,
            'signed' => true,
            'array' => false,
            'required' => false,
        ];
    }

    /**
     * Unregister a relationship field from the collection's attribute list.
     */
    protected function unregisterRelationshipField(string $collection, string $field): void
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return;
        }
        unset($this->data[$key]['attributes'][$this->filter($field)]);
    }

    /**
     * Rename a field across every document in a collection, preserving null
     * entries so subsequent reads that join on the new key still resolve.
     */
    protected function renameDocumentField(string $collection, string $oldKey, string $newKey): void
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return;
        }
        if (isset($this->data[$key]['attributes'][$oldKey])) {
            $this->data[$key]['attributes'][$newKey] = $this->data[$key]['attributes'][$oldKey];
            unset($this->data[$key]['attributes'][$oldKey]);
        }
        foreach ($this->data[$key]['documents'] as $storageKey => $document) {
            if (! \array_key_exists($oldKey, $document)) {
                continue;
            }
            $document[$newKey] = $document[$oldKey];
            unset($document[$oldKey]);
            $this->data[$key]['documents'][$storageKey] = $document;
        }
    }

    /**
     * Remove a field from every document in a collection.
     */
    protected function dropDocumentField(string $collection, string $field): void
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return;
        }
        unset($this->data[$key]['attributes'][$field]);
        foreach ($this->data[$key]['documents'] as $storageKey => $document) {
            if (\array_key_exists($field, $document)) {
                unset($document[$field]);
                $this->data[$key]['documents'][$storageKey] = $document;
            }
        }
    }

    /**
     * Resolve the junction collection name for a many-to-many relationship.
     * Mirrors Database::getJunctionCollection — the junction is named after
     * the parent/child sequence pair.
     */
    protected function resolveJunctionCollection(string $collection, string $relatedCollection, string $side): ?string
    {
        $metadataKey = $this->key(Database::METADATA);
        if (! isset($this->data[$metadataKey])) {
            return null;
        }

        $collectionDoc = $this->locateDocument($metadataKey, Database::METADATA, $collection);
        $relatedDoc = $this->locateDocument($metadataKey, Database::METADATA, $relatedCollection);
        if ($collectionDoc === null || $relatedDoc === null) {
            return null;
        }

        $collectionSequence = $collectionDoc[1]['_id'] ?? null;
        $relatedSequence = $relatedDoc[1]['_id'] ?? null;
        if ($collectionSequence === null || $relatedSequence === null) {
            return null;
        }

        return $side === Database::RELATION_SIDE_PARENT
            ? '_'.$collectionSequence.'_'.$relatedSequence
            : '_'.$relatedSequence.'_'.$collectionSequence;
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $old = $this->filter($old);
        $new = $this->filter($new);

        if (! isset($this->data[$key]['indexes'][$old])) {
            return true;
        }

        $this->data[$key]['indexes'][$new] = $this->data[$key]['indexes'][$old];
        unset($this->data[$key]['indexes'][$old]);

        return true;
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        if ($type === Database::INDEX_UNIQUE && ! empty($attributes)) {
            // MariaDB rejects CREATE UNIQUE INDEX with errno 1062 when existing
            // rows contain duplicates; Database::createIndex catches the resulting
            // DuplicateException and treats it as an "orphan index" (the metadata
            // is registered but the physical index is absent). Mirror that contract:
            // throw DuplicateException so callers see identical end-state behavior.
            $seen = [];
            foreach ($this->data[$key]['documents'] as $row) {
                $signature = [];
                foreach ($attributes as $attribute) {
                    $signature[] = $row[$this->mapAttribute($attribute)] ?? null;
                }
                if (\in_array(null, $signature, true)) {
                    continue;
                }
                $hash = \json_encode($signature);
                if (isset($seen[$hash])) {
                    throw new DuplicateException('Cannot create unique index: existing rows already contain duplicate values');
                }
                $seen[$hash] = true;
            }
        }

        $id = $this->filter($id);
        $this->data[$key]['indexes'][$id] = [
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders,
        ];

        return true;
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return true;
        }

        $id = $this->filter($id);
        unset($this->data[$key]['indexes'][$id]);

        return true;
    }

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            return new Document([]);
        }

        $located = $this->locateDocument($key, $collection->getId(), $id);
        if ($located === null) {
            return new Document([]);
        }

        $row = $this->rowToDocument($located[1], null, $key);

        // Apply Query::select projection — drop user attributes that were not
        // requested but always retain the document internals ($id, $sequence,
        // permissions etc.) the caller depends on.
        $selections = $this->getSelectAttributes($queries);
        if (! empty($selections)) {
            $row = $this->projectRow($row, $selections);
        }

        return new Document($row);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<string>
     */
    private function getSelectAttributes(array $queries): array
    {
        $selected = [];
        foreach ($queries as $query) {
            if (! $query instanceof Query) {
                continue;
            }
            if ($query->getMethod() === Query::TYPE_SELECT) {
                foreach ($query->getValues() as $value) {
                    $selected[] = (string) $value;
                }
            }
        }

        return $selected;
    }

    /**
     * @param  array<string, mixed>  $row
     * @param  array<string>  $selections
     * @return array<string, mixed>
     */
    private function projectRow(array $row, array $selections): array
    {
        // '*' means "all user attributes" — equivalent to no filter at the
        // adapter level since the Database casting layer fills defaults.
        if (\in_array('*', $selections, true)) {
            return $row;
        }

        $projected = [];
        foreach ($row as $field => $value) {
            // Always preserve internals — they are namespaced with `$` or `_`.
            if (\str_starts_with($field, '$') || \str_starts_with($field, '_')) {
                $projected[$field] = $value;

                continue;
            }
            if (\in_array($field, $selections, true)) {
                $projected[$field] = $value;
            }
        }

        return $projected;
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $docKey = $this->documentKey($document->getId(), $document->getTenant());
        if (isset($this->data[$key]['documents'][$docKey])) {
            if ($this->skipDuplicates) {
                // Mirrors MariaDB's `INSERT IGNORE` — duplicate primary key is
                // silently dropped and the existing row's sequence is returned.
                $existing = $this->data[$key]['documents'][$docKey];
                $document['$sequence'] = (string) $existing['_id'];

                return $document;
            }
            throw new DuplicateException('Document already exists');
        }

        try {
            $this->enforceUniqueIndexes($key, $document, null);
        } catch (DuplicateException $e) {
            if ($this->skipDuplicates) {
                return $document;
            }
            throw $e;
        }

        $sequence = $document->getSequence();
        if (empty($sequence)) {
            $this->data[$key]['sequence']++;
            $sequence = $this->data[$key]['sequence'];
        } else {
            $sequence = (int) $sequence;
            if ($sequence > $this->data[$key]['sequence']) {
                $this->data[$key]['sequence'] = $sequence;
            }
        }

        $row = $this->documentToRow($document);
        $row['_id'] = $sequence;

        $this->data[$key]['documents'][$docKey] = $row;
        $this->writePermissions($key, $document);

        $document['$sequence'] = (string) $sequence;

        return $document;
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        $created = [];
        foreach ($documents as $document) {
            $created[] = $this->createDocument($collection, $document);
        }

        return $created;
    }

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $located = $this->locateDocument($key, $collection->getId(), $id);
        if ($located === null) {
            throw new NotFoundException('Document not found');
        }
        [$oldKey, $existing] = $located;

        // Resolve any Operator-typed attributes against the existing row before
        // computing the new payload so unique-index checks see the post-update
        // values, matching MariaDB's atomic UPDATE semantics.
        $resolvedAttrs = $this->applyOperators($document->getAttributes(), $existing);
        foreach ($resolvedAttrs as $attribute => $value) {
            $document->setAttribute($attribute, $value);
        }

        $newId = $document->getId();
        $newKey = $this->documentKey($newId);
        if ($newId !== $id && isset($this->data[$key]['documents'][$newKey])) {
            throw new DuplicateException('Document already exists');
        }

        $this->enforceUniqueIndexes($key, $document, $id);

        $update = $this->documentToRow($document);

        // Sparse update — MariaDB's UPDATE only sets columns present in the
        // document; absent columns retain their previous values. The wrapper
        // relies on this for relationship updates, where it removes
        // unchanged relationship keys before calling the adapter.
        $row = \array_merge($existing, $update);
        $row['_id'] = $existing['_id'];
        if ($this->sharedTables && \array_key_exists('_tenant', $existing)) {
            // Preserve the row's stored tenant — MariaDB's UPDATE statements
            // never rewrite `_tenant` and tests rely on the original tenant
            // (e.g. the metadata NULL-tenant rows) surviving an update.
            $row['_tenant'] = $existing['_tenant'];
        }

        $newKey = $this->sharedTables
            ? ($existing['_tenant'] ?? $this->getTenant()).'|'.\strtolower($newId)
            : \strtolower($newId);

        if ($newId !== $id || $newKey !== $oldKey) {
            unset($this->data[$key]['documents'][$oldKey]);
        }
        $this->data[$key]['documents'][$newKey] = $row;

        if (! $skipPermissions) {
            // Remove any permissions keyed to the old uid and rewrite.
            $this->permissions[$key] = \array_values(\array_filter(
                $this->permissions[$key],
                fn (array $p) => $p['document'] !== $id && $p['document'] !== $newId
            ));
            $this->writePermissions($key, $document);
        } elseif ($newId !== $id) {
            foreach ($this->permissions[$key] as &$row) {
                if ($row['document'] === $id) {
                    $row['document'] = $newId;
                }
            }
            unset($row);
        }

        return $document;
    }

    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        if (empty($documents)) {
            return 0;
        }

        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $attrs = $updates->getAttributes();
        $hasCreatedAt = ! empty($updates->getCreatedAt());
        $hasUpdatedAt = ! empty($updates->getUpdatedAt());
        $hasPermissions = $updates->offsetExists('$permissions');
        if (empty($attrs) && ! $hasCreatedAt && ! $hasUpdatedAt && ! $hasPermissions) {
            return 0;
        }

        $count = 0;
        foreach ($documents as $doc) {
            $uid = $doc->getId();
            $docKey = $this->documentKey($uid);
            if (! isset($this->data[$key]['documents'][$docKey])) {
                continue;
            }

            // Resolve operators per-row — each document's existing values feed
            // back into operator evaluation, so $attrs cannot be evaluated
            // once and reused.
            $resolvedAttrs = $this->applyOperators($attrs, $this->data[$key]['documents'][$docKey]);

            if (! empty($resolvedAttrs)) {
                $merged = new Document(\array_merge(
                    $this->rowToDocument($this->data[$key]['documents'][$docKey]),
                    $resolvedAttrs,
                    ['$id' => $uid]
                ));
                $this->enforceUniqueIndexes($key, $merged, $uid);
            }

            $row = &$this->data[$key]['documents'][$docKey];
            foreach ($resolvedAttrs as $attribute => $value) {
                if (\is_array($value)) {
                    $value = \json_encode($value);
                }
                $row[$this->filter($attribute)] = $value;
            }

            if ($hasCreatedAt) {
                $row['_createdAt'] = $updates->getCreatedAt();
            }
            if ($hasUpdatedAt) {
                $row['_updatedAt'] = $updates->getUpdatedAt();
            }
            if ($hasPermissions) {
                $row['_permissions'] = \json_encode($updates->getPermissions());
                $this->permissions[$key] = \array_values(\array_filter(
                    $this->permissions[$key],
                    fn (array $p) => $p['document'] !== $uid
                ));
                foreach (Database::PERMISSIONS as $type) {
                    foreach ($updates->getPermissionsByType($type) as $permission) {
                        $this->permissions[$key][] = [
                            'document' => $uid,
                            'type' => $type,
                            'permission' => \str_replace('"', '', $permission),
                            'tenant' => $this->getTenant(),
                        ];
                    }
                }
            }
            $count++;
            unset($row);
        }

        return $count;
    }

    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        throw new DatabaseException('Upsert is not implemented in the Memory adapter');
    }

    public function getSequences(string $collection, array $documents): array
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return $documents;
        }

        foreach ($documents as $index => $doc) {
            if (! empty($doc->getSequence())) {
                continue;
            }
            // Mirror MariaDB::getSequences which binds :_tenant_$i to $document->getTenant()
            // — the lookup must use each document's own tenant, not the adapter's current tenant.
            $existing = $this->data[$key]['documents'][$this->documentKey($doc->getId(), $doc->getTenant())] ?? null;
            if ($existing !== null) {
                $documents[$index]->setAttribute('$sequence', (string) $existing['_id']);
            }
        }

        return $documents;
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            // MariaDB throws when the collection itself is gone (PDO unknown
            // table → NotFoundException). A missing document inside an existing
            // collection still returns false to mirror rowCount() == 0.
            throw new NotFoundException('Collection not found');
        }

        $docKey = $this->documentKey($id);
        if (! isset($this->data[$key]['documents'][$docKey])) {
            return false;
        }

        unset($this->data[$key]['documents'][$docKey]);
        $this->permissions[$key] = \array_values(\array_filter(
            $this->permissions[$key] ?? [],
            fn (array $p) => $p['document'] !== $id
        ));

        return true;
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $seqSet = [];
        foreach ($sequences as $seq) {
            $seqSet[(string) $seq] = true;
        }

        $count = 0;
        $deletedIds = [];
        foreach ($this->data[$key]['documents'] as $uid => $row) {
            // With sharedTables the row map is keyed by "tenant|uid" so sequence
            // collisions across tenants are possible. Skip rows that don't belong
            // to the current tenant so we never delete another tenant's data.
            if ($this->sharedTables && ($row['_tenant'] ?? null) !== $this->getTenant()) {
                continue;
            }
            if (isset($seqSet[(string) ($row['_id'] ?? '')])) {
                $deletedIds[(string) ($row['_uid'] ?? $uid)] = true;
                unset($this->data[$key]['documents'][$uid]);
                $count++;
            }
        }

        $permSet = ! empty($permissionIds)
            ? \array_flip(\array_map('strval', $permissionIds))
            : [];

        if (! empty($deletedIds) || ! empty($permSet)) {
            $this->permissions[$key] = \array_values(\array_filter(
                $this->permissions[$key] ?? [],
                fn (array $p) => ! isset($deletedIds[$p['document']]) && ! isset($permSet[$p['document']])
            ));
        }

        return $count;
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = \array_values($this->data[$key]['documents']);
        $rows = $this->applyTenantFilter($rows, $collection->getId());
        $rows = $this->applyQueries($rows, $queries);
        $rows = $this->applyPermissions($collection, $rows, $forPermission);
        $rows = $this->applyOrdering($rows, $orderAttributes, $orderTypes, $cursorDirection);
        $rows = $this->applyCursor($rows, $orderAttributes, $orderTypes, $cursor, $cursorDirection);

        if (! is_null($offset)) {
            $rows = \array_slice($rows, $offset);
        }
        if (! is_null($limit)) {
            $rows = \array_slice($rows, 0, $limit);
        }

        $selections = $this->extractSelections($queries);
        $results = [];
        foreach ($rows as $row) {
            $results[] = new Document($this->rowToDocument($row, $selections, $key));
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $results = \array_reverse($results);
        }

        return $results;
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = \array_values($this->data[$key]['documents']);
        $rows = $this->applyTenantFilter($rows, $collection->getId());
        $rows = $this->applyQueries($rows, $queries);
        $rows = $this->applyPermissions($collection, $rows, Database::PERMISSION_READ);

        if (! is_null($max)) {
            // MariaDB applies LIMIT :max inside the COUNT subquery — LIMIT 0
            // legitimately yields 0. Honour zero rather than ignoring it.
            $rows = \array_slice($rows, 0, $max);
        }

        return \count($rows);
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = \array_values($this->data[$key]['documents']);
        $rows = $this->applyTenantFilter($rows, $collection->getId());
        $rows = $this->applyQueries($rows, $queries);
        $rows = $this->applyPermissions($collection, $rows, Database::PERMISSION_READ);

        if (! is_null($max)) {
            $rows = \array_slice($rows, 0, $max);
        }

        $sum = 0;
        $isFloat = false;
        $column = $this->filter($attribute);
        foreach ($rows as $row) {
            if (! \array_key_exists($column, $row) || $row[$column] === null) {
                continue;
            }
            if (\is_float($row[$column])) {
                $isFloat = true;
            }
            $sum += $row[$column];
        }

        return $isFloat ? (float) $sum : (int) $sum;
    }

    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
    {
        $key = $this->key($collection);
        $docKey = $this->documentKey($id);
        if (! isset($this->data[$key]['documents'][$docKey])) {
            throw new NotFoundException('Document not found');
        }

        $column = $this->filter($attribute);
        $current = $this->data[$key]['documents'][$docKey][$column] ?? 0;
        $current = is_numeric($current) ? $current + 0 : 0;

        // MariaDB encodes the bound check as part of the WHERE clause against
        // the current column value (`attr <= :max` / `attr >= :min`); when the
        // bound is violated the UPDATE simply matches zero rows and the call
        // still returns true. Mirror that — silent no-op on bound violation.
        // The Database layer pre-subtracts $value from $max (and adds it to
        // $min), so the comparison stays against the pre-update value.
        if (! is_null($min) && $current < $min) {
            return true;
        }
        if (! is_null($max) && $current > $max) {
            return true;
        }

        $this->data[$key]['documents'][$docKey][$column] = $current + $value;
        $this->data[$key]['documents'][$docKey]['_updatedAt'] = $updatedAt;

        return true;
    }

    public function getSizeOfCollection(string $collection): int
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return 0;
        }

        return \strlen(\serialize($this->data[$key]));
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->getSizeOfCollection($collection);
    }

    public function getLimitForString(): int
    {
        return 4294967295;
    }

    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    public function getLimitForAttributes(): int
    {
        return 1017;
    }

    public function getLimitForIndexes(): int
    {
        return 64;
    }

    public function getMaxIndexLength(): int
    {
        // Memory does not enforce per-index byte limits, but the Database
        // layer expects a positive cap so callers can derive sizes via
        // arithmetic (e.g. `getMaxIndexLength() - 68`). Match Mongo's value.
        return 1024;
    }

    public function getMaxVarcharLength(): int
    {
        return 16381;
    }

    public function getMaxUIDLength(): int
    {
        return 255;
    }

    public function getMinDateTime(): \DateTime
    {
        return new \DateTime('0001-01-01 00:00:00');
    }

    public function getIdAttributeType(): string
    {
        return Database::VAR_INTEGER;
    }

    public function getSupportForSchemas(): bool
    {
        return true;
    }

    public function getSupportForAttributes(): bool
    {
        return $this->supportForAttributes;
    }

    public function setSupportForAttributes(bool $support): bool
    {
        $this->supportForAttributes = $support;

        return $this->supportForAttributes;
    }

    public function getSupportForSchemaAttributes(): bool
    {
        return false;
    }

    public function getSupportForSchemaIndexes(): bool
    {
        return false;
    }

    public function getSupportForIndex(): bool
    {
        return true;
    }

    public function getSupportForIndexArray(): bool
    {
        return false;
    }

    public function getSupportForCastIndexArray(): bool
    {
        return false;
    }

    public function getSupportForUniqueIndex(): bool
    {
        return true;
    }

    public function getSupportForFulltextIndex(): bool
    {
        return true;
    }

    public function getSupportForFulltextWildcardIndex(): bool
    {
        return false;
    }

    public function getSupportForCasting(): bool
    {
        // Memory stores native PHP types where possible but JSON-encodes array
        // attributes on write. Returning true asks the Database layer's
        // `casting` step to JSON-decode array columns and coerce scalar types
        // — same behaviour as the SQL adapters.
        return true;
    }

    public function getSupportForQueryContains(): bool
    {
        return true;
    }

    public function getSupportForTimeouts(): bool
    {
        return false;
    }

    public function getSupportForRelationships(): bool
    {
        return true;
    }

    public function getSupportForUpdateLock(): bool
    {
        return false;
    }

    public function getSupportForBatchOperations(): bool
    {
        return true;
    }

    public function getSupportForAttributeResizing(): bool
    {
        return true;
    }

    public function getSupportForGetConnectionId(): bool
    {
        return false;
    }

    public function getSupportForUpserts(): bool
    {
        return false;
    }

    public function getSupportForVectors(): bool
    {
        return false;
    }

    public function getSupportForCacheSkipOnFailure(): bool
    {
        return false;
    }

    public function getSupportForReconnection(): bool
    {
        return false;
    }

    public function getSupportForHostname(): bool
    {
        return false;
    }

    public function getSupportForBatchCreateAttributes(): bool
    {
        return true;
    }

    public function getSupportForSpatialAttributes(): bool
    {
        return false;
    }

    public function getSupportForObject(): bool
    {
        return true;
    }

    public function getSupportForObjectIndexes(): bool
    {
        return true;
    }

    public function getSupportForSpatialIndexNull(): bool
    {
        return false;
    }

    public function getSupportForOperators(): bool
    {
        return true;
    }

    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return false;
    }

    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }

    public function getSupportForSpatialAxisOrder(): bool
    {
        return false;
    }

    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return false;
    }

    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return false;
    }

    public function getSupportForMultipleFulltextIndexes(): bool
    {
        return false;
    }

    public function getSupportForIdenticalIndexes(): bool
    {
        return false;
    }

    public function getSupportForOrderRandom(): bool
    {
        return true;
    }

    public function getCountOfAttributes(Document $collection): int
    {
        return \count($collection->getAttribute('attributes', [])) + $this->getCountOfDefaultAttributes();
    }

    public function getCountOfIndexes(Document $collection): int
    {
        return \count($collection->getAttribute('indexes', [])) + $this->getCountOfDefaultIndexes();
    }

    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::INTERNAL_ATTRIBUTES);
    }

    public function getCountOfDefaultIndexes(): int
    {
        return \count(Database::INTERNAL_INDEXES);
    }

    public function getDocumentSizeLimit(): int
    {
        return 0;
    }

    public function getAttributeWidth(Document $collection): int
    {
        return 0;
    }

    public function getKeywords(): array
    {
        return [];
    }

    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        return $selections;
    }

    public function getConnectionId(): string
    {
        return '0';
    }

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    public function getSchemaIndexes(string $collection): array
    {
        return [];
    }

    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return '';
    }

    protected function execute(mixed $stmt): bool
    {
        return true;
    }

    protected function quote(string $string): string
    {
        return '"'.$string.'"';
    }

    public function decodePoint(string $wkb): array
    {
        throw new DatabaseException('Spatial types are not implemented in the Memory adapter');
    }

    public function decodeLinestring(string $wkb): array
    {
        throw new DatabaseException('Spatial types are not implemented in the Memory adapter');
    }

    public function decodePolygon(string $wkb): array
    {
        throw new DatabaseException('Spatial types are not implemented in the Memory adapter');
    }

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function getSupportForInternalCasting(): bool
    {
        return false;
    }

    public function getSupportForUTCCasting(): bool
    {
        return false;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return $value;
    }

    public function getSupportForIntegerBooleans(): bool
    {
        return false;
    }

    public function getSupportForAlterLocks(): bool
    {
        return false;
    }

    public function getSupportNonUtfCharacters(): bool
    {
        // Memory is a pass-through PHP array, so it does NOT actively reject
        // non-UTF-8 byte sequences. Returning false skips the inherited
        // non-UTF-character scope test that asserts adapter rejection.
        return false;
    }

    public function getSupportForTrigramIndex(): bool
    {
        return false;
    }

    public function getSupportForPCRERegex(): bool
    {
        return true;
    }

    public function getSupportForPOSIXRegex(): bool
    {
        return false;
    }

    public function getSupportForTransactionRetries(): bool
    {
        return false;
    }

    public function getSupportForNestedTransactions(): bool
    {
        return true;
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    /**
     * @param  array<mixed>  $value
     * @return array<mixed>
     */
    protected function deepCopy(array $value): array
    {
        return \unserialize(\serialize($value));
    }

    /**
     * @return array<string, mixed>
     */
    protected function documentToRow(Document $document): array
    {
        $attributes = $document->getAttributes();
        foreach ($attributes as $attribute => $value) {
            if (\is_array($value)) {
                $attributes[$attribute] = \json_encode($value);
            }
        }

        $row = [];
        foreach ($attributes as $attribute => $value) {
            $row[$this->filter($attribute)] = $value;
        }

        $row['_uid'] = $document->getId();
        $row['_createdAt'] = $document->getCreatedAt();
        $row['_updatedAt'] = $document->getUpdatedAt();
        $row['_permissions'] = \json_encode($document->getPermissions());
        if ($this->sharedTables) {
            // Mirror MariaDB: the row's `_tenant` follows the document's own
            // tenant — that matters in tenantPerDocument mode where the
            // adapter's current tenant is null but each document is tagged.
            $row['_tenant'] = $document->getTenant() ?? $this->getTenant();
        }

        return $row;
    }

    /**
     * Translate a stored row into a Document payload. Array attributes are kept
     * as JSON strings so the Database layer's `casting`/`decode` filters do the
     * decoding (mirroring how the SQL adapters return raw column values). Only
     * a SELECT projection — when supplied — is enforced here, restricting the
     * returned payload to the requested attributes plus the internal columns
     * MariaDB always projects (`$id`, `$sequence`, `$createdAt`, `$updatedAt`,
     * `$permissions`, `$tenant`, `$collection`).
     *
     * @param  array<string, mixed>  $row
     * @param  array<int, string>|null  $selections
     * @return array<string, mixed>
     */
    protected function rowToDocument(array $row, ?array $selections = null, ?string $storageKey = null): array
    {
        $allowed = null;
        if ($selections !== null && $selections !== [] && ! \in_array('*', $selections, true)) {
            $allowed = [];
            foreach ($selections as $selection) {
                $allowed[$this->filter($selection)] = true;
            }
        }

        $document = [];
        foreach ($row as $key => $value) {
            switch ($key) {
                case '_id':
                    $document['$sequence'] = (string) $value;
                    break;
                case '_uid':
                    $document['$id'] = $value;
                    break;
                case '_tenant':
                    $document['$tenant'] = $value;
                    break;
                case '_createdAt':
                    $document['$createdAt'] = $value;
                    break;
                case '_updatedAt':
                    $document['$updatedAt'] = $value;
                    break;
                case '_permissions':
                    $document['$permissions'] = \is_string($value) ? (\json_decode($value, true) ?? []) : ($value ?? []);
                    break;
                default:
                    if ($allowed !== null && ! isset($allowed[$key])) {
                        break;
                    }
                    $document[$key] = $value;
            }
        }

        // Surface registered relationship fields as null when missing — mirrors
        // MariaDB selecting a `DEFAULT NULL` column even when no row has set it.
        if ($storageKey !== null && isset($this->data[$storageKey]['attributes'])) {
            foreach ($this->data[$storageKey]['attributes'] as $attributeId => $definition) {
                if (($definition['type'] ?? null) !== Database::VAR_RELATIONSHIP) {
                    continue;
                }
                if ($allowed !== null && ! isset($allowed[$attributeId])) {
                    continue;
                }
                if (! \array_key_exists($attributeId, $document)) {
                    $document[$attributeId] = null;
                }
            }
        }

        return $document;
    }

    /**
     * @param  array<Query>  $queries
     * @return array<int, string>
     */
    protected function extractSelections(array $queries): array
    {
        $selections = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                foreach ($query->getValues() as $value) {
                    if (\is_string($value)) {
                        $selections[] = $value;
                    }
                }
            }
        }

        return $selections;
    }

    protected function writePermissions(string $key, Document $document): void
    {
        $uid = $document->getId();
        $tenant = $document->getTenant() ?? $this->getTenant();
        foreach (Database::PERMISSIONS as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $this->permissions[$key][] = [
                    'document' => $uid,
                    'type' => $type,
                    'permission' => \str_replace('"', '', $permission),
                    'tenant' => $tenant,
                ];
            }
        }
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @return array<array<string, mixed>>
     */
    protected function applyTenantFilter(array $rows, string $collectionId = ''): array
    {
        if (! $this->sharedTables) {
            return $rows;
        }

        $tenant = $this->getTenant();
        // Mirror MariaDB: rows in the metadata collection are visible across
        // tenants when their _tenant is NULL — the schema bookkeeping is
        // global, even with shared tables enabled.
        $allowNull = $collectionId === Database::METADATA;

        return \array_values(\array_filter(
            $rows,
            function (array $row) use ($tenant, $allowNull) {
                $rowTenant = $row['_tenant'] ?? null;
                if ($allowNull && $rowTenant === null) {
                    return true;
                }

                return $rowTenant === $tenant;
            }
        ));
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @param  array<Query>  $queries
     * @return array<array<string, mixed>>
     */
    protected function applyQueries(array $rows, array $queries): array
    {
        foreach ($queries as $query) {
            $method = $query->getMethod();

            if (\in_array($method, [Query::TYPE_SELECT, Query::TYPE_ORDER_ASC, Query::TYPE_ORDER_DESC, Query::TYPE_ORDER_RANDOM, Query::TYPE_LIMIT, Query::TYPE_OFFSET, Query::TYPE_CURSOR_AFTER, Query::TYPE_CURSOR_BEFORE], true)) {
                continue;
            }

            $rows = \array_values(\array_filter($rows, fn (array $row) => $this->matches($row, $query)));
        }

        return $rows;
    }

    /**
     * @param  array<string, mixed>  $row
     */
    protected function matches(array $row, Query $query): bool
    {
        $method = $query->getMethod();

        if ($method === Query::TYPE_AND) {
            foreach ($query->getValues() as $sub) {
                if (! ($sub instanceof Query) || ! $this->matches($row, $sub)) {
                    return false;
                }
            }

            return true;
        }

        if ($method === Query::TYPE_OR) {
            foreach ($query->getValues() as $sub) {
                if ($sub instanceof Query && $this->matches($row, $sub)) {
                    return true;
                }
            }

            return false;
        }

        $rawAttribute = $query->getAttribute();
        $isObjectQuery = $query->isObjectAttribute() && ! \str_contains($rawAttribute, '.');
        $value = $this->resolveAttributeValue($row, $rawAttribute);
        $queryValues = $query->getValues();

        if ($isObjectQuery) {
            return $this->matchesObject($value, $query);
        }

        switch ($method) {
            case Query::TYPE_EQUAL:
                foreach ($queryValues as $candidate) {
                    if ($this->looseEquals($value, $candidate)) {
                        return true;
                    }
                }

                return false;

            case Query::TYPE_NOT_EQUAL:
                foreach ($queryValues as $candidate) {
                    if ($this->looseEquals($value, $candidate)) {
                        return false;
                    }
                }

                return true;

            case Query::TYPE_LESSER:
                return $value !== null && $value < $queryValues[0];

            case Query::TYPE_LESSER_EQUAL:
                return $value !== null && $value <= $queryValues[0];

            case Query::TYPE_GREATER:
                return $value !== null && $value > $queryValues[0];

            case Query::TYPE_GREATER_EQUAL:
                return $value !== null && $value >= $queryValues[0];

            case Query::TYPE_IS_NULL:
                return $value === null;

            case Query::TYPE_IS_NOT_NULL:
                return $value !== null;

            case Query::TYPE_BETWEEN:
                return $value !== null && $value >= $queryValues[0] && $value <= $queryValues[1];

            case Query::TYPE_NOT_BETWEEN:
                return $value === null || $value < $queryValues[0] || $value > $queryValues[1];

            case Query::TYPE_STARTS_WITH:
                return \is_string($value) && \is_string($queryValues[0]) && \str_starts_with($value, $queryValues[0]);

            case Query::TYPE_NOT_STARTS_WITH:
                return ! \is_string($value) || ! \is_string($queryValues[0]) || ! \str_starts_with($value, $queryValues[0]);

            case Query::TYPE_ENDS_WITH:
                return \is_string($value) && \is_string($queryValues[0]) && \str_ends_with($value, $queryValues[0]);

            case Query::TYPE_NOT_ENDS_WITH:
                return ! \is_string($value) || ! \is_string($queryValues[0]) || ! \str_ends_with($value, $queryValues[0]);

            case Query::TYPE_CONTAINS:
                $haystack = $this->decodeArrayValue($value);
                if ($haystack === null && \is_string($value)) {
                    // Mirror MariaDB's default case-insensitive collation for
                    // CONTAINS-against-string. Array containment stays type/
                    // case sensitive (handled below via looseEquals).
                    foreach ($queryValues as $needle) {
                        if (\is_string($needle) && \stripos($value, $needle) !== false) {
                            return true;
                        }
                    }

                    return false;
                }
                if (! \is_array($haystack)) {
                    return false;
                }
                foreach ($queryValues as $needle) {
                    foreach ($haystack as $item) {
                        if ($this->looseEquals($item, $needle)) {
                            return true;
                        }
                    }
                }

                return false;

            case Query::TYPE_NOT_CONTAINS:
                return ! $this->matches($row, new Query(Query::TYPE_CONTAINS, $query->getAttribute(), $queryValues));

            case Query::TYPE_CONTAINS_ANY:
                // containsAny behaves like contains: array attributes match
                // any of the supplied needles, scalar string attributes fall
                // back to a case-insensitive substring search.
                $haystack = $this->decodeArrayValue($value);
                if ($haystack === null && \is_string($value)) {
                    foreach ($queryValues as $needle) {
                        if (\is_string($needle) && \stripos($value, $needle) !== false) {
                            return true;
                        }
                    }

                    return false;
                }
                if (! \is_array($haystack)) {
                    return false;
                }
                foreach ($queryValues as $needle) {
                    foreach ($haystack as $item) {
                        if ($this->looseEquals($item, $needle)) {
                            return true;
                        }
                    }
                }

                return false;

            case Query::TYPE_CONTAINS_ALL:
                $haystack = $this->decodeArrayValue($value);
                if (! \is_array($haystack)) {
                    return false;
                }
                foreach ($queryValues as $needle) {
                    $found = false;
                    foreach ($haystack as $item) {
                        if ($this->looseEquals($item, $needle)) {
                            $found = true;
                            break;
                        }
                    }
                    if (! $found) {
                        return false;
                    }
                }

                return true;

            case Query::TYPE_SEARCH:
                if (! \is_string($value)) {
                    return false;
                }
                $needle = (string) ($queryValues[0] ?? '');
                if ($needle === '') {
                    return false;
                }

                return $this->matchesFulltext($value, $needle);

            case Query::TYPE_NOT_SEARCH:
                if (! \is_string($value)) {
                    return true;
                }
                $needle = (string) ($queryValues[0] ?? '');
                if ($needle === '') {
                    return true;
                }

                return ! $this->matchesFulltext($value, $needle);

            case Query::TYPE_REGEX:
                if (! \is_string($value)) {
                    return false;
                }
                $pattern = (string) ($queryValues[0] ?? '');

                return $this->matchesRegex($value, $pattern);
        }

        throw new DatabaseException('Query method not implemented in the Memory adapter: '.$method);
    }

    /**
     * Tokenize a value and a needle on whitespace/punctuation and return true
     * if any needle token appears in the value's token set (MariaDB
     * MATCH AGAINST natural-language semantics — any matching word is
     * enough to surface the row). Quoted phrases enforce a contiguous
     * substring match, mirroring boolean-mode `"phrase"` queries.
     */
    protected function matchesFulltext(string $haystack, string $needle): bool
    {
        // Quoted phrase: exact substring match (case-insensitive).
        if (\preg_match('/^"(.*)"$/u', \trim($needle), $matches) === 1) {
            $phrase = \mb_strtolower($matches[1]);
            if ($phrase === '') {
                return false;
            }

            return \str_contains(\mb_strtolower($haystack), $phrase);
        }

        $haystackTokens = $this->tokenize($haystack);
        $needleTokens = $this->tokenize($needle);
        if (empty($needleTokens) || empty($haystackTokens)) {
            return false;
        }
        $set = \array_flip($haystackTokens);
        foreach ($needleTokens as $token) {
            // Mirror MariaDB MATCH AGAINST IN BOOLEAN MODE wildcard suffix
            // semantics — `term*` matches any token starting with `term`.
            if (\str_ends_with($token, '*')) {
                $prefix = \substr($token, 0, -1);
                if ($prefix === '') {
                    continue;
                }
                foreach ($haystackTokens as $candidate) {
                    if (\str_starts_with($candidate, $prefix)) {
                        return true;
                    }
                }

                continue;
            }
            if (isset($set[$token])) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return array<string>
     */
    protected function tokenize(string $text): array
    {
        $lower = \mb_strtolower($text);
        $parts = \preg_split('/[^\p{L}\p{N}*]+/u', $lower) ?: [];

        return \array_values(\array_filter($parts, fn (string $p) => $p !== ''));
    }

    /**
     * Apply the supplied regex against $value. Pattern is the raw expression
     * — wrap it in delimiters before passing to preg_match, mirroring how
     * MariaDB's REGEXP operator accepts the pattern verbatim.
     */
    protected function matchesRegex(string $value, string $pattern): bool
    {
        $delimited = '#'.\str_replace('#', '\\#', $pattern).'#u';
        $matched = @\preg_match($delimited, $value);

        return $matched === 1;
    }

    protected function looseEquals(mixed $a, mixed $b): bool
    {
        if ($a === $b) {
            return true;
        }
        if (\is_numeric($a) && \is_numeric($b)) {
            return $a + 0 === $b + 0;
        }

        return false;
    }

    /**
     * Return the decoded array if $value looks like a JSON-encoded array
     * or is already an array; null otherwise.
     *
     * @return array<mixed>|null
     */
    protected function decodeArrayValue(mixed $value): ?array
    {
        if (\is_array($value)) {
            return $value;
        }
        if (\is_string($value) && $value !== '' && ($value[0] === '[' || $value[0] === '{')) {
            $decoded = \json_decode($value, true);

            return \is_array($decoded) ? $decoded : null;
        }

        return null;
    }

    /**
     * Object-typed query semantics — equal/notEqual treat the supplied value
     * as a containment check (Postgres `@>` JSONB operator). contains/
     * containsAny/containsAll treat single-key entries with scalar values
     * as a wrapping shorthand for array containment.
     */
    protected function matchesObject(mixed $value, Query $query): bool
    {
        $haystack = $this->decodeObjectValue($value);
        $values = $query->getValues();
        $method = $query->getMethod();

        switch ($method) {
            case Query::TYPE_EQUAL:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $candidate)) {
                        return true;
                    }
                }

                return false;

            case Query::TYPE_NOT_EQUAL:
                if ($haystack === null) {
                    return true;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $candidate)) {
                        return false;
                    }
                }

                return true;

            case Query::TYPE_CONTAINS:
            case Query::TYPE_CONTAINS_ANY:
            case Query::TYPE_CONTAINS_ALL:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $this->wrapScalarObjectValue($candidate))) {
                        return true;
                    }
                }

                return false;

            case Query::TYPE_NOT_CONTAINS:
                if ($haystack === null) {
                    return true;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $this->wrapScalarObjectValue($candidate))) {
                        return false;
                    }
                }

                return true;

            case Query::TYPE_IS_NULL:
                return $value === null;

            case Query::TYPE_IS_NOT_NULL:
                return $value !== null;
        }

        throw new DatabaseException('Query method '.$method.' not supported for object attributes');
    }

    protected function decodeObjectValue(mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }
        if (\is_array($value)) {
            return $value;
        }
        if (\is_string($value) && $value !== '' && ($value[0] === '{' || $value[0] === '[')) {
            $decoded = \json_decode($value, true);
            if (\is_array($decoded)) {
                return $decoded;
            }
        }

        return $value;
    }

    /**
     * Postgres `@>` JSONB containment in PHP. A subset arrayKey/value is
     * considered contained when every key in the candidate is also present
     * in the haystack with the same (or recursively contained) value, OR
     * when the haystack is a list and contains the candidate item.
     */
    protected function jsonContains(mixed $haystack, mixed $candidate): bool
    {
        if (\is_array($haystack) && \array_is_list($haystack)) {
            if (\is_array($candidate) && \array_is_list($candidate)) {
                foreach ($candidate as $needle) {
                    $matched = false;
                    foreach ($haystack as $item) {
                        if ($this->jsonContains($item, $needle)) {
                            $matched = true;
                            break;
                        }
                    }
                    if (! $matched) {
                        return false;
                    }
                }

                return true;
            }
            foreach ($haystack as $item) {
                if ($this->jsonContains($item, $candidate)) {
                    return true;
                }
            }

            return false;
        }
        if (\is_array($haystack) && \is_array($candidate)) {
            foreach ($candidate as $key => $value) {
                if (! \array_key_exists($key, $haystack)) {
                    return false;
                }
                if (! $this->jsonContains($haystack[$key], $value)) {
                    return false;
                }
            }

            return true;
        }
        if ($haystack === $candidate) {
            return true;
        }
        if (\is_numeric($haystack) && \is_numeric($candidate)) {
            return $haystack + 0 === $candidate + 0;
        }

        return false;
    }

    /**
     * Mirror Postgres' contains/containsAny/containsAll wrapping convention —
     * a candidate of the form `['skills' => 'typescript']` is rewritten to
     * `['skills' => ['typescript']]` so containment hits an array element.
     */
    protected function wrapScalarObjectValue(mixed $candidate): mixed
    {
        if (! \is_array($candidate) || \count($candidate) !== 1) {
            return $candidate;
        }
        $key = \array_key_first($candidate);
        $value = $candidate[$key];
        if (\is_array($value)) {
            return $candidate;
        }

        return [$key => [$value]];
    }

    /**
     * Mirror resolveAttributeValue but read from a Document — used by
     * enforceUniqueIndexes when checking the new payload.
     */
    protected function resolveDocumentValue(Document $document, string $attribute): mixed
    {
        if (! \str_contains($attribute, '.')) {
            $value = $document->getAttribute($attribute);
            if ($value === null) {
                $value = $document->getAttribute($this->mapAttribute($attribute));
            }

            return $value;
        }

        [$head, $rest] = \explode('.', $attribute, 2);
        $value = $document->getAttribute($head);
        // Database::encode may have JSON-encoded the head attribute — decode
        // so the dotted-path walk descends into the underlying structure.
        if (\is_string($value) && $value !== '' && ($value[0] === '{' || $value[0] === '[')) {
            $decoded = \json_decode($value, true);
            if (\is_array($decoded)) {
                $value = $decoded;
            }
        }

        return $this->resolveNestedPath($value, $rest);
    }

    /**
     * Resolve an attribute reference (dotted path, internal alias, or bare
     * column) against a stored row. JSON-encoded blobs are decoded on demand
     * so dotted paths can descend into them.
     *
     * @param array<string, mixed> $row
     */
    protected function resolveAttributeValue(array $row, string $attribute): mixed
    {
        // MariaDB strips non-alphanumeric chars from column names before any
        // SELECT, so an attribute like `$symbols_coll.ection3` becomes a flat
        // `symbols_collection3` column. Mirror that: if the fully-filtered name
        // exists as a column on the row, return it directly without splitting
        // on dots — only fall back to nested object path traversal when the
        // flat lookup misses.
        $flatColumn = $this->mapAttribute($attribute);
        if (\array_key_exists($flatColumn, $row)) {
            return $row[$flatColumn];
        }
        if (! \str_contains($attribute, '.')) {
            return null;
        }

        [$head, $rest] = \explode('.', $attribute, 2);
        $column = $this->mapAttribute($head);
        $value = \array_key_exists($column, $row) ? $row[$column] : null;
        if (\is_string($value) && $value !== '' && ($value[0] === '{' || $value[0] === '[')) {
            $decoded = \json_decode($value, true);
            if (\is_array($decoded)) {
                $value = $decoded;
            }
        }

        return $this->resolveNestedPath($value, $rest);
    }

    protected function resolveNestedPath(mixed $value, string $path): mixed
    {
        $parts = \explode('.', $path);
        foreach ($parts as $part) {
            if (\is_array($value) && \array_key_exists($part, $value)) {
                $value = $value[$part];

                continue;
            }

            return null;
        }

        return $value;
    }

    protected function mapAttribute(string $attribute): string
    {
        return match ($attribute) {
            '$id' => '_uid',
            '$sequence' => '_id',
            '$tenant' => '_tenant',
            '$createdAt' => '_createdAt',
            '$updatedAt' => '_updatedAt',
            '$permissions' => '_permissions',
            default => $this->filter($attribute),
        };
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @return array<array<string, mixed>>
     */
    protected function applyPermissions(Document $collection, array $rows, string $forPermission): array
    {
        if (! $this->authorization->getStatus()) {
            return $rows;
        }

        $key = $this->key($collection->getId());
        $roles = $this->authorization->getRoles();
        $roleSet = \array_flip($roles);

        $allowed = [];
        foreach ($this->permissions[$key] ?? [] as $perm) {
            if ($perm['type'] !== $forPermission) {
                continue;
            }
            if ($this->sharedTables && ($perm['tenant'] ?? null) !== $this->getTenant()) {
                continue;
            }
            if (isset($roleSet[$perm['permission']])) {
                $allowed[$perm['document']] = true;
            }
        }

        return \array_values(\array_filter(
            $rows,
            fn (array $row) => isset($allowed[$row['_uid'] ?? ''])
        ));
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @param  array<string>  $orderAttributes
     * @param  array<string>  $orderTypes
     * @return array<array<string, mixed>>
     */
    protected function applyOrdering(array $rows, array $orderAttributes, array $orderTypes, string $cursorDirection): array
    {
        // Random ordering must short-circuit: a non-deterministic comparator
        // breaks usort's transitivity invariant. Shuffle once and return.
        foreach ($orderTypes as $type) {
            if ($type === Database::ORDER_RANDOM) {
                \shuffle($rows);

                return $rows;
            }
        }

        if (empty($orderAttributes)) {
            // Mirror MariaDB's clustered-index ordering when no explicit ORDER BY
            // is supplied — sort by the auto-incrementing _id ascending so
            // pagination via limit/offset is stable across calls.
            \usort($rows, function (array $a, array $b) use ($cursorDirection) {
                $av = $a['_id'] ?? 0;
                $bv = $b['_id'] ?? 0;
                if ($av === $bv) {
                    return 0;
                }
                $cmp = ($av < $bv) ? -1 : 1;

                return $cursorDirection === Database::CURSOR_BEFORE ? -$cmp : $cmp;
            });

            return $rows;
        }

        \usort($rows, function (array $a, array $b) use ($orderAttributes, $orderTypes, $cursorDirection) {
            foreach ($orderAttributes as $i => $attribute) {
                $direction = $orderTypes[$i] ?? Database::ORDER_ASC;
                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $direction = $direction === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                }

                $column = $this->mapAttribute($attribute);
                $av = $a[$column] ?? null;
                $bv = $b[$column] ?? null;

                if ($av == $bv) {
                    continue;
                }

                $cmp = ($av < $bv) ? -1 : 1;

                return $direction === Database::ORDER_ASC ? $cmp : -$cmp;
            }

            return 0;
        });

        return $rows;
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @param  array<string>  $orderAttributes
     * @param  array<string>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<array<string, mixed>>
     */
    protected function applyCursor(array $rows, array $orderAttributes, array $orderTypes, array $cursor, string $cursorDirection): array
    {
        if (empty($cursor)) {
            return $rows;
        }

        if (empty($orderAttributes)) {
            $orderAttributes = ['$sequence'];
            $orderTypes = [Database::ORDER_ASC];
        }

        return \array_values(\array_filter($rows, function (array $row) use ($orderAttributes, $orderTypes, $cursor, $cursorDirection) {
            foreach ($orderAttributes as $i => $attribute) {
                $direction = $orderTypes[$i] ?? Database::ORDER_ASC;
                if ($cursorDirection === Database::CURSOR_BEFORE) {
                    $direction = $direction === Database::ORDER_ASC ? Database::ORDER_DESC : Database::ORDER_ASC;
                }
                $column = $this->mapAttribute($attribute);
                $current = $row[$column] ?? null;
                $ref = $cursor[$attribute] ?? null;

                if ($current == $ref) {
                    continue;
                }

                if ($direction === Database::ORDER_ASC) {
                    return $current > $ref;
                }

                return $current < $ref;
            }

            return false;
        }));
    }

    protected function enforceUniqueIndexes(string $key, Document $document, ?string $previousId): void
    {
        $indexes = $this->data[$key]['indexes'] ?? [];
        foreach ($indexes as $index) {
            if (($index['type'] ?? '') !== Database::INDEX_UNIQUE) {
                continue;
            }

            $attributes = $index['attributes'] ?? [];
            if (empty($attributes)) {
                continue;
            }

            $signature = [];
            foreach ($attributes as $attribute) {
                $signature[] = $this->normalizeIndexValue($this->resolveDocumentValue($document, $attribute));
            }

            if (\in_array(null, $signature, true)) {
                continue;
            }

            foreach ($this->data[$key]['documents'] as $row) {
                $rowUid = (string) ($row['_uid'] ?? '');
                if ($previousId !== null && $rowUid === $previousId) {
                    continue;
                }
                if ($rowUid === $document->getId() && $previousId === null) {
                    continue;
                }
                if ($this->sharedTables && ($row['_tenant'] ?? null) !== $this->getTenant()) {
                    continue;
                }

                $rowSignature = [];
                foreach ($attributes as $attribute) {
                    $rowSignature[] = $this->normalizeIndexValue($this->resolveAttributeValue($row, $attribute));
                }

                if ($rowSignature === $signature) {
                    throw new DuplicateException('Document with the requested unique attributes already exists');
                }
            }
        }
    }

    /**
     * Normalize values for unique-index signature comparison.
     *
     * Documents store native PHP types (e.g. true) while stored rows often
     * have casted equivalents (e.g. 1). To avoid false negatives:
     * - bools are cast to int (true → 1, false → 0)
     * - numeric strings are coerced to numbers ("3" → 3, "3.0" → 3.0)
     * - arrays are JSON-encoded (canonical key/value order is the caller's job)
     * - null is preserved so callers can skip null signatures
     */
    protected function normalizeIndexValue(mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }
        if (\is_bool($value)) {
            return $value ? 1 : 0;
        }
        if (\is_array($value)) {
            return \json_encode($value);
        }
        if (\is_string($value) && \is_numeric($value)) {
            return $value + 0;
        }

        return $value;
    }

    /**
     * Apply a single Operator to a stored row value and return the new value.
     * Mirrors the semantics implemented in MariaDB::getOperatorSQL — the SQL
     * version uses CASE/JSON helpers; this is the in-PHP equivalent.
     */
    protected function applyOperator(mixed $current, Operator $operator): mixed
    {
        $values = $operator->getValues();
        $method = $operator->getMethod();

        switch ($method) {
            case Operator::TYPE_INCREMENT:
                $by = $values[0] ?? 1;
                $max = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;
                if ($max !== null) {
                    // Compare *remaining headroom* against $by so we never
                    // overflow PHP's int range (which would silently demote
                    // the result to float and corrupt downstream Range
                    // validators).
                    if ($base >= $max || ($max - $base) <= $by) {
                        return $this->preserveNumericType($base, $max);
                    }
                }

                return $this->preserveNumericType($base, $base + $by);

            case Operator::TYPE_DECREMENT:
                $by = $values[0] ?? 1;
                $min = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;
                if ($min !== null) {
                    if ($base <= $min || ($base - $min) <= $by) {
                        return $this->preserveNumericType($base, $min);
                    }
                }

                return $this->preserveNumericType($base, $base - $by);

            case Operator::TYPE_MULTIPLY:
                $by = $values[0] ?? 1;
                $max = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($base * $by, $max, true);

            case Operator::TYPE_DIVIDE:
                $by = $values[0] ?? 1;
                $min = $values[1] ?? null;
                if ($by == 0) {
                    return $current;
                }
                $base = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($base / $by, $min, false);

            case Operator::TYPE_MODULO:
                $by = $values[0] ?? 1;
                if ($by == 0) {
                    return $current;
                }
                $base = \is_numeric($current) ? (int) $current : 0;

                return $base % (int) $by;

            case Operator::TYPE_POWER:
                $by = $values[0] ?? 1;
                $max = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($base ** $by, $max, true);

            case Operator::TYPE_STRING_CONCAT:
                return ((string) ($current ?? '')).(string) ($values[0] ?? '');

            case Operator::TYPE_STRING_REPLACE:
                $search = (string) ($values[0] ?? '');
                $replace = (string) ($values[1] ?? '');
                if ($current === null) {
                    return null;
                }

                return \str_replace($search, $replace, (string) $current);

            case Operator::TYPE_TOGGLE:
                return ! (bool) $current;

            case Operator::TYPE_ARRAY_APPEND:
                $list = $this->coerceArray($current);

                return [...$list, ...\array_values($values)];

            case Operator::TYPE_ARRAY_PREPEND:
                $list = $this->coerceArray($current);

                return [...\array_values($values), ...$list];

            case Operator::TYPE_ARRAY_INSERT:
                $list = $this->coerceArray($current);
                $index = (int) ($values[0] ?? 0);
                $value = $values[1] ?? null;
                if ($index < 0) {
                    $index = 0;
                }
                if ($index > \count($list)) {
                    $index = \count($list);
                }
                \array_splice($list, $index, 0, [$value]);

                return $list;

            case Operator::TYPE_ARRAY_REMOVE:
                $list = $this->coerceArray($current);
                $needle = $values[0] ?? null;

                return \array_values(\array_filter($list, fn ($item) => $item !== $needle));

            case Operator::TYPE_ARRAY_UNIQUE:
                $list = $this->coerceArray($current);

                return \array_values(\array_unique($list, SORT_REGULAR));

            case Operator::TYPE_ARRAY_INTERSECT:
                $list = $this->coerceArray($current);
                $other = \array_values($values);

                return \array_values(\array_filter($list, fn ($item) => \in_array($item, $other, false)));

            case Operator::TYPE_ARRAY_DIFF:
                $list = $this->coerceArray($current);
                $other = \array_values($values);

                return \array_values(\array_filter($list, fn ($item) => ! \in_array($item, $other, false)));

            case Operator::TYPE_ARRAY_FILTER:
                $list = $this->coerceArray($current);
                $condition = (string) ($values[0] ?? '');
                $compare = $values[1] ?? null;

                return \array_values(\array_filter($list, fn ($item) => $this->matchesArrayFilter($item, $condition, $compare)));

            case Operator::TYPE_DATE_ADD_DAYS:
                $days = (int) ($values[0] ?? 0);

                return $this->shiftDate($current, $days * 86400);

            case Operator::TYPE_DATE_SUB_DAYS:
                $days = (int) ($values[0] ?? 0);

                return $this->shiftDate($current, -$days * 86400);

            case Operator::TYPE_DATE_SET_NOW:
                return DateTime::now();
        }

        throw new OperatorException("Invalid operator: {$method}");
    }

    /**
     * Clamp an arithmetic result against an optional bound.
     *
     * @param  bool  $isUpper  true = bound is a maximum, false = minimum
     */
    protected function applyNumericLimit(int|float $value, int|float|null $bound, bool $isUpper): int|float
    {
        if ($bound === null) {
            return $value;
        }

        return $isUpper ? \min($value, $bound) : \max($value, $bound);
    }

    /**
     * Preserve int-ness when the original value is an int. Without this,
     * downstream validators reject the column because PHP's arithmetic
     * promoted the result to float — which the Range validator rejects when
     * the attribute type is integer.
     */
    protected function preserveNumericType(int|float $original, int|float $result): int|float
    {
        if (\is_int($original) && \is_float($result) && $result === (float) (int) $result) {
            return (int) $result;
        }

        return $result;
    }

    /**
     * @return array<mixed>
     */
    protected function coerceArray(mixed $value): array
    {
        if (\is_array($value)) {
            return \array_values($value);
        }
        if (\is_string($value) && $value !== '') {
            $decoded = \json_decode($value, true);
            if (\is_array($decoded)) {
                return \array_values($decoded);
            }
        }

        return [];
    }

    /**
     * Mirror Operator::TYPE_ARRAY_FILTER's case-by-case predicate translation
     * (see MariaDB JSON_TABLE filter — `equal`, `greaterThan`, `isNull`, ...).
     */
    protected function matchesArrayFilter(mixed $item, string $condition, mixed $compare): bool
    {
        return match ($condition) {
            Query::TYPE_EQUAL => $item == $compare,
            Query::TYPE_NOT_EQUAL => $item != $compare,
            Query::TYPE_GREATER => \is_numeric($item) && \is_numeric($compare) && $item + 0 > $compare + 0,
            Query::TYPE_GREATER_EQUAL => \is_numeric($item) && \is_numeric($compare) && $item + 0 >= $compare + 0,
            Query::TYPE_LESSER => \is_numeric($item) && \is_numeric($compare) && $item + 0 < $compare + 0,
            Query::TYPE_LESSER_EQUAL => \is_numeric($item) && \is_numeric($compare) && $item + 0 <= $compare + 0,
            Query::TYPE_IS_NULL => $item === null,
            Query::TYPE_IS_NOT_NULL => $item !== null,
            default => true,
        };
    }

    /**
     * Add (or subtract, with negative seconds) seconds to a stored datetime
     * value and return the result in the same string format the Database
     * casting layer expects.
     */
    protected function shiftDate(mixed $current, int $seconds): ?string
    {
        if ($current === null) {
            return null;
        }
        try {
            $base = new \DateTime((string) $current);
        } catch (\Throwable) {
            return $current === '' ? null : (string) $current;
        }
        $base->modify(($seconds >= 0 ? '+' : '').$seconds.' seconds');

        return DateTime::format($base);
    }

    /**
     * Filter out any Operator-typed values from $attrs and apply them against
     * the stored row, returning the remaining (regular) attributes plus the
     * operator-derived assignments. The split mirrors how MariaDB's UPDATE
     * separates operator SQL fragments from bound parameters.
     *
     * @param  array<string, mixed>  $attrs  Incoming attributes (mix of operators and scalars)
     * @param  array<string, mixed>  $row  Stored row (post-filter on rowToDocument)
     * @return array<string, mixed> Regular attributes ready for write
     */
    protected function applyOperators(array $attrs, array $row): array
    {
        $result = [];
        foreach ($attrs as $attribute => $value) {
            if (Operator::isOperator($value)) {
                /** @var Operator $value */
                $current = $row[$this->filter($attribute)] ?? null;
                if (\is_string($current) && $current !== '' && ($current[0] === '[' || $current[0] === '{')) {
                    $decoded = \json_decode($current, true);
                    if (\is_array($decoded)) {
                        $current = $decoded;
                    }
                }
                $result[$attribute] = $this->applyOperator($current, $value);

                continue;
            }
            $result[$attribute] = $value;
        }

        return $result;
    }
}
