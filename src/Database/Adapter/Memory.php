<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * In-process drop-in for the SQL adapters that keeps all data in PHP
 * arrays. Intended for tests, fixtures, and ephemeral workloads.
 *
 * Implements the full adapter surface — schemas, collections, attributes,
 * indexes (including unique and fulltext), CRUD, transactions, query
 * operators (including SEARCH and PCRE regex), permissions, tenancy
 * (shared tables), object/nested attributes, schemaless mode, and
 * relationships (one-to-one, one-to-many, many-to-one, many-to-many).
 *
 * Spatial types and vector search throw a DatabaseException — those
 * features only make sense against a real engine.
 */
class Memory extends Adapter
{
    /**
     * Map of database name to the set of collection storage keys it owns.
     *
     * @var array<string, array<string, string>>
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
     * Inverted permission lookup: collectionKey → documentId → type → set<permissionString>.
     * Maintained alongside `$permissions` to give O(|doc-perms|) deletion on writes.
     *
     * @var array<string, array<string, array<string, array<string, true>>>>
     */
    protected array $permissionsByDocument = [];

    /**
     * Inverted permission lookup: collectionKey → type → tenantBucket → permissionString → set<documentId>.
     * Tenant bucket is the literal tenant value cast to string, or '__null__' for null.
     * Lets `applyPermissions` look up matching document ids per role string in O(roles).
     *
     * @var array<string, array<string, array<string, array<string, array<string, true>>>>>
     */
    protected array $permissionsByPermission = [];

    /**
     * Per-unique-index value→docKey hash table used for O(1) duplicate probes.
     * Structure: collectionKey → indexId → serialized(signature) → docKey.
     *
     * @var array<string, array<string, array<string, string>>>
     */
    protected array $uniqueIndexHashes = [];

    /**
     * Mutation journal stack — one frame per active transaction depth.
     * Each frame is a list of inverse operations (closures) that, when
     * invoked in reverse order, undo the writes performed in that depth.
     *
     * @var array<int, array<int, \Closure>>
     */
    protected array $journals = [];

    /**
     * Process-local cache for `filter()` results — the regex is otherwise
     * re-evaluated on every call inside the find inner loop.
     *
     * @var array<string, string>
     */
    protected array $filterCache = [];

    protected bool $supportForAttributes = true;

    public function __construct()
    {
        // No external resources to initialise
    }

    public function getDriver(): mixed
    {
        return 'memory';
    }

    /**
     * @return array<Capability>
     */
    public function capabilities(): array
    {
        return array_merge(parent::capabilities(), [
            Capability::Schemas,
            Capability::Fulltext,
            Capability::Casting,
            Capability::QueryContains,
            Capability::BatchOperations,
            Capability::BatchCreateAttributes,
            Capability::AttributeResizing,
            Capability::Objects,
            Capability::ObjectIndexes,
            Capability::Operators,
            Capability::OrderRandom,
            Capability::DefinedAttributes,
            Capability::NestedTransactions,
            Capability::PCRE,
            Capability::Regex,
            Capability::BoundaryInclusive,
        ]);
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
                $uid = $candidate['_uid'] ?? '';
                if (
                    \is_string($uid)
                    && \strtolower($uid) === $lower
                    && ($candidate['_tenant'] ?? null) === null
                ) {
                    return [$storageKey, $candidate];
                }
            }
        }

        return null;
    }

    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        // No-op: nothing to time out in-memory
    }

    public function clearTimeout(Event $event = Event::All): void
    {
        // No-op
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
        $this->journals[] = [];
        $this->inTransaction++;

        return true;
    }

    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        $frame = \array_pop($this->journals);
        $this->inTransaction--;

        // The committed frame's inverses must outlive the inner txn — if there
        // is still an outer transaction, splice them onto its journal so an
        // outer rollback still rewinds the inner work.
        if ($frame !== null && $frame !== [] && $this->inTransaction > 0) {
            $outer = \array_pop($this->journals);
            $outer ??= [];
            \array_push($outer, ...$frame);
            $this->journals[] = $outer;
        }

        return true;
    }

    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        $frame = \array_pop($this->journals);
        if ($frame !== null) {
            for ($i = \count($frame) - 1; $i >= 0; $i--) {
                ($frame[$i])();
            }
        }
        $this->inTransaction--;

        return true;
    }

    /**
     * Append an inverse operation to the active transaction frame, if any.
     * Outside a transaction the closure is dropped — non-transactional
     * writes pay zero overhead.
     */
    protected function journal(\Closure $inverse): void
    {
        if ($this->inTransaction === 0) {
            return;
        }
        $this->journals[\count($this->journals) - 1][] = $inverse;
    }

    /**
     * Memoised `Adapter::filter()` — the regex pass is the hottest call in
     * the find inner loop and the inputs (attribute names) are bounded.
     */
    public function filter(string $value): string
    {
        if (isset($this->filterCache[$value])) {
            return $this->filterCache[$value];
        }
        $filtered = parent::filter($value);
        $this->filterCache[$value] = $filtered;

        return $filtered;
    }

    public function create(string $name): bool
    {
        if (! isset($this->databases[$name])) {
            $this->databases[$name] = [];
            $this->journal(function () use ($name): void {
                unset($this->databases[$name]);
            });
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

        return isset($this->databases[$database][$this->filter($collection)]);
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

        $databaseEntry = $this->databases[$name];
        $previousData = [];
        $previousPermissions = [];
        $previousByDocument = [];
        $previousByPermission = [];
        $previousUniqueHashes = [];
        foreach ($databaseEntry as $collectionKey) {
            if (isset($this->data[$collectionKey])) {
                $previousData[$collectionKey] = $this->data[$collectionKey];
            }
            if (isset($this->permissions[$collectionKey])) {
                $previousPermissions[$collectionKey] = $this->permissions[$collectionKey];
            }
            if (isset($this->permissionsByDocument[$collectionKey])) {
                $previousByDocument[$collectionKey] = $this->permissionsByDocument[$collectionKey];
            }
            if (isset($this->permissionsByPermission[$collectionKey])) {
                $previousByPermission[$collectionKey] = $this->permissionsByPermission[$collectionKey];
            }
            if (isset($this->uniqueIndexHashes[$collectionKey])) {
                $previousUniqueHashes[$collectionKey] = $this->uniqueIndexHashes[$collectionKey];
            }
            unset(
                $this->data[$collectionKey],
                $this->permissions[$collectionKey],
                $this->permissionsByDocument[$collectionKey],
                $this->permissionsByPermission[$collectionKey],
                $this->uniqueIndexHashes[$collectionKey],
            );
        }
        unset($this->databases[$name]);

        $this->journal(function () use ($name, $databaseEntry, $previousData, $previousPermissions, $previousByDocument, $previousByPermission, $previousUniqueHashes): void {
            $this->databases[$name] = $databaseEntry;
            foreach ($previousData as $collectionKey => $value) {
                $this->data[$collectionKey] = $value;
            }
            foreach ($previousPermissions as $collectionKey => $value) {
                $this->permissions[$collectionKey] = $value;
            }
            foreach ($previousByDocument as $collectionKey => $value) {
                $this->permissionsByDocument[$collectionKey] = $value;
            }
            foreach ($previousByPermission as $collectionKey => $value) {
                $this->permissionsByPermission[$collectionKey] = $value;
            }
            foreach ($previousUniqueHashes as $collectionKey => $value) {
                $this->uniqueIndexHashes[$collectionKey] = $value;
            }
        });

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
        $databaseSlot = null;
        if ($database !== '') {
            if (! isset($this->databases[$database])) {
                $this->databases[$database] = [];
            }
            $databaseSlot = $this->filter($name);
            $this->databases[$database][$databaseSlot] = $key;
        }

        foreach ($attributes as $attribute) {
            $attrId = $this->filter($attribute->key);
            $this->data[$key]['attributes'][$attrId] = [
                'type' => $attribute->type->value,
                'size' => $attribute->size,
                'signed' => $attribute->signed,
                'array' => $attribute->array,
                'required' => $attribute->required,
            ];
        }

        foreach ($indexes as $index) {
            $indexId = $this->filter($index->key);
            $this->data[$key]['indexes'][$indexId] = [
                'type' => $index->type->value,
                'attributes' => $index->attributes,
                'lengths' => $index->lengths,
                'orders' => $index->orders,
            ];
        }

        $this->journal(function () use ($key, $database, $databaseSlot): void {
            unset(
                $this->data[$key],
                $this->permissions[$key],
                $this->permissionsByDocument[$key],
                $this->permissionsByPermission[$key],
                $this->uniqueIndexHashes[$key],
            );
            if ($databaseSlot !== null) {
                unset($this->databases[$database][$databaseSlot]);
            }
        });

        return true;
    }

    public function deleteCollection(string $id): bool
    {
        $key = $this->key($id);
        $previousData = $this->data[$key] ?? null;
        $previousPermissions = $this->permissions[$key] ?? null;
        $previousByDocument = $this->permissionsByDocument[$key] ?? null;
        $previousByPermission = $this->permissionsByPermission[$key] ?? null;
        $previousUniqueHashes = $this->uniqueIndexHashes[$key] ?? null;

        unset(
            $this->data[$key],
            $this->permissions[$key],
            $this->permissionsByDocument[$key],
            $this->permissionsByPermission[$key],
            $this->uniqueIndexHashes[$key],
        );
        $filtered = $this->filter($id);
        $databaseSlots = [];
        foreach ($this->databases as $name => $collections) {
            if (isset($collections[$filtered]) && $collections[$filtered] === $key) {
                $databaseSlots[$name] = $collections[$filtered];
                unset($this->databases[$name][$filtered]);
            }
        }

        $this->journal(function () use ($key, $previousData, $previousPermissions, $previousByDocument, $previousByPermission, $previousUniqueHashes, $filtered, $databaseSlots): void {
            if ($previousData !== null) {
                $this->data[$key] = $previousData;
            }
            if ($previousPermissions !== null) {
                $this->permissions[$key] = $previousPermissions;
            }
            if ($previousByDocument !== null) {
                $this->permissionsByDocument[$key] = $previousByDocument;
            }
            if ($previousByPermission !== null) {
                $this->permissionsByPermission[$key] = $previousByPermission;
            }
            if ($previousUniqueHashes !== null) {
                $this->uniqueIndexHashes[$key] = $previousUniqueHashes;
            }
            foreach ($databaseSlots as $name => $value) {
                $this->databases[$name][$filtered] = $value;
            }
        });

        return true;
    }

    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $id = $this->filter($attribute->key);
        $previous = $this->data[$key]['attributes'][$id] ?? null;
        $this->data[$key]['attributes'][$id] = [
            'type' => $attribute->type->value,
            'size' => $attribute->size,
            'signed' => $attribute->signed,
            'array' => $attribute->array,
            'required' => $attribute->required,
        ];

        $this->journal(function () use ($key, $id, $previous): void {
            if ($previous === null) {
                unset($this->data[$key]['attributes'][$id]);
            } else {
                $this->data[$key]['attributes'][$id] = $previous;
            }
        });

        return true;
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        foreach ($attributes as $attribute) {
            $this->createAttribute($collection, $attribute);
        }

        return true;
    }

    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $id = $this->filter($attribute->key);
        if (! empty($newKey) && $newKey !== $id) {
            $this->renameAttribute($collection, $id, $newKey);
            $id = $this->filter($newKey);
        }

        $previous = $this->data[$key]['attributes'][$id] ?? null;
        $this->data[$key]['attributes'][$id] = [
            'type' => $attribute->type->value,
            'size' => $attribute->size,
            'signed' => $attribute->signed,
            'array' => $attribute->array,
            'required' => $attribute->required,
        ];

        $this->journal(function () use ($key, $id, $previous): void {
            if ($previous === null) {
                unset($this->data[$key]['attributes'][$id]);
            } else {
                $this->data[$key]['attributes'][$id] = $previous;
            }
        });

        return true;
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return true;
        }

        $id = $this->filter($id);
        $previousAttribute = $this->data[$key]['attributes'][$id] ?? null;
        if ($previousAttribute === null) {
            // Nothing to do; attribute was never registered.
            return true;
        }

        $previousValues = [];
        unset($this->data[$key]['attributes'][$id]);
        foreach ($this->data[$key]['documents'] as $storageKey => &$document) {
            if (\array_key_exists($id, $document)) {
                $previousValues[$storageKey] = $document[$id];
                unset($document[$id]);
            }
        }
        unset($document);

        $previousIndexes = [];
        $previousUniqueHashes = [];
        foreach ($this->data[$key]['indexes'] as $indexId => $index) {
            $attributes = \is_array($index['attributes'] ?? null) ? $index['attributes'] : [];
            $indexLengths = \is_array($index['lengths'] ?? null) ? $index['lengths'] : [];
            $indexOrders = \is_array($index['orders'] ?? null) ? $index['orders'] : [];
            $filtered = [];
            $lengths = [];
            $orders = [];
            $touched = false;
            foreach ($attributes as $i => $attribute) {
                if (! \is_string($attribute)) {
                    continue;
                }
                if ($this->filter($attribute) === $id) {
                    $touched = true;

                    continue;
                }
                $filtered[] = $attribute;
                if (isset($indexLengths[$i])) {
                    $lengths[] = $indexLengths[$i];
                }
                if (isset($indexOrders[$i])) {
                    $orders[] = $indexOrders[$i];
                }
            }
            if ($touched) {
                $previousIndexes[$indexId] = $index;
                if (($index['type'] ?? '') === IndexType::Unique->value
                    && isset($this->uniqueIndexHashes[$key][$indexId])) {
                    $previousUniqueHashes[$indexId] = $this->uniqueIndexHashes[$key][$indexId];
                    unset($this->uniqueIndexHashes[$key][$indexId]);
                }
            }
            $index['attributes'] = $filtered;
            $index['lengths'] = $lengths;
            $index['orders'] = $orders;
            $this->data[$key]['indexes'][$indexId] = $index;
        }

        $this->journal(function () use ($key, $id, $previousAttribute, $previousValues, $previousIndexes, $previousUniqueHashes): void {
            if (! isset($this->data[$key])) {
                return;
            }
            $this->data[$key]['attributes'][$id] = $previousAttribute;
            foreach ($previousValues as $storageKey => $value) {
                if (isset($this->data[$key]['documents'][$storageKey])) {
                    $this->data[$key]['documents'][$storageKey][$id] = $value;
                }
            }
            foreach ($previousIndexes as $indexId => $previousIndex) {
                $this->data[$key]['indexes'][$indexId] = $previousIndex;
            }
            foreach ($previousUniqueHashes as $indexId => $hashes) {
                $this->uniqueIndexHashes[$key][$indexId] = $hashes;
            }
        });

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

        $touchedDocs = [];
        foreach ($this->data[$key]['documents'] as $storageKey => &$document) {
            if (\array_key_exists($old, $document)) {
                $document[$new] = $document[$old];
                unset($document[$old]);
                $touchedDocs[] = $storageKey;
            }
        }
        unset($document);

        $touchedIndexes = [];
        foreach ($this->data[$key]['indexes'] as $indexId => &$index) {
            $attributes = \is_array($index['attributes'] ?? null) ? $index['attributes'] : [];
            $changed = false;
            foreach ($attributes as $i => $attribute) {
                if (\is_string($attribute) && $this->filter($attribute) === $old) {
                    $attributes[$i] = $new;
                    $changed = true;
                }
            }
            if ($changed) {
                $touchedIndexes[] = $indexId;
                $index['attributes'] = $attributes;
            }
        }
        unset($index);

        $this->journal(function () use ($key, $old, $new, $touchedDocs, $touchedIndexes): void {
            if (! isset($this->data[$key])) {
                return;
            }
            $entry = &$this->data[$key];
            $entry['attributes'][$old] = $entry['attributes'][$new];
            unset($entry['attributes'][$new]);
            foreach ($touchedDocs as $storageKey) {
                if (! isset($entry['documents'][$storageKey])) {
                    continue;
                }
                $document = &$entry['documents'][$storageKey];
                $document[$old] = $document[$new];
                unset($document[$new]);
                unset($document);
            }
            foreach ($touchedIndexes as $indexId) {
                $attributes = \is_array($entry['indexes'][$indexId]['attributes'] ?? null)
                    ? $entry['indexes'][$indexId]['attributes']
                    : [];
                foreach ($attributes as $i => $attribute) {
                    if (\is_string($attribute) && $this->filter($attribute) === $new) {
                        $attributes[$i] = $old;
                    }
                }
                $entry['indexes'][$indexId]['attributes'] = $attributes;
            }
            unset($entry);
        });

        return true;
    }

    public function createRelationship(Relationship $relationship): bool
    {
        // Memory stores documents as flexible maps, so the relationship "column"
        // is registered on the attribute list rather than added as a physical
        // schema column. The registration ensures that reads always surface
        // the relationship key (as null when unpopulated) — matching MariaDB,
        // which selects the column even when no rows have a value.
        // The M2M junction collection itself is created by the wrapper through
        // the standard createCollection path.
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $id = $relationship->key;
        $twoWayKey = $relationship->twoWayKey;
        $twoWay = $relationship->twoWay;

        switch ($relationship->type) {
            case RelationType::OneToOne:
                $this->registerRelationshipField($collection, $id);
                if ($twoWay) {
                    $this->registerRelationshipField($relatedCollection, $twoWayKey);
                }
                break;
            case RelationType::OneToMany:
                $this->registerRelationshipField($relatedCollection, $twoWayKey);
                break;
            case RelationType::ManyToOne:
                $this->registerRelationshipField($collection, $id);
                break;
            case RelationType::ManyToMany:
                // Junction columns live on the junction collection, which is
                // created with explicit attributes by the wrapper.
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $key = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $newKey = $newKey !== null ? $this->filter($newKey) : null;
        $newTwoWayKey = $newTwoWayKey !== null ? $this->filter($newTwoWayKey) : null;
        $side = $relationship->side;
        $twoWay = $relationship->twoWay;

        switch ($relationship->type) {
            case RelationType::OneToOne:
                if ($newKey !== null && $newKey !== $key) {
                    $this->renameDocumentField($collection, $key, $newKey);
                }
                if ($twoWay && $newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                    $this->renameDocumentField($relatedCollection, $twoWayKey, $newTwoWayKey);
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameDocumentField($relatedCollection, $twoWayKey, $newTwoWayKey);
                    }
                } else {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameDocumentField($collection, $key, $newKey);
                    }
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameDocumentField($relatedCollection, $twoWayKey, $newTwoWayKey);
                    }
                } else {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameDocumentField($collection, $key, $newKey);
                    }
                }
                break;
            case RelationType::ManyToMany:
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

    public function deleteRelationship(Relationship $relationship): bool
    {
        $collection = $relationship->collection;
        $relatedCollection = $relationship->relatedCollection;
        $key = $this->filter($relationship->key);
        $twoWayKey = $this->filter($relationship->twoWayKey);
        $twoWay = $relationship->twoWay;
        $side = $relationship->side;

        switch ($relationship->type) {
            case RelationType::OneToOne:
                if ($side === RelationSide::Parent) {
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
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    $this->dropDocumentField($relatedCollection, $twoWayKey);
                } else {
                    $this->dropDocumentField($collection, $key);
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Parent) {
                    $this->dropDocumentField($collection, $key);
                } else {
                    $this->dropDocumentField($relatedCollection, $twoWayKey);
                }
                break;
            case RelationType::ManyToMany:
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
        $previous = $this->data[$key]['attributes'][$field] ?? null;
        $this->data[$key]['attributes'][$field] = [
            'type' => ColumnType::Relationship->value,
            'size' => 0,
            'signed' => true,
            'array' => false,
            'required' => false,
        ];
        $this->journal(function () use ($key, $field, $previous): void {
            if ($previous === null) {
                unset($this->data[$key]['attributes'][$field]);
            } else {
                $this->data[$key]['attributes'][$field] = $previous;
            }
        });
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
        $field = $this->filter($field);
        $previous = $this->data[$key]['attributes'][$field] ?? null;
        unset($this->data[$key]['attributes'][$field]);
        if ($previous !== null) {
            $this->journal(function () use ($key, $field, $previous): void {
                $this->data[$key]['attributes'][$field] = $previous;
            });
        }
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
        $hadAttribute = isset($this->data[$key]['attributes'][$oldKey]);
        if ($hadAttribute) {
            $this->data[$key]['attributes'][$newKey] = $this->data[$key]['attributes'][$oldKey];
            unset($this->data[$key]['attributes'][$oldKey]);
        }
        $touched = [];
        foreach ($this->data[$key]['documents'] as $storageKey => $document) {
            if (! \array_key_exists($oldKey, $document)) {
                continue;
            }
            $document[$newKey] = $document[$oldKey];
            unset($document[$oldKey]);
            $this->data[$key]['documents'][$storageKey] = $document;
            $touched[] = $storageKey;
        }
        $this->journal(function () use ($key, $oldKey, $newKey, $hadAttribute, $touched): void {
            if ($hadAttribute) {
                $this->data[$key]['attributes'][$oldKey] = $this->data[$key]['attributes'][$newKey];
                unset($this->data[$key]['attributes'][$newKey]);
            }
            foreach ($touched as $storageKey) {
                if (! isset($this->data[$key]['documents'][$storageKey])) {
                    continue;
                }
                $document = $this->data[$key]['documents'][$storageKey];
                $document[$oldKey] = $document[$newKey];
                unset($document[$newKey]);
                $this->data[$key]['documents'][$storageKey] = $document;
            }
        });
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
        $previousAttribute = $this->data[$key]['attributes'][$field] ?? null;
        unset($this->data[$key]['attributes'][$field]);
        $previousValues = [];
        foreach ($this->data[$key]['documents'] as $storageKey => $document) {
            if (\array_key_exists($field, $document)) {
                $previousValues[$storageKey] = $document[$field];
                unset($document[$field]);
                $this->data[$key]['documents'][$storageKey] = $document;
            }
        }
        $this->journal(function () use ($key, $field, $previousAttribute, $previousValues): void {
            if ($previousAttribute !== null) {
                $this->data[$key]['attributes'][$field] = $previousAttribute;
            }
            foreach ($previousValues as $storageKey => $value) {
                if (! isset($this->data[$key]['documents'][$storageKey])) {
                    continue;
                }
                $this->data[$key]['documents'][$storageKey][$field] = $value;
            }
        });
    }

    /**
     * Resolve the junction collection name for a many-to-many relationship.
     * Mirrors Database::getJunctionCollection — the junction is named after
     * the parent/child sequence pair.
     */
    protected function resolveJunctionCollection(string $collection, string $relatedCollection, RelationSide $side): ?string
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
        if (! \is_scalar($collectionSequence) || ! \is_scalar($relatedSequence)) {
            return null;
        }

        return $side === RelationSide::Parent
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

        $movedHash = false;
        if (isset($this->uniqueIndexHashes[$key][$old])) {
            $this->uniqueIndexHashes[$key][$new] = $this->uniqueIndexHashes[$key][$old];
            unset($this->uniqueIndexHashes[$key][$old]);
            $movedHash = true;
        }

        $this->journal(function () use ($key, $old, $new, $movedHash): void {
            $this->data[$key]['indexes'][$old] = $this->data[$key]['indexes'][$new];
            unset($this->data[$key]['indexes'][$new]);
            if ($movedHash) {
                $this->uniqueIndexHashes[$key][$old] = $this->uniqueIndexHashes[$key][$new];
                unset($this->uniqueIndexHashes[$key][$new]);
            }
        });

        return true;
    }

    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $id = $index->key;
        $type = $index->type->value;
        $attributes = $index->attributes;
        $lengths = $index->lengths;
        $orders = $index->orders;

        $hashTable = [];
        if ($type === IndexType::Unique->value && ! empty($attributes)) {
            // MariaDB rejects CREATE UNIQUE INDEX with errno 1062 when existing
            // rows contain duplicates; Database::createIndex catches the resulting
            // DuplicateException and treats it as an "orphan index" (the metadata
            // is registered but the physical index is absent). Mirror that contract:
            // throw DuplicateException so callers see identical end-state behavior.
            // Build the hash table while we scan so we can reuse it for fast
            // probes after the index lands — no second pass over the rows.
            foreach ($this->data[$key]['documents'] as $docKey => $row) {
                $signature = [];
                foreach ($attributes as $attribute) {
                    $signature[] = $this->normalizeIndexValue(
                        $this->resolveAttributeValue($row, $attribute)
                    );
                }
                if (\in_array(null, $signature, true)) {
                    continue;
                }
                if ($this->sharedTables) {
                    \array_unshift($signature, $row['_tenant'] ?? null);
                }
                $hash = \serialize($signature);
                if (isset($hashTable[$hash])) {
                    throw new DuplicateException('Cannot create unique index: existing rows already contain duplicate values');
                }
                $hashTable[$hash] = $docKey;
            }
        }

        $id = $this->filter($id);
        $this->data[$key]['indexes'][$id] = [
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders,
        ];
        if ($type === IndexType::Unique->value && ! empty($attributes)) {
            $this->uniqueIndexHashes[$key][$id] = $hashTable;
        }

        $this->journal(function () use ($key, $id, $type): void {
            unset($this->data[$key]['indexes'][$id]);
            if ($type === IndexType::Unique->value) {
                unset($this->uniqueIndexHashes[$key][$id]);
            }
        });

        return true;
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        $key = $this->key($collection);
        if (! isset($this->data[$key])) {
            return true;
        }

        $id = $this->filter($id);
        $previousIndex = $this->data[$key]['indexes'][$id] ?? null;
        $previousHash = $this->uniqueIndexHashes[$key][$id] ?? null;
        unset(
            $this->data[$key]['indexes'][$id],
            $this->uniqueIndexHashes[$key][$id],
        );

        $this->journal(function () use ($key, $id, $previousIndex, $previousHash): void {
            if ($previousIndex !== null) {
                $this->data[$key]['indexes'][$id] = $previousIndex;
            }
            if ($previousHash !== null) {
                $this->uniqueIndexHashes[$key][$id] = $previousHash;
            }
        });

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
            if ($query->getMethod() === Method::Select) {
                foreach ($query->getValues() as $value) {
                    if (\is_string($value)) {
                        $selected[] = $value;
                    }
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
                $existingId = $existing['_id'] ?? '';
                $document['$sequence'] = \is_scalar($existingId) ? (string) $existingId : '';

                return $document;
            }
            throw new DuplicateException('Document already exists');
        }

        $signatures = $this->documentUniqueSignatures($key, $document);
        try {
            $this->checkUniqueSignatures($key, $signatures, $docKey);
        } catch (DuplicateException $e) {
            if ($this->skipDuplicates) {
                return $document;
            }
            throw $e;
        }

        $entry = &$this->data[$key];
        $sequenceBefore = $entry['sequence'];
        $sequence = $document->getSequence();
        if (empty($sequence)) {
            $entry['sequence']++;
            $sequence = $entry['sequence'];
        } else {
            $sequence = (int) $sequence;
            if ($sequence > $entry['sequence']) {
                $entry['sequence'] = $sequence;
            }
        }

        $row = $this->documentToRow($document);
        $row['_id'] = $sequence;

        $entry['documents'][$docKey] = $row;
        unset($entry);
        $this->journal(function () use ($key, $docKey, $sequenceBefore): void {
            unset($this->data[$key]['documents'][$docKey]);
            $this->data[$key]['sequence'] = $sequenceBefore;
        });

        foreach ($signatures as $indexId => $hash) {
            $this->probeUniqueHash($key, $indexId, $hash, null, $docKey);
        }

        $this->writePermissions($key, $document);

        $document['$sequence'] = (string) $sequence;

        return $document;
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        // Mirror SQL's batch-level sequence consistency check: every document
        // in a batch must either set $sequence or omit it. SQL adapters reject
        // mixed batches up front; Memory must match so application code that
        // catches the resulting DatabaseException behaves the same.
        $hasSequence = null;
        foreach ($documents as $document) {
            $sequenceSet = ! empty($document->getSequence());
            if ($hasSequence === null) {
                $hasSequence = $sequenceSet;
            } elseif ($hasSequence !== $sequenceSet) {
                throw new DatabaseException('All documents must have an sequence if one is set');
            }
        }

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

        $update = $this->documentToRow($document);

        // Sparse update — MariaDB's UPDATE only sets columns present in the
        // document; absent columns retain their previous values. The wrapper
        // relies on this for relationship updates, where it removes
        // unchanged relationship keys before calling the adapter.
        $row = \array_merge($existing, $update);

        // Compute new signatures against the merged row so attributes the
        // sparse update did not touch still contribute to the unique-index
        // signature.
        $newSignatures = $this->rowUniqueSignatures($key, $row);
        $oldSignatures = $this->rowUniqueSignatures($key, $existing);
        $this->checkUniqueSignatures($key, $newSignatures, $oldKey);

        $row['_id'] = $existing['_id'];
        if ($this->sharedTables && \array_key_exists('_tenant', $existing)) {
            // Preserve the row's stored tenant — MariaDB's UPDATE statements
            // never rewrite `_tenant` and tests rely on the original tenant
            // (e.g. the metadata NULL-tenant rows) surviving an update.
            $row['_tenant'] = $existing['_tenant'];
        }

        $tenantValue = $existing['_tenant'] ?? $this->getTenant();
        $newKey = $this->sharedTables
            ? (\is_scalar($tenantValue) ? (string) $tenantValue : '').'|'.\strtolower($newId)
            : \strtolower($newId);

        $entry = &$this->data[$key];
        $oldKeyHadRow = isset($entry['documents'][$oldKey]);
        $previousAtNewKey = $entry['documents'][$newKey] ?? null;

        if ($newId !== $id || $newKey !== $oldKey) {
            unset($entry['documents'][$oldKey]);
        }
        $entry['documents'][$newKey] = $row;
        unset($entry);

        $this->journal(function () use ($key, $oldKey, $newKey, $existing, $oldKeyHadRow, $previousAtNewKey): void {
            if (! isset($this->data[$key])) {
                return;
            }
            $entry = &$this->data[$key];
            if ($oldKey !== $newKey) {
                if ($previousAtNewKey === null) {
                    unset($entry['documents'][$newKey]);
                } else {
                    $entry['documents'][$newKey] = $previousAtNewKey;
                }
                if ($oldKeyHadRow) {
                    $entry['documents'][$oldKey] = $existing;
                }
            } else {
                $entry['documents'][$oldKey] = $existing;
            }
        });

        // Sync unique-index hashes — for indexes the row was bound to
        // pre-update, drop the old binding; for indexes the row joins
        // post-update, register the new binding.
        $allIndexes = \array_unique([...\array_keys($oldSignatures), ...\array_keys($newSignatures)]);
        foreach ($allIndexes as $indexId) {
            $this->probeUniqueHash(
                $key,
                $indexId,
                $newSignatures[$indexId] ?? null,
                $oldSignatures[$indexId] ?? null,
                $newKey,
            );
            // Old key removal: if the docKey changed, also drop any binding
            // pointing at the old key (the probeUniqueHash above keys against
            // $newKey, so a stale binding under $oldKey is left untouched).
            if ($oldKey !== $newKey) {
                $oldHash = $oldSignatures[$indexId] ?? null;
                if ($oldHash !== null
                    && ($this->uniqueIndexHashes[$key][$indexId][$oldHash] ?? null) === $oldKey) {
                    unset($this->uniqueIndexHashes[$key][$indexId][$oldHash]);
                    $this->journal(function () use ($key, $indexId, $oldHash, $oldKey): void {
                        $this->uniqueIndexHashes[$key][$indexId][$oldHash] = $oldKey;
                    });
                }
            }
        }

        if (! $skipPermissions) {
            // Remove any permissions keyed to the old uid (within the
            // current tenant only — other tenants may legitimately hold a
            // permission for the same $id under shared tables) and rewrite.
            $tenant = $this->getTenant();
            $this->removePermissionsForDocument($key, $id, $tenant, $this->sharedTables);
            if ($newId !== $id) {
                $this->removePermissionsForDocument($key, $newId, $tenant, $this->sharedTables);
            }
            $this->writePermissions($key, $document);
        } elseif ($newId !== $id) {
            // Rename-only path: rebind every permission entry whose document
            // is $id to $newId — preserving the original tenant on each entry
            // so shared-tables siblings stay correctly tagged.
            $existingEntries = [];
            foreach ($this->permissions[$key] ?? [] as $entry) {
                if ($entry['document'] === $id) {
                    $existingEntries[] = $entry;
                }
            }
            $this->removePermissionsForDocument($key, $id, null, false);
            foreach ($existingEntries as $entry) {
                $this->addPermissionEntry(
                    $key,
                    $newId,
                    (string) $entry['type'],
                    (string) $entry['permission'],
                    $entry['tenant'] ?? null,
                );
            }
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

        // Two-phase: validate every row (including unique-index checks)
        // before any write lands, so a uniqueness violation on document N
        // does not leave documents 0..N-1 partially committed.
        $prepared = [];
        foreach ($documents as $doc) {
            $uid = $doc->getId();
            $docKey = $this->documentKey($uid);
            if (! isset($this->data[$key]['documents'][$docKey])) {
                continue;
            }

            $existingRow = $this->data[$key]['documents'][$docKey];

            // Resolve operators per-row — each document's existing values feed
            // back into operator evaluation, so $attrs cannot be evaluated
            // once and reused.
            $resolvedAttrs = $this->applyOperators($attrs, $existingRow);

            $merged = ! empty($resolvedAttrs)
                ? new Document(\array_merge(
                    $this->rowToDocument($existingRow),
                    $resolvedAttrs,
                    ['$id' => $uid]
                ))
                : null;

            $newSignatures = $merged !== null ? $this->documentUniqueSignatures($key, $merged) : [];
            $oldSignatures = $this->rowUniqueSignatures($key, $existingRow);

            $prepared[] = [
                'uid' => $uid,
                'docKey' => $docKey,
                'attrs' => $resolvedAttrs,
                'newSignatures' => $newSignatures,
                'oldSignatures' => $oldSignatures,
            ];
        }

        // Phase-1: hashed unique-index validation. For each pending row, probe
        // the hash table — but recognise that any hashed entry currently
        // pointing at the row's own docKey is about to be replaced by the new
        // signature, so it is not a conflict. We also build a per-batch
        // pending map so two siblings collapsing to the same value collide.
        $pendingByIndex = [];
        foreach ($prepared as $entry) {
            $docKey = $entry['docKey'];
            foreach ($entry['newSignatures'] as $indexId => $hash) {
                $existing = $this->uniqueIndexHashes[$key][$indexId][$hash] ?? null;
                if ($existing !== null && $existing !== $docKey) {
                    // The entry's own pre-update hash binding does not count
                    // as a conflict — it'll be removed when its row is rewritten.
                    $existingIsSelf = false;
                    foreach ($prepared as $other) {
                        if ($other['docKey'] === $existing && ($other['oldSignatures'][$indexId] ?? null) === $hash) {
                            $existingIsSelf = true;
                            break;
                        }
                    }
                    if (! $existingIsSelf) {
                        throw new DuplicateException('Document with the requested unique attributes already exists');
                    }
                }
                if (isset($pendingByIndex[$indexId][$hash]) && $pendingByIndex[$indexId][$hash] !== $docKey) {
                    throw new DuplicateException('Document with the requested unique attributes already exists');
                }
                $pendingByIndex[$indexId][$hash] = $docKey;
            }
        }

        $tenant = $this->getTenant();
        foreach ($prepared as $entry) {
            $uid = $entry['uid'];
            $docKey = $entry['docKey'];
            $resolvedAttrs = $entry['attrs'];

            $previousRow = $this->data[$key]['documents'][$docKey];

            $row = &$this->data[$key]['documents'][$docKey];
            foreach ($resolvedAttrs as $attribute => $value) {
                $row[$this->filter($attribute)] = $value;
            }

            if ($hasCreatedAt) {
                $row['_createdAt'] = $updates->getCreatedAt();
            }
            if ($hasUpdatedAt) {
                $row['_updatedAt'] = $updates->getUpdatedAt();
            }
            if ($hasPermissions) {
                $row['_permissions'] = $updates->getPermissions();
            }
            unset($row);

            $this->journal(function () use ($key, $docKey, $previousRow): void {
                $this->data[$key]['documents'][$docKey] = $previousRow;
            });

            if ($hasPermissions) {
                $this->removePermissionsForDocument($key, $uid, $tenant, $this->sharedTables);
                foreach ([PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete] as $type) {
                    foreach ($updates->getPermissionsByType($type) as $permission) {
                        $this->addPermissionEntry($key, $uid, $type->value, (string) $permission, $tenant);
                    }
                }
            }

            // Sync unique-index hashes per-row.
            $allIndexes = \array_unique([...\array_keys($entry['oldSignatures']), ...\array_keys($entry['newSignatures'])]);
            foreach ($allIndexes as $indexId) {
                $this->probeUniqueHash(
                    $key,
                    $indexId,
                    $entry['newSignatures'][$indexId] ?? null,
                    $entry['oldSignatures'][$indexId] ?? null,
                    $docKey,
                );
            }
        }

        return \count($prepared);
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
                $existingId = $existing['_id'] ?? '';
                $documents[$index]->setAttribute('$sequence', \is_scalar($existingId) ? (string) $existingId : '');
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

        $existing = $this->data[$key]['documents'][$docKey];
        $oldSignatures = $this->rowUniqueSignatures($key, $existing);

        unset($this->data[$key]['documents'][$docKey]);
        $this->journal(function () use ($key, $docKey, $existing): void {
            $this->data[$key]['documents'][$docKey] = $existing;
        });

        // Drop unique-index hash bindings for this row.
        foreach ($oldSignatures as $indexId => $hash) {
            if (($this->uniqueIndexHashes[$key][$indexId][$hash] ?? null) === $docKey) {
                unset($this->uniqueIndexHashes[$key][$indexId][$hash]);
                $this->journal(function () use ($key, $indexId, $hash, $docKey): void {
                    $this->uniqueIndexHashes[$key][$indexId][$hash] = $docKey;
                });
            }
        }

        $tenant = $this->getTenant();
        $this->removePermissionsForDocument($key, $id, $tenant, $this->sharedTables);

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
        foreach ($this->data[$key]['documents'] as $docKey => $row) {
            // With sharedTables the row map is keyed by "tenant|uid" so sequence
            // collisions across tenants are possible. Skip rows that don't belong
            // to the current tenant so we never delete another tenant's data.
            if ($this->sharedTables && ($row['_tenant'] ?? null) !== $this->getTenant()) {
                continue;
            }
            $rowId = $row['_id'] ?? '';
            $rowUid = $row['_uid'] ?? $docKey;
            if (isset($seqSet[\is_scalar($rowId) ? (string) $rowId : ''])) {
                $deletedIds[\is_scalar($rowUid) ? (string) $rowUid : $docKey] = true;
                $oldSignatures = $this->rowUniqueSignatures($key, $row);
                unset($this->data[$key]['documents'][$docKey]);
                $this->journal(function () use ($key, $docKey, $row): void {
                    $this->data[$key]['documents'][$docKey] = $row;
                });
                foreach ($oldSignatures as $indexId => $hash) {
                    if (($this->uniqueIndexHashes[$key][$indexId][$hash] ?? null) === $docKey) {
                        unset($this->uniqueIndexHashes[$key][$indexId][$hash]);
                        $this->journal(function () use ($key, $indexId, $hash, $docKey): void {
                            $this->uniqueIndexHashes[$key][$indexId][$hash] = $docKey;
                        });
                    }
                }
                $count++;
            }
        }

        $permSet = ! empty($permissionIds)
            ? \array_flip(\array_map('strval', $permissionIds))
            : [];

        if (! empty($deletedIds) || ! empty($permSet)) {
            $tenant = $this->getTenant();
            foreach (\array_keys($deletedIds) as $documentId) {
                $this->removePermissionsForDocument($key, (string) $documentId, $tenant, $this->sharedTables);
            }
            foreach (\array_keys($permSet) as $documentId) {
                if (isset($deletedIds[$documentId])) {
                    continue;
                }
                $this->removePermissionsForDocument($key, (string) $documentId, $tenant, $this->sharedTables);
            }
        }

        return $count;
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        $key = $this->key($collection->getId());
        if (! isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = $this->fusedFilter($key, $collection->getId(), $queries, $forPermission->value);
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

        if ($cursorDirection === CursorDirection::Before) {
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

        $rows = $this->fusedFilter($key, $collection->getId(), $queries, PermissionType::Read->value);

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

        $rows = $this->fusedFilter($key, $collection->getId(), $queries, PermissionType::Read->value);

        if (! is_null($max)) {
            $rows = \array_slice($rows, 0, $max);
        }

        $sum = 0;
        $isFloat = false;
        $column = $this->filter($attribute);
        foreach ($rows as $row) {
            $value = $row[$column] ?? null;
            if ($value === null || ! \is_numeric($value)) {
                continue;
            }
            if (\is_float($value)) {
                $isFloat = true;
            }
            $sum += $value;
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
        $previousValue = $this->data[$key]['documents'][$docKey][$column] ?? null;
        $previousUpdatedAt = $this->data[$key]['documents'][$docKey]['_updatedAt'] ?? null;
        $current = $previousValue ?? 0;
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

        $this->journal(function () use ($key, $docKey, $column, $previousValue, $previousUpdatedAt): void {
            if (! isset($this->data[$key]['documents'][$docKey])) {
                return;
            }
            $row = &$this->data[$key]['documents'][$docKey];
            if ($previousValue === null) {
                unset($row[$column]);
            } else {
                $row[$column] = $previousValue;
            }
            if ($previousUpdatedAt === null) {
                unset($row['_updatedAt']);
            } else {
                $row['_updatedAt'] = $previousUpdatedAt;
            }
            unset($row);
        });

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
        return ColumnType::Integer->value;
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

    public function getSupportForUpsertOnUniqueIndex(): bool
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
        $attributes = $collection->getAttribute('attributes', []);

        return (\is_array($attributes) ? \count($attributes) : 0) + $this->getCountOfDefaultAttributes();
    }

    public function getCountOfIndexes(Document $collection): int
    {
        $indexes = $collection->getAttribute('indexes', []);

        return (\is_array($indexes) ? \count($indexes) : 0) + $this->getCountOfDefaultIndexes();
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
     * @return array<string, mixed>
     */
    protected function documentToRow(Document $document): array
    {
        $row = [];
        foreach ($document->getAttributes() as $attribute => $value) {
            // Store native PHP values directly — no JSON encoding for arrays.
            // The Database casting layer accepts already-decoded arrays
            // (decodeArrayValue / decodeObjectValue both pass through arrays).
            $row[$this->filter($attribute)] = $value;
        }

        $row['_uid'] = $document->getId();
        $row['_createdAt'] = $document->getCreatedAt();
        $row['_updatedAt'] = $document->getUpdatedAt();
        $row['_permissions'] = $document->getPermissions();
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
                    $document['$sequence'] = \is_scalar($value) ? (string) $value : '';
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
                    $document['$permissions'] = $value ?? [];
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
                if (($definition['type'] ?? null) !== ColumnType::Relationship->value) {
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
            if ($query->getMethod() === Method::Select) {
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
        foreach ([PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete] as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $this->addPermissionEntry($key, $uid, $type->value, (string) $permission, $tenant);
            }
        }
    }

    /**
     * Append a single permission entry to all three storage shapes (flat list,
     * by-document index, by-permission index) and journal the inverse.
     */
    protected function addPermissionEntry(string $key, string $document, string $type, string $permission, int|string|null $tenant): void
    {
        $clean = \str_replace('"', '', $permission);
        $entry = [
            'document' => $document,
            'type' => $type,
            'permission' => $clean,
            'tenant' => $tenant,
        ];
        $this->permissions[$key][] = $entry;
        $this->permissionsByDocument[$key][$document][$type][$clean] = true;
        $bucket = $tenant === null ? '__null__' : (string) $tenant;
        $this->permissionsByPermission[$key][$type][$bucket][$clean][$document] = true;

        $flatIndex = \array_key_last($this->permissions[$key]);
        $this->journal(function () use ($key, $flatIndex, $document, $type, $clean, $bucket): void {
            unset($this->permissions[$key][$flatIndex]);
            unset($this->permissionsByDocument[$key][$document][$type][$clean]);
            if (empty($this->permissionsByDocument[$key][$document][$type])) {
                unset($this->permissionsByDocument[$key][$document][$type]);
                if (empty($this->permissionsByDocument[$key][$document])) {
                    unset($this->permissionsByDocument[$key][$document]);
                }
            }
            unset($this->permissionsByPermission[$key][$type][$bucket][$clean][$document]);
            if (empty($this->permissionsByPermission[$key][$type][$bucket][$clean])) {
                unset($this->permissionsByPermission[$key][$type][$bucket][$clean]);
                if (empty($this->permissionsByPermission[$key][$type][$bucket])) {
                    unset($this->permissionsByPermission[$key][$type][$bucket]);
                }
            }
        });
    }

    /**
     * Drop every permission entry for $documentId, optionally restricted to
     * the supplied tenant under shared tables. Returns the removed entries
     * (with their flat-list keys) so callers can replay them on rollback.
     *
     * @return array<int, array{document: string, type: string, permission: string, tenant: int|string|null}>
     */
    protected function removePermissionsForDocument(string $key, string $documentId, int|string|null $tenantScope, bool $sharedTablesScope): array
    {
        $byType = $this->permissionsByDocument[$key][$documentId] ?? null;
        if ($byType === null) {
            return [];
        }

        $removed = [];
        foreach ($byType as $type => $set) {
            foreach (\array_keys($set) as $permission) {
                $removed[] = ['document' => $documentId, 'type' => (string) $type, 'permission' => (string) $permission];
            }
        }

        // Walk the flat list once, dropping matching entries while respecting
        // the tenant scope. We collect the original flat-list keys because
        // rollback restores entries at their original numeric positions.
        $tenantValue = $tenantScope;
        $flat = $this->permissions[$key] ?? [];
        $journalEntries = [];
        foreach ($flat as $index => $entry) {
            if ($entry['document'] !== $documentId) {
                continue;
            }
            if ($sharedTablesScope && ($entry['tenant'] ?? null) !== $tenantValue) {
                continue;
            }
            $journalEntries[$index] = $entry;
            unset($this->permissions[$key][$index]);
            $bucket = $entry['tenant'] === null ? '__null__' : (string) $entry['tenant'];
            unset($this->permissionsByPermission[$key][$entry['type']][$bucket][$entry['permission']][$documentId]);
            if (empty($this->permissionsByPermission[$key][$entry['type']][$bucket][$entry['permission']])) {
                unset($this->permissionsByPermission[$key][$entry['type']][$bucket][$entry['permission']]);
                if (empty($this->permissionsByPermission[$key][$entry['type']][$bucket])) {
                    unset($this->permissionsByPermission[$key][$entry['type']][$bucket]);
                }
            }
            unset($this->permissionsByDocument[$key][$documentId][$entry['type']][$entry['permission']]);
            if (empty($this->permissionsByDocument[$key][$documentId][$entry['type']])) {
                unset($this->permissionsByDocument[$key][$documentId][$entry['type']]);
            }
        }
        if (empty($this->permissionsByDocument[$key][$documentId] ?? [])) {
            unset($this->permissionsByDocument[$key][$documentId]);
        }

        $this->journal(function () use ($key, $journalEntries): void {
            foreach ($journalEntries as $index => $entry) {
                $this->permissions[$key][$index] = $entry;
                $this->permissionsByDocument[$key][$entry['document']][$entry['type']][$entry['permission']] = true;
                $bucket = $entry['tenant'] === null ? '__null__' : (string) $entry['tenant'];
                $this->permissionsByPermission[$key][$entry['type']][$bucket][$entry['permission']][$entry['document']] = true;
            }
        });

        return \array_values($journalEntries);
    }

    /**
     * Update the unique-index hash table for a row mutation. Pass the new
     * signature ($newSignature) and the old signature ($oldSignature) — pass
     * null for "row didn't exist before / doesn't exist after". Throws on a
     * collision against another docKey.
     */
    protected function probeUniqueHash(string $key, string $indexId, ?string $newHash, ?string $oldHash, string $docKey): void
    {
        if ($newHash !== null && isset($this->uniqueIndexHashes[$key][$indexId][$newHash])
            && $this->uniqueIndexHashes[$key][$indexId][$newHash] !== $docKey) {
            throw new DuplicateException('Document with the requested unique attributes already exists');
        }

        $previousValueAtNew = $newHash !== null ? ($this->uniqueIndexHashes[$key][$indexId][$newHash] ?? null) : null;

        if ($oldHash !== null && $oldHash !== $newHash
            && ($this->uniqueIndexHashes[$key][$indexId][$oldHash] ?? null) === $docKey) {
            unset($this->uniqueIndexHashes[$key][$indexId][$oldHash]);
        }
        if ($newHash !== null) {
            $this->uniqueIndexHashes[$key][$indexId][$newHash] = $docKey;
        }

        $this->journal(function () use ($key, $indexId, $newHash, $oldHash, $docKey, $previousValueAtNew): void {
            if ($newHash !== null) {
                if ($previousValueAtNew === null) {
                    unset($this->uniqueIndexHashes[$key][$indexId][$newHash]);
                } else {
                    $this->uniqueIndexHashes[$key][$indexId][$newHash] = $previousValueAtNew;
                }
            }
            if ($oldHash !== null && $oldHash !== $newHash) {
                $this->uniqueIndexHashes[$key][$indexId][$oldHash] = $docKey;
            }
        });
    }

    /**
     * Build per-index normalized signatures for a stored row. Returns a map of
     * indexId → serialized signature string, omitting indexes that have any
     * null component (NULLs are treated as distinct under MariaDB's UNIQUE
     * semantics).
     *
     * @param  array<string, mixed>  $row
     * @return array<string, string>
     */
    protected function rowUniqueSignatures(string $key, array $row): array
    {
        $result = [];
        foreach ($this->data[$key]['indexes'] ?? [] as $indexId => $index) {
            if (($index['type'] ?? '') !== IndexType::Unique->value) {
                continue;
            }
            $attributes = $index['attributes'] ?? [];
            if (! \is_array($attributes) || empty($attributes)) {
                continue;
            }
            $signature = [];
            foreach ($attributes as $attribute) {
                if (! \is_string($attribute)) {
                    continue;
                }
                $signature[] = $this->normalizeIndexValue($this->resolveAttributeValue($row, $attribute));
            }
            if (\in_array(null, $signature, true)) {
                continue;
            }
            // Under shared tables uniqueness is per-tenant (MariaDB models
            // this as a composite (attr, _tenant) index). Bake the row's
            // tenant into the hash key so two tenants holding the same
            // value do not collide.
            if ($this->sharedTables) {
                \array_unshift($signature, $row['_tenant'] ?? null);
            }
            $result[$indexId] = \serialize($signature);
        }

        return $result;
    }

    /**
     * Build per-index normalized signatures for a Document being written.
     *
     * @return array<string, string>
     */
    protected function documentUniqueSignatures(string $key, Document $document): array
    {
        $result = [];
        foreach ($this->data[$key]['indexes'] ?? [] as $indexId => $index) {
            if (($index['type'] ?? '') !== IndexType::Unique->value) {
                continue;
            }
            $attributes = $index['attributes'] ?? [];
            if (! \is_array($attributes) || empty($attributes)) {
                continue;
            }
            $signature = [];
            foreach ($attributes as $attribute) {
                if (! \is_string($attribute)) {
                    continue;
                }
                $signature[] = $this->normalizeIndexValue($this->resolveDocumentValue($document, $attribute));
            }
            if (\in_array(null, $signature, true)) {
                continue;
            }
            // Match rowUniqueSignatures: under shared tables, scope by the
            // tenant the row will actually be stored under. documentToRow
            // writes `_tenant = $document->getTenant() ?? $this->getTenant()`,
            // so the read- and write-side signatures must agree on that
            // fallback or duplicate detection skips across tenants.
            if ($this->sharedTables) {
                \array_unshift($signature, $document->getTenant() ?? $this->getTenant());
            }
            $result[$indexId] = \serialize($signature);
        }

        return $result;
    }

    /**
     * Single-pass row filter: tenant scoping + WHERE-clause queries +
     * permission allow-set, materialised into a single output array. Any
     * stage that has no work simplifies on the fly (no queries → no per-row
     * matches calls; no shared tables → no tenant probe; auth disabled → no
     * permission lookup).
     *
     * @param  array<Query>  $queries
     * @return array<array<string, mixed>>
     */
    protected function fusedFilter(string $key, string $collectionId, array $queries, string $forPermission): array
    {
        $documents = $this->data[$key]['documents'] ?? [];
        if (empty($documents)) {
            return [];
        }

        $effectiveQueries = [];
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if (\in_array($method, [Method::Select, Method::OrderAsc, Method::OrderDesc, Method::OrderRandom, Method::Limit, Method::Offset, Method::CursorAfter, Method::CursorBefore], true)) {
                continue;
            }
            $effectiveQueries[] = $query;
        }

        $tenantCheck = $this->sharedTables;
        $tenant = $tenantCheck ? $this->getTenant() : null;
        $allowNullTenant = $tenantCheck && $collectionId === Database::METADATA;

        $allowSet = $this->buildPermissionAllowSet($key, $forPermission);

        $output = [];
        foreach ($documents as $row) {
            if ($tenantCheck) {
                $rowTenant = $row['_tenant'] ?? null;
                if ($allowNullTenant && $rowTenant === null) {
                    // visible
                } elseif ($rowTenant !== $tenant) {
                    continue;
                }
            }

            $rowUid = $row['_uid'] ?? '';
            if ($allowSet !== null && (! \is_string($rowUid) || ! isset($allowSet[$rowUid]))) {
                continue;
            }

            $matched = true;
            foreach ($effectiveQueries as $query) {
                if (! $this->matches($row, $query)) {
                    $matched = false;
                    break;
                }
            }
            if (! $matched) {
                continue;
            }

            $output[] = $row;
        }

        return $output;
    }

    /**
     * @param  array<string, mixed>  $row
     */
    protected function matches(array $row, Query $query): bool
    {
        $method = $query->getMethod();

        if ($method === Method::And) {
            foreach ($query->getValues() as $sub) {
                if (! ($sub instanceof Query) || ! $this->matches($row, $sub)) {
                    return false;
                }
            }

            return true;
        }

        if ($method === Method::Or) {
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
            case Method::Equal:
                // SQL three-valued logic: `col = NULL` is unknown — null rows
                // never match an explicit equality, even when callers pass
                // `[null]`. Use `Query::isNull()` for that case.
                if ($value === null) {
                    return false;
                }
                foreach ($queryValues as $candidate) {
                    if ($candidate === null) {
                        continue;
                    }
                    if ($this->looseEquals($value, $candidate)) {
                        return true;
                    }
                }

                return false;

            case Method::NotEqual:
                // SQL: NULL != x evaluates to NULL (i.e. excluded), not true.
                if ($value === null) {
                    return false;
                }
                foreach ($queryValues as $candidate) {
                    // SQL three-valued logic: `col NOT IN (..., NULL, ...)`
                    // is unknown for every row — exclude. Mirrors the null-
                    // candidate handling in Method::Equal above. Use
                    // `Query::isNotNull()` for the explicit not-null intent.
                    if ($candidate === null) {
                        return false;
                    }
                    if ($this->looseEquals($value, $candidate)) {
                        return false;
                    }
                }

                return true;

            case Method::LessThan:
                return $value !== null && $value < $queryValues[0];

            case Method::LessThanEqual:
                return $value !== null && $value <= $queryValues[0];

            case Method::GreaterThan:
                return $value !== null && $value > $queryValues[0];

            case Method::GreaterThanEqual:
                return $value !== null && $value >= $queryValues[0];

            case Method::IsNull:
                return $value === null;

            case Method::IsNotNull:
                return $value !== null;

            case Method::Between:
                return $value !== null && $value >= $queryValues[0] && $value <= $queryValues[1];

            case Method::NotBetween:
                // SQL: NULL NOT BETWEEN x AND y evaluates to NULL (excluded).
                if ($value === null) {
                    return false;
                }

                return $value < $queryValues[0] || $value > $queryValues[1];

            case Method::StartsWith:
                return \is_string($value) && \is_string($queryValues[0]) && \str_starts_with($value, $queryValues[0]);

            case Method::NotStartsWith:
                if ($value === null) {
                    return false;
                }

                return ! \is_string($value) || ! \is_string($queryValues[0]) || ! \str_starts_with($value, $queryValues[0]);

            case Method::EndsWith:
                return \is_string($value) && \is_string($queryValues[0]) && \str_ends_with($value, $queryValues[0]);

            case Method::NotEndsWith:
                if ($value === null) {
                    return false;
                }

                return ! \is_string($value) || ! \is_string($queryValues[0]) || ! \str_ends_with($value, $queryValues[0]);

            case Method::Contains:
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

            case Method::NotContains:
                // SQL: NULL NOT LIKE '%x%' / JSON_CONTAINS(NULL, ...) evaluates
                // to NULL — null-valued rows are excluded, not matched.
                if ($value === null) {
                    return false;
                }

                return ! $this->matches($row, new Query(Method::Contains, $query->getAttribute(), $queryValues));

            case Method::ContainsAny:
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

            case Method::ContainsAll:
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

            case Method::Search:
                if (! \is_string($value)) {
                    return false;
                }
                $searchNeedle = $queryValues[0] ?? '';
                if (! \is_string($searchNeedle) || $searchNeedle === '') {
                    return false;
                }

                return $this->matchesFulltext($value, $searchNeedle);

            case Method::NotSearch:
                // SQL: NULL NOT MATCH evaluates to NULL — null rows excluded.
                if ($value === null) {
                    return false;
                }
                if (! \is_string($value)) {
                    return true;
                }
                $notSearchNeedle = $queryValues[0] ?? '';
                if (! \is_string($notSearchNeedle) || $notSearchNeedle === '') {
                    return true;
                }

                return ! $this->matchesFulltext($value, $notSearchNeedle);

            case Method::Regex:
                if (! \is_string($value)) {
                    return false;
                }
                $pattern = $queryValues[0] ?? '';
                if (! \is_string($pattern)) {
                    return false;
                }

                return $this->matchesRegex($value, $pattern);
        }

        throw new DatabaseException('Query method not implemented in the Memory adapter: '.$method->value);
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
            // Compare numerically with `==` so cross-type pairs like
            // ("3", "3.0") or (3, 3.0) match the way SQL `WHERE col = '3.0'`
            // matches an int column holding 3. Strict `===` after `+0`
            // splits int/float and silently misses parity.
            return $a == $b;
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
            case Method::Equal:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $candidate)) {
                        return true;
                    }
                }

                return false;

            case Method::NotEqual:
                // Postgres: NOT (NULL @> x) evaluates to NULL — null/invalid
                // JSON rows are excluded, mirroring SQL three-valued logic.
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $candidate)) {
                        return false;
                    }
                }

                return true;

            case Method::Contains:
            case Method::ContainsAny:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $this->wrapScalarObjectValue($candidate))) {
                        return true;
                    }
                }

                return false;

            case Method::ContainsAll:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if (! $this->jsonContains($haystack, $this->wrapScalarObjectValue($candidate))) {
                        return false;
                    }
                }

                return true;

            case Method::NotContains:
                // Postgres three-valued logic: NULL field excluded from negation.
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContains($haystack, $this->wrapScalarObjectValue($candidate))) {
                        return false;
                    }
                }

                return true;

            case Method::IsNull:
                return $value === null;

            case Method::IsNotNull:
                return $value !== null;
        }

        throw new DatabaseException('Query method '.$method->value.' not supported for object attributes');
    }

    protected function decodeObjectValue(mixed $value): ?array
    {
        if (\is_array($value)) {
            return $value;
        }
        if (\is_string($value) && $value !== '' && ($value[0] === '{' || $value[0] === '[')) {
            $decoded = \json_decode($value, true);

            return \is_array($decoded) ? $decoded : null;
        }

        return null;
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
     * @param  array<string, mixed>  $row
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
     * Build the set of document ids the current authorization context is
     * allowed to see at $forPermission level. Returns null when authorization
     * is disabled (no filter required). Uses the inverted permission index so
     * each role lookup is O(1).
     *
     * @return array<string, true>|null
     */
    protected function buildPermissionAllowSet(string $key, string $forPermission): ?array
    {
        if (! $this->authorization->getStatus()) {
            return null;
        }

        $roles = $this->authorization->getRoles();
        $allowed = [];
        if (! isset($this->permissionsByPermission[$key][$forPermission])) {
            return $allowed;
        }

        $tenant = $this->getTenant();
        $tenantBucket = $tenant === null ? '__null__' : (string) $tenant;
        $buckets = [];
        if ($this->sharedTables) {
            if (isset($this->permissionsByPermission[$key][$forPermission][$tenantBucket])) {
                $buckets[] = $this->permissionsByPermission[$key][$forPermission][$tenantBucket];
            }
        } else {
            $buckets = $this->permissionsByPermission[$key][$forPermission];
        }

        foreach ($buckets as $bucket) {
            foreach ($roles as $role) {
                if (isset($bucket[$role])) {
                    foreach ($bucket[$role] as $documentId => $_) {
                        $allowed[$documentId] = true;
                    }
                }
            }
        }

        return $allowed;
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @return array<array<string, mixed>>
     */
    protected function applyOrdering(array $rows, array $orderAttributes, array $orderTypes, CursorDirection $cursorDirection): array
    {
        // Random ordering must short-circuit: a non-deterministic comparator
        // breaks usort's transitivity invariant. Shuffle once and return.
        foreach ($orderTypes as $type) {
            if ($type === OrderDirection::Random) {
                \shuffle($rows);

                return $rows;
            }
        }

        $reverse = $cursorDirection === CursorDirection::Before;

        if (empty($orderAttributes)) {
            // Mirror MariaDB's clustered-index ordering when no explicit ORDER BY
            // is supplied — sort by the auto-incrementing _id ascending so
            // pagination via limit/offset is stable across calls.
            \usort($rows, function (array $a, array $b) use ($reverse) {
                $av = $a['_id'] ?? 0;
                $bv = $b['_id'] ?? 0;
                if ($av === $bv) {
                    return 0;
                }
                $cmp = ($av < $bv) ? -1 : 1;

                return $reverse ? -$cmp : $cmp;
            });

            return $rows;
        }

        // Schwartzian transform: precompute the resolved column name and the
        // direction sign per ordering attribute, then sort an index array of
        // [originalIndex, ...sortKeys] tuples so PHP's usort does not move the
        // full row hashes during the comparison.
        $columns = [];
        $directions = [];
        foreach ($orderAttributes as $i => $attribute) {
            $columns[$i] = $this->mapAttribute($attribute);
            $direction = $orderTypes[$i] ?? OrderDirection::Asc;
            if ($reverse) {
                $direction = $direction === OrderDirection::Asc ? OrderDirection::Desc : OrderDirection::Asc;
            }
            $directions[$i] = $direction === OrderDirection::Asc ? 1 : -1;
        }

        $count = \count($rows);
        $indices = [];
        for ($i = 0; $i < $count; $i++) {
            $indices[] = $i;
        }

        \usort($indices, function (int $a, int $b) use ($rows, $columns, $directions): int {
            foreach ($columns as $i => $column) {
                $av = $rows[$a][$column] ?? null;
                $bv = $rows[$b][$column] ?? null;
                if ($av === $bv) {
                    continue;
                }
                // SQL collation sorts NULLs first under ASC; mirror that
                // explicitly so PHP's loose ordering does not equate
                // null with 0 / "".
                if ($av === null) {
                    $cmp = -1;
                } elseif ($bv === null) {
                    $cmp = 1;
                } else {
                    $cmp = ($av < $bv) ? -1 : 1;
                }

                return $cmp * $directions[$i];
            }

            return 0;
        });

        $sorted = [];
        foreach ($indices as $i) {
            $sorted[] = $rows[$i];
        }

        return $sorted;
    }

    /**
     * @param  array<array<string, mixed>>  $rows
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<array<string, mixed>>
     */
    protected function applyCursor(array $rows, array $orderAttributes, array $orderTypes, array $cursor, CursorDirection $cursorDirection): array
    {
        if (empty($cursor)) {
            return $rows;
        }

        if (empty($orderAttributes)) {
            $orderAttributes = ['$sequence'];
            $orderTypes = [OrderDirection::Asc];
        }

        $reverse = $cursorDirection === CursorDirection::Before;
        $resolved = [];
        foreach ($orderAttributes as $i => $attribute) {
            $direction = $orderTypes[$i] ?? OrderDirection::Asc;
            if ($reverse) {
                $direction = $direction === OrderDirection::Asc ? OrderDirection::Desc : OrderDirection::Asc;
            }
            $resolved[] = [
                'column' => $this->mapAttribute($attribute),
                'asc' => $direction === OrderDirection::Asc,
                'ref' => $cursor[$attribute] ?? null,
            ];
        }

        $output = [];
        foreach ($rows as $row) {
            foreach ($resolved as $entry) {
                $current = $row[$entry['column']] ?? null;
                $ref = $entry['ref'];
                if ($current === $ref) {
                    continue;
                }
                // Match applyOrdering: NULLs sort first under ASC.
                if ($current === null) {
                    if (! $entry['asc']) {
                        $output[] = $row;
                    }

                    continue 2;
                }
                if ($ref === null) {
                    if ($entry['asc']) {
                        $output[] = $row;
                    }

                    continue 2;
                }
                if ($entry['asc'] ? ($current > $ref) : ($current < $ref)) {
                    $output[] = $row;
                }

                continue 2;
            }
        }

        return $output;
    }

    /**
     * Probe the unique-index hash maps for the new payload's signatures.
     * Throws DuplicateException on the first collision against any other
     * docKey. Pure read — does not mutate the hash table.
     *
     * @param  array<string, string>  $newSignatures  indexId → serialized signature
     */
    protected function checkUniqueSignatures(string $key, array $newSignatures, string $docKey): void
    {
        foreach ($newSignatures as $indexId => $hash) {
            $existing = $this->uniqueIndexHashes[$key][$indexId][$hash] ?? null;
            if ($existing !== null && $existing !== $docKey) {
                throw new DuplicateException('Document with the requested unique attributes already exists');
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
            case OperatorType::Increment:
                $byInc = $this->numericValue($values[0] ?? null, 1);
                $maxInc = $this->numericValue($values[1] ?? null, null);
                $baseInc = \is_numeric($current) ? $current + 0 : 0;
                if ($maxInc !== null) {
                    // SQL allows the cap exactly (`col <= max - by`), so use a
                    // strict `<` for the headroom guard. `<=` would short the
                    // increment by one boundary step versus MariaDB.
                    if ($baseInc >= $maxInc || ($maxInc - $baseInc) < $byInc) {
                        return $this->preserveNumericType($baseInc, $maxInc);
                    }
                }

                return $this->preserveNumericType($baseInc, $baseInc + $byInc);

            case OperatorType::Decrement:
                $byDec = $this->numericValue($values[0] ?? null, 1);
                $minDec = $this->numericValue($values[1] ?? null, null);
                $baseDec = \is_numeric($current) ? $current + 0 : 0;
                if ($minDec !== null) {
                    if ($baseDec <= $minDec || ($baseDec - $minDec) < $byDec) {
                        return $this->preserveNumericType($baseDec, $minDec);
                    }
                }

                return $this->preserveNumericType($baseDec, $baseDec - $byDec);

            case OperatorType::Multiply:
                $byMul = $this->numericValue($values[0] ?? null, 1);
                $maxMul = $this->numericValue($values[1] ?? null, null);
                $baseMul = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($baseMul * $byMul, $maxMul, true);

            case OperatorType::Divide:
                $byDiv = $this->numericValue($values[0] ?? null, 1);
                $minDiv = $this->numericValue($values[1] ?? null, null);
                if ($byDiv == 0) {
                    return $current;
                }
                $baseDiv = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($baseDiv / $byDiv, $minDiv, false);

            case OperatorType::Modulo:
                $byMod = (int) $this->numericValue($values[0] ?? null, 1);
                if ($byMod == 0) {
                    return $current;
                }
                $baseMod = \is_numeric($current) ? (int) $current : 0;

                return $baseMod % $byMod;

            case OperatorType::Power:
                $byPow = $this->numericValue($values[0] ?? null, 1);
                $maxPow = $this->numericValue($values[1] ?? null, null);
                $basePow = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($basePow ** $byPow, $maxPow, true);

            case OperatorType::StringConcat:
                $appendValue = $values[0] ?? '';

                return $this->stringValue($current).$this->stringValue($appendValue);

            case OperatorType::StringReplace:
                $search = $this->stringValue($values[0] ?? '');
                $replace = $this->stringValue($values[1] ?? '');
                if ($current === null) {
                    return null;
                }

                return \str_replace($search, $replace, $this->stringValue($current));

            case OperatorType::Toggle:
                return ! (bool) $current;

            case OperatorType::ArrayAppend:
                $list = $this->coerceArray($current);

                return [...$list, ...\array_values($values)];

            case OperatorType::ArrayPrepend:
                $list = $this->coerceArray($current);

                return [...\array_values($values), ...$list];

            case OperatorType::ArrayInsert:
                $list = $this->coerceArray($current);
                $index = (int) $this->numericValue($values[0] ?? null, 0);
                $value = $values[1] ?? null;
                if ($index < 0) {
                    $index = 0;
                }
                if ($index > \count($list)) {
                    $index = \count($list);
                }
                \array_splice($list, $index, 0, [$value]);

                return $list;

            case OperatorType::ArrayRemove:
                $list = $this->coerceArray($current);
                $needle = $values[0] ?? null;

                return \array_values(\array_filter($list, fn ($item) => $item !== $needle));

            case OperatorType::ArrayUnique:
                $list = $this->coerceArray($current);

                return \array_values(\array_unique($list, SORT_REGULAR));

            case OperatorType::ArrayIntersect:
                $list = $this->coerceArray($current);
                $other = \array_values($values);

                return \array_values(\array_filter($list, fn ($item) => \in_array($item, $other, false)));

            case OperatorType::ArrayDiff:
                $list = $this->coerceArray($current);
                $other = \array_values($values);

                return \array_values(\array_filter($list, fn ($item) => ! \in_array($item, $other, false)));

            case OperatorType::ArrayFilter:
                $list = $this->coerceArray($current);
                $condition = $this->stringValue($values[0] ?? '');
                $compare = $values[1] ?? null;

                return \array_values(\array_filter($list, fn ($item) => $this->matchesArrayFilter($item, $condition, $compare)));

            case OperatorType::DateAddDays:
                $days = (int) $this->numericValue($values[0] ?? null, 0);

                return $this->shiftDate($current, $days * 86400);

            case OperatorType::DateSubDays:
                $days = (int) $this->numericValue($values[0] ?? null, 0);

                return $this->shiftDate($current, -$days * 86400);

            case OperatorType::DateSetNow:
                return DateTime::now();
        }
    }

    /**
     * Coerce a mixed value to int|float, falling back to $default when the
     * value is not numeric. Centralises the narrow-to-numeric pattern used
     * across the operator implementations.
     */
    protected function numericValue(mixed $value, int|float|null $default): int|float|null
    {
        if (\is_int($value) || \is_float($value)) {
            return $value;
        }
        if (\is_string($value) && \is_numeric($value)) {
            return $value + 0;
        }

        return $default;
    }

    /**
     * Coerce a mixed value to string, falling back to '' when the value is
     * not stringable. Centralises the narrow-to-string pattern used across
     * the string-operator implementations.
     */
    protected function stringValue(mixed $value): string
    {
        if (\is_string($value)) {
            return $value;
        }
        if (\is_scalar($value) || $value === null) {
            return (string) $value;
        }

        return '';
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
     * Mirror OperatorType::ArrayFilter's case-by-case predicate translation
     * (see MariaDB JSON_TABLE filter — `equal`, `greaterThan`, `isNull`, ...).
     */
    protected function matchesArrayFilter(mixed $item, string $condition, mixed $compare): bool
    {
        return match ($condition) {
            Method::Equal->value => $item == $compare,
            Method::NotEqual->value => $item != $compare,
            Method::GreaterThan->value => \is_numeric($item) && \is_numeric($compare) && $item + 0 > $compare + 0,
            Method::GreaterThanEqual->value => \is_numeric($item) && \is_numeric($compare) && $item + 0 >= $compare + 0,
            Method::LessThan->value => \is_numeric($item) && \is_numeric($compare) && $item + 0 < $compare + 0,
            Method::LessThanEqual->value => \is_numeric($item) && \is_numeric($compare) && $item + 0 <= $compare + 0,
            Method::IsNull->value => $item === null,
            Method::IsNotNull->value => $item !== null,
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
        $stringValue = $this->stringValue($current);
        try {
            $base = new \DateTime($stringValue);
        } catch (\Throwable) {
            return $stringValue === '' ? null : $stringValue;
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
