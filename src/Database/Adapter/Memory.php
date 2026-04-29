<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
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
     * @var array<string, bool>
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

    /**
     * @var bool
     */
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
        return $this->getNamespace() . '_' . $this->filter($collection);
    }

    protected function documentKey(string $id, int|string|null $tenant = null): string
    {
        if (!$this->sharedTables) {
            return $id;
        }
        return ($tenant ?? $this->getTenant()) . '|' . $id;
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
        $this->databases[$name] = true;
        return true;
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        if ($collection === null) {
            return isset($this->databases[$database]);
        }

        return isset($this->data[$this->key($collection)]);
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
        unset($this->databases[$name]);
        $prefix = $this->getNamespace() . '_';
        foreach (\array_keys($this->data) as $key) {
            if (\str_starts_with($key, $prefix)) {
                unset($this->data[$key]);
                unset($this->permissions[$key]);
            }
        }
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
        return true;
    }

    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        $key = $this->key($collection);
        if (!isset($this->data[$key])) {
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
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $id = $this->filter($id);
        if (!empty($newKey) && $newKey !== $id) {
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
        if (!isset($this->data[$key])) {
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
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $old = $this->filter($old);
        $new = $this->filter($new);

        if (!isset($this->data[$key]['attributes'][$old])) {
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
        throw new DatabaseException('Relationships are not implemented in the Memory adapter');
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        throw new DatabaseException('Relationships are not implemented in the Memory adapter');
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        throw new DatabaseException('Relationships are not implemented in the Memory adapter');
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $key = $this->key($collection);
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $old = $this->filter($old);
        $new = $this->filter($new);

        if (!isset($this->data[$key]['indexes'][$old])) {
            return true;
        }

        $this->data[$key]['indexes'][$new] = $this->data[$key]['indexes'][$old];
        unset($this->data[$key]['indexes'][$old]);
        return true;
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1): bool
    {
        $key = $this->key($collection);
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        if ($type === Database::INDEX_FULLTEXT) {
            throw new DatabaseException('Fulltext indexes are not implemented in the Memory adapter');
        }

        if ($type === Database::INDEX_UNIQUE && !empty($attributes)) {
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
        if (!isset($this->data[$key])) {
            return true;
        }

        $id = $this->filter($id);
        unset($this->data[$key]['indexes'][$id]);
        return true;
    }

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $key = $this->key($collection->getId());
        if (!isset($this->data[$key])) {
            return new Document([]);
        }

        $doc = $this->data[$key]['documents'][$this->documentKey($id)] ?? null;
        if ($doc === null) {
            return new Document([]);
        }

        return new Document($this->rowToDocument($doc));
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        $key = $this->key($collection->getId());
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $docKey = $this->documentKey($document->getId());
        if (isset($this->data[$key]['documents'][$docKey])) {
            throw new DuplicateException('Document already exists');
        }

        $this->enforceUniqueIndexes($key, $document, null);

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
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $oldKey = $this->documentKey($id);
        $existing = $this->data[$key]['documents'][$oldKey] ?? null;
        if ($existing === null) {
            throw new NotFoundException('Document not found');
        }

        $newId = $document->getId();
        $newKey = $this->documentKey($newId);
        if ($newId !== $id && isset($this->data[$key]['documents'][$newKey])) {
            throw new DuplicateException('Document already exists');
        }

        $this->enforceUniqueIndexes($key, $document, $id);

        $row = $this->documentToRow($document);
        $row['_id'] = $existing['_id'];

        if ($newId !== $id) {
            unset($this->data[$key]['documents'][$oldKey]);
        }
        $this->data[$key]['documents'][$newKey] = $row;

        if (!$skipPermissions) {
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
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $attrs = $updates->getAttributes();
        $hasCreatedAt = !empty($updates->getCreatedAt());
        $hasUpdatedAt = !empty($updates->getUpdatedAt());
        $hasPermissions = $updates->offsetExists('$permissions');
        if (empty($attrs) && !$hasCreatedAt && !$hasUpdatedAt && !$hasPermissions) {
            return 0;
        }

        $count = 0;
        foreach ($documents as $doc) {
            $uid = $doc->getId();
            $docKey = $this->documentKey($uid);
            if (!isset($this->data[$key]['documents'][$docKey])) {
                continue;
            }

            if (!empty($attrs)) {
                $merged = new Document(\array_merge(
                    $this->rowToDocument($this->data[$key]['documents'][$docKey]),
                    $attrs,
                    ['$id' => $uid]
                ));
                $this->enforceUniqueIndexes($key, $merged, $uid);
            }

            $row = &$this->data[$key]['documents'][$docKey];
            foreach ($attrs as $attribute => $value) {
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
        if (!isset($this->data[$key])) {
            return $documents;
        }

        foreach ($documents as $index => $doc) {
            if (!empty($doc->getSequence())) {
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
        if (!isset($this->data[$key])) {
            // MariaDB throws when the collection itself is gone (PDO unknown
            // table → NotFoundException). A missing document inside an existing
            // collection still returns false to mirror rowCount() == 0.
            throw new NotFoundException('Collection not found');
        }

        $docKey = $this->documentKey($id);
        if (!isset($this->data[$key]['documents'][$docKey])) {
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
        if (!isset($this->data[$key])) {
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

        $permSet = !empty($permissionIds)
            ? \array_flip(\array_map('strval', $permissionIds))
            : [];

        if (!empty($deletedIds) || !empty($permSet)) {
            $this->permissions[$key] = \array_values(\array_filter(
                $this->permissions[$key] ?? [],
                fn (array $p) => !isset($deletedIds[$p['document']]) && !isset($permSet[$p['document']])
            ));
        }

        return $count;
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        $key = $this->key($collection->getId());
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = \array_values($this->data[$key]['documents']);
        $rows = $this->applyTenantFilter($rows);
        $rows = $this->applyQueries($rows, $queries);
        $rows = $this->applyPermissions($collection, $rows, $forPermission);
        $rows = $this->applyOrdering($rows, $orderAttributes, $orderTypes, $cursorDirection);
        $rows = $this->applyCursor($rows, $orderAttributes, $orderTypes, $cursor, $cursorDirection);

        if (!is_null($offset)) {
            $rows = \array_slice($rows, $offset);
        }
        if (!is_null($limit)) {
            $rows = \array_slice($rows, 0, $limit);
        }

        $selections = $this->extractSelections($queries);
        $results = [];
        foreach ($rows as $row) {
            $results[] = new Document($this->rowToDocument($row, $selections));
        }

        if ($cursorDirection === Database::CURSOR_BEFORE) {
            $results = \array_reverse($results);
        }

        return $results;
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $key = $this->key($collection->getId());
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = \array_values($this->data[$key]['documents']);
        $rows = $this->applyTenantFilter($rows);
        $rows = $this->applyQueries($rows, $queries);
        $rows = $this->applyPermissions($collection, $rows, Database::PERMISSION_READ);

        if (!is_null($max)) {
            // MariaDB applies LIMIT :max inside the COUNT subquery — LIMIT 0
            // legitimately yields 0. Honour zero rather than ignoring it.
            $rows = \array_slice($rows, 0, $max);
        }
        return \count($rows);
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $key = $this->key($collection->getId());
        if (!isset($this->data[$key])) {
            throw new NotFoundException('Collection not found');
        }

        $rows = \array_values($this->data[$key]['documents']);
        $rows = $this->applyTenantFilter($rows);
        $rows = $this->applyQueries($rows, $queries);
        $rows = $this->applyPermissions($collection, $rows, Database::PERMISSION_READ);

        if (!is_null($max)) {
            $rows = \array_slice($rows, 0, $max);
        }

        $sum = 0;
        $isFloat = false;
        $column = $this->filter($attribute);
        foreach ($rows as $row) {
            if (!\array_key_exists($column, $row) || $row[$column] === null) {
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
        if (!isset($this->data[$key]['documents'][$docKey])) {
            throw new NotFoundException('Document not found');
        }

        $column = $this->filter($attribute);
        $current = $this->data[$key]['documents'][$docKey][$column] ?? 0;
        $current = is_numeric($current) ? $current + 0 : 0;
        $next = $current + $value;

        // MariaDB encodes the bound check as part of the WHERE clause; when the
        // bound is violated the UPDATE simply matches zero rows and the call
        // still returns true. Mirror that — silent no-op on bound violation.
        if (!is_null($min) && $next < $min) {
            return true;
        }
        if (!is_null($max) && $next > $max) {
            return true;
        }

        $this->data[$key]['documents'][$docKey][$column] = $next;
        $this->data[$key]['documents'][$docKey]['_updatedAt'] = $updatedAt;
        return true;
    }

    public function getSizeOfCollection(string $collection): int
    {
        $key = $this->key($collection);
        if (!isset($this->data[$key])) {
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
        return 0;
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
        return false;
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
        return false;
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
        return false;
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
        return false;
    }

    public function getSupportForObjectIndexes(): bool
    {
        return false;
    }

    public function getSupportForSpatialIndexNull(): bool
    {
        return false;
    }

    public function getSupportForOperators(): bool
    {
        return false;
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
        return '"' . $string . '"';
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
        return true;
    }

    public function getSupportForTrigramIndex(): bool
    {
        return false;
    }

    public function getSupportForPCRERegex(): bool
    {
        return false;
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
     * @param array<mixed> $value
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
            $row['_tenant'] = $this->getTenant();
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
     * @param array<string, mixed> $row
     * @param array<int, string>|null $selections
     * @return array<string, mixed>
     */
    protected function rowToDocument(array $row, ?array $selections = null): array
    {
        $allowed = null;
        if ($selections !== null && $selections !== [] && !\in_array('*', $selections, true)) {
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
                    if ($allowed !== null && !isset($allowed[$key])) {
                        break;
                    }
                    $document[$key] = $value;
            }
        }
        return $document;
    }

    /**
     * @param array<Query> $queries
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

    /**
     * @param string $key
     * @param Document $document
     */
    protected function writePermissions(string $key, Document $document): void
    {
        $uid = $document->getId();
        foreach (Database::PERMISSIONS as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $this->permissions[$key][] = [
                    'document' => $uid,
                    'type' => $type,
                    'permission' => \str_replace('"', '', $permission),
                    'tenant' => $this->getTenant(),
                ];
            }
        }
    }

    /**
     * @param array<array<string, mixed>> $rows
     * @return array<array<string, mixed>>
     */
    protected function applyTenantFilter(array $rows): array
    {
        if (!$this->sharedTables) {
            return $rows;
        }

        $tenant = $this->getTenant();
        return \array_values(\array_filter(
            $rows,
            fn (array $row) => ($row['_tenant'] ?? null) === $tenant
        ));
    }

    /**
     * @param array<array<string, mixed>> $rows
     * @param array<Query> $queries
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
     * @param array<string, mixed> $row
     */
    protected function matches(array $row, Query $query): bool
    {
        $method = $query->getMethod();

        if ($method === Query::TYPE_AND) {
            foreach ($query->getValues() as $sub) {
                if (!($sub instanceof Query) || !$this->matches($row, $sub)) {
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

        $attribute = $this->mapAttribute($query->getAttribute());
        $value = \array_key_exists($attribute, $row) ? $row[$attribute] : null;
        $queryValues = $query->getValues();

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
                return !\is_string($value) || !\is_string($queryValues[0]) || !\str_starts_with($value, $queryValues[0]);

            case Query::TYPE_ENDS_WITH:
                return \is_string($value) && \is_string($queryValues[0]) && \str_ends_with($value, $queryValues[0]);

            case Query::TYPE_NOT_ENDS_WITH:
                return !\is_string($value) || !\is_string($queryValues[0]) || !\str_ends_with($value, $queryValues[0]);

            case Query::TYPE_CONTAINS:
                $haystack = $this->decodeArrayValue($value);
                if ($haystack === null && \is_string($value)) {
                    foreach ($queryValues as $needle) {
                        if (\is_string($needle) && \str_contains($value, $needle)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (!\is_array($haystack)) {
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
                return !$this->matches($row, new Query(Query::TYPE_CONTAINS, $query->getAttribute(), $queryValues));

            case Query::TYPE_SEARCH:
            case Query::TYPE_NOT_SEARCH:
            case Query::TYPE_REGEX:
                throw new DatabaseException('Search and regex queries are not implemented in the Memory adapter');
        }

        throw new DatabaseException('Query method not implemented in the Memory adapter: ' . $method);
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
     * @param Document $collection
     * @param array<array<string, mixed>> $rows
     * @return array<array<string, mixed>>
     */
    protected function applyPermissions(Document $collection, array $rows, string $forPermission): array
    {
        if (!$this->authorization->getStatus()) {
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
     * @param array<array<string, mixed>> $rows
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
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
     * @param array<array<string, mixed>> $rows
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
     * @param array<string, mixed> $cursor
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

    /**
     * @param string $key
     * @param Document $document
     * @param string|null $previousId
     */
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
                $column = $this->mapAttribute($attribute);
                $docValue = $document->getAttribute($attribute);
                if ($docValue === null) {
                    $docValue = $document->getAttribute($column);
                }
                $signature[] = $this->normalizeIndexValue($docValue);
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
                    $column = $this->mapAttribute($attribute);
                    $rowSignature[] = $this->normalizeIndexValue($row[$column] ?? null);
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
}
