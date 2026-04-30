<?php

declare(strict_types=1);

namespace Utopia\Database\Adapter;

use Redis as RedisClient;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Redis-backed adapter mirroring Memory adapter's surface.
 *
 * Wave-2 architects fill in the body of each marked method group below;
 * the surface, helper signatures, constants, and contract are locked in
 * Wave 1 and must not be modified by downstream architects. If you find
 * a contract gap, escalate via CONTRACT_GAP.md instead of editing this
 * file's locked regions.
 *
 * Storage key schema:
 *
 *     {ns}                      = getNamespace()
 *     {db}                      = current setDatabase() value
 *     {col}                     = collection ID
 *
 *     Key                                          | Type | Holds
 *     ---------------------------------------------+------+----------------------------------
 *     {ns}:{db}:dbs                                | SET  | database names
 *     {ns}:{db}:cols                               | SET  | collection IDs in this db
 *     {ns}:{db}:meta:{col}                         | HASH | fields: schema, attrs, indexes, docCount, sizeBytes
 *     {ns}:{db}:doc:{col}:{id}                     | STRING | JSON-encoded Document
 *     {ns}:{db}:idx:{col}                          | SET  | doc IDs in collection (for SCAN/list)
 *     {ns}:{db}:perm:{col}:r/w/u/d:{role}          | SET  | doc IDs by action+role
 *     {ns}:{db}:perm:doc:{col}:{id}                | HASH | role -> csv("read,update,delete")
 *     {ns}:{db}:tenants:{col}:{tenant}             | SET  | doc IDs filtered by tenant (shared mode)
 *     {ns}:{db}:journal:{txid}                     | LIST | WAL entries for rollback (T56 owns)
 *
 * DSN format: redis://[user:pass@]host:port[/db]
 * No query parameters; the path segment is the namespace, defaulting
 * to "utopia" when the segment is omitted.
 *
 * Transaction model: optimistic via WATCH/MULTI/EXEC retry (max 3
 * retries with 10/50/250 ms back-off). Pessimistic update locks are
 * intentionally unsupported; getSupportForUpdateLock returns false.
 *
 * Rollback contract: rollbackJournal() MUST use raw \Redis client
 * commands only — never public adapter methods, which would re-enter
 * the journal and infinitely recurse. T56 owns the implementation.
 *
 * Wave-2 architects throw the imported exception types from their
 * implementations:
 *
 * @see DuplicateException Raised on unique-index collisions in T20/T30/T40.
 * @see NotFoundException Raised when a document or collection is missing.
 * @see OrderException Raised by T40 on invalid order/cursor combinations.
 * @see QueryException Raised by T40 on malformed queries.
 * @see TimeoutException Raised by T56 on transaction timeout escalation.
 * @see TransactionException Raised by T56 on commit/rollback failures.
 * @see Authorization Used by T50 when applying permission filters.
 * @see Permission Used by T50 when serialising permission strings.
 * @see ID Used by T30 when generating new document identifiers.
 * @see Query Argument type for T40 evaluateQueries.
 */
class Redis extends Adapter
{
    public const string KEY_PREFIX = 'utopia';

    public const string SEP = ':';

    /** @phpstan-ignore-next-line classConstant.unused */
    private const int TX_MAX_RETRIES = 3;

    /** @phpstan-ignore-next-line classConstant.unused */
    private const array TX_BACKOFF_MS = [10, 50, 250];

    private RedisClient $client;

    /**
     * @var array<int, array<int, array{op: string, payload: array<string, mixed>}>>
     *
     * @phpstan-ignore-next-line property.onlyWritten
     */
    private array $journalStack = [];

    public function __construct(RedisClient $client)
    {
        $this->client = $client;
    }

    private function key(string ...$parts): string
    {
        return self::KEY_PREFIX . self::SEP . \implode(self::SEP, $parts);
    }

    private function ns(): string
    {
        return self::KEY_PREFIX . self::SEP . $this->getNamespace() . self::SEP . $this->getDatabase();
    }

    private function encode(Document $document): string
    {
        return \json_encode($document->getArrayCopy(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
    }

    private function decode(string $payload): Document
    {
        /** @var array<string, mixed> $data */
        $data = \json_decode($payload, true, 512, JSON_THROW_ON_ERROR);

        return new Document($data);
    }

    /**
     * Pass-through executor used while the transaction layer is being
     * implemented. T56 replaces this body with a WATCH/MULTI/EXEC retry
     * loop in Wave 2; do not inline the body at call sites.
     *
     * @param callable(RedisClient): mixed $fn
     */
    protected function tx(callable $fn): mixed
    {
        // PASS-THROUGH: T56 replaces with WATCH/MULTI/EXEC retry loop in Wave 2.
        return $fn($this->client);
    }

    private function writePermissions(string $collection, string $id, Document $document): void
    {
        throw new \LogicException('owned by T50');
    }

    private function clearPermissions(string $collection, string $id): void
    {
        throw new \LogicException('owned by T50');
    }

    /**
     * @param array<int, string> $ids
     * @return array<int, string>
     *
     * @phpstan-ignore-next-line method.unused
     */
    private function applyPermissionFilter(string $collection, array $ids, string $action): array
    {
        throw new \LogicException('owned by T50');
    }

    /**
     * @param array<string, mixed> $payload
     */
    protected function journal(string $op, array $payload): void
    {
        throw new \LogicException('owned by T56');
    }

    protected function rollbackJournal(): void
    {
        throw new \LogicException('owned by T56');
    }

    protected function commitJournal(): void
    {
        throw new \LogicException('owned by T56');
    }

    /** @phpstan-ignore-next-line method.unused */
    private function rawDeleteDoc(string $collection, string $id): void
    {
        throw new \LogicException('owned by T56');
    }

    /** @phpstan-ignore-next-line method.unused */
    private function rawRestoreDoc(string $collection, string $id, string $payload): void
    {
        throw new \LogicException('owned by T56');
    }

    /**
     * @param array<int, Query> $queries
     * @param array<int, string> $orderAttributes
     * @param array<int, string> $orderTypes
     * @param array<string, mixed> $cursor
     * @return array<int, Document>
     */
    protected function evaluateQueries(string $collection, array $queries, ?int $limit, ?int $offset, array $orderAttributes, array $orderTypes, array $cursor, string $cursorDirection): array
    {
        throw new \LogicException('owned by T40');
    }

    public function getDriver(): mixed
    {
        return 'redis';
    }

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
    }

    public function ping(): bool
    {
        return (bool) $this->client->ping();
    }

    public function reconnect(): void
    {
    }

    protected function quote(string $string): string
    {
        return '"' . $string . '"';
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
        return Database::VAR_STRING;
    }

    public function getSupportForSchemas(): bool
    {
        return true;
    }

    public function getSupportForAttributes(): bool
    {
        return true;
    }

    public function setSupportForAttributes(bool $support): bool
    {
        return true;
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
        return false;
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

    public function getSupportForInternalCasting(): bool
    {
        return false;
    }

    public function getSupportForUTCCasting(): bool
    {
        return false;
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
        return true;
    }

    public function getSupportForNestedTransactions(): bool
    {
        return true;
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

    /**
     * @param array<int, string> $selections
     */
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

    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return '';
    }

    protected function execute(mixed $stmt): bool
    {
        return true;
    }

    public function decodePoint(string $wkb): array
    {
        throw new DatabaseException('Spatial types are not implemented in the Redis adapter');
    }

    public function decodeLinestring(string $wkb): array
    {
        throw new DatabaseException('Spatial types are not implemented in the Redis adapter');
    }

    public function decodePolygon(string $wkb): array
    {
        throw new DatabaseException('Spatial types are not implemented in the Redis adapter');
    }

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return $value;
    }

    // === @architect:T20 owns: schema + collection + attribute ops ===

    public function create(string $name): bool
    {
        $name = $this->filter($name);
        $dbsKey = $this->key($this->getNamespace(), $this->getDatabase(), 'dbs');

        $this->tx(fn (RedisClient $client) => $client->sAdd($dbsKey, $name));

        return true;
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);
        $dbsKey = $this->key($this->getNamespace(), $this->getDatabase(), 'dbs');

        if ((bool) $this->client->sIsMember($dbsKey, $database) === false) {
            return false;
        }

        if ($collection === null) {
            return true;
        }

        $collection = $this->filter($collection);
        $colsKey = $this->key($this->getNamespace(), $database, 'cols');

        return (bool) $this->client->sIsMember($colsKey, $collection);
    }

    public function list(): array
    {
        $dbsKey = $this->key($this->getNamespace(), $this->getDatabase(), 'dbs');
        /** @var array<int, string>|false $names */
        $names = $this->client->sMembers($dbsKey);
        if ($names === false) {
            $names = [];
        }

        $databases = [];
        foreach ($names as $name) {
            $databases[] = new Document(['name' => $name]);
        }

        return $databases;
    }

    public function delete(string $name): bool
    {
        $name = $this->filter($name);
        $namespace = $this->getNamespace();
        $dbsKey = $this->key($namespace, $this->getDatabase(), 'dbs');
        $colsKey = $this->key($namespace, $name, 'cols');

        $this->tx(function (RedisClient $client) use ($name, $namespace, $dbsKey, $colsKey): void {
            /** @var array<int, string>|false $collections */
            $collections = $client->sMembers($colsKey);
            if (\is_array($collections)) {
                foreach ($collections as $collection) {
                    $this->purgeCollectionKeys($client, $namespace, $name, $collection);
                }
            }

            $client->del($colsKey);
            $client->sRem($dbsKey, $name);
        });

        return true;
    }

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->filter($name);
        $namespace = $this->getNamespace();
        $database = $this->getDatabase();
        $colsKey = $this->key($namespace, $database, 'cols');
        $metaKey = $this->key($namespace, $database, 'meta', $id);
        $idxKey = $this->key($namespace, $database, 'idx', $id);

        if ((bool) $this->client->exists($metaKey)) {
            throw new DuplicateException('Collection already exists');
        }

        $attributePayload = [];
        foreach ($attributes as $attribute) {
            $attributePayload[] = $attribute->getArrayCopy();
        }
        $indexPayload = [];
        foreach ($indexes as $index) {
            $indexPayload[] = $index->getArrayCopy();
        }

        $schema = new Document([
            '$id' => $id,
            'name' => $name,
            'attributes' => $attributePayload,
            'indexes' => $indexPayload,
        ]);

        $this->tx(function (RedisClient $client) use ($id, $colsKey, $metaKey, $idxKey, $schema, $attributePayload, $indexPayload): void {
            $client->hMSet($metaKey, [
                'schema' => \json_encode($schema->getArrayCopy(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
                'attrs' => \json_encode($attributePayload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
                'indexes' => \json_encode($indexPayload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
                'docCount' => '0',
                'sizeBytes' => '0',
            ]);
            // Reserve the doc-id index set so SCAN/list operations work even
            // before the first document write. Redis cannot persist empty
            // sets, so we materialise the key on first write — but we still
            // delete it on collection drop to clean up any prior contents.
            $client->del($idxKey);
            $client->sAdd($colsKey, $id);
        });

        return true;
    }

    public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);
        $namespace = $this->getNamespace();
        $database = $this->getDatabase();
        $colsKey = $this->key($namespace, $database, 'cols');

        $this->tx(function (RedisClient $client) use ($id, $namespace, $database, $colsKey): void {
            $this->purgeCollectionKeys($client, $namespace, $database, $id);
            $client->sRem($colsKey, $id);
        });

        return true;
    }

    public function analyzeCollection(string $collection): bool
    {
        // Redis maintains no internal table statistics; mirrors Memory's
        // behavior for adapters without a stats subsystem.
        return false;
    }

    public function getSizeOfCollection(string $collection): int
    {
        return $this->computeCollectionSize($collection);
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        // Redis stores the working set in memory; on-disk size mirrors
        // logical size for the purposes of the size-tracking tests.
        return $this->computeCollectionSize($collection);
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $metaKey = $this->key($this->getNamespace(), $this->getDatabase(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $id, $type, $size, $signed, $array, $required): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $attrs = $this->upsertAttributeRecord($attrs, [
                '$id' => $id,
                'key' => $id,
                'type' => $type,
                'size' => $size,
                'signed' => $signed,
                'array' => $array,
                'required' => $required,
            ]);
            $client->hSet(
                $metaKey,
                'attrs',
                \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
            );
        });

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
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $metaKey = $this->key($this->getNamespace(), $this->getDatabase(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        if (! empty($newKey) && $newKey !== $id) {
            $this->renameAttribute($collection, $id, $newKey);
            $id = $this->filter($newKey);
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $id, $type, $size, $signed, $array, $required): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $attrs = $this->upsertAttributeRecord($attrs, [
                '$id' => $id,
                'key' => $id,
                'type' => $type,
                'size' => $size,
                'signed' => $signed,
                'array' => $array,
                'required' => $required,
            ]);
            $client->hSet(
                $metaKey,
                'attrs',
                \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
            );
        });

        return true;
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $metaKey = $this->key($this->getNamespace(), $this->getDatabase(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return true;
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $id): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $filtered = [];
            foreach ($attrs as $attribute) {
                $existingId = (string) ($attribute['$id'] ?? $attribute['key'] ?? '');
                if ($this->filter($existingId) === $id) {
                    continue;
                }
                $filtered[] = $attribute;
            }
            $client->hSet(
                $metaKey,
                'attrs',
                \json_encode($filtered, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
            );
        });

        return true;
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $metaKey = $this->key($this->getNamespace(), $this->getDatabase(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $old, $new): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $touched = false;
            foreach ($attrs as $i => $attribute) {
                $existingId = (string) ($attribute['$id'] ?? $attribute['key'] ?? '');
                if ($this->filter($existingId) !== $old) {
                    continue;
                }
                $attribute['$id'] = $new;
                $attribute['key'] = $new;
                $attrs[$i] = $attribute;
                $touched = true;
            }
            if (! $touched) {
                return;
            }
            $client->hSet(
                $metaKey,
                'attrs',
                \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
            );
        });

        return true;
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    public function getCountOfAttributes(Document $collection): int
    {
        return \count($collection->getAttribute('attributes', [])) + $this->getCountOfDefaultAttributes();
    }

    /**
     * Read and decode the `attrs` JSON field on a collection meta hash. Returns
     * a plain list of attribute record arrays (empty when the field is absent
     * or stored empty).
     *
     * @return array<int, array<string, mixed>>
     */
    private function readAttributesField(RedisClient $client, string $metaKey): array
    {
        $raw = $client->hGet($metaKey, 'attrs');
        if (! \is_string($raw) || $raw === '') {
            return [];
        }
        /** @var array<int, array<string, mixed>> $decoded */
        $decoded = \json_decode($raw, true, 512, JSON_THROW_ON_ERROR);

        return \array_values($decoded);
    }

    /**
     * Insert or replace an attribute record matched by `$id`/`key`. Returns a
     * fresh list (re-indexed) so the JSON encodes as an array, never an object.
     *
     * @param array<int, array<string, mixed>> $attrs
     * @param array<string, mixed> $record
     * @return array<int, array<string, mixed>>
     */
    private function upsertAttributeRecord(array $attrs, array $record): array
    {
        $targetId = (string) ($record['$id'] ?? '');
        $replaced = false;
        foreach ($attrs as $i => $existing) {
            $existingId = (string) ($existing['$id'] ?? $existing['key'] ?? '');
            if ($existingId !== $targetId) {
                continue;
            }
            $attrs[$i] = $record;
            $replaced = true;
            break;
        }
        if (! $replaced) {
            $attrs[] = $record;
        }

        return \array_values($attrs);
    }

    /**
     * Drop every key associated with a single collection inside `{ns}:{db}`.
     * Used by both deleteCollection and the cascading delete() path. Permission
     * sets and document blobs are SCANned because we can't enumerate them
     * without an index — the doc-id set under `idx:{col}` is authoritative for
     * existing documents but permission roles vary, so we SCAN the prefix.
     */
    private function purgeCollectionKeys(RedisClient $client, string $namespace, string $database, string $collection): void
    {
        $collection = $this->filter($collection);
        $metaKey = $this->key($namespace, $database, 'meta', $collection);
        $idxKey = $this->key($namespace, $database, 'idx', $collection);

        /** @var array<int, string>|false $docIds */
        $docIds = $client->sMembers($idxKey);
        if (\is_array($docIds)) {
            foreach ($docIds as $docId) {
                $client->del(
                    $this->key($namespace, $database, 'doc', $collection, $docId),
                    $this->key($namespace, $database, 'perm', 'doc', $collection, $docId),
                );
            }
        }

        $this->deleteByPattern($client, $this->key($namespace, $database, 'perm', $collection) . self::SEP . '*');
        $this->deleteByPattern($client, $this->key($namespace, $database, 'tenants', $collection) . self::SEP . '*');

        $client->del($metaKey, $idxKey);
    }

    /**
     * SCAN-and-DEL helper — MATCHes the supplied glob in batches so we don't
     * block the server with a giant KEYS call. Honours the same 500-key batch
     * size used by the test harness teardown.
     */
    private function deleteByPattern(RedisClient $client, string $pattern): void
    {
        $cursor = null;
        do {
            /** @var array<int, string>|false $batch */
            $batch = $client->scan($cursor, $pattern, 500);
            if (\is_array($batch) && $batch !== []) {
                $client->del(...$batch);
            }
        } while ($cursor !== 0 && $cursor !== null);
    }

    /**
     * Compute the size of a collection by summing memory used by its meta
     * hash, every document blob, the doc-id index, and any permission sets.
     *
     * Redis `MEMORY USAGE` is used when supported (Redis 4.0+). We fall back
     * to STRLEN/HLEN approximations so the adapter still produces a non-zero
     * size on builds (or test doubles) where MEMORY USAGE isn't routed.
     */
    private function computeCollectionSize(string $collection): int
    {
        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $database = $this->getDatabase();
        $metaKey = $this->key($namespace, $database, 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return 0;
        }

        $total = $this->measureKey($metaKey);

        $idxKey = $this->key($namespace, $database, 'idx', $collection);
        $total += $this->measureKey($idxKey);

        /** @var array<int, string>|false $docIds */
        $docIds = $this->client->sMembers($idxKey);
        if (\is_array($docIds)) {
            foreach ($docIds as $docId) {
                $total += $this->measureKey($this->key($namespace, $database, 'doc', $collection, $docId));
                $total += $this->measureKey($this->key($namespace, $database, 'perm', 'doc', $collection, $docId));
            }
        }

        $permPrefix = $this->key($namespace, $database, 'perm', $collection) . self::SEP . '*';
        $cursor = null;
        do {
            /** @var array<int, string>|false $batch */
            $batch = $this->client->scan($cursor, $permPrefix, 500);
            if (\is_array($batch)) {
                foreach ($batch as $key) {
                    $total += $this->measureKey($key);
                }
            }
        } while ($cursor !== 0 && $cursor !== null);

        return $total;
    }

    /**
     * Best-effort size probe for a single Redis key. Prefers `MEMORY USAGE`
     * (returns the bytes Redis itself reports). Falls back to the encoded
     * payload length when MEMORY USAGE is unavailable, so the result remains
     * a stable monotonically-growing integer for size-tracking tests.
     */
    private function measureKey(string $key): int
    {
        try {
            /** @var int|false|null $usage */
            $usage = $this->client->rawCommand('MEMORY', 'USAGE', $key);
            if (\is_int($usage)) {
                return $usage;
            }
        } catch (\Throwable) {
            // Fall through to the structural fallback below.
        }

        $type = $this->client->type($key);
        switch ($type) {
            case RedisClient::REDIS_STRING:
                $value = $this->client->get($key);

                return \is_string($value) ? \strlen($value) + \strlen($key) : 0;
            case RedisClient::REDIS_HASH:
                $entries = $this->client->hGetAll($key);
                $bytes = \strlen($key);
                if (\is_array($entries)) {
                    foreach ($entries as $field => $value) {
                        $bytes += \strlen((string) $field) + \strlen((string) $value);
                    }
                }

                return $bytes;
            case RedisClient::REDIS_SET:
                $members = $this->client->sMembers($key);
                $bytes = \strlen($key);
                if (\is_array($members)) {
                    foreach ($members as $member) {
                        $bytes += \strlen((string) $member);
                    }
                }

                return $bytes;
            default:
                return 0;
        }
    }

    // === @architect:T20 end ===





    // === @architect:T30 owns: document CRUD + bulk + increase ===

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $col = $collection->getId();
        $payload = $this->client->get($this->key($this->ns(), 'doc', $col, \strtolower($id)));

        if (! \is_string($payload) || $payload === '') {
            return new Document([]);
        }

        $document = $this->decode($payload);

        $selections = [];
        foreach ($queries as $query) {
            if ($query instanceof Query && $query->getMethod() === Query::TYPE_SELECT) {
                foreach ($query->getValues() as $value) {
                    $selections[] = (string) $value;
                }
            }
        }

        if (! empty($selections) && ! \in_array('*', $selections, true)) {
            $projected = [];
            foreach ($document->getArrayCopy() as $field => $value) {
                if (\str_starts_with((string) $field, '$') || \str_starts_with((string) $field, '_')) {
                    $projected[$field] = $value;

                    continue;
                }
                if (\in_array($field, $selections, true)) {
                    $projected[$field] = $value;
                }
            }
            $document = new Document($projected);
        }

        return $document;
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        $col = $collection->getId();
        $id = $document->getId();
        if ($id === '') {
            $id = ID::unique();
            $document->setAttribute('$id', $id);
        }
        $docKey = $this->key($this->ns(), 'doc', $col, \strtolower($id));
        $idxKey = $this->key($this->ns(), 'idx', $col);
        $seqKey = $this->key($this->ns(), 'seq', $col);

        return $this->tx(function (RedisClient $r) use ($col, $id, $document, $docKey, $idxKey, $seqKey): Document {
            if ((bool) $r->exists($docKey)) {
                throw new DuplicateException('Document already exists');
            }

            $sequence = $document->getSequence();
            if (empty($sequence)) {
                $next = $r->incr($seqKey);
                $sequence = (string) $next;
            } else {
                $sequence = (string) $sequence;
                $current = $r->get($seqKey);
                if (! \is_string($current) || (int) $sequence > (int) $current) {
                    $r->set($seqKey, $sequence);
                }
            }
            $document->setAttribute('$sequence', $sequence);

            $r->set($docKey, $this->encode($document));
            $r->sAdd($idxKey, \strtolower($id));

            $this->writePermissions($col, $id, $document);
            $this->journal('createDoc', ['col' => $col, 'id' => $id]);

            return $document;
        });
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
        $col = $collection->getId();
        $oldKey = $this->key($this->ns(), 'doc', $col, \strtolower($id));
        $idxKey = $this->key($this->ns(), 'idx', $col);

        return $this->tx(function (RedisClient $r) use ($col, $id, $document, $skipPermissions, $oldKey, $idxKey): Document {
            $existingPayload = $r->get($oldKey);
            if (! \is_string($existingPayload) || $existingPayload === '') {
                throw new NotFoundException('Document not found');
            }

            $existing = $this->decode($existingPayload);
            $newId = $document->getId() !== '' ? $document->getId() : $id;
            $newKey = $this->key($this->ns(), 'doc', $col, \strtolower($newId));

            if ($newId !== $id && (bool) $r->exists($newKey)) {
                throw new DuplicateException('Document already exists');
            }

            $merged = \array_merge($existing->getArrayCopy(), $document->getArrayCopy());
            $merged['$id'] = $newId;
            $mergedDocument = new Document($merged);

            $payload = $this->encode($mergedDocument);

            if ($newId !== $id) {
                $r->del($oldKey);
                $r->sRem($idxKey, \strtolower($id));
            }
            $r->set($newKey, $payload);
            $r->sAdd($idxKey, \strtolower($newId));

            $this->journal('updateDoc', [
                'col' => $col,
                'id' => $id,
                'newId' => $newId,
                'before' => $existingPayload,
            ]);

            if (! $skipPermissions) {
                $this->clearPermissions($col, $id);
                if ($newId !== $id) {
                    $this->clearPermissions($col, $newId);
                }
                $this->writePermissions($col, $newId, $mergedDocument);
            }

            return $mergedDocument;
        });
    }

    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        if (empty($documents)) {
            return 0;
        }

        $attrs = $updates->getAttributes();
        $hasCreatedAt = ! empty($updates->getCreatedAt());
        $hasUpdatedAt = ! empty($updates->getUpdatedAt());
        $hasPermissions = $updates->offsetExists('$permissions');
        if (empty($attrs) && ! $hasCreatedAt && ! $hasUpdatedAt && ! $hasPermissions) {
            return 0;
        }

        $col = $collection->getId();

        return $this->tx(function (RedisClient $r) use ($col, $documents, $updates, $attrs, $hasCreatedAt, $hasUpdatedAt, $hasPermissions): int {
            $count = 0;
            foreach ($documents as $doc) {
                $uid = $doc->getId();
                $docKey = $this->key($this->ns(), 'doc', $col, \strtolower($uid));
                $existingPayload = $r->get($docKey);
                if (! \is_string($existingPayload) || $existingPayload === '') {
                    continue;
                }

                $existing = $this->decode($existingPayload);
                $merged = $existing->getArrayCopy();
                foreach ($attrs as $attribute => $value) {
                    $merged[$attribute] = $value;
                }
                if ($hasCreatedAt) {
                    $merged['$createdAt'] = $updates->getCreatedAt();
                }
                if ($hasUpdatedAt) {
                    $merged['$updatedAt'] = $updates->getUpdatedAt();
                }
                if ($hasPermissions) {
                    $merged['$permissions'] = $updates->getPermissions();
                }

                $mergedDocument = new Document($merged);
                $r->set($docKey, $this->encode($mergedDocument));

                $this->journal('updateDoc', [
                    'col' => $col,
                    'id' => $uid,
                    'newId' => $uid,
                    'before' => $existingPayload,
                ]);

                if ($hasPermissions) {
                    $this->clearPermissions($col, $uid);
                    $this->writePermissions($col, $uid, $mergedDocument);
                }

                $count++;
            }

            return $count;
        });
    }

    public function upsertDocuments(
        Document $collection,
        string $attribute,
        array $changes
    ): array {
        if (empty($changes)) {
            return $changes;
        }

        $col = $collection->getId();
        $idxKey = $this->key($this->ns(), 'idx', $col);
        $seqKey = $this->key($this->ns(), 'seq', $col);

        return $this->tx(function (RedisClient $r) use ($col, $attribute, $changes, $idxKey, $seqKey): array {
            $results = [];

            // Phase 1: pipeline GETs of every doc so we know create vs update
            // in a single round trip.
            $r->multi(\Redis::PIPELINE);
            foreach ($changes as $change) {
                $document = $change->getNew();
                $r->get($this->key($this->ns(), 'doc', $col, \strtolower($document->getId())));
            }
            $existingPayloads = $r->exec();
            if (! \is_array($existingPayloads)) {
                $existingPayloads = [];
            }

            foreach ($changes as $i => $change) {
                $document = $change->getNew();
                $id = $document->getId();
                $docKey = $this->key($this->ns(), 'doc', $col, \strtolower($id));
                $existingPayload = $existingPayloads[$i] ?? false;

                if (\is_string($existingPayload) && $existingPayload !== '') {
                    $existing = $this->decode($existingPayload);
                    $merged = \array_merge($existing->getArrayCopy(), $document->getArrayCopy());
                    $merged['$id'] = $id;

                    if ($attribute !== '') {
                        $previous = $existing->getAttribute($attribute);
                        $delta = $document->getAttribute($attribute);
                        $previousNumeric = \is_numeric($previous) ? $previous + 0 : 0;
                        $deltaNumeric = \is_numeric($delta) ? $delta + 0 : 0;
                        $merged[$attribute] = $previousNumeric + $deltaNumeric;
                    }

                    $mergedDocument = new Document($merged);
                    $r->set($docKey, $this->encode($mergedDocument));

                    $this->journal('updateDoc', [
                        'col' => $col,
                        'id' => $id,
                        'newId' => $id,
                        'before' => $existingPayload,
                    ]);

                    $this->clearPermissions($col, $id);
                    $this->writePermissions($col, $id, $mergedDocument);

                    $results[] = $mergedDocument;
                } else {
                    $sequence = $document->getSequence();
                    if (empty($sequence)) {
                        $next = $r->incr($seqKey);
                        $sequence = (string) $next;
                    } else {
                        $sequence = (string) $sequence;
                        $current = $r->get($seqKey);
                        if (! \is_string($current) || (int) $sequence > (int) $current) {
                            $r->set($seqKey, $sequence);
                        }
                    }
                    $document->setAttribute('$sequence', $sequence);

                    $r->set($docKey, $this->encode($document));
                    $r->sAdd($idxKey, \strtolower($id));

                    $this->writePermissions($col, $id, $document);
                    $this->journal('createDoc', ['col' => $col, 'id' => $id]);

                    $results[] = $document;
                }
            }

            return $results;
        });
    }

    public function getSequences(string $collection, array $documents): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $this->client->multi(\Redis::PIPELINE);
        $indexes = [];
        foreach ($documents as $index => $doc) {
            if (! empty($doc->getSequence())) {
                continue;
            }
            $this->client->get($this->key($this->ns(), 'doc', $collection, \strtolower($doc->getId())));
            $indexes[] = $index;
        }
        $payloads = $this->client->exec();
        if (! \is_array($payloads)) {
            return $documents;
        }

        foreach ($indexes as $position => $index) {
            $payload = $payloads[$position] ?? false;
            if (! \is_string($payload) || $payload === '') {
                continue;
            }
            $existing = $this->decode($payload);
            $sequence = $existing->getSequence();
            if (! empty($sequence)) {
                $documents[$index]->setAttribute('$sequence', (string) $sequence);
            }
        }

        return $documents;
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        $docKey = $this->key($this->ns(), 'doc', $collection, \strtolower($id));
        $idxKey = $this->key($this->ns(), 'idx', $collection);

        return $this->tx(function (RedisClient $r) use ($collection, $id, $docKey, $idxKey): bool {
            $payload = $r->get($docKey);
            if (! \is_string($payload) || $payload === '') {
                return false;
            }

            $this->journal('deleteDoc', [
                'col' => $collection,
                'id' => $id,
                'before' => $payload,
            ]);

            $this->clearPermissions($collection, $id);
            $r->del($docKey);
            $r->sRem($idxKey, \strtolower($id));

            return true;
        });
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        if (empty($sequences) && empty($permissionIds)) {
            return 0;
        }

        $idxKey = $this->key($this->ns(), 'idx', $collection);

        return $this->tx(function (RedisClient $r) use ($collection, $sequences, $permissionIds, $idxKey): int {
            $sequenceSet = [];
            foreach ($sequences as $sequence) {
                $sequenceSet[(string) $sequence] = true;
            }

            $allIds = $r->sMembers($idxKey);
            if (! \is_array($allIds)) {
                $allIds = [];
            }

            $r->multi(\Redis::PIPELINE);
            foreach ($allIds as $id) {
                $r->get($this->key($this->ns(), 'doc', $collection, (string) $id));
            }
            $payloads = $r->exec();
            if (! \is_array($payloads)) {
                $payloads = [];
            }

            $deleted = [];
            foreach ($allIds as $position => $id) {
                $payload = $payloads[$position] ?? false;
                if (! \is_string($payload) || $payload === '') {
                    continue;
                }
                $document = $this->decode($payload);
                $matchesSequence = isset($sequenceSet[(string) $document->getSequence()]);
                if ($matchesSequence) {
                    $deleted[$document->getId()] = $payload;
                }
            }

            foreach ($deleted as $documentId => $payload) {
                $this->journal('deleteDoc', [
                    'col' => $collection,
                    'id' => (string) $documentId,
                    'before' => $payload,
                ]);
                $this->clearPermissions($collection, (string) $documentId);
                $r->del($this->key($this->ns(), 'doc', $collection, \strtolower((string) $documentId)));
                $r->sRem($idxKey, \strtolower((string) $documentId));
            }

            // Permission-only cleanup for ids the caller listed but that did
            // not match by sequence — mirrors Memory adapter semantics.
            foreach ($permissionIds as $permissionId) {
                $documentId = (string) $permissionId;
                if (isset($deleted[$documentId])) {
                    continue;
                }
                $this->clearPermissions($collection, $documentId);
            }

            return \count($deleted);
        });
    }

    public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value,
        string $updatedAt,
        int|float|null $min = null,
        int|float|null $max = null
    ): bool {
        $docKey = $this->key($this->ns(), 'doc', $collection, \strtolower($id));

        return $this->tx(function (RedisClient $r) use ($collection, $id, $attribute, $value, $updatedAt, $min, $max, $docKey): bool {
            $payload = $r->get($docKey);
            if (! \is_string($payload) || $payload === '') {
                throw new NotFoundException('Document not found');
            }

            $document = $this->decode($payload);
            $current = $document->getAttribute($attribute);
            $current = \is_numeric($current) ? $current + 0 : 0;

            // Mirrors MariaDB's bound semantics — silent no-op when bounds
            // exclude the row. Caller has pre-adjusted bounds by $value.
            if (! \is_null($min) && $current < $min) {
                return true;
            }
            if (! \is_null($max) && $current > $max) {
                return true;
            }

            $document->setAttribute($attribute, $current + $value);
            $document->setAttribute('$updatedAt', $updatedAt);

            $r->set($docKey, $this->encode($document));

            $this->journal('updateDoc', [
                'col' => $collection,
                'id' => $id,
                'newId' => $id,
                'before' => $payload,
            ]);

            return true;
        });
    }

    // === @architect:T30 end ===





    // === @architect:T40 owns: indexes + queries + counts ===

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1): bool
    {
        throw new \LogicException('owned by T40');
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        throw new \LogicException('owned by T40');
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        throw new \LogicException('owned by T40');
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        throw new \LogicException('owned by T40');
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        throw new \LogicException('owned by T40');
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        throw new \LogicException('owned by T40');
    }

    public function getSchemaIndexes(string $collection): array
    {
        throw new \LogicException('owned by T40');
    }

    public function getCountOfIndexes(Document $collection): int
    {
        throw new \LogicException('owned by T40');
    }

    // === @architect:T40 end ===





    // === @architect:T50 owns: permissions + relationships ===

    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        throw new \LogicException('owned by T50');
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        throw new \LogicException('owned by T50');
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        throw new \LogicException('owned by T50');
    }

    // === @architect:T50 end ===





    // === @architect:T56 owns: transactions + journal ===

    public function startTransaction(): bool
    {
        throw new \LogicException('owned by T56');
    }

    public function commitTransaction(): bool
    {
        throw new \LogicException('owned by T56');
    }

    public function rollbackTransaction(): bool
    {
        throw new \LogicException('owned by T56');
    }

    // === @architect:T56 end ===
}
