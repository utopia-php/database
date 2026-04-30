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

    /** @phpstan-ignore-next-line method.unused */
    private function key(string ...$parts): string
    {
        return self::KEY_PREFIX . self::SEP . \implode(self::SEP, $parts);
    }

    /** @phpstan-ignore-next-line method.unused */
    private function ns(): string
    {
        return self::KEY_PREFIX . self::SEP . $this->getNamespace() . self::SEP . $this->getDatabase();
    }

    /** @phpstan-ignore-next-line method.unused */
    private function encode(Document $document): string
    {
        return \json_encode($document->getArrayCopy(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
    }

    /** @phpstan-ignore-next-line method.unused */
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

    /** @phpstan-ignore-next-line method.unused */
    private function writePermissions(string $collection, string $id, Document $document): void
    {
        throw new \LogicException('owned by T50');
    }

    /** @phpstan-ignore-next-line method.unused */
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
        throw new \LogicException('owned by T20');
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function list(): array
    {
        throw new \LogicException('owned by T20');
    }

    public function delete(string $name): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function deleteCollection(string $id): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function analyzeCollection(string $collection): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function getSizeOfCollection(string $collection): int
    {
        throw new \LogicException('owned by T20');
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        throw new \LogicException('owned by T20');
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        throw new \LogicException('owned by T20');
    }

    public function getSchemaAttributes(string $collection): array
    {
        throw new \LogicException('owned by T20');
    }

    public function getCountOfAttributes(Document $collection): int
    {
        throw new \LogicException('owned by T20');
    }

    // === @architect:T20 end ===





    // === @architect:T30 owns: document CRUD + bulk + increase ===

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        throw new \LogicException('owned by T30');
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        throw new \LogicException('owned by T30');
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        throw new \LogicException('owned by T30');
    }

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        throw new \LogicException('owned by T30');
    }

    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        throw new \LogicException('owned by T30');
    }

    public function upsertDocuments(
        Document $collection,
        string $attribute,
        array $changes
    ): array {
        throw new \LogicException('owned by T30');
    }

    public function getSequences(string $collection, array $documents): array
    {
        throw new \LogicException('owned by T30');
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        throw new \LogicException('owned by T30');
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        throw new \LogicException('owned by T30');
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
        throw new \LogicException('owned by T30');
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
