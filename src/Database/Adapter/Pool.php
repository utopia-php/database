<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Pools\Pool as UtopiaPool;

class Pool extends Adapter
{
    /**
     * @var UtopiaPool<covariant Adapter>
     */
    protected UtopiaPool $pool;

    /**
     * @param UtopiaPool<covariant Adapter> $pool The pool to use for connections. Must contain instances of Adapter.
     * @throws DatabaseException
     */
    public function __construct(UtopiaPool $pool)
    {
        $this->pool = $pool;

        $this->pool->use(function (mixed $resource) {
            if (!($resource instanceof Adapter)) {
                throw new DatabaseException('Pool must contain instances of ' . Adapter::class);
            }

            // Run setters in case the pooled adapter has its own config
            $this->setDatabase($resource->getDatabase());
            $this->setNamespace($resource->getNamespace());
            $this->setSharedTables($resource->getSharedTables());
            $this->setTenant($resource->getTenant());

            if ($resource->getTimeout() > 0) {
                $this->setTimeout($resource->getTimeout());
            }
            $this->resetDebug();
            foreach ($resource->getDebug() as $key => $value) {
                $this->setDebug($key, $value);
            }
            $this->resetMetadata();
            foreach ($resource->getMetadata() as $key => $value) {
                $this->setMetadata($key, $value);
            }
        });
    }

    /**
     * Forward method calls to the internal adapter instance via the pool.
     *
     * Required because __call() can't be used to implement abstract methods.
     *
     * @param string $method
     * @param array<mixed> $args
     * @return mixed
     * @throws DatabaseException
     */
    public function delegate(string $method, array $args): mixed
    {
        return $this->pool->use(function (Adapter $adapter) use ($method, $args) {
            // Run setters in case config changed since this connection was last used
            $adapter->setDatabase($this->getDatabase());
            $adapter->setNamespace($this->getNamespace());
            $adapter->setSharedTables($this->getSharedTables());
            $adapter->setTenant($this->getTenant());

            if ($this->getTimeout() > 0) {
                $adapter->setTimeout($this->getTimeout());
            }
            $adapter->resetDebug();
            foreach ($this->getDebug() as $key => $value) {
                $adapter->setDebug($key, $value);
            }
            $adapter->resetMetadata();
            foreach ($this->getMetadata() as $key => $value) {
                $adapter->setMetadata($key, $value);
            }

            return $adapter->{$method}(...$args);
        });
    }

    public function before(string $event, string $name = '', ?callable $callback = null): static
    {
        $this->delegate(__FUNCTION__, \func_get_args());

        return $this;
    }

    protected function trigger(string $event, mixed $query): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function startTransaction(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function commitTransaction(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function rollbackTransaction(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    protected function quote(string $string): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function ping(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function reconnect(): void
    {
        $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function create(string $name): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function list(): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function delete(string $name): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteCollection(string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function analyzeCollection(string $collection): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = []): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSizeOfCollection(string $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getLimitForString(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getLimitForInt(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getLimitForAttributes(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getLimitForIndexes(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getMaxIndexLength(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getMaxUIDLength(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getMinDateTime(): \DateTime
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSchemas(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForAttributes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSchemaAttributes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForIndex(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForIndexArray(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForCastIndexArray(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForUniqueIndex(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForFulltextIndex(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForFulltextWildcardIndex(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForCasting(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForQueryContains(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForTimeouts(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForRelationships(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForUpdateLock(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForBatchOperations(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForAttributeResizing(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForOperators(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForGetConnectionId(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForUpserts(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForVectors(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForCacheSkipOnFailure(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForReconnection(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForHostname(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForBatchCreateAttributes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSpatialAttributes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSpatialIndexNull(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getCountOfAttributes(Document $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getCountOfIndexes(Document $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getCountOfDefaultAttributes(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getCountOfDefaultIndexes(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getDocumentSizeLimit(): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getAttributeWidth(Document $collection): int
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getKeywords(): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, float|int $value, string $updatedAt, float|int|null $min = null, float|int|null $max = null): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getConnectionId(): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getInternalIndexesKeys(): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSchemaAttributes(string $collection): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    protected function execute(mixed $stmt): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getIdAttributeType(): string
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSequences(string $collection, array $documents): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSpatialIndexOrder(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForSpatialAxisOrder(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForMultipleFulltextIndexes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForIdenticalIndexes(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForOrderRandom(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function decodePoint(string $wkb): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function decodeLinestring(string $wkb): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function decodePolygon(string $wkb): array
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForObject(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForIndexObject(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForInternalCasting(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForUTCCasting(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function setUTCDatetime(string $value): mixed
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function setSupportForAttributes(bool $support): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForIntegerBooleans(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }

    public function getSupportForAlterLocks(): bool
    {
        return $this->delegate(__FUNCTION__, \func_get_args());
    }
}
