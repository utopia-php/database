<?php

declare(strict_types=1);

namespace Utopia\Database\Adapter;

use Redis as RedisClient;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Redis\Write;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

/**
 * Redis-backed adapter mirroring the Memory adapter's surface.
 *
 * Storage key schema (every key is prefixed with `KEY_PREFIX:`):
 *
 *     {ns}:dbs                                | SET  | database names
 *     {ns}:{db}:cols                          | SET  | collection IDs
 *     {ns}:{db}:meta:{col}                    | HASH | schema/attrs/indexes
 *     {ns}:{db}:doc:{col}:{id}                | STRING | JSON Document
 *     {ns}:{db}:idx:{col}                     | SET  | doc IDs in collection
 *     {ns}:{db}:perm:{col}:{letter}:{role}    | SET  | doc IDs by action+role
 *     {ns}:{db}:perm:doc:{col}:{id}           | HASH | role -> csv letters
 *     {ns}:{db}:grants:{col}                  | SET  | perm keys written for the collection
 *
 * Shared-tables variants bucket on tenant under `t:{tenant}` segments.
 */
class Redis extends Adapter implements
    Feature\Relationships,
    Feature\Upserts,
    Feature\ConnectionId
{
    public const string KEY_PREFIX = 'utopia';

    public const string SEP = ':';

    private const int SCAN_BATCH_SIZE = 500;

    private const int JSON_DECODE_DEPTH = 512;

    private RedisClient $client;

    /**
     * @var array<int, array<int, array{op: string, payload: array<string, mixed>}>>
     */
    private array $journalStack = [];

    public function __construct(RedisClient $client)
    {
        $this->client = $client;
    }

    public function getDriver(): mixed
    {
        return 'redis';
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
            Capability::Operators,
            Capability::OrderRandom,
            Capability::DefinedAttributes,
            Capability::NestedTransactions,
            Capability::PCRE,
            Capability::Regex,
        ]);
    }

    public function ping(): bool
    {
        return (bool) $this->client->ping();
    }

    public function reconnect(): void
    {
    }

    public function startTransaction(): bool
    {
        $this->journalStack[] = [];
        $this->inTransaction++;

        return true;
    }

    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        $frame = \array_pop($this->journalStack);
        if ($frame !== null && $frame !== [] && $this->journalStack !== []) {
            $outerIndex = \count($this->journalStack) - 1;
            \array_push($this->journalStack[$outerIndex], ...$frame);
        }
        $this->inTransaction--;

        return true;
    }

    public function rollbackTransaction(): bool
    {
        if ($this->inTransaction === 0) {
            return false;
        }

        try {
            $this->rollbackJournal();
            $this->inTransaction--;
        } catch (\Throwable $error) {
            $this->inTransaction = 0;
            $this->journalStack = [];

            throw $error;
        }

        return true;
    }

    public function create(string $name): bool
    {
        $name = $this->filter($name);
        $dbsKey = $this->key($this->nsBase(), 'dbs');

        $this->tx(fn (RedisClient $client) => $client->sAdd($dbsKey, $name));

        return true;
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);
        $dbsKey = $this->key($this->nsBase(), 'dbs');

        if ((bool) $this->client->sIsMember($dbsKey, $database) === false) {
            return false;
        }

        if ($collection === null) {
            return true;
        }

        $collection = $this->filter($collection);
        $namespace = $this->getNamespace();
        $colsKey = $this->key($this->nsFor($namespace, $database), 'cols');

        return (bool) $this->client->sIsMember($colsKey, $collection);
    }

    public function list(): array
    {
        $dbsKey = $this->key($this->nsBase(), 'dbs');
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
        $dbsKey = $this->key($this->nsBase(), 'dbs');
        $colsKey = $this->key($this->nsFor($namespace, $name), 'cols');

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

    /**
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     */
    public function createCollection(string $collection, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->filter($collection);
        $colsKey = $this->key($this->ns(), 'cols');
        $metaKey = $this->key($this->ns(), 'meta', $id);
        $idxKey = $this->idxKey($id);

        if ((bool) $this->client->exists($metaKey)) {
            throw new DuplicateException('Collection already exists');
        }

        $attributePayload = [];
        foreach ($attributes as $attribute) {
            $attributePayload[] = self::attributeRecord($attribute->key, $attribute);
        }

        $indexPayload = [];
        foreach ($indexes as $index) {
            $indexPayload[] = [
                Document::ID => $index->key,
                'key' => $index->key,
                'type' => $index->type->value,
                'attributes' => $index->attributes,
                'lengths' => $index->lengths,
                'orders' => self::orderValues($index),
            ];
        }

        $schema = new Document([
            Document::ID => $id,
            'name' => $collection,
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
            $client->del($idxKey);
            $client->sAdd($colsKey, $id);
        });

        return true;
    }

/**
     * @return array<string, mixed>
     */
    private static function attributeRecord(string $id, Attribute $attribute): array
    {
        return [
            Document::ID => $id,
            'key' => $id,
            'type' => Attribute::storedType($attribute->type),
            'size' => $attribute->size ?? 0,
            'signed' => $attribute->signed,
            'array' => $attribute->array,
            'required' => $attribute->required,
        ];
    }

    /**
     * @return list<?string>
     */
    private static function orderValues(Index $index): array
    {
        return \array_map(static fn (?OrderDirection $order): ?string => $order?->value, $index->orders);
    }

        public function deleteCollection(string $id): bool
    {
        $id = $this->filter($id);
        $namespace = $this->getNamespace();
        $database = $this->getDatabase();
        $colsKey = $this->key($this->ns(), 'cols');

        $this->tx(function (RedisClient $client) use ($id, $namespace, $database, $colsKey): void {
            $this->purgeCollectionKeys($client, $namespace, $database, $id);
            $client->sRem($colsKey, $id);
        });

        return true;
    }

    public function analyzeCollection(string $collection): bool
    {
        return false;
    }

    public function getSizeOfCollection(string $collection): int
    {
        return $this->computeCollectionSize($collection);
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        return $this->computeCollectionSize($collection);
    }

    public function createAttribute(string $collection, Attribute $attribute): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($attribute->key);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        $record = self::attributeRecord($id, $attribute);

        $this->tx(function (RedisClient $client) use ($metaKey, $record): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $attrs = $this->upsertAttributeRecord($attrs, $record);
            $client->hSet($metaKey, 'attrs', \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        return true;
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    public function createAttributes(string $collection, array $attributes): bool
    {
        foreach ($attributes as $attribute) {
            $this->createAttribute($collection, $attribute);
        }

        return true;
    }

    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($key);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        if ($attribute->key !== $key) {
            $this->renameAttribute($collection, $id, $attribute->key);
            $id = $this->filter($attribute->key);
        }

        $record = self::attributeRecord($id, $attribute);

        $this->tx(function (RedisClient $client) use ($metaKey, $record): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $attrs = $this->upsertAttributeRecord($attrs, $record);
            $client->hSet($metaKey, 'attrs', \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        return true;
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return true;
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $id): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $filtered = [];
            foreach ($attrs as $attribute) {
                $existingId = $this->recordIdentifier($attribute);
                if ($this->filter($existingId) === $id) {
                    continue;
                }
                $filtered[] = $attribute;
            }
            $client->hSet($metaKey, 'attrs', \json_encode($filtered, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        $this->dropDocumentField($collection, $id);

        return true;
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $old, $new): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $touched = false;
            foreach ($attrs as $i => $attribute) {
                $existingId = $this->recordIdentifier($attribute);
                if ($this->filter($existingId) !== $old) {
                    continue;
                }
                $attribute[Document::ID] = $new;
                $attribute['key'] = $new;
                $attrs[$i] = $attribute;
                $touched = true;
            }
            if (! $touched) {
                return;
            }
            $client->hSet($metaKey, 'attrs', \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        $this->renameDocumentField($collection, $old, $new);

        return true;
    }

    #[\Override]
    public function createRelationship(Relationship $relationship): bool
    {
        $collection = $relationship->getSourceCollection();
        $relatedCollection = $relationship->getRelatedCollection();
        $id = $relationship->getKey();
        $twoWayKey = $relationship->getTwoWayKey();
        $twoWay = $relationship->isTwoWay();

        switch ($relationship->getType()) {
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
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    #[\Override]
    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        $collection = $relationship->getSourceCollection();
        $relatedCollection = $relationship->getRelatedCollection();
        $key = $this->filter($relationship->getKey());
        $twoWayKey = $this->filter($relationship->getTwoWayKey());
        $newKey = $newKey !== null ? $this->filter($newKey) : null;
        $newTwoWayKey = $newTwoWayKey !== null ? $this->filter($newTwoWayKey) : null;
        $side = $relationship->getSide();
        $twoWay = $relationship->isTwoWay();

        switch ($relationship->getType()) {
            case RelationType::OneToOne:
                if ($newKey !== null && $newKey !== $key) {
                    $this->renameAttribute($collection, $key, $newKey);
                }
                if ($twoWay && $newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                    $this->renameAttribute($relatedCollection, $twoWayKey, $newTwoWayKey);
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameAttribute($relatedCollection, $twoWayKey, $newTwoWayKey);
                    }
                } else {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameAttribute($collection, $key, $newKey);
                    }
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Child) {
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameAttribute($relatedCollection, $twoWayKey, $newTwoWayKey);
                    }
                } else {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameAttribute($collection, $key, $newKey);
                    }
                }
                break;
            case RelationType::ManyToMany:
                $junction = $this->resolveJunctionCollection($collection, $relatedCollection, $side);
                if ($junction !== null) {
                    if ($newKey !== null && $newKey !== $key) {
                        $this->renameAttribute($junction, $key, $newKey);
                    }
                    if ($newTwoWayKey !== null && $newTwoWayKey !== $twoWayKey) {
                        $this->renameAttribute($junction, $twoWayKey, $newTwoWayKey);
                    }
                }
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    #[\Override]
    public function deleteRelationship(Relationship $relationship): bool
    {
        $collection = $relationship->getSourceCollection();
        $relatedCollection = $relationship->getRelatedCollection();
        $key = $this->filter($relationship->getKey());
        $twoWayKey = $this->filter($relationship->getTwoWayKey());
        $twoWay = $relationship->isTwoWay();
        $side = $relationship->getSide();

        switch ($relationship->getType()) {
            case RelationType::OneToOne:
                if ($side === RelationSide::Parent) {
                    $this->deleteAttribute($collection, $key);
                    if ($twoWay) {
                        $this->deleteAttribute($relatedCollection, $twoWayKey);
                    }
                } else {
                    $this->deleteAttribute($relatedCollection, $twoWayKey);
                    if ($twoWay) {
                        $this->deleteAttribute($collection, $key);
                    }
                }
                break;
            case RelationType::OneToMany:
                if ($side === RelationSide::Parent) {
                    $this->deleteAttribute($relatedCollection, $twoWayKey);
                } else {
                    $this->deleteAttribute($collection, $key);
                }
                break;
            case RelationType::ManyToOne:
                if ($side === RelationSide::Parent) {
                    $this->deleteAttribute($collection, $key);
                } else {
                    $this->deleteAttribute($relatedCollection, $twoWayKey);
                }
                break;
            case RelationType::ManyToMany:
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        return true;
    }

    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($index->key);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        $type = $index->type->value;
        $attributes = $index->attributes;
        $lengths = $index->lengths;
        $orders = self::orderValues($index);

        $this->tx(function (RedisClient $client) use ($metaKey, $collection, $id, $type, $attributes, $lengths, $orders): void {
            $indexes = $this->readIndexesField($client, $metaKey);

            foreach ($indexes as $existing) {
                if (($existing[Document::ID] ?? $existing['key'] ?? null) === $id) {
                    throw new DuplicateException('Index already exists');
                }
            }

            if ($type === IndexType::Unique->value && ! empty($attributes)) {
                $idxKey = $this->idxKey($collection);
                /** @var array<int, string>|false $docIds */
                $docIds = $client->sMembers($idxKey);
                if (\is_array($docIds) && $docIds !== []) {
                    $sharedTables = $this->getSharedTables();
                    $currentTenant = $sharedTables ? $this->getTenant() : null;
                    $docKeys = [];
                    foreach ($docIds as $docId) {
                        $docKeys[] = $this->docKey($collection, (string) $docId);
                    }
                    /** @var array<int, mixed> $payloads */
                    $payloads = $client->mGet($docKeys);
                    $seen = [];
                    foreach ($payloads as $payload) {
                        if (! \is_string($payload)) {
                            continue;
                        }
                        $document = $this->decode($payload);
                        if ($sharedTables) {
                            $rowTenant = $document->getAttribute(Document::TENANT);
                            if ($rowTenant !== $currentTenant) {
                                continue;
                            }
                        }
                        $signature = [];
                        $hasNull = false;
                        foreach ($attributes as $attribute) {
                            $value = $this->resolveDocumentAttribute($document, (string) $attribute);
                            if ($value === null) {
                                $hasNull = true;
                                break;
                            }
                            $signature[] = $this->normalizeIndexValue($value);
                        }
                        if ($hasNull) {
                            continue;
                        }
                        if ($sharedTables) {
                            \array_unshift($signature, $currentTenant);
                        }
                        $hash = \serialize($signature);
                        if (isset($seen[$hash])) {
                            throw new DuplicateException('Cannot create unique index: existing rows already contain duplicate values');
                        }
                        $seen[$hash] = true;
                    }
                }
            }

            $indexes[] = [
                Document::ID => $id,
                'key' => $id,
                'type' => $type,
                'attributes' => $attributes,
                'lengths' => $lengths,
                'orders' => $orders,
            ];

            $client->hSet($metaKey, 'indexes', \json_encode($indexes, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        return true;
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        $collection = $this->filter($collection);
        $id = $this->filter($id);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return true;
        }

        $this->tx(function (RedisClient $client) use ($metaKey, $id): void {
            $indexes = $this->readIndexesField($client, $metaKey);
            $filtered = [];
            foreach ($indexes as $index) {
                if (($index[Document::ID] ?? $index['key'] ?? null) === $id) {
                    continue;
                }
                $filtered[] = $index;
            }
            $client->hSet($metaKey, 'indexes', \json_encode($filtered, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        return true;
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->filter($collection);
        $old = $this->filter($old);
        $new = $this->filter($new);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        return $this->tx(function (RedisClient $client) use ($metaKey, $old, $new): bool {
            $indexes = $this->readIndexesField($client, $metaKey);
            $ids = \array_map(static fn (array $index): mixed => $index[Document::ID] ?? $index['key'] ?? null, $indexes);
            $position = \array_search($old, $ids, true);
            if ($position === false) {
                return \in_array($new, $ids, true);
            }
            $indexes[$position][Document::ID] = $new;
            $indexes[$position]['key'] = $new;
            $client->hSet($metaKey, 'indexes', \json_encode(\array_values($indexes), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));

            return true;
        }) === true;
    }

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $col = $this->filter($collection->getId());
        $payload = $this->client->get($this->docKey($col, $id));

        if ((! \is_string($payload) || $payload === '') && $this->getSharedTables() && $col === Database::METADATA) {
            $payload = $this->client->get($this->docKey($col, $id, '_'));
        }

        if (! \is_string($payload) || $payload === '') {
            return new Document([]);
        }

        $document = $this->decode($payload);

        if ($this->getSharedTables()) {
            $rowTenant = $document->getAttribute(Document::TENANT);
            $tenant = $this->getTenant();
            $allowNullTenant = $col === Database::METADATA && $rowTenant === null;
            if (! $allowNullTenant && $rowTenant !== $tenant) {
                return new Document([]);
            }
        }

        if ($col !== Database::METADATA) {
            $document = $this->surfaceRelationshipAttributes($col, $document);
        }

        $selections = $this->extractSelections($queries);
        if (! empty($selections) && ! \in_array('*', $selections, true)) {
            $document = $this->projectDocument($document, $selections);
        }

        return $document;
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        return $this->insertDocument($collection, $document) ?? $document;
    }

    /**
     * @return Document|null The stored document, or null when skipDuplicates() skipped it
     */
    private function insertDocument(Document $collection, Document $document): ?Document
    {
        $col = $this->filter($collection->getId());
        $id = $document->getId();
        if ($id === '') {
            $id = ID::unique();
            $document->setAttribute(Document::ID, $id);
        }
        $tenant = $document->getTenant();
        $docKey = $this->docKey($col, $id, $tenant);
        $idxKey = $this->idxKey($col, $tenant);
        $seqKey = $this->seqKey($col, $tenant);
        $permDocKey = $this->permDocKey($col, $id, $tenant);

        return $this->tx(function (RedisClient $redis) use ($col, $id, $document, $docKey, $idxKey, $seqKey, $permDocKey): ?Document {
            if ((bool) $redis->exists($docKey)) {
                if ($this->skippingDuplicates()) {
                    $existingPayload = $redis->get($docKey);
                    if (\is_string($existingPayload) && $existingPayload !== '') {
                        $existing = $this->decode($existingPayload);
                        $document->setAttribute(Document::SEQUENCE, $existing->getSequence() ?? '');
                    }

                    return null;
                }
                throw new DuplicateException('Document already exists');
            }

            try {
                $this->enforceUniqueIndexes($redis, $col, $document);
            } catch (DuplicateException $e) {
                if ($this->skippingDuplicates()) {
                    return null;
                }
                throw $e;
            }

            $sequence = $document->getSequence();
            if (empty($sequence)) {
                $next = $redis->incr($seqKey);
                $sequence = (string) $next;
            } else {
                $sequence = (string) $sequence;
                $current = $redis->get($seqKey);
                if (! \is_string($current) || (int) $sequence > (int) $current) {
                    $redis->set($seqKey, $sequence);
                }
            }
            $document->setAttribute(Document::SEQUENCE, $sequence);

            $redis->set($docKey, $this->encode($document));
            $redis->sAdd($idxKey, \strtolower($id));

            $this->writePermissions($col, $id, $document);
            $this->journal('createDoc', [
                'collection' => $col,
                'id' => $id,
                'docKey' => $docKey,
                'idxKey' => $idxKey,
                'permDocKey' => $permDocKey,
            ]);

            return $document;
        });
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        $created = [];
        foreach ($documents as $document) {
            $inserted = $this->insertDocument($collection, $document);
            if ($inserted !== null) {
                $created[] = $inserted;
            }
        }

        return $created;
    }

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $col = $this->filter($collection->getId());
        $oldKey = $this->docKey($col, $id);
        $idxKey = $this->idxKey($col);

        $useNullTenant = false;
        if ($col === Database::METADATA && $this->getSharedTables() && $this->getTenant() !== null) {
            if ((bool) $this->client->exists($oldKey) === false) {
                $oldKey = $this->docKey($col, $id, '_');
                $useNullTenant = true;
            }
        }

        return $this->tx(function (RedisClient $redis) use ($col, $id, $document, $skipPermissions, $oldKey, $idxKey, $useNullTenant): Document {
            $existingPayload = $redis->get($oldKey);
            if (! \is_string($existingPayload) || $existingPayload === '') {
                throw new NotFoundException('Document not found');
            }

            $existing = $this->decode($existingPayload);
            if ($col !== Database::METADATA) {
                $existing = $this->surfaceRelationshipAttributes($col, $existing);
            }
            $newId = $document->getId() !== '' ? $document->getId() : $id;
            $newKey = $useNullTenant ? $this->docKey($col, $newId, '_') : $this->docKey($col, $newId);
            $effectiveIdxKey = $useNullTenant ? $this->idxKey($col, '_') : $idxKey;

            if ($newKey !== $oldKey && (bool) $redis->exists($newKey)) {
                throw new DuplicateException('Document already exists');
            }

            $resolved = $this->applyOperators($document->getArrayCopy(), $existing->getArrayCopy());
            $merged = \array_merge($existing->getArrayCopy(), $resolved);
            $merged[Document::ID] = $newId;
            $mergedDocument = new Document($merged);

            $this->enforceUniqueIndexes($redis, $col, $mergedDocument, $id);

            $payload = $this->encode($mergedDocument);

            if ($newId !== $id) {
                $redis->del($oldKey);
                $redis->sRem($effectiveIdxKey, \strtolower($id));
            }
            $redis->set($newKey, $payload);
            $redis->sAdd($effectiveIdxKey, \strtolower($newId));

            $this->journal('updateDoc', [
                'collection' => $col,
                'id' => $id,
                'newId' => $newId,
                'payload' => $existingPayload,
                'docKey' => $oldKey,
                'newDocKey' => $newKey,
                'idxKey' => $effectiveIdxKey,
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
        $hasPermissions = $updates->offsetExists(Document::PERMISSIONS);
        if (empty($attrs) && ! $hasCreatedAt && ! $hasUpdatedAt && ! $hasPermissions) {
            return 0;
        }

        $col = $this->filter($collection->getId());
        $documents = \array_values($documents);

        return $this->tx(function (RedisClient $redis) use ($col, $documents, $updates, $attrs, $hasCreatedAt, $hasUpdatedAt, $hasPermissions): int {
            $docKeys = [];
            foreach ($documents as $doc) {
                $docKeys[] = $this->docKey($col, $doc->getId());
            }

            $redis->multi(\Redis::PIPELINE);
            foreach ($docKeys as $docKey) {
                $redis->get($docKey);
            }
            $existingPayloads = $redis->exec();
            if (! \is_array($existingPayloads)) {
                $existingPayloads = [];
            }

            $relationshipKeys = [];
            if ($col !== Database::METADATA) {
                $metaKey = $this->key($this->ns(), 'meta', $col);
                $attributes = $this->readAttributesField($redis, $metaKey);
                $relationshipKeys = $this->extractRelationshipKeys($attributes);
            }

            $writes = [];
            foreach ($documents as $i => $doc) {
                $existingPayload = $existingPayloads[$i] ?? false;
                if (! \is_string($existingPayload) || $existingPayload === '') {
                    continue;
                }

                $existing = $this->decode($existingPayload);
                if (! empty($relationshipKeys)) {
                    $existing = $this->surfaceRelationshipAttributesUsing($relationshipKeys, $existing);
                }
                $merged = $existing->getArrayCopy();
                $resolved = $this->applyOperators($attrs, $merged);
                foreach ($resolved as $attribute => $value) {
                    $merged[$attribute] = $value;
                }
                if ($hasCreatedAt) {
                    $merged[Document::CREATED_AT] = $updates->getCreatedAt();
                }
                if ($hasUpdatedAt) {
                    $merged[Document::UPDATED_AT] = $updates->getUpdatedAt();
                }
                if ($hasPermissions) {
                    $merged[Document::PERMISSIONS] = $updates->getPermissions();
                }

                $writes[] = new Write($doc->getId(), $docKeys[$i], $existingPayload, new Document($merged));
            }

            if ($attrs !== []) {
                $this->enforceUniqueIndexesForDocuments(
                    $redis,
                    $col,
                    \array_map(static fn (Write $write): Document => $write->document, $writes),
                    \array_map(static fn (Write $write): string => $write->id, $writes),
                );
            }

            foreach ($writes as $write) {
                $redis->set($write->key, $this->encode($write->document));

                $this->journal('updateDoc', [
                    'collection' => $col,
                    'id' => $write->id,
                    'newId' => $write->id,
                    'payload' => $write->payload,
                    'docKey' => $write->key,
                ]);

                if ($hasPermissions) {
                    $this->clearPermissions($col, $write->id);
                    $this->writePermissions($col, $write->id, $write->document);
                }
            }

            return \count($writes);
        });
    }

    #[\Override]
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        if (empty($changes)) {
            return $changes;
        }

        $col = $this->filter($collection->getId());

        return $this->tx(function (RedisClient $redis) use ($col, $attribute, $changes): array {
            $results = [];

            $redis->multi(\Redis::PIPELINE);
            foreach ($changes as $change) {
                $document = $change->getNew();
                $redis->get($this->docKey($col, $document->getId(), $document->getTenant()));
            }
            $existingPayloads = $redis->exec();
            if (! \is_array($existingPayloads)) {
                $existingPayloads = [];
            }

            $relationshipKeys = [];
            if ($col !== Database::METADATA) {
                $metaKey = $this->key($this->ns(), 'meta', $col);
                $attributes = $this->readAttributesField($redis, $metaKey);
                $relationshipKeys = $this->extractRelationshipKeys($attributes);
            }

            $writes = [];
            foreach ($changes as $i => $change) {
                $document = $change->getNew();
                $id = $document->getId();
                $existingPayload = $existingPayloads[$i] ?? false;

                if (! \is_string($existingPayload) || $existingPayload === '') {
                    $writes[] = new Write($id, $this->docKey($col, $id, $document->getTenant()), null, $document, $document->getTenant());

                    continue;
                }

                $existing = $this->decode($existingPayload);
                if (! empty($relationshipKeys)) {
                    $existing = $this->surfaceRelationshipAttributesUsing($relationshipKeys, $existing);
                }
                $existingArray = $existing->getArrayCopy();
                $resolved = $this->applyOperators($document->getArrayCopy(), $existingArray);
                $merged = \array_merge($existingArray, $resolved);
                $merged[Document::ID] = $id;

                if ($attribute !== '') {
                    $previous = $existing->getAttribute($attribute);
                    $delta = $document->getAttribute($attribute);
                    $previousNumeric = \is_numeric($previous) ? $previous + 0 : 0;
                    $deltaNumeric = \is_numeric($delta) ? $delta + 0 : 0;
                    $merged[$attribute] = $previousNumeric + $deltaNumeric;
                }

                $writes[] = new Write($id, $this->docKey($col, $id, $document->getTenant()), $existingPayload, new Document($merged), $document->getTenant());
            }

            $this->enforceUniqueIndexesInOrder($redis, $col, $writes);

            foreach ($writes as $write) {
                $id = $write->id;
                $document = $write->document;
                $tenant = $write->tenant;

                if ($write->payload !== null) {
                    $redis->set($write->key, $this->encode($document));

                    $this->journal('updateDoc', [
                        'collection' => $col,
                        'id' => $id,
                        'newId' => $id,
                        'payload' => $write->payload,
                        'docKey' => $write->key,
                    ]);

                    $this->clearPermissions($col, $id, $tenant);
                    $this->writePermissions($col, $id, $document);

                    $results[] = $document;

                    continue;
                }

                $idxKey = $this->idxKey($col, $tenant);
                $seqKey = $this->seqKey($col, $tenant);
                $sequence = $document->getSequence();
                if (empty($sequence)) {
                    $next = $redis->incr($seqKey);
                    $sequence = (string) $next;
                } else {
                    $sequence = (string) $sequence;
                    $current = $redis->get($seqKey);
                    if (! \is_string($current) || (int) $sequence > (int) $current) {
                        $redis->set($seqKey, $sequence);
                    }
                }
                $document->setAttribute(Document::SEQUENCE, $sequence);

                $resolved = $this->applyOperators($document->getArrayCopy(), []);
                foreach ($resolved as $attr => $value) {
                    $document->setAttribute($attr, $value);
                }

                $redis->set($write->key, $this->encode($document));
                $redis->sAdd($idxKey, \strtolower($id));

                $this->writePermissions($col, $id, $document);
                $this->journal('createDoc', [
                    'collection' => $col,
                    'id' => $id,
                    'docKey' => $write->key,
                    'idxKey' => $idxKey,
                    'permDocKey' => $this->permDocKey($col, $id, $tenant),
                ]);

                $results[] = $document;
            }

            return $results;
        });
    }

    public function getSequences(string $collection, array $documents): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $col = $this->filter($collection);

        $this->client->multi(\Redis::PIPELINE);
        try {
            $indexes = [];
            foreach ($documents as $index => $doc) {
                if (! empty($doc->getSequence())) {
                    continue;
                }
                $this->client->get($this->docKey($col, $doc->getId(), $doc->getTenant()));
                $indexes[] = $index;
            }
            if ($indexes === []) {
                try {
                    $this->client->discard();
                } catch (\Throwable) {
                    // PIPELINE-mode discard is version-dependent across phpredis.
                }

                return $documents;
            }
            $payloads = $this->client->exec();
        } catch (\Throwable $e) {
            try {
                $this->client->discard();
            } catch (\Throwable) {
                // PIPELINE-mode discard is version-dependent across phpredis.
            }
            throw new TransactionException('Failed to load sequences: '.$e->getMessage(), 0, $e);
        }
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
                $documents[$index]->setAttribute(Document::SEQUENCE, (string) $sequence);
            }
        }

        return $documents;
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        $collection = $this->filter($collection);
        $docKey = $this->docKey($collection, $id);
        $idxKey = $this->idxKey($collection);

        return $this->tx(function (RedisClient $redis) use ($collection, $id, $docKey, $idxKey): bool {
            $payload = $redis->get($docKey);
            if (! \is_string($payload) || $payload === '') {
                return false;
            }

            $this->journal('deleteDoc', [
                'collection' => $collection,
                'id' => $id,
                'payload' => $payload,
                'docKey' => $docKey,
                'idxKey' => $idxKey,
            ]);

            $this->clearPermissions($collection, $id);
            $redis->del($docKey);
            $redis->sRem($idxKey, \strtolower($id));

            return true;
        });
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        if (empty($sequences) && empty($permissionIds)) {
            return 0;
        }

        $collection = $this->filter($collection);
        $idxKey = $this->idxKey($collection);

        return $this->tx(function (RedisClient $redis) use ($collection, $sequences, $permissionIds, $idxKey): int {
            $sequenceSet = [];
            foreach ($sequences as $sequence) {
                $sequenceSet[(string) $sequence] = true;
            }

            $allIds = $redis->sMembers($idxKey);
            if (! \is_array($allIds)) {
                $allIds = [];
            }

            $docKeys = [];
            $redis->multi(\Redis::PIPELINE);
            foreach ($allIds as $id) {
                $docKey = $this->docKey($collection, (string) $id);
                $docKeys[(string) $id] = $docKey;
                $redis->get($docKey);
            }
            $payloads = $redis->exec();
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
                    $deleted[$document->getId()] = ['payload' => $payload, 'docKey' => $docKeys[(string) $id]];
                }
            }

            foreach ($deleted as $documentId => $deleteEntry) {
                $deletedDocKey = $deleteEntry['docKey'];
                $this->journal('deleteDoc', [
                    'collection' => $collection,
                    'id' => (string) $documentId,
                    'payload' => $deleteEntry['payload'],
                    'docKey' => $deletedDocKey,
                    'idxKey' => $idxKey,
                ]);
                $this->clearPermissions($collection, (string) $documentId);
                $redis->del($deletedDocKey);
                $redis->sRem($idxKey, \strtolower((string) $documentId));
            }

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

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
    {
        $collectionId = $this->filter($collection->getId());
        $metaKey = $this->key($this->ns(), 'meta', $collectionId);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        return $this->tx(function (RedisClient $client) use ($collectionId, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $forPermission): array {
            $documents = $this->loadCollectionDocuments($client, $collectionId, $forPermission);
            $documents = $this->filterDocumentsByQueries($collectionId, $documents, $queries);
            $documents = $this->orderDocuments($documents, $orderAttributes, $orderTypes, $cursorDirection);
            $documents = $this->cursorDocuments($documents, $orderAttributes, $orderTypes, $cursor, $cursorDirection);

            if (! \is_null($offset)) {
                $documents = \array_slice($documents, $offset);
            }
            if (! \is_null($limit)) {
                $documents = \array_slice($documents, 0, $limit);
            }

            $selections = $this->extractSelections($queries);
            if (! empty($selections)) {
                $projected = [];
                foreach ($documents as $document) {
                    $projected[] = $this->projectDocument($document, $selections);
                }
                $documents = $projected;
            }

            if ($cursorDirection === CursorDirection::Before) {
                $documents = \array_reverse($documents);
            }

            return $documents;
        });
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $collectionId = $this->filter($collection->getId());
        $metaKey = $this->key($this->ns(), 'meta', $collectionId);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        return $this->tx(function (RedisClient $client) use ($collectionId, $attribute, $queries, $max): float|int {
            $documents = $this->loadCollectionDocuments($client, $collectionId, PermissionType::Read);
            $documents = $this->filterDocumentsByQueries($collectionId, $documents, $queries);

            if (! \is_null($max)) {
                $documents = \array_slice($documents, 0, $max);
            }

            $sum = 0;
            $isFloat = false;
            foreach ($documents as $document) {
                $value = $this->resolveDocumentAttribute($document, $attribute);
                if ($value === null) {
                    continue;
                }
                if (\is_float($value)) {
                    $isFloat = true;
                }
                if (\is_numeric($value)) {
                    $sum += $value;
                }
            }

            return $isFloat ? (float) $sum : (int) $sum;
        });
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $collectionId = $this->filter($collection->getId());
        $metaKey = $this->key($this->ns(), 'meta', $collectionId);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        if (
            empty($queries)
            && $this->authorization->getStatus() === false
            && $this->getSharedTables() === false
        ) {
            $idxKey = $this->idxKey($collectionId);
            $cardinality = $this->client->sCard($idxKey);
            if (\is_int($cardinality)) {
                return $max === null ? $cardinality : \min($max, $cardinality);
            }
        }

        return $this->tx(function (RedisClient $client) use ($collectionId, $queries, $max): int {
            $documents = $this->loadCollectionDocuments($client, $collectionId, PermissionType::Read);
            $documents = $this->filterDocumentsByQueries($collectionId, $documents, $queries);

            if (! \is_null($max)) {
                $documents = \array_slice($documents, 0, $max);
            }

            return \count($documents);
        });
    }

    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value, string $updatedAt, int|float|string|null $min = null, int|float|string|null $max = null): bool
    {
        $collection = $this->filter($collection);
        $docKey = $this->docKey($collection, $id);

        return $this->tx(function (RedisClient $redis) use ($collection, $id, $attribute, $value, $updatedAt, $min, $max, $docKey): bool {
            $payload = $redis->get($docKey);
            if (! \is_string($payload) || $payload === '') {
                throw new NotFoundException('Document not found');
            }

            $document = $this->decode($payload);
            $current = $document->getAttribute($attribute);
            $exact = (\is_int($current) || (\is_string($current) && BigInt::isIntegerString($current)))
                && (\is_int($value) || (\is_string($value) && BigInt::isIntegerString($value)));
            if ($exact) {
                $current = BigInt::toNative($current);
                $value = BigInt::toNative($value);
                if (! \is_null($min) && BigInt::compare($current, $min) < 0) {
                    return true;
                }
                if (! \is_null($max) && BigInt::compare($current, $max) > 0) {
                    return true;
                }
                $result = BigInt::add($current, $value);
            } else {
                $current = $this->numericOr($current, 0);
                $value = $this->numericOr($value, 0);
                if (! \is_null($min) && $current < $min) {
                    return true;
                }
                if (! \is_null($max) && $current > $max) {
                    return true;
                }
                $result = $current + $value;
            }

            $document->setAttribute($attribute, $result);
            $document->setAttribute(Document::UPDATED_AT, $updatedAt);

            $redis->set($docKey, $this->encode($document));

            $this->journal('updateDoc', [
                'collection' => $collection,
                'id' => $id,
                'newId' => $id,
                'payload' => $payload,
                'docKey' => $docKey,
            ]);

            return true;
        });
    }

    public function getLimitForString(): int
    {
        return 4294967295;
    }

    public function getLimitForInt(): int
    {
        return 4294967295;
    }

    public function getLimitForBigInt(): int
    {
        return Database::MAX_BIG_INT;
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
        return ColumnType::Integer->value;
    }

    public function getCountOfAttributes(Document $collection): int
    {
        return \count(self::collectionAttributes($collection)) + $this->getCountOfDefaultAttributes();
    }

    public function getCountOfIndexes(Document $collection): int
    {
        return \count(self::collectionIndexes($collection)) + $this->getCountOfDefaultIndexes();
    }

    public function getCountOfDefaultAttributes(): int
    {
        return \count(Database::internalAttributesFor(true));
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

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function setSupportForAttributes(bool $support): bool
    {
        return true;
    }

    #[\Override]
    public function getConnectionId(): string
    {
        return '0';
    }

    protected function execute(mixed $statement): bool
    {
        return true;
    }

    protected function quote(string $string): string
    {
        return '"'.$string.'"';
    }

    private function key(string ...$parts): string
    {
        return \implode(self::SEP, $parts);
    }

    private function ns(): string
    {
        return $this->nsFor($this->getNamespace(), $this->getDatabase());
    }

    private function nsFor(string $namespace, string $database): string
    {
        return self::KEY_PREFIX.self::SEP.$namespace.self::SEP.$database;
    }

    private function nsBase(): string
    {
        return self::KEY_PREFIX.self::SEP.$this->getNamespace();
    }

    private function docKey(string $collection, string $id, int|string|null $tenant = null): string
    {
        $id = \strtolower($id);
        if (! $this->getSharedTables()) {
            return $this->key($this->ns(), 'doc', $collection, $id);
        }

        $bucket = $this->bucketFor($tenant);

        return $this->key($this->ns(), 'doc', 't', $bucket, $collection, $id);
    }

    private function idxKey(string $collection, int|string|null $tenant = null): string
    {
        if (! $this->getSharedTables()) {
            return $this->key($this->ns(), 'idx', $collection);
        }

        return $this->key($this->ns(), 'idx', 't', $this->bucketFor($tenant), $collection);
    }

    private function seqKey(string $collection, int|string|null $tenant = null): string
    {
        if (! $this->getSharedTables()) {
            return $this->key($this->ns(), 'seq', $collection);
        }

        return $this->key($this->ns(), 'seq', 't', $this->bucketFor($tenant), $collection);
    }

    private function bucketFor(int|string|null $tenant): string
    {
        if ($tenant === null) {
            $tenant = $this->getTenant();
        }

        return $tenant === null ? '_' : (string) $tenant;
    }

    private function tenantBucket(int|string|null $tenant = null): ?string
    {
        if (! $this->getSharedTables()) {
            return null;
        }

        return $this->bucketFor($tenant);
    }

    private function permKey(string $collection, string $letter, string $role, int|string|null $tenant = null): string
    {
        $bucket = $this->tenantBucket($tenant);
        if ($bucket !== null) {
            return $this->ns().self::SEP.'perm'.self::SEP.'t'.self::SEP.$bucket.self::SEP.$collection.self::SEP.$letter.self::SEP.$role;
        }

        return $this->ns().self::SEP.'perm'.self::SEP.$collection.self::SEP.$letter.self::SEP.$role;
    }

    private function permDocKey(string $collection, string $id, int|string|null $tenant = null): string
    {
        $id = \strtolower($id);
        $bucket = $this->tenantBucket($tenant);
        if ($bucket !== null) {
            return $this->ns().self::SEP.'perm'.self::SEP.'t'.self::SEP.$bucket.self::SEP.'doc'.self::SEP.$collection.self::SEP.$id;
        }

        return $this->ns().self::SEP.'perm'.self::SEP.'doc'.self::SEP.$collection.self::SEP.$id;
    }

    private function encode(Document $document): string
    {
        return \json_encode(
            $document->getArrayCopy(),
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
        );
    }

    private function decode(string $payload): Document
    {
        try {
            /** @var array<string, mixed> $data */
            $data = \json_decode($payload, true, self::JSON_DECODE_DEPTH, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new DatabaseException('Document decode failed: '.$e->getMessage(), 0, $e);
        }

        return Document::fromStorage($data);
    }

    /**
     * @template T
     * @param callable(RedisClient): T $fn
     * @return T
     */
    protected function tx(callable $fn): mixed
    {
        try {
            return $fn($this->client);
        } catch (\RedisException $exception) {
            throw new TransactionException('tx failed: '.$exception->getMessage(), 0, $exception);
        }
    }

    private function writePermissions(string $collection, string $id, Document $document): void
    {
        $id = \strtolower($id);
        $tenant = $document->getTenant();

        $byRole = [];
        foreach ([PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete] as $type) {
            foreach ($document->getPermissionsByType($type) as $role) {
                $byRole[(string) $role][] = $this->actionLetter($type);
            }
        }

        if ($byRole === []) {
            return;
        }

        $hashKey = $this->permDocKey($collection, $id, $tenant);
        $hashFields = [];
        $writes = [];
        foreach ($byRole as $role => $letters) {
            $unique = \array_values(\array_unique($letters));
            \sort($unique);
            $hashFields[$role] = \implode(',', $unique);
            foreach ($unique as $letter) {
                $writes[] = [$role, $letter, $this->permKey($collection, $letter, $role, $tenant)];
            }
        }

        $this->client->multi(\Redis::PIPELINE);
        try {
            foreach ($writes as [, , $setKey]) {
                $this->client->sAdd($setKey, $id);
            }
            $this->client->hMSet($hashKey, $hashFields);
            $this->client->sAdd($this->grantsKey($this->ns(), $collection), $hashKey, ...\array_column($writes, 2));
            $this->client->exec();
        } catch (\Throwable $e) {
            try {
                $this->client->discard();
            } catch (\Throwable) {
                // ignore
            }
            throw $e;
        }

        foreach ($writes as [$role, $letter, $setKey]) {
            $this->journal('createPerm', [
                'collection' => $collection,
                'id' => $id,
                'role' => $role,
                'letter' => $letter,
                'permKey' => $setKey,
                'permDocKey' => $hashKey,
            ]);
        }
    }

    private function clearPermissions(string $collection, string $id, int|string|null $tenant = null): void
    {
        $id = \strtolower($id);
        $hashKey = $this->permDocKey($collection, $id, $tenant);
        /** @var array<string, string>|false $hash */
        $hash = $this->client->hGetAll($hashKey);
        if ($hash === false || $hash === []) {
            return;
        }

        $removals = [];
        foreach ($hash as $role => $letterCsv) {
            if ($letterCsv === '') {
                continue;
            }
            foreach (\explode(',', $letterCsv) as $letter) {
                $removals[] = [$role, $letter, $this->permKey($collection, $letter, $role, $tenant)];
            }
        }

        $this->client->multi(\Redis::PIPELINE);
        try {
            foreach ($removals as [, , $setKey]) {
                $this->client->sRem($setKey, $id);
            }
            $this->client->del($hashKey);
            $this->client->sRem($this->grantsKey($this->ns(), $collection), $hashKey);
            $this->client->exec();
        } catch (\Throwable $e) {
            try {
                $this->client->discard();
            } catch (\Throwable) {
                // ignore
            }
            throw $e;
        }

        foreach ($removals as [$role, $letter, $setKey]) {
            $this->journal('deletePerm', [
                'collection' => $collection,
                'id' => $id,
                'role' => $role,
                'letter' => $letter,
                'previous' => $hash[$role] ?? '',
                'permKey' => $setKey,
                'permDocKey' => $hashKey,
            ]);
        }
    }

    /**
     * @param array<int, string> $ids
     * @return array<int, string>
     */
    private function applyPermissionFilter(string $collection, array $ids, PermissionType $action): array
    {
        if ($ids === []) {
            return $ids;
        }
        if ($this->authorization->getStatus() === false) {
            return $ids;
        }

        $roles = $this->authorization->getRoles();
        if ($roles === []) {
            return [];
        }

        $letter = $this->actionLetter($action);
        $keys = [];
        foreach ($roles as $role) {
            $keys[] = $this->permKey($collection, $letter, $role);
        }

        if (\count($keys) === 1) {
            /** @var array<int, string>|false $allowed */
            $allowed = $this->client->sMembers($keys[0]);
        } else {
            $first = \array_shift($keys);
            /** @var array<int, string>|false $allowed */
            $allowed = $this->client->sUnion($first, ...$keys);
        }
        if ($allowed === false || $allowed === []) {
            return [];
        }

        $allowedSet = \array_flip($allowed);

        return \array_values(\array_filter($ids, static fn (string $id): bool => isset($allowedSet[$id])));
    }

    private function actionLetter(PermissionType $action): string
    {
        return match ($action) {
            PermissionType::Read => 'r',
            PermissionType::Create => 'c',
            PermissionType::Update => 'u',
            PermissionType::Delete => 'd',
            PermissionType::Write => 'w',
        };
    }

    /**
     * @param array<string, mixed> $payload
     */
    protected function journal(string $op, array $payload): void
    {
        if ($this->inTransaction === 0) {
            return;
        }
        $this->journalStack[\count($this->journalStack) - 1][] = [
            'op' => $op,
            'payload' => $payload,
        ];
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function payloadString(array $payload, string $key): ?string
    {
        $value = $payload[$key] ?? null;

        return \is_string($value) ? $value : null;
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function payloadStringOr(array $payload, string $key, string $default): string
    {
        $value = $payload[$key] ?? null;

        return \is_string($value) ? $value : $default;
    }

    private function stringOrEmpty(mixed $value): string
    {
        return \is_string($value) ? $value : '';
    }

    private function numericOr(mixed $value, int|float $default): int|float
    {
        return \is_numeric($value) ? $value + 0 : $default;
    }

    private function intOr(mixed $value, int $default): int
    {
        return \is_numeric($value) ? (int) $value : $default;
    }

    /**
     * @param array<string, mixed> $record
     */
    private function recordIdentifier(array $record): string
    {
        $id = $record[Document::ID] ?? null;
        if (\is_string($id)) {
            return $id;
        }
        $key = $record['key'] ?? null;

        return \is_string($key) ? $key : '';
    }

    protected function rollbackJournal(): void
    {
        $frame = \array_pop($this->journalStack);
        if ($frame === null) {
            return;
        }

        for ($i = \count($frame) - 1; $i >= 0; $i--) {
            $entry = $frame[$i];
            $op = $entry['op'];
            $payload = $entry['payload'];

            switch ($op) {
                case 'createDoc':
                    $collection = $this->payloadStringOr($payload, 'collection', '');
                    $id = $this->payloadStringOr($payload, 'id', '');
                    $this->rawDeleteDoc(
                        $collection,
                        $id,
                        $this->payloadString($payload, 'docKey'),
                        $this->payloadString($payload, 'idxKey'),
                        $this->payloadString($payload, 'permDocKey'),
                    );
                    break;

                case 'deleteDoc':
                    $collection = $this->payloadStringOr($payload, 'collection', '');
                    $id = $this->payloadStringOr($payload, 'id', '');
                    $beforePayload = $this->payloadStringOr($payload, 'payload', '');
                    $this->rawRestoreDoc(
                        $collection,
                        $id,
                        $beforePayload,
                        $this->payloadString($payload, 'docKey'),
                        $this->payloadString($payload, 'idxKey'),
                    );
                    break;

                case 'updateDoc':
                    $collection = $this->payloadStringOr($payload, 'collection', '');
                    $id = $this->payloadStringOr($payload, 'id', '');
                    $beforePayload = $this->payloadStringOr($payload, 'payload', '');
                    $docKey = $this->payloadString($payload, 'docKey') ?? $this->docKey($collection, $id);
                    $this->client->set($docKey, $beforePayload);
                    $newId = $this->payloadString($payload, 'newId');
                    if ($newId !== null && $newId !== $id) {
                        $newDocKey = $this->payloadString($payload, 'newDocKey') ?? $this->docKey($collection, $newId);
                        if ($newDocKey !== $docKey) {
                            $this->client->del($newDocKey);
                        }
                        $idxKey = $this->payloadString($payload, 'idxKey') ?? $this->idxKey($collection);
                        $this->client->sRem($idxKey, \strtolower($newId));
                        $this->client->sAdd($idxKey, \strtolower($id));
                    }
                    break;

                case 'createPerm':
                    $collection = $this->payloadStringOr($payload, 'collection', '');
                    $letter = $this->payloadStringOr($payload, 'letter', '');
                    $role = $this->payloadStringOr($payload, 'role', '');
                    $id = $this->payloadStringOr($payload, 'id', '');
                    $setKey = $this->payloadString($payload, 'permKey') ?? $this->permKey($collection, $letter, $role);
                    $hashKey = $this->payloadString($payload, 'permDocKey') ?? $this->permDocKey($collection, $id);
                    $this->client->sRem($setKey, $id);
                    $this->client->hDel($hashKey, $role);
                    break;

                case 'deletePerm':
                    $collection = $this->payloadStringOr($payload, 'collection', '');
                    $letter = $this->payloadStringOr($payload, 'letter', '');
                    $role = $this->payloadStringOr($payload, 'role', '');
                    $id = $this->payloadStringOr($payload, 'id', '');
                    $setKey = $this->payloadString($payload, 'permKey') ?? $this->permKey($collection, $letter, $role);
                    $hashKey = $this->payloadString($payload, 'permDocKey') ?? $this->permDocKey($collection, $id);
                    $this->client->sAdd($setKey, $id);
                    $previous = $this->payloadString($payload, 'previous');
                    if ($previous !== null && $previous !== '') {
                        $this->client->hSet($hashKey, $role, $previous);
                    }
                    break;

                default:
                    throw new TransactionException('Unknown journal op: '.$op);
            }
        }
    }

    private function rawDeleteDoc(string $collection, string $id, ?string $docKey = null, ?string $idxKey = null, ?string $permDocKey = null): void
    {
        $lowerId = \strtolower($id);
        $this->client->del($docKey ?? $this->docKey($collection, $lowerId));
        $this->client->sRem($idxKey ?? $this->idxKey($collection), $lowerId);
        $this->client->del($permDocKey ?? $this->permDocKey($collection, $lowerId));
    }

    private function rawRestoreDoc(string $collection, string $id, string $payload, ?string $docKey = null, ?string $idxKey = null): void
    {
        $lowerId = \strtolower($id);
        $this->client->set($docKey ?? $this->docKey($collection, $lowerId), $payload);
        $this->client->sAdd($idxKey ?? $this->idxKey($collection), $lowerId);
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function readAttributesField(RedisClient $client, string $metaKey): array
    {
        $raw = $client->hGet($metaKey, 'attrs');
        if (! \is_string($raw) || $raw === '') {
            return [];
        }
        /** @var array<int, array<string, mixed>> $decoded */
        $decoded = \json_decode($raw, true, self::JSON_DECODE_DEPTH, JSON_THROW_ON_ERROR);

        return \array_values($decoded);
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function readIndexesField(RedisClient $client, string $metaKey): array
    {
        $raw = $client->hGet($metaKey, 'indexes');
        if (! \is_string($raw) || $raw === '') {
            return [];
        }
        $decoded = \json_decode($raw, true, self::JSON_DECODE_DEPTH, JSON_THROW_ON_ERROR);
        if (! \is_array($decoded)) {
            return [];
        }

        /** @var array<int, array<string, mixed>> $decoded */
        return $decoded;
    }

    /**
     * @param array<int, array<string, mixed>> $attrs
     * @param array<string, mixed> $record
     * @return array<int, array<string, mixed>>
     */
    private function upsertAttributeRecord(array $attrs, array $record): array
    {
        $targetId = $this->stringOrEmpty($record[Document::ID] ?? '');
        $replaced = false;
        foreach ($attrs as $i => $existing) {
            $existingId = $this->recordIdentifier($existing);
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

    private function enforceUniqueIndexes(RedisClient $client, string $collection, Document $document, ?string $excludeId = null): void
    {
        $this->enforceUniqueIndexesForDocuments($client, $collection, [$document], $excludeId === null ? [] : [$excludeId]);
    }

    /**
     * Rejects a write when two of its documents share a unique value, or when one shares a
     * unique value with a stored document it does not replace.
     *
     * @param array<int, Document> $documents
     * @param array<int, string> $replacedIds the stored id each document overwrites, by document position
     */
    private function enforceUniqueIndexesForDocuments(RedisClient $client, string $collection, array $documents, array $replacedIds): void
    {
        $uniqueIndexes = $this->uniqueIndexAttributes($client, $collection);
        if ($uniqueIndexes === []) {
            return;
        }

        $sharedTables = $this->getSharedTables();
        $claimed = [];
        $replaced = [];
        $tenants = [];
        foreach ($documents as $position => $document) {
            $tenant = $sharedTables ? ($document->getTenant() ?? $this->getTenant()) : null;
            $idxKey = $this->idxKey($collection, $tenant);
            $tenants[$idxKey] = $tenant;
            if (isset($replacedIds[$position])) {
                $replaced[$idxKey][\strtolower($replacedIds[$position])] = true;
            }
            foreach ($this->uniqueSignatures($document, $uniqueIndexes, $tenant) as $index => $signature) {
                if (isset($claimed[$idxKey][$index][$signature])) {
                    throw new UniqueException(UniqueException::MESSAGE);
                }
                $claimed[$idxKey][$index][$signature] = true;
            }
        }

        foreach ($claimed as $idxKey => $signatures) {
            [$owners] = $this->storedUniqueValues($client, $collection, $idxKey, $tenants[$idxKey], $uniqueIndexes, $replaced[$idxKey] ?? []);
            foreach ($owners as $index => $values) {
                if (\array_intersect_key($values, $signatures[$index] ?? []) !== []) {
                    throw new UniqueException(UniqueException::MESSAGE);
                }
            }
        }
    }

    /**
     * Rejects an upsert batch as writing its documents one after another would: each is checked against the stored
     * documents and the batch documents before it, and a document that replaces a stored one frees its id's values.
     *
     * @param list<Write> $writes
     */
    private function enforceUniqueIndexesInOrder(RedisClient $client, string $collection, array $writes): void
    {
        $uniqueIndexes = $this->uniqueIndexAttributes($client, $collection);
        if ($uniqueIndexes === []) {
            return;
        }

        $sharedTables = $this->getSharedTables();
        $owners = [];
        $held = [];
        foreach ($writes as $write) {
            $tenant = $sharedTables ? ($write->document->getTenant() ?? $this->getTenant()) : null;
            $idxKey = $this->idxKey($collection, $tenant);
            if (! isset($owners[$idxKey])) {
                [$owners[$idxKey], $held[$idxKey]] = $this->storedUniqueValues($client, $collection, $idxKey, $tenant, $uniqueIndexes);
            }

            $id = \strtolower($write->id);
            $signatures = $this->uniqueSignatures($write->document, $uniqueIndexes, $tenant);
            foreach ($signatures as $index => $signature) {
                $owner = $owners[$idxKey][$index][$signature] ?? null;
                if ($owner !== null && ($write->payload === null || $owner !== $id)) {
                    throw new UniqueException(UniqueException::MESSAGE);
                }
            }

            foreach ($held[$idxKey][$id] ?? [] as $index => $signature) {
                unset($owners[$idxKey][$index][$signature]);
            }
            $held[$idxKey][$id] = $signatures;
            foreach ($signatures as $index => $signature) {
                $owners[$idxKey][$index][$signature] = $id;
            }
        }
    }

    /**
     * The unique values the stored documents of one tenant's bucket hold: by index and value the lowercased id of the
     * document holding it, and by that id its values.
     *
     * @param array<int, array<int, string>> $uniqueIndexes
     * @param array<string, true> $skipped lowercased ids whose stored documents are left out
     * @return array{array<int, array<string, string>>, array<string, array<int, string>>}
     */
    private function storedUniqueValues(RedisClient $client, string $collection, string $idxKey, int|string|null $tenant, array $uniqueIndexes, array $skipped = []): array
    {
        /** @var array<int, string>|false $docIds */
        $docIds = $client->sMembers($idxKey);
        $ids = [];
        $docKeys = [];
        foreach (\is_array($docIds) ? $docIds : [] as $docId) {
            $id = \strtolower((string) $docId);
            if (isset($skipped[$id])) {
                continue;
            }
            $ids[] = $id;
            $docKeys[] = $this->docKey($collection, (string) $docId, $tenant);
        }
        if ($docKeys === []) {
            return [[], []];
        }

        $owners = [];
        $held = [];
        /** @var array<int, mixed>|false $payloads */
        $payloads = $client->mGet($docKeys);
        foreach (\is_array($payloads) ? $payloads : [] as $position => $payload) {
            if (! \is_string($payload) || $payload === '') {
                continue;
            }
            $existing = $this->decode($payload);
            if ($this->getSharedTables() && $existing->getTenant() !== $tenant) {
                continue;
            }
            $id = $ids[$position];
            $held[$id] = $this->uniqueSignatures($existing, $uniqueIndexes, $tenant);
            foreach ($held[$id] as $index => $signature) {
                $owners[$index][$signature] = $id;
            }
        }

        return [$owners, $held];
    }

    /**
     * @return array<int, array<int, string>>
     */
    private function uniqueIndexAttributes(RedisClient $client, string $collection): array
    {
        $uniqueIndexes = [];
        foreach ($this->readIndexesField($client, $this->key($this->ns(), 'meta', $collection)) as $index) {
            if (($index['type'] ?? '') !== IndexType::Unique->value) {
                continue;
            }
            $attributes = $index['attributes'] ?? [];
            if (empty($attributes) || ! \is_array($attributes)) {
                continue;
            }
            $names = [];
            foreach ($attributes as $attribute) {
                if (\is_string($attribute) && $attribute !== '') {
                    $names[] = $attribute;
                }
            }
            if ($names !== []) {
                $uniqueIndexes[] = $names;
            }
        }

        return $uniqueIndexes;
    }

    /**
     * Signatures of the unique values a document holds, by index position. An index where the
     * document holds a null is left out: nulls never collide.
     *
     * @param array<int, array<int, string>> $uniqueIndexes
     * @return array<int, string>
     */
    private function uniqueSignatures(Document $document, array $uniqueIndexes, int|string|null $tenant): array
    {
        $signatures = [];
        foreach ($uniqueIndexes as $index => $attributes) {
            $signature = [];
            foreach ($attributes as $attribute) {
                $value = $this->resolveDocumentAttribute($document, $attribute);
                if ($value === null) {
                    continue 2;
                }
                $signature[] = $this->normalizeIndexValue($value);
            }
            if ($this->getSharedTables()) {
                \array_unshift($signature, $tenant);
            }
            $signatures[$index] = \serialize($signature);
        }

        return $signatures;
    }

    private function purgeCollectionKeys(RedisClient $client, string $namespace, string $database, string $collection): void
    {
        $collection = $this->filter($collection);
        $prefix = $this->nsFor($namespace, $database);
        $grantsKey = $this->grantsKey($prefix, $collection);

        /** @var array<int, string>|false $registered */
        $registered = $client->sMembers($grantsKey);
        $keys = \is_array($registered) ? $registered : [];

        $buckets = [null, ...$this->tenantBuckets($client, $prefix, 'idx', $collection), ...$this->tenantBuckets($client, $prefix, 'seq', $collection)];
        foreach (\array_unique($buckets) as $bucket) {
            $idxKey = $this->scopedKey($prefix, 'idx', $bucket, $collection);
            $keys[] = $idxKey;
            $keys[] = $this->scopedKey($prefix, 'seq', $bucket, $collection);

            /** @var array<int, string>|false $docIds */
            $docIds = $client->sMembers($idxKey);
            if (! \is_array($docIds) || $docIds === []) {
                continue;
            }

            $permDocKeys = [];
            foreach ($docIds as $docId) {
                $keys[] = $this->scopedKey($prefix, 'doc', $bucket, $collection, (string) $docId);
                $permDocKeys[] = $this->scopedKey($prefix, 'perm', $bucket, 'doc', $collection, (string) $docId);
            }
            \array_push($keys, ...$permDocKeys, ...$this->roleSetKeys($client, $prefix, $bucket, $collection, $permDocKeys));
        }

        $keys[] = $this->key($prefix, 'meta', $collection);
        $keys[] = $grantsKey;
        foreach (\array_chunk(\array_values(\array_unique($keys)), self::SCAN_BATCH_SIZE) as $batch) {
            $client->del(...$batch);
        }
    }

    private function grantsKey(string $prefix, string $collection): string
    {
        return $this->key($prefix, 'grants', $collection);
    }

    /**
     * @param  array<int, string>  $permDocKeys
     * @return array<int, string>
     */
    private function roleSetKeys(RedisClient $client, string $prefix, ?string $bucket, string $collection, array $permDocKeys): array
    {
        if ($permDocKeys === []) {
            return [];
        }

        $client->multi(\Redis::PIPELINE);
        foreach ($permDocKeys as $permDocKey) {
            $client->hGetAll($permDocKey);
        }
        $grantsByDocument = $client->exec();

        $keys = [];
        foreach (\is_array($grantsByDocument) ? $grantsByDocument : [] as $grants) {
            foreach (\is_array($grants) ? $grants : [] as $role => $letters) {
                foreach (\explode(',', \is_string($letters) ? $letters : '') as $letter) {
                    if ($letter !== '') {
                        $keys[] = $this->scopedKey($prefix, 'perm', $bucket, $collection, $letter, (string) $role);
                    }
                }
            }
        }

        return $keys;
    }

    /**
     * The registered permission keys of the collection that belong to the tenant bucket. A bucket
     * never contains the separator, so one bucket's scope is never a prefix of another's.
     *
     * @return array<int, string>
     */
    private function registeredGrantKeys(RedisClient $client, string $prefix, ?string $bucket, string $collection): array
    {
        /** @var array<int, string>|false $registered */
        $registered = $client->sMembers($this->grantsKey($prefix, $collection));
        $keys = \is_array($registered) ? $registered : [];

        if ($bucket === null) {
            return $keys;
        }

        $scope = $this->scopedKey($prefix, 'perm', $bucket).self::SEP;

        return \array_values(\array_filter($keys, static fn (string $key): bool => \str_starts_with($key, $scope)));
    }

    private function scopedKey(string $prefix, string $family, ?string $bucket, string ...$parts): string
    {
        $scope = $bucket === null ? [] : ['t', $bucket];

        return $this->key($prefix, $family, ...$scope, ...$parts);
    }

    /**
     * Tenant buckets that hold a {family}:t:{bucket}:{collection} key. A bucket never contains the
     * separator, so a key the pattern also matches for another layout is skipped.
     *
     * @return array<int, string>
     */
    private function tenantBuckets(RedisClient $client, string $prefix, string $family, string $collection): array
    {
        $head = $this->key($prefix, $family, 't').self::SEP;
        $tail = self::SEP.$collection;
        $buckets = [];
        $cursor = null;
        do {
            /** @var array<int, string>|false $batch */
            $batch = $client->scan($cursor, $head.'*'.$tail, self::SCAN_BATCH_SIZE);
            foreach (\is_array($batch) ? $batch : [] as $key) {
                if (! \str_starts_with($key, $head) || ! \str_ends_with($key, $tail)) {
                    continue;
                }
                $bucket = \substr($key, \strlen($head), -\strlen($tail));
                if ($bucket !== '' && ! \str_contains($bucket, self::SEP)) {
                    $buckets[] = $bucket;
                }
            }
        } while ($cursor !== 0 && $cursor !== null);

        return $buckets;
    }

    private function computeCollectionSize(string $collection): int
    {
        $collection = $this->filter($collection);
        $prefix = $this->ns();
        $metaKey = $this->key($prefix, 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return 0;
        }

        $bucket = $this->tenantBucket();
        $idxKey = $this->idxKey($collection);
        $keys = [$metaKey, $idxKey];

        /** @var array<int, string>|false $docIds */
        $docIds = $this->client->sMembers($idxKey);
        $permDocKeys = [];
        foreach (\is_array($docIds) ? $docIds : [] as $docId) {
            $keys[] = $this->docKey($collection, (string) $docId);
            $permDocKeys[] = $this->permDocKey($collection, (string) $docId);
        }
        \array_push(
            $keys,
            ...$permDocKeys,
            ...$this->roleSetKeys($this->client, $prefix, $bucket, $collection, $permDocKeys),
            ...$this->registeredGrantKeys($this->client, $prefix, $bucket, $collection),
        );

        $total = 0;
        foreach (\array_unique($keys) as $key) {
            $total += $this->measureKey($key);
        }

        return $total;
    }

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

    private function registerRelationshipField(string $collection, string $field): void
    {
        $collection = $this->filter($collection);
        $field = $this->filter($field);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return;
        }

        $record = [
            Document::ID => $field,
            'key' => $field,
            'type' => ColumnType::Relationship->value,
            'size' => 0,
            'signed' => true,
            'array' => false,
            'required' => false,
        ];

        $this->tx(function (RedisClient $client) use ($metaKey, $record): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $attrs = $this->upsertAttributeRecord($attrs, $record);
            $client->hSet($metaKey, 'attrs', \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });
    }

    private function renameDocumentField(string $collection, string $oldKey, string $newKey): void
    {
        $collection = $this->filter($collection);
        $oldKey = $this->filter($oldKey);
        $newKey = $this->filter($newKey);

        if ($oldKey === $newKey) {
            return;
        }

        $idxKey = $this->idxKey($collection);

        $this->tx(function (RedisClient $client) use ($collection, $oldKey, $newKey, $idxKey): void {
            /** @var array<int, string>|false $docIds */
            $docIds = $client->sMembers($idxKey);
            if (! \is_array($docIds) || $docIds === []) {
                return;
            }

            foreach ($docIds as $docId) {
                $docKey = $this->docKey($collection, $docId);
                $payload = $client->get($docKey);
                if (! \is_string($payload) || $payload === '') {
                    continue;
                }

                /** @var array<string, mixed> $decoded */
                $decoded = \json_decode($payload, true, self::JSON_DECODE_DEPTH, JSON_THROW_ON_ERROR);
                if (! \array_key_exists($oldKey, $decoded)) {
                    continue;
                }

                $decoded[$newKey] = $decoded[$oldKey];
                unset($decoded[$oldKey]);

                $client->set(
                    $docKey,
                    \json_encode($decoded, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION),
                );
            }
        });
    }

    private function dropDocumentField(string $collection, string $field): void
    {
        $collection = $this->filter($collection);
        $field = $this->filter($field);
        $idxKey = $this->idxKey($collection);

        $this->tx(function (RedisClient $client) use ($collection, $field, $idxKey): void {
            /** @var array<int, string>|false $docIds */
            $docIds = $client->sMembers($idxKey);
            if (! \is_array($docIds) || $docIds === []) {
                return;
            }

            foreach ($docIds as $docId) {
                $docKey = $this->docKey($collection, $docId);
                $payload = $client->get($docKey);
                if (! \is_string($payload) || $payload === '') {
                    continue;
                }

                /** @var array<string, mixed> $decoded */
                $decoded = \json_decode($payload, true, self::JSON_DECODE_DEPTH, JSON_THROW_ON_ERROR);
                if (! \array_key_exists($field, $decoded)) {
                    continue;
                }

                unset($decoded[$field]);

                $client->set(
                    $docKey,
                    \json_encode($decoded, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION),
                );
            }
        });
    }

    private function resolveJunctionCollection(string $collection, string $relatedCollection, RelationSide $side): ?string
    {
        $collectionDoc = $this->loadMetadataDocument($collection);
        $relatedDoc = $this->loadMetadataDocument($relatedCollection);
        if ($collectionDoc === null || $relatedDoc === null) {
            return null;
        }

        $collectionSequence = $collectionDoc->getSequence();
        $relatedSequence = $relatedDoc->getSequence();
        if ($collectionSequence === null || $relatedSequence === null || $collectionSequence === '' || $relatedSequence === '') {
            return null;
        }

        return $side === RelationSide::Parent
            ? '_'.$collectionSequence.'_'.$relatedSequence
            : '_'.$relatedSequence.'_'.$collectionSequence;
    }

    private function loadMetadataDocument(string $collection): ?Document
    {
        $id = $this->filter($collection);
        $payload = $this->client->get($this->docKey(Database::METADATA, $id));
        if ((! \is_string($payload) || $payload === '') && $this->getSharedTables()) {
            $payload = $this->client->get($this->docKey(Database::METADATA, $id, '_'));
        }
        if (! \is_string($payload) || $payload === '') {
            return null;
        }

        return $this->decode($payload);
    }

    private function surfaceRelationshipAttributes(string $collection, Document $document): Document
    {
        if ($collection === Database::METADATA) {
            return $document;
        }

        $metaKey = $this->key($this->ns(), 'meta', $this->filter($collection));
        $attributes = $this->readAttributesField($this->client, $metaKey);
        $relationshipKeys = $this->extractRelationshipKeys($attributes);
        if ($relationshipKeys === []) {
            return $document;
        }

        return $this->surfaceRelationshipAttributesUsing($relationshipKeys, $document);
    }

    /**
     * @param array<int, string> $relationshipKeys
     */
    private function surfaceRelationshipAttributesUsing(array $relationshipKeys, Document $document): Document
    {
        if ($relationshipKeys === []) {
            return $document;
        }

        $payload = $document->getArrayCopy();
        foreach ($relationshipKeys as $key) {
            if (! \array_key_exists($key, $payload)) {
                $document->setAttribute($key, null);
            }
        }

        return $document;
    }

    /**
     * @param array<int, array<string, mixed>> $attributes
     * @return array<int, string>
     */
    private function extractRelationshipKeys(array $attributes): array
    {
        $keys = [];
        foreach ($attributes as $attribute) {
            if (($attribute['type'] ?? null) !== ColumnType::Relationship->value) {
                continue;
            }
            $key = $this->recordIdentifier($attribute);
            if ($key === '') {
                continue;
            }
            $keys[] = $key;
        }

        return $keys;
    }

    /**
     * @return array<int, Document>
     */
    private function loadCollectionDocuments(RedisClient $client, string $collection, PermissionType $forPermission): array
    {
        $idxKey = $this->idxKey($collection);
        /** @var array<int, string>|false $ids */
        $ids = $client->sMembers($idxKey);
        if (! \is_array($ids) || empty($ids)) {
            return [];
        }

        if ($this->authorization->getStatus()) {
            $ids = $this->applyPermissionFilter($collection, $ids, $forPermission);
            if (empty($ids)) {
                return [];
            }
        }

        $keys = [];
        foreach ($ids as $id) {
            $keys[] = $this->docKey($collection, (string) $id);
        }

        /** @var array<int, mixed> $payloads */
        $payloads = $client->mGet($keys);
        $sharedTables = $this->getSharedTables();
        $tenant = $sharedTables ? $this->getTenant() : null;
        $allowNullTenant = $sharedTables && $collection === Database::METADATA;

        $relationshipKeys = [];
        if ($collection !== Database::METADATA) {
            $metaKey = $this->key($this->ns(), 'meta', $this->filter($collection));
            $attributes = $this->readAttributesField($client, $metaKey);
            $relationshipKeys = $this->extractRelationshipKeys($attributes);
        }

        $documents = [];
        foreach ($payloads as $payload) {
            if (! \is_string($payload) || $payload === '') {
                continue;
            }
            $document = $this->decode($payload);

            if ($sharedTables) {
                $rowTenant = $document->getAttribute(Document::TENANT);
                $crossTenant = $rowTenant !== $tenant
                    && ! ($allowNullTenant && $rowTenant === null);
                if ($crossTenant) {
                    continue;
                }
            }

            if (! empty($relationshipKeys)) {
                $document = $this->surfaceRelationshipAttributesUsing($relationshipKeys, $document);
            }

            $documents[] = $document;
        }

        return $documents;
    }

    /**
     * @param array<int, Document> $documents
     * @param array<Query> $queries
     * @return array<int, Document>
     */
    private function filterDocumentsByQueries(string $collection, array $documents, array $queries): array
    {
        if (empty($documents)) {
            return [];
        }

        $effective = [];
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if (\in_array($method, [
                Method::Select,
                Method::OrderAsc,
                Method::OrderDesc,
                Method::OrderRandom,
                Method::Limit,
                Method::Offset,
                Method::CursorAfter,
                Method::CursorBefore,
            ], true)) {
                continue;
            }
            $effective[] = $query;
        }

        if (empty($effective)) {
            return \array_values($documents);
        }

        $output = [];
        foreach ($documents as $document) {
            $matched = true;
            foreach ($effective as $query) {
                if (! $this->matchesDocument($document, $query)) {
                    $matched = false;
                    break;
                }
            }
            if ($matched) {
                $output[] = $document;
            }
        }

        return $output;
    }

    private function matchesDocument(Document $document, Query $query): bool
    {
        $method = $query->getMethod();

        if ($method === Method::And) {
            foreach ($query->getValues() as $sub) {
                if (! ($sub instanceof Query) || ! $this->matchesDocument($document, $sub)) {
                    return false;
                }
            }

            return true;
        }

        if ($method === Method::Or) {
            foreach ($query->getValues() as $sub) {
                if ($sub instanceof Query && $this->matchesDocument($document, $sub)) {
                    return true;
                }
            }

            return false;
        }

        $attribute = $query->getAttribute();
        $value = $this->resolveDocumentAttribute($document, $attribute);
        $values = $query->getValues();

        if ($query->isObjectAttribute() && ! \str_contains($attribute, '.')) {
            return $this->matchesDocumentObject($value, $query);
        }

        switch ($method) {
            case Method::Equal:
                if ($value === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($candidate === null) {
                        continue;
                    }
                    if ($this->valuesEqual($value, $candidate)) {
                        return true;
                    }
                }

                return false;

            case Method::NotEqual:
                if ($value === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($candidate === null) {
                        return false;
                    }
                    if ($this->valuesEqual($value, $candidate)) {
                        return false;
                    }
                }

                return true;

            case Method::LessThan:
                return $value !== null && $value < $values[0];

            case Method::LessThanEqual:
                return $value !== null && $value <= $values[0];

            case Method::GreaterThan:
                return $value !== null && $value > $values[0];

            case Method::GreaterThanEqual:
                return $value !== null && $value >= $values[0];

            case Method::IsNull:
                return $value === null;

            case Method::IsNotNull:
                return $value !== null;

            case Method::Between:
                return $value !== null && $value >= $values[0] && $value <= $values[1];

            case Method::NotBetween:
                if ($value === null) {
                    return false;
                }

                return $value < $values[0] || $value > $values[1];

            case Method::StartsWith:
                return \is_string($value) && isset($values[0]) && \is_string($values[0]) && \str_starts_with($value, $values[0]);

            case Method::NotStartsWith:
                if ($value === null) {
                    return false;
                }

                return ! \is_string($value) || ! isset($values[0]) || ! \is_string($values[0]) || ! \str_starts_with($value, $values[0]);

            case Method::EndsWith:
                return \is_string($value) && isset($values[0]) && \is_string($values[0]) && \str_ends_with($value, $values[0]);

            case Method::NotEndsWith:
                if ($value === null) {
                    return false;
                }

                return ! \is_string($value) || ! isset($values[0]) || ! \is_string($values[0]) || ! \str_ends_with($value, $values[0]);

            case Method::Contains:
            case Method::ContainsAny:
                $haystack = $this->coerceArrayValue($value);
                if ($haystack === null && \is_string($value)) {
                    foreach ($values as $needle) {
                        if (\is_string($needle) && \stripos($value, $needle) !== false) {
                            return true;
                        }
                    }

                    return false;
                }
                if (! \is_array($haystack)) {
                    return false;
                }
                foreach ($values as $needle) {
                    foreach ($haystack as $item) {
                        if ($this->valuesEqual($item, $needle)) {
                            return true;
                        }
                    }
                }

                return false;

            case Method::NotContains:
                if ($value === null) {
                    return false;
                }

                return ! $this->matchesDocument($document, new Query(Method::Contains, $attribute, $values));

            case Method::ContainsAll:
                $haystack = $this->coerceArrayValue($value);
                if (! \is_array($haystack)) {
                    return false;
                }
                foreach ($values as $needle) {
                    $found = false;
                    foreach ($haystack as $item) {
                        if ($this->valuesEqual($item, $needle)) {
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
                $needle = $this->stringOrEmpty($values[0] ?? '');
                if ($needle === '') {
                    return false;
                }

                return $this->matchesFulltextRedis($value, $needle);

            case Method::NotSearch:
                if ($value === null) {
                    return false;
                }
                if (! \is_string($value)) {
                    return true;
                }
                $needle = $this->stringOrEmpty($values[0] ?? '');
                if ($needle === '') {
                    return true;
                }

                return ! $this->matchesFulltextRedis($value, $needle);

            case Method::Regex:
                if (! \is_string($value)) {
                    return false;
                }
                $pattern = $this->stringOrEmpty($values[0] ?? '');
                $delimited = '#'.\str_replace('#', '\\#', $pattern).'#u';

                return @\preg_match($delimited, $value) === 1;
        }

        throw new QueryException('Query method not supported by Redis adapter: '.$method->value);
    }

    private function matchesDocumentObject(mixed $value, Query $query): bool
    {
        $haystack = $this->decodeObjectishValue($value);
        $values = $query->getValues();
        $method = $query->getMethod();

        switch ($method) {
            case Method::Equal:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContainment($haystack, $candidate)) {
                        return true;
                    }
                }

                return false;

            case Method::NotEqual:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContainment($haystack, $candidate)) {
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
                    if ($this->jsonContainment($haystack, $this->wrapScalarObjectCandidate($candidate))) {
                        return true;
                    }
                }

                return false;

            case Method::ContainsAll:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if (! $this->jsonContainment($haystack, $this->wrapScalarObjectCandidate($candidate))) {
                        return false;
                    }
                }

                return true;

            case Method::NotContains:
                if ($haystack === null) {
                    return false;
                }
                foreach ($values as $candidate) {
                    if ($this->jsonContainment($haystack, $this->wrapScalarObjectCandidate($candidate))) {
                        return false;
                    }
                }

                return true;

            case Method::IsNull:
                return $value === null;

            case Method::IsNotNull:
                return $value !== null;
        }

        throw new QueryException('Query method '.$method->value.' not supported for object attributes');
    }

    /**
     * @param array<int, Document> $documents
     * @param array<string> $orderAttributes
     * @param array<OrderDirection> $orderTypes
     * @return array<int, Document>
     */
    private function orderDocuments(array $documents, array $orderAttributes, array $orderTypes, CursorDirection $cursorDirection): array
    {
        foreach ($orderTypes as $type) {
            if ($type === OrderDirection::Random) {
                \shuffle($documents);

                return $documents;
            }
        }

        $reverse = $cursorDirection === CursorDirection::Before;

        if (empty($orderAttributes)) {
            \usort($documents, function (Document $a, Document $b) use ($reverse): int {
                $av = $a->getAttribute(Document::SEQUENCE, 0);
                $bv = $b->getAttribute(Document::SEQUENCE, 0);
                $av = \is_numeric($av) ? $av + 0 : 0;
                $bv = \is_numeric($bv) ? $bv + 0 : 0;
                if ($av === $bv) {
                    return 0;
                }
                $cmp = ($av < $bv) ? -1 : 1;

                return $reverse ? -$cmp : $cmp;
            });

            return $documents;
        }

        $directions = [];
        foreach ($orderAttributes as $i => $attribute) {
            $direction = $orderTypes[$i] ?? OrderDirection::Asc;
            if ($reverse) {
                $direction = $direction === OrderDirection::Asc ? OrderDirection::Desc : OrderDirection::Asc;
            }
            $directions[$i] = $direction === OrderDirection::Asc ? 1 : -1;
        }

        \usort($documents, function (Document $a, Document $b) use ($orderAttributes, $directions): int {
            foreach ($orderAttributes as $i => $attribute) {
                $av = $this->resolveDocumentAttribute($a, $attribute);
                $bv = $this->resolveDocumentAttribute($b, $attribute);
                if ($av === $bv) {
                    continue;
                }
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

        return $documents;
    }

    /**
     * @param array<int, Document> $documents
     * @param array<string> $orderAttributes
     * @param array<OrderDirection> $orderTypes
     * @param array<string, mixed> $cursor
     * @return array<int, Document>
     */
    private function cursorDocuments(array $documents, array $orderAttributes, array $orderTypes, array $cursor, CursorDirection $cursorDirection): array
    {
        if (empty($cursor)) {
            return $documents;
        }

        if (empty($orderAttributes)) {
            $orderAttributes = [Document::SEQUENCE];
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
                'attribute' => $attribute,
                'asc' => $direction === OrderDirection::Asc,
                'ref' => $cursor[$attribute] ?? null,
            ];
        }

        $output = [];
        foreach ($documents as $document) {
            foreach ($resolved as $entry) {
                $current = $this->resolveDocumentAttribute($document, $entry['attribute']);
                $ref = $entry['ref'];
                if ($current === $ref) {
                    continue;
                }
                if ($current === null) {
                    if (! $entry['asc']) {
                        $output[] = $document;
                    }

                    continue 2;
                }
                if ($ref === null) {
                    if ($entry['asc']) {
                        $output[] = $document;
                    }

                    continue 2;
                }
                if ($entry['asc'] ? ($current > $ref) : ($current < $ref)) {
                    $output[] = $document;
                }

                continue 2;
            }
        }

        return $output;
    }

    private function resolveDocumentAttribute(Document $document, string $attribute): mixed
    {
        if ($document->offsetExists($attribute)) {
            return $document->getAttribute($attribute);
        }

        $filtered = $this->filter($attribute);
        if ($filtered !== $attribute && $document->offsetExists($filtered)) {
            return $document->getAttribute($filtered);
        }

        if (! \str_contains($attribute, '.')) {
            return null;
        }

        [$head, $rest] = \explode('.', $attribute, 2);
        $value = $document->getAttribute($head);
        if (\is_string($value) && $value !== '' && ($value[0] === '{' || $value[0] === '[')) {
            $decoded = \json_decode($value, true);
            if (\is_array($decoded)) {
                $value = $decoded;
            }
        }
        if ($value instanceof Document) {
            $value = $value->getArrayCopy();
        }

        return $this->traverseNestedPath($value, $rest);
    }

    private function traverseNestedPath(mixed $value, string $path): mixed
    {
        foreach (\explode('.', $path) as $part) {
            if ($value instanceof Document) {
                $value = $value->getArrayCopy();
            }
            if (\is_array($value) && \array_key_exists($part, $value)) {
                $value = $value[$part];

                continue;
            }

            return null;
        }

        return $value;
    }

    private function normalizeIndexValue(mixed $value): mixed
    {
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

    private function valuesEqual(mixed $a, mixed $b): bool
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
     * @return array<mixed>|null
     */
    private function coerceArrayValue(mixed $value): ?array
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

    private function decodeObjectishValue(mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }
        if (\is_array($value)) {
            return $value;
        }
        if ($value instanceof Document) {
            return $value->getArrayCopy();
        }
        if (\is_string($value) && $value !== '' && ($value[0] === '{' || $value[0] === '[')) {
            $decoded = \json_decode($value, true);
            if (\is_array($decoded)) {
                return $decoded;
            }
        }

        return $value;
    }

    private function jsonContainment(mixed $haystack, mixed $candidate): bool
    {
        if (\is_array($haystack) && \array_is_list($haystack)) {
            if (\is_array($candidate) && \array_is_list($candidate)) {
                foreach ($candidate as $needle) {
                    $matched = false;
                    foreach ($haystack as $item) {
                        if ($this->jsonContainment($item, $needle)) {
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
                if ($this->jsonContainment($item, $candidate)) {
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
                if (! $this->jsonContainment($haystack[$key], $value)) {
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

    private function wrapScalarObjectCandidate(mixed $candidate): mixed
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

    private function matchesFulltextRedis(string $haystack, string $needle): bool
    {
        if (\preg_match('/^"(.*)"$/u', \trim($needle), $matches) === 1) {
            $phrase = \mb_strtolower($matches[1]);
            if ($phrase === '') {
                return false;
            }

            return \str_contains(\mb_strtolower($haystack), $phrase);
        }

        $haystackTokens = $this->tokenizeForSearch($haystack);
        $needleTokens = $this->tokenizeForSearch($needle);
        if (empty($needleTokens) || empty($haystackTokens)) {
            return false;
        }
        $set = \array_flip($haystackTokens);
        foreach ($needleTokens as $token) {
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
     * @return array<int, string>
     */
    private function tokenizeForSearch(string $text): array
    {
        $lower = \mb_strtolower($text);
        $parts = \preg_split('/[^\p{L}\p{N}*]+/u', $lower) ?: [];

        return \array_values(\array_filter($parts, fn (string $p): bool => $p !== ''));
    }

    /**
     * @param array<Query> $queries
     * @return array<int, string>
     */
    protected function extractSelections(array $queries): array
    {
        $selections = [];
        foreach ($queries as $query) {
            if ($query->getMethod() !== Method::Select) {
                continue;
            }
            foreach ($query->getValues() as $value) {
                if (\is_string($value)) {
                    $selections[] = $value;
                }
            }
        }

        return $selections;
    }

    /**
     * @param array<int, string> $selections
     */
    private function projectDocument(Document $document, array $selections): Document
    {
        if (\in_array('*', $selections, true)) {
            return $document;
        }

        $projected = [];
        foreach ($document->getArrayCopy() as $field => $value) {
            if (\str_starts_with($field, '$') || \str_starts_with($field, '_')) {
                $projected[$field] = $value;

                continue;
            }
            if (\in_array($field, $selections, true)) {
                $projected[$field] = $value;
            }
        }

        return new Document($projected);
    }

    /**
     * @param array<string, mixed> $attrs
     * @param array<string, mixed> $existing
     * @return array<string, mixed>
     */
    protected function applyOperators(array $attrs, array $existing): array
    {
        $result = [];
        foreach ($attrs as $attribute => $value) {
            if (Operator::isOperator($value)) {
                /** @var Operator $value */
                $result[$attribute] = $this->applyOperator($existing[$attribute] ?? null, $value);

                continue;
            }
            $result[$attribute] = $value;
        }

        return $result;
    }

    protected function applyOperator(mixed $current, Operator $operator): mixed
    {
        $values = $operator->getValues();
        $method = $operator->getMethod();
        $exact = BigInt::calculateOutsideNative($method, $current ?? 0, $values[0] ?? 1);
        if ($exact !== null) {
            $bound = $values[1] ?? null;
            if ($method === OperatorType::Modulo || ! \is_numeric($bound) || (\is_float($bound) && ! \is_finite($bound))) {
                return $exact;
            }

            $limit = BigInt::integralValue($bound);
            if ($limit === null) {
                throw new OperatorException("Cannot apply {$method->value} operator: max/min limit must be a whole number, got {$bound}");
            }

            return $this->applyNumericLimit(
                $current ?? 0,
                $exact,
                $limit,
                \in_array($method, [OperatorType::Increment, OperatorType::Multiply, OperatorType::Power], true)
            );
        }

        switch ($method) {
            case OperatorType::Increment:
                $by = $this->numericOr($values[0] ?? 1, 1);
                $max = $values[1] ?? null;
                $base = $this->numericOr($current, 0);

                return $this->applyNumericLimit($base, $base + $by, $max, true);

            case OperatorType::Decrement:
                $by = $this->numericOr($values[0] ?? 1, 1);
                $min = $values[1] ?? null;
                $base = $this->numericOr($current, 0);

                return $this->applyNumericLimit($base, $base - $by, $min, false);

            case OperatorType::Multiply:
                $by = $this->numericOr($values[0] ?? 1, 1);
                $max = $values[1] ?? null;
                $base = $this->numericOr($current, 0);

                return $this->applyNumericLimit($base, $base * $by, $max, true);

            case OperatorType::Divide:
                $by = $values[0] ?? 1;
                $min = $values[1] ?? null;
                if (! \is_numeric($by) || $by == 0) {
                    return $current;
                }
                $base = $this->numericOr($current, 0);

                return $this->applyNumericLimit($base, $base / ($by + 0), $min, false);

            case OperatorType::Modulo:
                $by = $values[0] ?? 1;
                if (! \is_numeric($by) || $by == 0) {
                    return $current;
                }
                $base = \is_numeric($current) ? (int) $current : 0;

                return $base % (int) $by;

            case OperatorType::Power:
                $by = $this->numericOr($values[0] ?? 1, 1);
                $max = $values[1] ?? null;
                $base = $this->numericOr($current, 0);
                if (($base == 0 && $by < 0) || ($base < 0 && \floor($by) != $by)) {
                    if (\is_numeric($max)) {
                        return $base;
                    }

                    throw new LimitException('Value out of range');
                }

                $candidate = $base ** $by;
                if (! \is_finite((float) $candidate)) {
                    if (\is_numeric($max)) {
                        return $base;
                    }

                    throw new LimitException('Value out of range');
                }

                return $this->applyNumericLimit($base, $candidate, $max, true);

            case OperatorType::StringConcat:
                return $this->stringOrEmpty($current).$this->stringOrEmpty($values[0] ?? '');

            case OperatorType::StringReplace:
                $search = $this->stringOrEmpty($values[0] ?? '');
                $replace = $this->stringOrEmpty($values[1] ?? '');
                if ($current === null) {
                    return null;
                }

                return \str_replace($search, $replace, $this->stringOrEmpty($current));

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
                $index = $this->intOr($values[0] ?? 0, 0);
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
                $condition = $this->stringOrEmpty($values[0] ?? '');
                $compare = $values[1] ?? null;

                return \array_values(\array_filter($list, fn ($item) => $this->matchesArrayFilter($item, $condition, $compare)));

            case OperatorType::DateAddDays:
                $days = $this->intOr($values[0] ?? 0, 0);

                return $this->shiftDate($current, $days * 86400);

            case OperatorType::DateSubDays:
                $days = $this->intOr($values[0] ?? 0, 0);

                return $this->shiftDate($current, -$days * 86400);

            case OperatorType::DateSetNow:
                return DateTime::now();
        }
    }

    protected function applyNumericLimit(mixed $original, mixed $candidate, mixed $bound, bool $isUpper): int|float|string
    {
        if (BigInt::isIntegerValue($original) && BigInt::isIntegerValue($candidate) && BigInt::isIntegerValue($bound)) {
            $crossed = $isUpper
                ? BigInt::compare($candidate, $bound) > 0
                : BigInt::compare($candidate, $bound) < 0;

            return $crossed ? BigInt::toNative($original) : BigInt::toNative($candidate);
        }

        $numericOriginal = \is_numeric($original) ? $original + 0 : 0;
        $numericCandidate = \is_numeric($candidate) ? $candidate + 0 : 0;

        if (\is_numeric($bound)) {
            $numericBound = $bound + 0;
            if (($isUpper && $numericCandidate > $numericBound) || (! $isUpper && $numericCandidate < $numericBound)) {
                return $numericOriginal;
            }
        }

        return $this->preserveNumericType($numericOriginal, $numericCandidate);
    }

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

    protected function shiftDate(mixed $current, int $seconds): ?string
    {
        if ($current === null) {
            return null;
        }
        $stringified = $this->stringOrEmpty($current);
        try {
            $base = new \DateTime($stringified);
        } catch (\Throwable) {
            return $stringified === '' ? null : $stringified;
        }
        $base->modify(($seconds >= 0 ? '+' : '').$seconds.' seconds');

        return DateTime::format($base);
    }
}
