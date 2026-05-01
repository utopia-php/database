<?php

declare(strict_types=1);

namespace Utopia\Database\Adapter;

use Redis as RedisClient;
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
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\ID;
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

    private bool $supportForAttributes = true;

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

    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
    }

    public function clearTimeout(Event $event = Event::All): void
    {
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

        $this->rollbackJournal();
        $this->inTransaction--;

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

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $id = $this->filter($name);
        $colsKey = $this->key($this->ns(), 'cols');
        $metaKey = $this->key($this->ns(), 'meta', $id);
        $idxKey = $this->idxKey($id);

        if ((bool) $this->client->exists($metaKey)) {
            throw new DuplicateException('Collection already exists');
        }

        $attributePayload = [];
        foreach ($attributes as $attribute) {
            $attributePayload[] = [
                '$id' => $attribute->key,
                'key' => $attribute->key,
                'type' => $attribute->type->value,
                'size' => $attribute->size,
                'signed' => $attribute->signed,
                'array' => $attribute->array,
                'required' => $attribute->required,
            ];
        }

        $indexPayload = [];
        foreach ($indexes as $index) {
            $indexPayload[] = [
                '$id' => $index->key,
                'key' => $index->key,
                'type' => $index->type->value,
                'attributes' => $index->attributes,
                'lengths' => $index->lengths,
                'orders' => $index->orders,
            ];
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

        $record = [
            '$id' => $id,
            'key' => $id,
            'type' => $attribute->type->value,
            'size' => $attribute->size,
            'signed' => $attribute->signed,
            'array' => $attribute->array,
            'required' => $attribute->required,
        ];

        $this->tx(function (RedisClient $client) use ($metaKey, $record): void {
            $attrs = $this->readAttributesField($client, $metaKey);
            $attrs = $this->upsertAttributeRecord($attrs, $record);
            $client->hSet($metaKey, 'attrs', \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
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
        $collection = $this->filter($collection);
        $id = $this->filter($attribute->key);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            throw new NotFoundException('Collection not found');
        }

        if (! empty($newKey) && $newKey !== $id) {
            $this->renameAttribute($collection, $id, $newKey);
            $id = $this->filter($newKey);
        }

        $record = [
            '$id' => $id,
            'key' => $id,
            'type' => $attribute->type->value,
            'size' => $attribute->size,
            'signed' => $attribute->signed,
            'array' => $attribute->array,
            'required' => $attribute->required,
        ];

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
                $existingId = (string) ($attribute['$id'] ?? $attribute['key'] ?? '');
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
            $client->hSet($metaKey, 'attrs', \json_encode($attrs, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        $this->renameDocumentField($collection, $old, $new);

        return true;
    }

    public function createRelationship(Relationship $relationship): bool
    {
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
        $orders = $index->orders;

        $this->tx(function (RedisClient $client) use ($metaKey, $collection, $id, $type, $attributes, $lengths, $orders): void {
            $indexes = $this->readIndexesField($client, $metaKey);

            foreach ($indexes as $existing) {
                if (($existing['$id'] ?? $existing['key'] ?? null) === $id) {
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
                            $rowTenant = $document->getAttribute('$tenant');
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
                '$id' => $id,
                'key' => $id,
                'type' => $type,
                'attributes' => \array_values($attributes),
                'lengths' => \array_values($lengths),
                'orders' => \array_values($orders),
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
                if (($index['$id'] ?? $index['key'] ?? null) === $id) {
                    continue;
                }
                $filtered[] = $index;
            }
            $client->hSet($metaKey, 'indexes', \json_encode(\array_values($filtered), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
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

        $this->tx(function (RedisClient $client) use ($metaKey, $old, $new): void {
            $indexes = $this->readIndexesField($client, $metaKey);
            $changed = false;
            foreach ($indexes as $i => $index) {
                if (($index['$id'] ?? $index['key'] ?? null) === $old) {
                    $indexes[$i]['$id'] = $new;
                    $indexes[$i]['key'] = $new;
                    $changed = true;
                    break;
                }
            }
            if (! $changed) {
                return;
            }
            $client->hSet($metaKey, 'indexes', \json_encode(\array_values($indexes), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
        });

        return true;
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
            $rowTenant = $document->getAttribute('$tenant');
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
        $col = $this->filter($collection->getId());
        $id = $document->getId();
        if ($id === '') {
            $id = ID::unique();
            $document->setAttribute('$id', $id);
        }
        $tenant = $document->getTenant();
        $docKey = $this->docKey($col, $id, $tenant);
        $idxKey = $this->idxKey($col, $tenant);
        $seqKey = $this->seqKey($col, $tenant);
        $permDocKey = $this->permDocKey($col, $id);

        return $this->tx(function (RedisClient $redis) use ($col, $id, $document, $docKey, $idxKey, $seqKey, $permDocKey): Document {
            if ((bool) $redis->exists($docKey)) {
                if ($this->skipDuplicates) {
                    $existingPayload = $redis->get($docKey);
                    if (\is_string($existingPayload) && $existingPayload !== '') {
                        $existing = $this->decode($existingPayload);
                        $document->setAttribute('$sequence', $existing->getSequence() ?? '');
                    }

                    return $document;
                }
                throw new DuplicateException('Document already exists');
            }

            try {
                $this->enforceUniqueIndexes($redis, $col, $document);
            } catch (DuplicateException $e) {
                if ($this->skipDuplicates) {
                    return $document;
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
            $document->setAttribute('$sequence', $sequence);

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
            $created[] = $this->createDocument($collection, $document);
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

            if ($newId !== $id && (bool) $redis->exists($newKey)) {
                throw new DuplicateException('Document already exists');
            }

            $resolved = $this->applyOperators($document->getArrayCopy(), $existing->getArrayCopy());
            $merged = \array_merge($existing->getArrayCopy(), $resolved);
            $merged['$id'] = $newId;
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
        $hasPermissions = $updates->offsetExists('$permissions');
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

            $count = 0;
            foreach ($documents as $i => $doc) {
                $uid = $doc->getId();
                $docKey = $docKeys[$i];
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
                    $merged['$createdAt'] = $updates->getCreatedAt();
                }
                if ($hasUpdatedAt) {
                    $merged['$updatedAt'] = $updates->getUpdatedAt();
                }
                if ($hasPermissions) {
                    $merged['$permissions'] = $updates->getPermissions();
                }

                $mergedDocument = new Document($merged);
                $redis->set($docKey, $this->encode($mergedDocument));

                $this->journal('updateDoc', [
                    'collection' => $col,
                    'id' => $uid,
                    'newId' => $uid,
                    'payload' => $existingPayload,
                    'docKey' => $docKey,
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

            foreach ($changes as $i => $change) {
                $document = $change->getNew();
                $id = $document->getId();
                $tenant = $document->getTenant();
                $docKey = $this->docKey($col, $id, $tenant);
                $idxKey = $this->idxKey($col, $tenant);
                $seqKey = $this->seqKey($col, $tenant);
                $existingPayload = $existingPayloads[$i] ?? false;

                if (\is_string($existingPayload) && $existingPayload !== '') {
                    $existing = $this->decode($existingPayload);
                    if (! empty($relationshipKeys)) {
                        $existing = $this->surfaceRelationshipAttributesUsing($relationshipKeys, $existing);
                    }
                    $existingArray = $existing->getArrayCopy();
                    $resolved = $this->applyOperators($document->getArrayCopy(), $existingArray);
                    $merged = \array_merge($existingArray, $resolved);
                    $merged['$id'] = $id;

                    if ($attribute !== '') {
                        $previous = $existing->getAttribute($attribute);
                        $delta = $document->getAttribute($attribute);
                        $previousNumeric = \is_numeric($previous) ? $previous + 0 : 0;
                        $deltaNumeric = \is_numeric($delta) ? $delta + 0 : 0;
                        $merged[$attribute] = $previousNumeric + $deltaNumeric;
                    }

                    $mergedDocument = new Document($merged);
                    $redis->set($docKey, $this->encode($mergedDocument));

                    $this->journal('updateDoc', [
                        'collection' => $col,
                        'id' => $id,
                        'newId' => $id,
                        'payload' => $existingPayload,
                        'docKey' => $docKey,
                    ]);

                    $this->clearPermissions($col, $id);
                    $this->writePermissions($col, $id, $mergedDocument);

                    $results[] = $mergedDocument;
                } else {
                    $this->enforceUniqueIndexes($redis, $col, $document);

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
                    $document->setAttribute('$sequence', $sequence);

                    $resolved = $this->applyOperators($document->getArrayCopy(), []);
                    foreach ($resolved as $attr => $value) {
                        $document->setAttribute($attr, $value);
                    }

                    $redis->set($docKey, $this->encode($document));
                    $redis->sAdd($idxKey, \strtolower($id));

                    $this->writePermissions($col, $id, $document);
                    $this->journal('createDoc', [
                        'collection' => $col,
                        'id' => $id,
                        'docKey' => $docKey,
                        'idxKey' => $idxKey,
                        'permDocKey' => $this->permDocKey($col, $id),
                    ]);

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
                $documents[$index]->setAttribute('$sequence', (string) $sequence);
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

    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value, string $updatedAt, int|float|null $min = null, int|float|null $max = null): bool
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
            $current = \is_numeric($current) ? $current + 0 : 0;

            if (! \is_null($min) && $current < $min) {
                return true;
            }
            if (! \is_null($max) && $current > $max) {
                return true;
            }

            $document->setAttribute($attribute, $current + $value);
            $document->setAttribute('$updatedAt', $updatedAt);

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

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return '';
    }

    public function setSupportForAttributes(bool $support): bool
    {
        $this->supportForAttributes = $support;

        return $this->supportForAttributes;
    }

    public function getConnectionId(): string
    {
        return '0';
    }

    /**
     * @param array<int, string> $selections
     */
    protected function getAttributeProjection(array $selections, string $prefix): mixed
    {
        return $selections;
    }

    protected function execute(mixed $stmt): bool
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

    private function tenantBucket(): ?string
    {
        if (! $this->getSharedTables()) {
            return null;
        }
        $tenant = $this->getTenant();

        return $tenant === null ? '_' : (string) $tenant;
    }

    private function permKey(string $collection, string $letter, string $role): string
    {
        $bucket = $this->tenantBucket();
        if ($bucket !== null) {
            return $this->ns().self::SEP.'perm'.self::SEP.'t'.self::SEP.$bucket.self::SEP.$collection.self::SEP.$letter.self::SEP.$role;
        }

        return $this->ns().self::SEP.'perm'.self::SEP.$collection.self::SEP.$letter.self::SEP.$role;
    }

    private function permDocKey(string $collection, string $id): string
    {
        $id = \strtolower($id);
        $bucket = $this->tenantBucket();
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

        return new Document($data);
    }

    /**
     * @param callable(RedisClient): mixed $fn
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

        $byRole = [];
        foreach ([PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete] as $type) {
            foreach ($document->getPermissionsByType($type) as $role) {
                $byRole[(string) $role][] = $this->actionLetter($type);
            }
        }

        if ($byRole === []) {
            return;
        }

        $hashKey = $this->permDocKey($collection, $id);
        $hashFields = [];
        $writes = [];
        foreach ($byRole as $role => $letters) {
            $unique = \array_values(\array_unique($letters));
            \sort($unique);
            $hashFields[$role] = \implode(',', $unique);
            foreach ($unique as $letter) {
                $writes[] = [$role, $letter];
            }
        }

        $this->client->multi(\Redis::PIPELINE);
        try {
            foreach ($writes as [$role, $letter]) {
                $this->client->sAdd($this->permKey($collection, $letter, $role), $id);
            }
            $this->client->hMSet($hashKey, $hashFields);
            $this->client->exec();
        } catch (\Throwable $e) {
            try {
                $this->client->discard();
            } catch (\Throwable) {
                // ignore
            }
            throw $e;
        }

        foreach ($writes as [$role, $letter]) {
            $this->journal('createPerm', [
                'collection' => $collection,
                'id' => $id,
                'role' => $role,
                'letter' => $letter,
            ]);
        }
    }

    private function clearPermissions(string $collection, string $id): void
    {
        $id = \strtolower($id);
        $hashKey = $this->permDocKey($collection, $id);
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
                $removals[] = [$role, $letter];
            }
        }

        $this->client->multi(\Redis::PIPELINE);
        try {
            foreach ($removals as [$role, $letter]) {
                $this->client->sRem($this->permKey($collection, $letter, $role), $id);
            }
            $this->client->del($hashKey);
            $this->client->exec();
        } catch (\Throwable $e) {
            try {
                $this->client->discard();
            } catch (\Throwable) {
                // ignore
            }
            throw $e;
        }

        foreach ($removals as [$role, $letter]) {
            $this->journal('deletePerm', [
                'collection' => $collection,
                'id' => $id,
                'role' => $role,
                'letter' => $letter,
                'previous' => $hash[$role] ?? '',
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
                    /** @var string $collection */
                    $collection = $payload['collection'];
                    /** @var string $id */
                    $id = $payload['id'];
                    $this->rawDeleteDoc(
                        $collection,
                        $id,
                        isset($payload['docKey']) ? (string) $payload['docKey'] : null,
                        isset($payload['idxKey']) ? (string) $payload['idxKey'] : null,
                        isset($payload['permDocKey']) ? (string) $payload['permDocKey'] : null,
                    );
                    break;

                case 'deleteDoc':
                    /** @var string $collection */
                    $collection = $payload['collection'];
                    /** @var string $id */
                    $id = $payload['id'];
                    /** @var string $beforePayload */
                    $beforePayload = $payload['payload'];
                    $this->rawRestoreDoc(
                        $collection,
                        $id,
                        $beforePayload,
                        isset($payload['docKey']) ? (string) $payload['docKey'] : null,
                        isset($payload['idxKey']) ? (string) $payload['idxKey'] : null,
                    );
                    break;

                case 'updateDoc':
                    /** @var string $collection */
                    $collection = $payload['collection'];
                    /** @var string $id */
                    $id = $payload['id'];
                    /** @var string $beforePayload */
                    $beforePayload = $payload['payload'];
                    $docKey = isset($payload['docKey']) ? (string) $payload['docKey'] : $this->docKey($collection, $id);
                    $this->client->set($docKey, $beforePayload);
                    if (isset($payload['newId']) && \is_string($payload['newId']) && $payload['newId'] !== $id) {
                        $newId = $payload['newId'];
                        $newDocKey = isset($payload['newDocKey']) ? (string) $payload['newDocKey'] : $this->docKey($collection, $newId);
                        $this->client->del($newDocKey);
                        $idxKey = isset($payload['idxKey']) ? (string) $payload['idxKey'] : $this->idxKey($collection);
                        $this->client->sRem($idxKey, \strtolower($newId));
                        $this->client->sAdd($idxKey, \strtolower($id));
                    }
                    break;

                case 'createPerm':
                    /** @var string $collection */
                    $collection = $payload['collection'];
                    /** @var string $letter */
                    $letter = $payload['letter'];
                    /** @var string $role */
                    $role = $payload['role'];
                    /** @var string $id */
                    $id = $payload['id'];
                    $this->client->sRem($this->permKey($collection, $letter, $role), $id);
                    $this->client->hDel($this->permDocKey($collection, $id), $role);
                    break;

                case 'deletePerm':
                    /** @var string $collection */
                    $collection = $payload['collection'];
                    /** @var string $letter */
                    $letter = $payload['letter'];
                    /** @var string $role */
                    $role = $payload['role'];
                    /** @var string $id */
                    $id = $payload['id'];
                    $this->client->sAdd($this->permKey($collection, $letter, $role), $id);
                    if (isset($payload['previous']) && \is_string($payload['previous']) && $payload['previous'] !== '') {
                        $this->client->hSet($this->permDocKey($collection, $id), $role, $payload['previous']);
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

    private function enforceUniqueIndexes(RedisClient $client, string $collection, Document $document, ?string $excludeId = null): void
    {
        $metaKey = $this->key($this->ns(), 'meta', $collection);
        $indexes = $this->readIndexesField($client, $metaKey);

        $uniqueIndexes = [];
        foreach ($indexes as $index) {
            if (($index['type'] ?? '') !== IndexType::Unique->value) {
                continue;
            }
            $attributes = $index['attributes'] ?? [];
            if (empty($attributes) || ! \is_array($attributes)) {
                continue;
            }
            $uniqueIndexes[] = $attributes;
        }

        if ($uniqueIndexes === []) {
            return;
        }

        $newSignatures = [];
        $sharedTables = $this->getSharedTables();
        $tenant = $sharedTables ? ($document->getAttribute('$tenant') ?? $this->getTenant()) : null;
        foreach ($uniqueIndexes as $i => $attributes) {
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
                \array_unshift($signature, $tenant);
            }
            $newSignatures[$i] = \serialize($signature);
        }

        if ($newSignatures === []) {
            return;
        }

        $idxKey = $this->idxKey($collection);
        /** @var array<int, string>|false $docIds */
        $docIds = $client->sMembers($idxKey);
        if (! \is_array($docIds) || empty($docIds)) {
            return;
        }

        $excludeKey = $excludeId !== null ? \strtolower($excludeId) : null;
        $docKeys = [];
        foreach ($docIds as $docId) {
            if ($excludeKey !== null && \strtolower((string) $docId) === $excludeKey) {
                continue;
            }
            $docKeys[(string) $docId] = $this->docKey($collection, (string) $docId);
        }
        if ($docKeys === []) {
            return;
        }

        /** @var array<int, mixed> $payloads */
        $payloads = $client->mGet(\array_values($docKeys));
        $position = 0;
        foreach ($docKeys as $docId => $_) {
            $payload = $payloads[$position++] ?? null;
            if (! \is_string($payload) || $payload === '') {
                continue;
            }
            $existing = $this->decode($payload);
            if ($sharedTables) {
                $rowTenant = $existing->getAttribute('$tenant');
                if ($rowTenant !== $tenant) {
                    continue;
                }
            }
            foreach ($newSignatures as $i => $newHash) {
                $attributes = $uniqueIndexes[$i];
                $signature = [];
                $hasNull = false;
                foreach ($attributes as $attribute) {
                    $value = $this->resolveDocumentAttribute($existing, (string) $attribute);
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
                    \array_unshift($signature, $tenant);
                }
                if (\serialize($signature) === $newHash) {
                    throw new DuplicateException('Document with the requested unique attributes already exists');
                }
            }
        }
    }

    private function purgeCollectionKeys(RedisClient $client, string $namespace, string $database, string $collection): void
    {
        $collection = $this->filter($collection);
        $prefix = $this->nsFor($namespace, $database);
        $metaKey = $this->key($prefix, 'meta', $collection);
        $idxKey = $this->key($prefix, 'idx', $collection);
        $seqKey = $this->key($prefix, 'seq', $collection);

        /** @var array<int, string>|false $docIds */
        $docIds = $client->sMembers($idxKey);
        if (\is_array($docIds) && $docIds !== []) {
            $keys = [];
            foreach ($docIds as $docId) {
                $keys[] = $this->key($prefix, 'doc', $collection, $docId);
                $keys[] = $this->key($prefix, 'perm', 'doc', $collection, $docId);
                if (\count($keys) >= self::SCAN_BATCH_SIZE) {
                    $client->del(...$keys);
                    $keys = [];
                }
            }
            if ($keys !== []) {
                $client->del(...$keys);
            }
        }

        $this->deleteByPattern($client, $prefix.self::SEP.'doc'.self::SEP.'t'.self::SEP.'*'.self::SEP.$collection.self::SEP.'*');
        $this->deleteByPattern($client, $prefix.self::SEP.'idx'.self::SEP.'t'.self::SEP.'*'.self::SEP.$collection);
        $this->deleteByPattern($client, $prefix.self::SEP.'seq'.self::SEP.'t'.self::SEP.'*'.self::SEP.$collection);

        $this->deleteByPattern($client, $this->key($prefix, 'perm', $collection).self::SEP.'*');
        $this->deleteByPattern($client, $prefix.self::SEP.'perm'.self::SEP.'t'.self::SEP.'*'.self::SEP.$collection.self::SEP.'*');
        $this->deleteByPattern($client, $prefix.self::SEP.'perm'.self::SEP.'t'.self::SEP.'*'.self::SEP.'doc'.self::SEP.$collection.self::SEP.'*');
        $this->deleteByPattern($client, $this->key($prefix, 'tenants', $collection).self::SEP.'*');

        $client->del($metaKey, $idxKey, $seqKey);
    }

    private function deleteByPattern(RedisClient $client, string $pattern): void
    {
        $cursor = null;
        do {
            /** @var array<int, string>|false $batch */
            $batch = $client->scan($cursor, $pattern, self::SCAN_BATCH_SIZE);
            if (\is_array($batch) && $batch !== []) {
                $client->del(...$batch);
            }
        } while ($cursor !== 0 && $cursor !== null);
    }

    private function computeCollectionSize(string $collection): int
    {
        $collection = $this->filter($collection);
        $metaKey = $this->key($this->ns(), 'meta', $collection);

        if ((bool) $this->client->exists($metaKey) === false) {
            return 0;
        }

        $total = $this->measureKey($metaKey);

        $idxKey = $this->idxKey($collection);
        $total += $this->measureKey($idxKey);

        /** @var array<int, string>|false $docIds */
        $docIds = $this->client->sMembers($idxKey);
        if (\is_array($docIds)) {
            foreach ($docIds as $docId) {
                $total += $this->measureKey($this->docKey($collection, (string) $docId));
                $total += $this->measureKey($this->permDocKey($collection, (string) $docId));
            }
        }

        $bucket = $this->tenantBucket();
        if ($bucket !== null) {
            $permPrefix = $this->ns().self::SEP.'perm'.self::SEP.'t'.self::SEP.$bucket.self::SEP.$collection.self::SEP.'*';
        } else {
            $permPrefix = $this->key($this->ns(), 'perm', $collection).self::SEP.'*';
        }
        $cursor = null;
        do {
            /** @var array<int, string>|false $batch */
            $batch = $this->client->scan($cursor, $permPrefix, self::SCAN_BATCH_SIZE);
            if (\is_array($batch)) {
                foreach ($batch as $key) {
                    $total += $this->measureKey($key);
                }
            }
        } while ($cursor !== 0 && $cursor !== null);

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
            '$id' => $field,
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
            $key = (string) ($attribute['$id'] ?? $attribute['key'] ?? '');
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
                $rowTenant = $document->getAttribute('$tenant');
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
     * @param array<int, Query> $queries
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
                $needle = (string) ($values[0] ?? '');
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
                $needle = (string) ($values[0] ?? '');
                if ($needle === '') {
                    return true;
                }

                return ! $this->matchesFulltextRedis($value, $needle);

            case Method::Regex:
                if (! \is_string($value)) {
                    return false;
                }
                $pattern = (string) ($values[0] ?? '');
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
     * @param array<int, string> $orderAttributes
     * @param array<int, OrderDirection> $orderTypes
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
                $av = $a->getAttribute('$sequence', 0);
                $bv = $b->getAttribute('$sequence', 0);
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
     * @param array<int, string> $orderAttributes
     * @param array<int, OrderDirection> $orderTypes
     * @param array<string, mixed> $cursor
     * @return array<int, Document>
     */
    private function cursorDocuments(array $documents, array $orderAttributes, array $orderTypes, array $cursor, CursorDirection $cursorDirection): array
    {
        if (empty($cursor)) {
            return $documents;
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
     * @param array<int, Query> $queries
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
            if (\is_string($field) && (\str_starts_with($field, '$') || \str_starts_with($field, '_'))) {
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

        switch ($method) {
            case OperatorType::Increment:
                $by = $values[0] ?? 1;
                $max = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;
                if ($max !== null && \is_numeric($by) && \is_numeric($max)) {
                    if ($base >= $max || ($max - $base) <= $by) {
                        return $this->preserveNumericType($base, $max);
                    }
                }

                return $this->preserveNumericType($base, $base + $by);

            case OperatorType::Decrement:
                $by = $values[0] ?? 1;
                $min = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;
                if ($min !== null && \is_numeric($by) && \is_numeric($min)) {
                    if ($base <= $min || ($base - $min) <= $by) {
                        return $this->preserveNumericType($base, $min);
                    }
                }

                return $this->preserveNumericType($base, $base - $by);

            case OperatorType::Multiply:
                $by = $values[0] ?? 1;
                $max = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($base * $by, $max, true);

            case OperatorType::Divide:
                $by = $values[0] ?? 1;
                $min = $values[1] ?? null;
                if (! \is_numeric($by) || $by == 0) {
                    return $current;
                }
                $base = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($base / $by, $min, false);

            case OperatorType::Modulo:
                $by = $values[0] ?? 1;
                if (! \is_numeric($by) || $by == 0) {
                    return $current;
                }
                $base = \is_numeric($current) ? (int) $current : 0;

                return $base % (int) $by;

            case OperatorType::Power:
                $by = $values[0] ?? 1;
                $max = $values[1] ?? null;
                $base = \is_numeric($current) ? $current + 0 : 0;

                return $this->applyNumericLimit($base ** $by, $max, true);

            case OperatorType::StringConcat:
                return ((string) ($current ?? '')).(string) ($values[0] ?? '');

            case OperatorType::StringReplace:
                $search = (string) ($values[0] ?? '');
                $replace = (string) ($values[1] ?? '');
                if ($current === null) {
                    return null;
                }

                return \str_replace($search, $replace, (string) $current);

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
                $condition = (string) ($values[0] ?? '');
                $compare = $values[1] ?? null;

                return \array_values(\array_filter($list, fn ($item) => $this->matchesArrayFilter($item, $condition, $compare)));

            case OperatorType::DateAddDays:
                $days = (int) ($values[0] ?? 0);

                return $this->shiftDate($current, $days * 86400);

            case OperatorType::DateSubDays:
                $days = (int) ($values[0] ?? 0);

                return $this->shiftDate($current, -$days * 86400);

            case OperatorType::DateSetNow:
                return DateTime::now();
        }

        throw new OperatorException('Invalid operator: '.$method->value);
    }

    protected function applyNumericLimit(mixed $value, mixed $bound, bool $isUpper): int|float
    {
        $numericValue = \is_numeric($value) ? $value + 0 : 0;
        if (! \is_numeric($bound)) {
            return $numericValue;
        }
        $numericBound = $bound + 0;

        return $isUpper ? \min($numericValue, $numericBound) : \max($numericValue, $numericBound);
    }

    protected function preserveNumericType(mixed $original, mixed $result): mixed
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
        try {
            $base = new \DateTime((string) $current);
        } catch (\Throwable) {
            return $current === '' ? null : (string) $current;
        }
        $base->modify(($seconds >= 0 ? '+' : '').$seconds.' seconds');

        return DateTime::format($base);
    }
}
