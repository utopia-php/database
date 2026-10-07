<?php

namespace Utopia\Database\Hook;

use Closure;
use Exception;
use Swoole\Coroutine;
use Throwable;
use Utopia\Async\Promise;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\State\Snapshot;
use Utopia\Database\State\Value;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Query\Hook;
use Utopia\Query\Method;

/**
 * Handles relationship side effects for document CRUD, populates nested relationships
 * on read, and converts relationship filter queries into adapter-compatible subqueries.
 */
class Relationships implements Attachable, Hook
{
    /**
     * The most chunks of related ids one population reads at the same time
     */
    public const int READ_CONCURRENCY = 4;

    /**
     * @var Value<bool>
     */
    private Value $enabled;

    /**
     * @var Value<bool>
     */
    private Value $checkExist;

    private int $fetchDepth = 0;

    /**
     * @var Value<bool>
     */
    private Value $inBatchPopulation;

    /**
     * @var array<int, list<string>> The collections of each coroutine's relationship writes in progress, innermost
     *                               last, by coroutine id
     */
    private array $writeStacks = [];

    /**
     * @var array<int, list<Cascade>> The relationships of each coroutine's cascading deletes in progress,
     *                                innermost last, by coroutine id
     */
    private array $deleteStacks = [];

    /**
     * @var array<int, PreparedCreate> Each coroutine's create in progress whose related documents are prepared, by
     *                                 coroutine id
     */
    private array $prepared = [];

    /**
     * @var array<int, int> How many creates each coroutine is relating one document at a time, by coroutine id
     */
    private array $replays = [];

    private Database $database;

    /**
     * @param bool $prepare Whether a create whose related documents are all new prepares them instead of creating
     *                      each through createDocument(), which reads it before and after writing it
     */
    public function __construct(
        private readonly bool $prepare = true,
    ) {
        $this->enabled = new Value(true);
        $this->checkExist = new Value(true);
        $this->inBatchPopulation = new Value(false);
    }

    /**
     * A copy configured like this hook, with none of its state, for another database to attach.
     */
    public function __clone()
    {
        $this->enabled = new Value(true);
        $this->checkExist = new Value(true);
        $this->inBatchPopulation = new Value(false);
        $this->fetchDepth = 0;
        $this->writeStacks = [];
        $this->deleteStacks = [];
        $this->prepared = [];
        $this->replays = [];
    }

    public function attach(Database $database): void
    {
        $this->database = $database;
    }

    /**
     * Capped by RELATION_QUERY_CHUNK_SIZE as a memory bound, but never larger than the configured maxQueryValues,
     * otherwise a caller that lowers the validator cap would still see relationship updates throw QueryException on
     * the chunked find/update fallback.
     *
     * @return int<1, max>
     */
    private function relationQueryChunkSize(): int
    {
        return \max(1, \min(Database::RELATION_QUERY_CHUNK_SIZE, $this->database->getMaxQueryValues()));
    }

    /**
     * Run one read per chunk and return their documents in chunk order. Several reads run at the same time only where
     * each can borrow its own connection: inside a coroutine, on a pooled adapter whose connection no transaction
     * has pinned, and at most as many as {@see self::READ_CONCURRENCY} and the pool's idle connections allow. Each
     * concurrent read starts from its caller's authorization, relationship and silence state, and what it changes
     * stays in its own coroutine.
     *
     * @param  array<Closure(): array<Document>>  $reads
     * @return array<Document>
     */
    private function readChunks(array $reads): array
    {
        $reads = \array_values($reads);
        $concurrency = $this->readConcurrency(\count($reads));

        if ($concurrency > 1) {
            $chunks = $this->readConcurrently($reads, $concurrency);
        } else {
            $chunks = [];
            foreach ($reads as $read) {
                $chunks[] = $read();
            }
        }

        $documents = [];
        foreach ($chunks as $chunk) {
            \array_push($documents, ...$chunk);
        }

        return $documents;
    }

    /**
     * @param  list<Closure(): array<Document>>  $reads
     * @param  int<2, max>  $concurrency
     * @return array<int, array<Document>>
     */
    private function readConcurrently(array $reads, int $concurrency): array
    {
        $snapshot = $this->database->snapshot();
        $chunks = [];
        $next = 0;

        $reader = function () use ($reads, $snapshot, &$chunks, &$next): void {
            while (isset($reads[$next])) {
                $index = $next++;

                try {
                    $chunks[$index] = $this->database->withSnapshot($snapshot, $reads[$index]);
                } catch (Throwable $error) {
                    $next = \count($reads);

                    throw $error;
                }
            }
        };

        Promise::map(\array_fill(0, $concurrency, $reader))->await();
        \ksort($chunks);

        return $chunks;
    }

    private function readConcurrency(int $reads): int
    {
        if ($reads < 2 || ! \extension_loaded('swoole') || Coroutine::getCid() <= 0) {
            return 1;
        }

        $adapter = $this->database->getAdapter();
        if (! $adapter instanceof Pool) {
            return 1;
        }

        return \min($reads, self::READ_CONCURRENCY, $adapter->getReadConcurrency());
    }

    /**
     * @param  array<string>  $ids
     * @param  Closure(array<string>): array<Document>  $read
     * @return array<Document>
     */
    private function readByIds(array $ids, Closure $read): array
    {
        $documents = [];
        foreach (\array_chunk($ids, $this->relationQueryChunkSize()) as $chunk) {
            \array_push($documents, ...$read($chunk));
        }

        return $documents;
    }

    private function coerceToDocument(Document $document, string $key, mixed $value): mixed
    {
        if (\is_array($value) && ! \array_is_list($value)) {
            try {
                $value = new Document($value); // @phpstan-ignore argument.type
            } catch (StructureException $e) {
                throw new RelationshipException('Invalid relationship value. ' . $e->getMessage());
            }
            $document->setAttribute($key, $value);
        }

        return $value;
    }

    /**
     * @internal
     */
    public function isEnabled(): bool
    {
        return $this->enabled->get();
    }

    /**
     * @internal
     */
    public function setEnabled(bool $enabled): void
    {
        $this->enabled->set($enabled);
    }

    /**
     * Run the callback with relationships enabled or disabled for the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @internal
     */
    public function withEnabled(bool $enabled, callable $callback): mixed
    {
        return $this->enabled->with($enabled, $callback);
    }

    /**
     * @internal
     */
    public function shouldCheckExist(): bool
    {
        return $this->checkExist->get();
    }

    /**
     * Run the callback with existence checks on or off for the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @internal
     */
    public function withCheckExist(bool $check, callable $callback): mixed
    {
        return $this->checkExist->with($check, $callback);
    }

    /**
     * @internal
     */
    public function getWriteStackCount(): int
    {
        return \count($this->writeStacks[$this->coroutine()] ?? []);
    }

    /**
     * @internal
     */
    public function getFetchDepth(): int
    {
        return $this->fetchDepth;
    }

    /**
     * @internal
     */
    public function isInBatchPopulation(): bool
    {
        return $this->inBatchPopulation->get();
    }

    /**
     * Run the callback under the relationship state a snapshot carries.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     *
     * @internal
     */
    public function withSnapshot(Snapshot $snapshot, callable $callback): mixed
    {
        return $this->enabled->with(
            $snapshot->relationships,
            fn () => $this->checkExist->with(
                $snapshot->existCheck,
                fn () => $this->inBatchPopulation->with($snapshot->population, $callback),
            ),
        );
    }

    /**
     * Relate the related documents of a new document, removing or replacing its relationship values with what is
     * stored for them.
     *
     * @param  array<int, array{Document, array<string, mixed>}>|null  $copies  Given an array, receives the document
     *                                                                            and the documents nested in it as
     *                                                                            they were before relating changed
     *                                                                            them, for restore() to give back to an
     *                                                                            attempt the transaction retries
     *
     * @throws DuplicateException If a related document already exists
     * @throws RelationshipException If a relationship constraint is violated
     *
     * @internal
     */
    public function afterDocumentCreate(Document $collection, Document $document, ?array &$copies = null): Document
    {
        $coroutine = $this->coroutine();
        $relate = function (?PreparedCreate $prepared) use ($collection, $document, $coroutine, &$copies): Document {
            return $this->relate($collection, $document, $coroutine, $prepared, $copies);
        };

        if (! $this->canPrepare($coroutine) || ! $this->hasRelatedDocuments($collection, $document)) {
            return $relate(null);
        }

        $prepared = $this->createPrepared();
        $created = [];
        $visited = [];
        if (! $this->collectCreated($prepared, $collection, $document, $this->writeStacks[$coroutine] ?? [], $created, $visited)) {
            return $this->relateOneByOne($coroutine, $relate);
        }

        if ($this->isReferencedBack($collection, $document)) {
            $created[$collection->getId()][\strtolower($document->getId())] = $document->getId();
            $prepared->preparing[$collection->getId()][$document->getId()] = true;
            $prepared->collections[$collection->getId()] ??= $collection;
        }

        return $this->relatePrepared($prepared, $coroutine, $created, [$document], $relate);
    }

    /**
     * Relate new related documents through $relate without reading each before creating it and reading it back
     * after, when none of them is stored yet. Where the adapter has savepoints, the new related documents and
     * their junction documents are prepared without being written, then written in the order they would have
     * been written one by one, and any failure rolls the attempt back and relates them one by one instead, so a
     * failing write fails the way it always has. Elsewhere each is written where it would have been written on its
     * own. Either way a failure restores every document it changed, so relating one by one, or a transaction that
     * retries the write after the engine rolled it back, starts over from the documents it was given.
     *
     * @template T
     *
     * @param  array<string, array<string, string>>  $created  The ids of the related documents to create, by collection
     * @param  list<Document>  $documents  The documents the relating changes
     * @param  Closure(?PreparedCreate): T  $relate  Relates the documents, prepared or, given null, one by one
     * @return T
     */
    private function relatePrepared(PreparedCreate $prepared, int $coroutine, array $created, array $documents, Closure $relate): mixed
    {
        if ($this->anyStored($prepared, $created)) {
            return $this->relateOneByOne($coroutine, $relate);
        }

        $copies = [];
        foreach ($documents as $document) {
            $this->copy($document, $copies);
        }

        $attempt = function () use ($prepared, $coroutine, $relate, $copies): mixed {
            $this->prepared[$coroutine] = $prepared;

            try {
                $result = $relate($prepared);
                $this->writePrepared($prepared);

                return $result;
            } catch (Throwable $error) {
                $this->restore($copies);

                throw $error;
            } finally {
                unset($this->prepared[$coroutine]);
            }
        };

        if (! $prepared->deferred) {
            return $attempt();
        }

        $replayed = false;
        $replay = function () use ($coroutine, $relate, &$replayed): mixed {
            $replayed = true;

            return $this->relateOneByOne($coroutine, $relate);
        };

        try {
            return $this->database->withSavepoint($attempt, $replay);
        } catch (Throwable $error) {
            if (! $replayed) {
                $this->restore($copies);
            }

            throw $error;
        }
    }

    /**
     * Relate an updated document's new related documents through $relate, prepared when none of them is stored.
     *
     * @param  list<Document>  $documents
     * @param  Closure(?PreparedCreate): void  $relate  Relates the documents, prepared or, given null, one by one
     */
    private function relateUpdated(int $coroutine, Document $relatedCollection, array $documents, Closure $relate): void
    {
        if ($documents === [] || ! $this->canPrepare($coroutine)) {
            $relate(null);

            return;
        }

        $prepared = $this->createPrepared();
        $prepared->collections[$relatedCollection->getId()] = $relatedCollection;
        $created = [];
        $visited = [];
        if (! $this->collectRelated($prepared, $relatedCollection, $documents, $this->writeStacks[$coroutine] ?? [], $created, $visited)) {
            $this->relateOneByOne($coroutine, $relate);

            return;
        }

        $this->relatePrepared($prepared, $coroutine, $created, $documents, $relate);
    }

    private function createPrepared(): PreparedCreate
    {
        return new PreparedCreate($this->database->getAdapter()->supports(Capability::TransactionNested));
    }

    /**
     * @template T
     *
     * @param  Closure(?PreparedCreate): T  $relate
     * @return T
     */
    private function relateOneByOne(int $coroutine, Closure $relate): mixed
    {
        $this->replays[$coroutine] = ($this->replays[$coroutine] ?? 0) + 1;

        try {
            return $relate(null);
        } finally {
            if (--$this->replays[$coroutine] === 0) {
                unset($this->replays[$coroutine]);
            }
        }
    }

    /**
     * Whether this coroutine's write may prepare its new related documents: none of its writes is preparing or
     * relating one by one already, and it runs in a transaction.
     */
    private function canPrepare(int $coroutine): bool
    {
        if (! $this->prepare || isset($this->prepared[$coroutine]) || isset($this->replays[$coroutine])) {
            return false;
        }

        $adapter = $this->database->getAdapter();

        return $adapter->inTransaction() && ! $adapter->getTenantPerDocument();
    }

    private function hasRelatedDocuments(Document $collection, Document $document): bool
    {
        foreach (self::attributes($collection) as $attribute) {
            if ($attribute->relationship === null) {
                continue;
            }

            $value = $document->getAttribute($attribute->key);
            if ($value instanceof Document) {
                return true;
            }

            if (\is_array($value)) {
                foreach ($value as $related) {
                    if ($related instanceof Document) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Collect the ids of the related documents a new document's relationships would create, by related
     * collection, as deep as relate() goes from the given write stack. Returns false when a related document
     * appears twice, as its second appearance may find the first one written.
     *
     * @param  list<string>  $writeStack
     * @param  array<string, array<string, string>>  $created  Ids by related collection id and lower-cased id
     * @param  array<int, true>  $visited
     */
    private function collectCreated(PreparedCreate $prepared, Document $collection, Document $document, array $writeStack, array &$created, array &$visited): bool
    {
        $depth = \count($writeStack);

        foreach (self::attributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            if ($relationship === null) {
                continue;
            }

            $key = $attribute->key;
            try {
                $value = $this->coerceToDocument($document, $key, $document->getAttribute($key));
            } catch (RelationshipException) {
                return false;
            }
            $related = \array_values(\array_filter(
                $value instanceof Document ? [$value] : (\is_array($value) ? $value : []),
                static fn (mixed $item): bool => $item instanceof Document,
            ));
            if ($related === []) {
                continue;
            }

            $relatedCollection = $this->collection($prepared, $relationship->relatedCollection);

            if ($depth >= Database::RELATION_MAX_DEPTH - 1 && $writeStack[$depth - 1] !== $relatedCollection->getId()) {
                continue;
            }

            if (! $this->collectRelated($prepared, $relatedCollection, $related, [...$writeStack, $collection->getId()], $created, $visited)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Collect the ids of related documents, and of the related documents they would create in turn.
     *
     * @param  list<Document>  $documents
     * @param  list<string>  $writeStack  The write stack relating each of them sees
     * @param  array<string, array<string, string>>  $created
     * @param  array<int, true>  $visited
     */
    private function collectRelated(PreparedCreate $prepared, Document $relatedCollection, array $documents, array $writeStack, array &$created, array &$visited): bool
    {
        foreach ($documents as $document) {
            $object = \spl_object_id($document);
            if (isset($visited[$object])) {
                return false;
            }
            $visited[$object] = true;

            $id = $document->getId();
            if ($id !== '') {
                $lowered = \strtolower($id);
                if (isset($created[$relatedCollection->getId()][$lowered])) {
                    return false;
                }
                $created[$relatedCollection->getId()][$lowered] = $id;
            }

            if (! $this->collectCreated($prepared, $relatedCollection, $document, $writeStack, $created, $visited)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Whether a new document's related documents are given a reference back to it that relating them writes
     * through, which reads it first.
     */
    private function isReferencedBack(Document $collection, Document $document): bool
    {
        foreach (self::attributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            if ($relationship === null || $relationship->type !== RelationshipType::OneToOne || ! $relationship->twoWay) {
                continue;
            }

            $value = $document->getAttribute($attribute->key);
            if ($value instanceof Document || \is_array($value)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Whether any of the documents is already stored, readable by the caller or not, or cannot be looked up.
     *
     * @param  array<string, array<string, string>>  $ids  Ids by collection id
     */
    private function anyStored(PreparedCreate $prepared, array $ids): bool
    {
        $adapter = $this->database->getAdapter();
        $tenant = $adapter->getSharedTables() ? $adapter->getTenant() : null;

        try {
            foreach ($ids as $collection => $collectionIds) {
                foreach (\array_chunk(\array_values($collectionIds), $this->relationQueryChunkSize()) as $chunk) {
                    $documents = \array_map(
                        static fn (string $id): Document => new Document([Document::ID => $id, Document::TENANT => $tenant]),
                        $chunk,
                    );
                    foreach ($adapter->getSequences($this->collection($prepared, $collection), $documents) as $document) {
                        if ($document->getSequence() !== null) {
                            return true;
                        }
                    }
                }
            }
        } catch (Throwable) {
            return true;
        }

        return false;
    }

    /**
     * A collection read once for the whole prepared create, as it cannot change while the create's transaction
     * is open.
     */
    private function collection(PreparedCreate $prepared, string $id): Document
    {
        return $prepared->collections[$id] ??= $this->database->getCollection($id);
    }

    /**
     * The attributes $collection declares, hydrated at most once per collection instance.
     *
     * @return list<Attribute>
     */
    private static function attributes(Document $collection): array
    {
        return Collection::fromDocument($collection)->attributes();
    }

    /**
     * The relationship attributes $collection declares.
     *
     * @return list<Attribute>
     */
    private static function relationships(Document $collection): array
    {
        $relationships = [];
        foreach (self::attributes($collection) as $attribute) {
            if ($attribute->relationship !== null) {
                $relationships[] = $attribute;
            }
        }

        return $relationships;
    }

    /**
     * Record the attributes of the document and of every document nested in it, so they can be restored.
     *
     * @param  array<int, array{Document, array<string, mixed>}>  $copies
     */
    private function copy(Document $document, array &$copies): void
    {
        $id = \spl_object_id($document);
        if (isset($copies[$id])) {
            return;
        }

        /** @var array<string, mixed> $attributes */
        $attributes = (array) $document;
        $copies[$id] = [$document, $attributes];

        foreach ($attributes as $value) {
            if ($value instanceof Document) {
                $this->copy($value, $copies);
            } elseif (\is_array($value)) {
                foreach ($value as $item) {
                    if ($item instanceof Document) {
                        $this->copy($item, $copies);
                    }
                }
            }
        }
    }

    /**
     * Give each copied document back the attributes it was copied with.
     *
     * @internal
     *
     * @param  array<int, array{Document, array<string, mixed>}>  $copies
     */
    public function restore(array $copies): void
    {
        foreach ($copies as [$document, $attributes]) {
            $document->exchangeArray($attributes);
        }
    }

    /**
     * Prepare a new related document through the checks createDocument() applies, relate its own related
     * documents, and write or queue it after them, where createDocument() would have written it.
     */
    private function prepare(PreparedCreate $prepared, Document $collection, Document $document, int $coroutine): string
    {
        $document = $this->database->prepareCreate($collection, $document);
        $id = $document->getId();

        $prepared->preparing[$collection->getId()][$id] = true;
        $document = $this->relate($collection, $document, $coroutine, $prepared);
        unset($prepared->preparing[$collection->getId()][$id]);

        $prepared->documents[] = [$collection, $document];
        if (! $prepared->deferred) {
            $this->writePrepared($prepared);
        }

        return $id;
    }

    /**
     * Write the prepared documents queued so far, so whatever comes next sees the database as it would be had
     * each been written on its own.
     */
    private function writePrepared(PreparedCreate $prepared): void
    {
        if ($prepared->documents === []) {
            return;
        }

        $documents = $prepared->documents;
        $prepared->documents = [];
        $this->database->createPrepared($documents);
    }

    /**
     * @param  array<int, array{Document, array<string, mixed>}>|null  $copies  Receives the document and the
     *                                                                            documents nested in it before
     *                                                                            relating changes them, unless the
     *                                                                            relating is nested in another write
     */
    private function relate(Document $collection, Document $document, int $coroutine, ?PreparedCreate $prepared, ?array &$copies = null): Document
    {
        $writeStack = $this->writeStacks[$coroutine] ?? [];
        $stackCount = \count($writeStack);

        foreach (self::attributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            $side = $attribute->side;
            if ($relationship === null || $side === null) {
                continue;
            }

            $key = $attribute->key;
            $value = $document->getAttribute($key);
            if ($copies !== null && $stackCount === 0 && (\is_array($value) || $value instanceof Document)) {
                $this->copy($document, $copies);
            }
            $relatedCollection = $prepared === null
                ? $this->database->getCollection($relationship->relatedCollection)
                : $this->collection($prepared, $relationship->relatedCollection);
            $relationType = $relationship->type;
            $twoWay = $relationship->twoWay;
            $twoWayKey = $relationship->twoWayKey ?? '';

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $writeStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);

                continue;
            }

            $this->writeStacks[$coroutine][] = $collection->getId();

            try {
                $value = $this->coerceToDocument($document, $key, $value);

                if (\is_array($value)) {
                    if ($relationType === RelationshipType::OneToOne && ! $twoWay && $side === RelationshipSide::Child) {
                        throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                    }

                    if (
                        ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Parent) ||
                        ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Child) ||
                        ($relationType === RelationshipType::OneToOne)
                    ) {
                        throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document, array given.');
                    }

                    foreach ($value as $related) {
                        if ($related instanceof Document) {
                            $this->relateDocuments(
                                $collection,
                                $relatedCollection,
                                $key,
                                $document,
                                $related,
                                $relationType,
                                $twoWay,
                                $twoWayKey,
                                $side,
                                $coroutine,
                                $prepared,
                            );
                        } elseif (\is_string($related)) {
                            $this->relateDocumentsById(
                                $collection,
                                $relatedCollection,
                                $key,
                                $document->getId(),
                                $related,
                                $relationType,
                                $twoWay,
                                $twoWayKey,
                                $side,
                                $prepared,
                            );
                        } else {
                            throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                        }
                    }
                    $document->removeAttribute($key);
                } elseif ($value instanceof Document) {
                    if ($relationType === RelationshipType::OneToOne && ! $twoWay && $side === RelationshipSide::Child) {
                        throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                    }

                    if (
                        ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Parent) ||
                        ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Child) ||
                        ($relationType === RelationshipType::ManyToMany)
                    ) {
                        throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, document given.');
                    }

                    $relatedId = $this->relateDocuments(
                        $collection,
                        $relatedCollection,
                        $key,
                        $document,
                        $value,
                        $relationType,
                        $twoWay,
                        $twoWayKey,
                        $side,
                        $coroutine,
                        $prepared,
                    );
                    $document->setAttribute($key, $relatedId);
                } elseif (\is_string($value)) {
                    if ($relationType === RelationshipType::OneToOne && $twoWay === false && $side === RelationshipSide::Child) {
                        throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                    }

                    if (
                        ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Parent) ||
                        ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Child) ||
                        ($relationType === RelationshipType::ManyToMany)
                    ) {
                        throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, document ID given.');
                    }

                    $this->relateDocumentsById(
                        $collection,
                        $relatedCollection,
                        $key,
                        $document->getId(),
                        $value,
                        $relationType,
                        $twoWay,
                        $twoWayKey,
                        $side,
                        $prepared,
                    );
                } elseif ($value === null) {
                    if (
                        !(($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Child) ||
                        ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Parent) ||
                        ($relationType === RelationshipType::OneToOne && $side === RelationshipSide::Parent) ||
                        ($relationType === RelationshipType::OneToOne && $twoWay === true))
                    ) {
                        $document->removeAttribute($key);
                    }
                } else {
                    throw new RelationshipException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                }
            } finally {
                $this->leaveWrite($coroutine);
            }
        }

        return $document;
    }

    /**
     * Relate the related documents of an updated document.
     *
     * @throws DuplicateException If a related document already exists
     * @throws RelationshipException If a relationship constraint is violated
     * @throws RestrictedException If a restricted relationship is violated
     *
     * @internal
     */
    public function afterDocumentUpdate(Document $collection, Document $old, Document $document): Document
    {
        $coroutine = $this->coroutine();
        $writeStack = $this->writeStacks[$coroutine] ?? [];
        $stackCount = \count($writeStack);

        foreach (self::attributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            $side = $attribute->side;
            if ($relationship === null || $side === null) {
                continue;
            }

            $key = $attribute->key;
            $value = $document->getAttribute($key);

            $value = $this->coerceToDocument($document, $key, $value);

            $oldValue = $old->getAttribute($key);
            $relatedCollection = $this->database->getCollection($relationship->relatedCollection);
            $relationType = $relationship->type;
            $twoWay = $relationship->twoWay;
            $twoWayKey = $relationship->twoWayKey ?? '';

            if (Operator::isOperator($value)) {
                /** @var Operator $operator */
                $operator = $value;
                if ($operator->isArrayOperation()) {
                    $existingIds = [];
                    if (\is_array($oldValue)) {
                        /** @var array<Document|string> $oldValue */
                        $existingIds = \array_map(fn ($item) => $item instanceof Document ? $item->getId() : (string) $item, $oldValue);
                    }

                    $value = $this->applyRelationshipOperator($operator, $existingIds);
                    $document->setAttribute($key, $value);
                }
            }

            if ($oldValue == $value) {
                if (
                    ($relationType === RelationshipType::OneToOne
                        || ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Parent)) &&
                    $value instanceof Document
                ) {
                    $document->setAttribute($key, $value->getId());

                    continue;
                }
                $document->removeAttribute($key);

                continue;
            }

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $writeStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);

                continue;
            }

            $this->writeStacks[$coroutine][] = $collection->getId();

            try {
                switch ($relationType) {
                    case RelationshipType::OneToOne:
                        if (! $twoWay) {
                            if ($side === RelationshipSide::Child) {
                                throw new RelationshipException('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.');
                            }

                            if (\is_string($value)) {
                                $related = $this->database->skipRelationships(fn () => $this->database->getDocument($relatedCollection->getId(), $value, [Query::select([Document::ID])]));
                                if ($related->isEmpty()) {
                                    $document->setAttribute($key, null);
                                }
                            } elseif ($value instanceof Document) {
                                $relationId = $this->relateDocuments(
                                    $collection,
                                    $relatedCollection,
                                    $key,
                                    $document,
                                    $value,
                                    $relationType,
                                    false,
                                    $twoWayKey,
                                    $side,
                                    $coroutine,
                                    null,
                                );
                                $document->setAttribute($key, $relationId);
                            } elseif (is_array($value)) {
                                throw new RelationshipException('Invalid relationship value. Must be either a document, document ID or null. Array given.');
                            }

                            break;
                        }

                        if (\is_string($value)) {
                            $related = $this->database->skipRelationships(
                                fn () => $this->database->getDocument($relatedCollection->getId(), $value, [Query::select([Document::ID])])
                            );

                            if ($related->isEmpty()) {
                                $document->setAttribute($key, null);
                            } else {
                                /** @var Document|null $oldValueDoc */
                                $oldValueDoc = $oldValue instanceof Document ? $oldValue : null;
                                if (
                                    $oldValueDoc?->getId() !== $value
                                    && $this->isLinkedElsewhere($collection, $key, $value, $document)
                                ) {
                                    throw new DuplicateException('Document already has a related document');
                                }

                                $this->database->skipRelationships(fn () => $this->database->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $related->setAttribute($twoWayKey, $document->getId())
                                ));
                            }
                        } elseif ($value instanceof Document) {
                            $related = $this->database->skipRelationships(fn () => $this->database->getDocument($relatedCollection->getId(), $value->getId()));

                            /** @var Document|null $oldValueDoc2 */
                            $oldValueDoc2 = $oldValue instanceof Document ? $oldValue : null;
                            if (
                                $oldValueDoc2?->getId() !== $value->getId()
                                && $this->isLinkedElsewhere($collection, $key, $value->getId(), $document)
                            ) {
                                throw new DuplicateException('Document already has a related document');
                            }

                            if ($related->isEmpty()) {
                                if (! isset($value[Document::PERMISSIONS])) {
                                    $value->setAttribute(Document::PERMISSIONS, $document->getAttribute(Document::PERMISSIONS));
                                }
                                $related = $this->database->createDocument(
                                    $relatedCollection->getId(),
                                    $value->setAttribute($twoWayKey, $document->getId())
                                );
                            } else {
                                $related = $this->database->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $value->setAttribute($twoWayKey, $document->getId())
                                );
                            }

                            $document->setAttribute($key, $related->getId());
                        } elseif ($value === null) {
                            /** @var Document|null $oldValueDocNull */
                            $oldValueDocNull = $oldValue instanceof Document ? $oldValue : null;
                            if ($oldValueDocNull?->getId() !== null) {
                                $oldRelated = $this->database->skipRelationships(
                                    fn () => $this->database->getDocument($relatedCollection->getId(), $oldValueDocNull->getId())
                                );
                                $this->database->skipRelationships(fn () => $this->database->updateDocument(
                                    $relatedCollection->getId(),
                                    $oldRelated->getId(),
                                    new Document([$twoWayKey => null])
                                ));
                            }
                        } else {
                            throw new RelationshipException('Invalid relationship value. Must be either a document, document ID or null.');
                        }
                        break;
                    case RelationshipType::OneToMany:
                    case RelationshipType::ManyToOne:
                        if (
                            ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Parent) ||
                            ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Child)
                        ) {
                            if (! \is_array($value) || ! \array_is_list($value)) {
                                throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, '.\gettype($value).' given.');
                            }

                            /** @var array<Document> $oldValueArr */
                            $oldValueArr = \is_array($oldValue) ? $oldValue : [];
                            $oldIds = \array_map(fn (Document $document) => $document->getId(), $oldValueArr);

                            $newIds = \array_map(function ($item) {
                                if (\is_string($item)) {
                                    return $item;
                                } elseif ($item instanceof Document) {
                                    return $item->getId();
                                } else {
                                    throw new RelationshipException('Invalid relationship value. Must be either a document or document ID.');
                                }
                            }, $value);

                            $removedDocuments = \array_values(\array_diff($oldIds, $newIds));

                            if (! empty($removedDocuments)) {
                                // Chunk to honor the validator's maxQueryValues cap; without
                                // this a relationship update with thousands of removed
                                // children would throw QueryException.
                                foreach (\array_chunk($removedDocuments, $this->relationQueryChunkSize()) as $chunk) {
                                    $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(fn () => $this->database->updateDocuments(
                                        $relatedCollection->getId(),
                                        new Document([$twoWayKey => null]),
                                        [Query::equal(Document::ID, $chunk)],
                                    )));
                                }
                            }

                            $stringRelations = [];
                            $documentRelations = [];
                            foreach ($value as $relation) {
                                if (\is_string($relation)) {
                                    $stringRelations[] = $relation;
                                } elseif ($relation instanceof Document) {
                                    $documentRelations[] = $relation;
                                } else {
                                    throw new RelationshipException('Invalid relationship value.');
                                }
                            }

                            if (! empty($stringRelations)) {
                                $unlinkedIds = [];
                                foreach (\array_chunk($stringRelations, $this->relationQueryChunkSize()) as $chunk) {
                                    $unlinked = $this->database->skipRelationships(
                                        fn () => $this->database->find($relatedCollection->getId(), [
                                            Query::select([Document::ID]),
                                            Query::equal(Document::ID, $chunk),
                                            $this->notReferencing($twoWayKey, $document->getId()),
                                            Query::limit(\count($chunk)),
                                        ])
                                    );
                                    foreach ($unlinked as $related) {
                                        $unlinkedIds[] = $related->getId();
                                    }
                                }

                                $this->linkRelatedDocuments($relatedCollection, $twoWayKey, $document->getId(), $unlinkedIds);
                            }

                            $this->relateUpdated($coroutine, $relatedCollection, $documentRelations, function (?PreparedCreate $prepared) use ($documentRelations, $relatedCollection, $document, $twoWayKey, $coroutine): void {
                                foreach ($documentRelations as $relation) {
                                    if ($prepared !== null) {
                                        if (! isset($relation[Document::PERMISSIONS])) {
                                            $relation->setAttribute(Document::PERMISSIONS, $document->getAttribute(Document::PERMISSIONS));
                                        }
                                        $this->prepare($prepared, $relatedCollection, $relation->setAttribute($twoWayKey, $document->getId()), $coroutine);

                                        continue;
                                    }

                                    $related = $this->database->skipRelationships(
                                        fn () => $this->database->getDocument($relatedCollection->getId(), $relation->getId(), [Query::select([Document::ID])])
                                    );

                                    if ($related->isEmpty()) {
                                        if (! isset($relation[Document::PERMISSIONS])) {
                                            $relation->setAttribute(Document::PERMISSIONS, $document->getAttribute(Document::PERMISSIONS));
                                        }
                                        $this->database->createDocument(
                                            $relatedCollection->getId(),
                                            $relation->setAttribute($twoWayKey, $document->getId())
                                        );
                                    } else {
                                        $this->database->updateDocument(
                                            $relatedCollection->getId(),
                                            $related->getId(),
                                            $relation->setAttribute($twoWayKey, $document->getId())
                                        );
                                    }
                                }
                            });

                            $document->removeAttribute($key);
                            break;
                        }

                        if (\is_string($value)) {
                            $related = $this->database->skipRelationships(
                                fn () => $this->database->getDocument($relatedCollection->getId(), $value, [Query::select([Document::ID])])
                            );

                            if ($related->isEmpty()) {
                                $document->setAttribute($key, null);
                            }
                            $this->database->purgeCachedDocument($relatedCollection->getId(), $value);
                        } elseif ($value instanceof Document) {
                            if ($value->getId() === '') {
                                throw new RelationshipException('Invalid relationship value. Document must have a valid '.Document::ID.'.');
                            }

                            $related = $this->database->skipRelationships(
                                fn () => $this->database->getDocument($relatedCollection->getId(), $value->getId(), [Query::select([Document::ID])])
                            );

                            if ($related->isEmpty()) {
                                if (! isset($value[Document::PERMISSIONS])) {
                                    $value->setAttribute(Document::PERMISSIONS, $document->getAttribute(Document::PERMISSIONS));
                                }
                                $this->database->createDocument(
                                    $relatedCollection->getId(),
                                    $value
                                );
                            } elseif ($related->getAttributes() != $value->getAttributes()) {
                                $this->database->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $value
                                );
                                $this->database->purgeCachedDocument($relatedCollection->getId(), $related->getId());
                            }

                            $document->setAttribute($key, $value->getId());
                        } elseif ($value === null) {
                            break;
                        } elseif (is_array($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document, array given.');
                        } elseif (empty($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be either a document ID or a document.');
                        } else {
                            throw new RelationshipException('Invalid relationship value.');
                        }

                        break;
                    case RelationshipType::ManyToMany:
                        if ($value === null) {
                            break;
                        }
                        if (! \is_array($value)) {
                            throw new RelationshipException('Invalid relationship value. Must be an array of documents or document IDs.');
                        }

                        /** @var array<Document> $oldValueArrM2M */
                        $oldValueArrM2M = \is_array($oldValue) ? $oldValue : [];
                        $oldIds = \array_map(fn (Document $document) => $document->getId(), $oldValueArrM2M);

                        $newIds = \array_map(function ($item) {
                            if (\is_string($item)) {
                                return $item;
                            } elseif ($item instanceof Document) {
                                return $item->getId();
                            } else {
                                throw new RelationshipException('Invalid relationship value. Must be either a document or document ID.');
                            }
                        }, $value);

                        $removedDocuments = \array_values(\array_diff($oldIds, $newIds));

                        if (! empty($removedDocuments)) {
                            $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                            // Chunk both the lookup and the delete so a many-to-many
                            // diff with thousands of removed peers stays within the
                            // validator's maxQueryValues ceiling.
                            $junctionIds = [];
                            foreach (\array_chunk($removedDocuments, $this->relationQueryChunkSize()) as $chunk) {
                                $junctions = $this->database->find($junction, [
                                    Query::select([Document::ID]),
                                    Query::equal($key, $chunk),
                                    Query::equal($twoWayKey, [$document->getId()]),
                                    Query::limit(PHP_INT_MAX),
                                ]);
                                foreach ($junctions as $junctionDoc) {
                                    $junctionIds[] = $junctionDoc->getId();
                                }
                            }

                            if (! empty($junctionIds)) {
                                foreach (\array_chunk($junctionIds, $this->relationQueryChunkSize()) as $chunk) {
                                    $this->database->getAuthorization()->skip(fn () => $this->database->deleteDocuments(
                                        $junction,
                                        [Query::equal(Document::ID, $chunk)],
                                    ));
                                }
                            }
                        }

                        $relatedDocuments = \array_values(\array_filter($value, static fn (mixed $relation): bool => $relation instanceof Document));
                        $this->relateUpdated($coroutine, $relatedCollection, $relatedDocuments, function (?PreparedCreate $prepared) use ($value, $oldIds, $collection, $relatedCollection, $document, $key, $twoWayKey, $side, $coroutine): void {
                            foreach ($value as $relation) {
                                if ($prepared !== null) {
                                    if ($relation instanceof Document) {
                                        if (! isset($relation[Document::PERMISSIONS])) {
                                            $relation->setAttribute(Document::PERMISSIONS, $document->getAttribute(Document::PERMISSIONS));
                                        }
                                        $relatedId = $this->prepare($prepared, $relatedCollection, $relation, $coroutine);
                                        $this->prepare(
                                            $prepared,
                                            $this->collection($prepared, $this->getJunctionCollection($collection, $relatedCollection, $side)),
                                            $this->junctionDocument($key, $relatedId, $twoWayKey, $document->getId()),
                                            $coroutine,
                                        );

                                        continue;
                                    }

                                    $this->writePrepared($prepared);
                                }

                                if (\is_string($relation)) {
                                    if (\in_array($relation, $oldIds)) {
                                        continue;
                                    }

                                    $related = $this->database->getDocument($relatedCollection->getId(), $relation, [Query::select([Document::ID])]);

                                    if ($related->isEmpty()) {
                                        continue;
                                    }

                                    $this->authorizeLink($relatedCollection, $related);
                                } elseif ($relation instanceof Document) {
                                    $related = $this->database->getDocument($relatedCollection->getId(), $relation->getId(), [Query::select([Document::ID])]);

                                    if (! $related->isEmpty() && ! \in_array($relation->getId(), $oldIds)) {
                                        $this->authorizeLink($relatedCollection, $related);
                                    }

                                    if ($related->isEmpty()) {
                                        if (! isset($relation[Document::PERMISSIONS])) {
                                            $relation->setAttribute(Document::PERMISSIONS, $document->getAttribute(Document::PERMISSIONS));
                                        }
                                        $related = $this->database->createDocument(
                                            $relatedCollection->getId(),
                                            $relation
                                        );
                                    } elseif ($related->getAttributes() != $relation->getAttributes()) {
                                        $related = $this->database->updateDocument(
                                            $relatedCollection->getId(),
                                            $related->getId(),
                                            $relation
                                        );
                                    }

                                    if (\in_array($relation->getId(), $oldIds)) {
                                        continue;
                                    }

                                    $relation = $related->getId();
                                } else {
                                    throw new RelationshipException('Invalid relationship value. Must be either a document or document ID.');
                                }

                                $this->database->skipRelationships(fn () => $this->database->createDocument(
                                    $this->getJunctionCollection($collection, $relatedCollection, $side),
                                    $this->junctionDocument($key, $relation, $twoWayKey, $document->getId()),
                                ));
                            }
                        });

                        $document->removeAttribute($key);
                        break;
                }
            } finally {
                $this->leaveWrite($coroutine);
            }
        }

        return $document;
    }

    /**
     * Apply the onDelete of each relationship on $collection before $document is deleted.
     *
     * With $report set, returns the documents on the other side of a two-way relationship that
     * the delete leaves changed: the ones it wrote, and the ones it left holding a reference to
     * the deleted document without writing them. One-way peers are left out, and so is every
     * peer a cascade removed, anywhere down its chain.
     *
     * A peer is the copy the delete itself worked with: read and written with permissions
     * skipped, like the rest of the delete, and returned without a read check on the principal
     * running it. Whoever can read the peer is who needs to hear that it changed, and that is
     * rarely whoever deleted the other side, so the report carries the trust the bulk callbacks
     * already carry: deleteDocuments() hands $onNext each whole document it deleted, and
     * upsertDocuments() hands it a pre-image read with permissions skipped.
     *
     * How the delete reached a peer decides its shape. One the delete wrote is the copy that
     * write returned, carrying the key it cleared. One it did not write is the copy read off the
     * deleted document, where relationship population has already stripped the back-reference,
     * so that key is absent rather than null. Read a peer back to use more than its identity.
     *
     * @return list<Document>
     *
     * @throws RestrictedException If a restricted relationship prevents deletion
     *
     * @internal
     */
    public function beforeDocumentDelete(Document $collection, Document $document, bool $report = false): array
    {
        /** @var array<string, array<string, Document>> $changed */
        $changed = [];
        $cascaded = false;

        foreach (self::attributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            $side = $attribute->side;
            if ($relationship === null || $side === null) {
                continue;
            }

            $key = $attribute->key;
            $value = $document->getAttribute($key);
            $relatedCollection = $this->database->getCollection($relationship->relatedCollection);
            $relationType = $relationship->type;
            $twoWay = $relationship->twoWay;
            $twoWayKey = $relationship->twoWayKey ?? '';
            $onDelete = $relationship->onDelete;

            $holdsKey = ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Child)
                || ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Parent);
            $unwritten = false;

            switch ($onDelete) {
                case RelationshipDeleteAction::Restrict:
                    $this->deleteRestrict($collection, $relatedCollection, $document, $key, $relationType, $twoWay, $twoWayKey, $side);
                    $unwritten = true;
                    break;
                case RelationshipDeleteAction::SetNull:
                    $written = $this->deleteSetNull($collection, $relatedCollection, $document, $relationType, $twoWay, $twoWayKey, $side, $report && $twoWay);

                    foreach ($written as $related) {
                        $changed[$relatedCollection->getId()][$related->getId()] = $related;
                    }

                    $unwritten = $holdsKey || $relationType === RelationshipType::ManyToMany;
                    break;
                case RelationshipDeleteAction::Cascade:
                    $unwritten = $holdsKey || ($relationType === RelationshipType::ManyToMany && $side === RelationshipSide::Child);
                    $cascade = new Cascade($collection->getId(), $document->getId(), $attribute);

                    foreach ($this->deleteStacks[$this->coroutine()] ?? [] as $processed) {
                        $existingKey = $processed->attribute->key;
                        $existingCollection = $processed->collection;
                        $existingRelatedCollection = $processed->attribute->relationship?->relatedCollection;
                        $existingTwoWayKey = $processed->attribute->relationship->twoWayKey ?? '';
                        $existingSide = $processed->attribute->side;

                        $reflexive = $processed == $cascade;

                        $symmetric = $existingKey === $twoWayKey
                            && $existingTwoWayKey === $key
                            && $existingRelatedCollection === $collection->getId()
                            && $existingCollection === $relatedCollection->getId()
                            && $existingSide !== $side;

                        $transitive = (($existingKey === $twoWayKey
                                && $existingCollection === $relatedCollection->getId()
                                && $existingSide !== $side)
                            || ($existingTwoWayKey === $key
                                && $existingRelatedCollection === $collection->getId()
                                && $existingSide !== $side)
                            || ($existingKey === $key
                                && $existingTwoWayKey !== $twoWayKey
                                && $existingRelatedCollection === $relatedCollection->getId()
                                && $existingSide !== $side)
                            || ($existingKey !== $key
                                && $existingTwoWayKey === $twoWayKey
                                && $existingRelatedCollection === $relatedCollection->getId()
                                && $existingSide !== $side));

                        if ($reflexive || $symmetric || $transitive) {
                            break 2;
                        }
                    }
                    $this->deleteCascade($collection, $relatedCollection, $document, $key, $relationType, $twoWay, $twoWayKey, $side, $cascade);
                    break;
            }

            foreach (\is_array($value) ? $value : [$value] as $related) {
                if (! $related instanceof Document || $related->isEmpty()) {
                    continue;
                }

                if ($onDelete === RelationshipDeleteAction::Cascade && ! $unwritten) {
                    $cascaded = true;
                } elseif ($twoWay && $unwritten) {
                    $changed[$relatedCollection->getId()][$related->getId()] = $related;
                }
            }
        }

        if (! $report) {
            return [];
        }

        unset($changed[$collection->getId()][$document->getId()]);

        if ($cascaded) {
            $changed = $this->withoutRemoved($changed);
        }

        $reported = [];
        foreach ($changed as $documents) {
            \array_push($reported, ...\array_values($documents));
        }

        return $reported;
    }

    /**
     * Keep the documents that still exist: a cascade can remove one anywhere down its chain.
     *
     * @param  array<string, array<string, Document>>  $documents  Keyed by collection, then by id
     * @return array<string, array<string, Document>>
     */
    private function withoutRemoved(array $documents): array
    {
        $remaining = [];

        foreach ($documents as $collectionId => $byId) {
            $ids = \array_values(\array_map(fn (Document $document): string => $document->getId(), $byId));

            foreach (\array_chunk($ids, $this->relationQueryChunkSize()) as $chunk) {
                $found = $this->database->getAuthorization()->skip(fn () => $this->database->find($collectionId, [
                    Query::equal(Document::ID, $chunk),
                    Query::select([Document::ID]),
                    Query::limit(\count($chunk)),
                ]));

                foreach ($found as $existing) {
                    $remaining[$collectionId][$existing->getId()] = $byId[$existing->getId()];
                }
            }
        }

        return $remaining;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<string, array<Query>>  $selects
     * @return array<Document>
     *
     * @internal
     */
    public function populateDocuments(array $documents, Document $collection, int $fetchDepth, array $selects = []): array
    {
        return $this->inBatchPopulation->with(true, function () use ($documents, $collection, $fetchDepth, $selects): array {
            $queue = [
                [
                    'documents' => $documents,
                    'collection' => $collection,
                    'depth' => $fetchDepth,
                    'selects' => $selects,
                    'skipKey' => null,
                    'hasExplicitSelects' => ! empty($selects),
                ],
            ];

            $currentDepth = $fetchDepth;

            while (! empty($queue) && $currentDepth < Database::RELATION_MAX_DEPTH) {
                $nextQueue = [];

                foreach ($queue as $item) {
                    $batchDocuments = $item['documents'];
                    $batchCollection = $item['collection'];
                    $batchSelects = $item['selects'];
                    $skipKey = $item['skipKey'] ?? null;
                    $parentHasExplicitSelects = $item['hasExplicitSelects'];

                    if (empty($batchDocuments)) {
                        continue;
                    }

                    foreach (self::attributes($batchCollection) as $attribute) {
                        $relationship = $attribute->relationship;
                        $side = $attribute->side;
                        $key = $attribute->key;
                        if (
                            $relationship === null
                            || $side === null
                            || $key === $skipKey
                            || ($parentHasExplicitSelects && ! \array_key_exists($key, $batchSelects))
                        ) {
                            continue;
                        }

                        $queries = $batchSelects[$key] ?? [];
                        $isAtMaxDepth = ($currentDepth + 1) >= Database::RELATION_MAX_DEPTH;

                        if ($isAtMaxDepth) {
                            foreach ($batchDocuments as $document) {
                                $document->removeAttribute($key);
                            }

                            continue;
                        }

                        $relatedDocuments = $this->populateSingleRelationshipBatch(
                            $batchDocuments,
                            $batchCollection,
                            $key,
                            $relationship,
                            $side,
                            $queries
                        );

                        $twoWay = $relationship->twoWay;
                        $twoWayKey = $relationship->twoWayKey ?? '';

                        $hasNestedSelects = isset($batchSelects[$key]);
                        $shouldQueue = ! empty($relatedDocuments) &&
                            ($hasNestedSelects || ! $parentHasExplicitSelects);

                        if ($shouldQueue) {
                            $relatedCollectionId = $relationship->relatedCollection;
                            $relatedCollection = $this->database->silent(fn () => $this->database->findCollection($relatedCollectionId));

                            if ($relatedCollection !== null) {
                                $relationshipQueries = $hasNestedSelects ? $batchSelects[$key] : [];

                                $nextSelects = $this->processQueries(self::relationships($relatedCollection), $relationshipQueries);

                                $childHasExplicitSelects = $parentHasExplicitSelects;

                                $nextQueue[] = [
                                    'documents' => $relatedDocuments,
                                    'collection' => $relatedCollection,
                                    'depth' => $currentDepth + 1,
                                    'selects' => $nextSelects,
                                    'skipKey' => $twoWay ? $twoWayKey : null,
                                    'hasExplicitSelects' => $childHasExplicitSelects,
                                ];
                            }
                        }

                        if ($twoWay && ! empty($relatedDocuments)) {
                            foreach ($relatedDocuments as $relatedDocument) {
                                $relatedDocument->removeAttribute($twoWayKey);
                            }
                        }
                    }
                }

                $queue = $nextQueue;
                $currentDepth++;
            }

            return $documents;
        });
    }

    /**
     * @param  array<Attribute>  $relationships  The relationship attributes of the collection the queries read
     * @param  array<Query>  $queries
     * @return array<string, array<Query>>
     *
     * @internal
     */
    public function processQueries(array $relationships, array $queries): array
    {
        $nestedSelections = [];

        // Fast exit: collections without relationships short-circuit before
        // walking the query list. This is the common case for flat tables.
        if (empty($relationships)) {
            return $nestedSelections;
        }

        // Pre-index relationships by key once so per-value lookups are O(1)
        // instead of O(relationships) with a fresh array_filter each iteration.
        /** @var array<string, Attribute> $relationshipsByKey */
        $relationshipsByKey = [];
        foreach ($relationships as $relationship) {
            $relationshipsByKey[$relationship->key] = $relationship;
        }

        foreach ($queries as $query) {
            if ($query->getMethod() !== Method::Select) {
                continue;
            }

            $values = $query->getValues();
            foreach ($values as $valueIndex => $value) {
                if (! \is_string($value)) {
                    throw new QueryException('Select queries must contain only string attributes.');
                }

                if (! \str_contains($value, '.')) {
                    continue;
                }

                $nesting = \explode('.', $value);
                $selectedKey = \array_shift($nesting);

                $attribute = $relationshipsByKey[$selectedKey] ?? null;
                $relationship = $attribute?->relationship;

                if ($relationship === null) {
                    continue;
                }

                $nestingPath = \implode('.', $nesting);

                if (empty($nestingPath)) {
                    $nestedSelections[$selectedKey][] = Query::select(['*']);
                } else {
                    $nestedSelections[$selectedKey][] = Query::select([$nestingPath]);
                }

                switch ($relationship->type) {
                    case RelationshipType::ManyToMany:
                        unset($values[$valueIndex]);
                        break;
                    case RelationshipType::OneToMany:
                        if ($attribute->side === RelationshipSide::Parent) {
                            unset($values[$valueIndex]);
                        } else {
                            $values[$valueIndex] = $selectedKey;
                        }
                        break;
                    case RelationshipType::ManyToOne:
                        if ($attribute->side === RelationshipSide::Parent) {
                            $values[$valueIndex] = $selectedKey;
                        } else {
                            unset($values[$valueIndex]);
                        }
                        break;
                    case RelationshipType::OneToOne:
                        $values[$valueIndex] = $selectedKey;
                        break;
                }
            }

            $finalValues = \array_values($values);
            if (empty($finalValues)) {
                $finalValues = ['*'];
            }
            $query->setValues($finalValues);
        }

        return $nestedSelections;
    }

    /**
     * @param  array<Attribute>  $relationships  The relationship attributes of $collection
     * @param  array<Query>  $queries
     * @return array<Query>|null
     *
     * @throws QueryException If a relationship query references an invalid attribute
     *
     * @internal
     */
    public function convertQueries(array $relationships, array $queries, ?Document $collection = null): ?array
    {
        // Fast exit: nothing to convert when the collection has no
        // relationship attributes — saves the per-find query walk.
        if (empty($relationships)) {
            return $queries;
        }

        $hasRelationshipQuery = false;
        foreach ($queries as $query) {
            $attr = $query->getAttribute();
            if (\str_contains($attr, '.') || $query->getMethod() === Method::ContainsAll) {
                $hasRelationshipQuery = true;
                break;
            }
        }

        if (! $hasRelationshipQuery) {
            return $queries;
        }

        /** @var array<string, Attribute> $relationshipsByKey */
        $relationshipsByKey = [];
        foreach ($relationships as $relationship) {
            $relationshipsByKey[$relationship->key] = $relationship;
        }

        $additionalQueries = [];
        $groupedQueries = [];
        $indicesToRemove = [];

        foreach ($queries as $index => $query) {
            if ($query->getMethod() !== Method::ContainsAll) {
                continue;
            }

            $attribute = $query->getAttribute();

            if (! \str_contains($attribute, '.')) {
                continue;
            }

            $parts = \explode('.', $attribute);
            $relationshipKey = \array_shift($parts);
            $nestedAttribute = \implode('.', $parts);
            $relationship = $relationshipsByKey[$relationshipKey] ?? null;

            if (! $relationship) {
                continue;
            }

            $parentIdSets = [];
            $resolvedAttribute = Document::ID;
            foreach ($query->getValues() as $value) {
                /** @var string|int|float|bool|null $value */
                $relatedQuery = Query::equal($nestedAttribute, [$value]);
                $result = $this->resolveRelationshipGroupToIds($relationship, [$relatedQuery], $collection);

                if ($result === null) {
                    return null;
                }

                $resolvedAttribute = $result['attribute'];
                $parentIdSets[] = $result['ids'];
            }

            $ids = \count($parentIdSets) > 1
                ? \array_values(\array_intersect(...$parentIdSets))
                : ($parentIdSets[0] ?? []);

            if (empty($ids)) {
                return null;
            }

            $additionalQueries[] = Query::equal($resolvedAttribute, $ids);
            $indicesToRemove[] = $index;
        }

        foreach ($queries as $index => $query) {
            if ($query->getMethod() === Method::Select || $query->getMethod() === Method::ContainsAll) {
                continue;
            }

            $attribute = $query->getAttribute();

            if (! \str_contains($attribute, '.')) {
                continue;
            }

            $parts = \explode('.', $attribute);
            $relationshipKey = \array_shift($parts);
            $nestedAttribute = \implode('.', $parts);
            $relationship = $relationshipsByKey[$relationshipKey] ?? null;

            if (! $relationship) {
                continue;
            }

            if (! isset($groupedQueries[$relationshipKey])) {
                $groupedQueries[$relationshipKey] = [
                    'relationship' => $relationship,
                    'queries' => [],
                    'indices' => [],
                ];
            }

            $groupedQueries[$relationshipKey]['queries'][] = [
                'method' => $query->getMethod(),
                'attribute' => $nestedAttribute,
                'values' => $query->getValues(),
            ];

            $groupedQueries[$relationshipKey]['indices'][] = $index;
        }

        foreach ($groupedQueries as $relationshipKey => $group) {
            $relationship = $group['relationship'];

            $equalAttrs = [];
            foreach ($group['queries'] as $queryData) {
                if ($queryData['method'] === Method::Equal) {
                    $attr = $queryData['attribute'];
                    if (isset($equalAttrs[$attr])) {
                        throw new QueryException("Multiple equal queries on '{$relationshipKey}.{$attr}' will never match a single document. Use Query::containsAll() to match across different related documents.");
                    }
                    $equalAttrs[$attr] = true;
                }
            }

            $relatedQueries = [];
            foreach ($group['queries'] as $queryData) {
                $relatedQueries[] = new Query(
                    $queryData['method'],
                    $queryData['attribute'],
                    $queryData['values']
                );
            }

            try {
                $result = $this->resolveRelationshipGroupToIds($relationship, $relatedQueries, $collection);

                if ($result === null) {
                    return null;
                }

                $additionalQueries[] = Query::equal($result['attribute'], $result['ids']);

                foreach ($group['indices'] as $originalIndex) {
                    $indicesToRemove[] = $originalIndex;
                }
            } catch (QueryException $e) {
                throw $e;
            } catch (Exception $e) {
                return null;
            }
        }

        foreach ($indicesToRemove as $index) {
            unset($queries[$index]);
        }

        return \array_merge(\array_values($queries), $additionalQueries);
    }

    private function relateDocuments(
        Document $collection,
        Document $relatedCollection,
        string $key,
        Document $document,
        Document $relation,
        RelationshipType $relationType,
        bool $twoWay,
        string $twoWayKey,
        RelationshipSide $side,
        int $coroutine,
        ?PreparedCreate $prepared,
    ): string {
        switch ($relationType) {
            case RelationshipType::OneToOne:
                if ($twoWay) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case RelationshipType::OneToMany:
                if ($side === RelationshipSide::Parent) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case RelationshipType::ManyToOne:
                if ($side === RelationshipSide::Child) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
        }

        if ($prepared !== null) {
            return $this->prepareRelated($prepared, $collection, $relatedCollection, $key, $document, $relation, $relationType, $twoWayKey, $side, $coroutine);
        }

        $related = $this->database->getDocument($relatedCollection->getId(), $relation->getId());

        if ($relationType === RelationshipType::ManyToMany && ! $related->isEmpty()) {
            $this->authorizeLink($relatedCollection, $related);
        }

        if ($related->isEmpty()) {
            if (! isset($relation[Document::PERMISSIONS])) {
                $relation->setAttribute(Document::PERMISSIONS, $document->getPermissions());
            }

            $related = $this->database->createDocument($relatedCollection->getId(), $relation);
        } elseif ($related->getAttributes() != $relation->getAttributes()) {
            foreach ($relation->getAttributes() as $attribute => $value) {
                $related->setAttribute($attribute, $value);
            }

            $related = $this->database->updateDocument($relatedCollection->getId(), $related->getId(), $related);
        }

        if ($relationType === RelationshipType::ManyToMany) {
            $this->database->createDocument(
                $this->getJunctionCollection($collection, $relatedCollection, $side),
                $this->junctionDocument($key, $related->getId(), $twoWayKey, $document->getId()),
            );
        }

        return $related->getId();
    }

    /**
     * Prepare a related document as new, with its junction document: relatePrepared() found none of the related
     * documents stored, where relateDocuments() reads each one to find out.
     */
    private function prepareRelated(
        PreparedCreate $prepared,
        Document $collection,
        Document $relatedCollection,
        string $key,
        Document $document,
        Document $relation,
        RelationshipType $relationType,
        string $twoWayKey,
        RelationshipSide $side,
        int $coroutine,
    ): string {
        if (! isset($relation[Document::PERMISSIONS])) {
            $relation->setAttribute(Document::PERMISSIONS, $document->getPermissions());
        }

        $relatedId = $this->prepare($prepared, $relatedCollection, $relation, $coroutine);

        if ($relationType === RelationshipType::ManyToMany) {
            $this->prepare(
                $prepared,
                $this->collection($prepared, $this->getJunctionCollection($collection, $relatedCollection, $side)),
                $this->junctionDocument($key, $relatedId, $twoWayKey, $document->getId()),
                $coroutine,
            );
        }

        return $relatedId;
    }

    private function junctionDocument(string $key, string $relatedId, string $twoWayKey, string $documentId): Document
    {
        return new Document([
            $key => $relatedId,
            $twoWayKey => $documentId,
            Document::PERMISSIONS => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);
    }

    private function relateDocumentsById(
        Document $collection,
        Document $relatedCollection,
        string $key,
        string $documentId,
        string $relationId,
        RelationshipType $relationType,
        bool $twoWay,
        string $twoWayKey,
        RelationshipSide $side,
        ?PreparedCreate $prepared,
    ): void {
        if ($prepared !== null) {
            if (! $this->writesById($relationType, $twoWay, $side)) {
                return;
            }

            // One by one, a document still being prepared is written after this read, and it was not stored before.
            if ($this->checkExist->get() && isset($prepared->preparing[$relatedCollection->getId()][$relationId])) {
                return;
            }

            $this->writePrepared($prepared);
        }

        $related = $this->database->skipRelationships(fn () => $this->database->getDocument($relatedCollection->getId(), $relationId));

        if ($related->isEmpty() && $this->checkExist->get()) {
            return;
        }

        switch ($relationType) {
            case RelationshipType::OneToOne:
                if ($twoWay) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->database->skipRelationships(fn () => $this->database->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationshipType::OneToMany:
                if ($side === RelationshipSide::Parent) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->database->skipRelationships(fn () => $this->database->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationshipType::ManyToOne:
                if ($side === RelationshipSide::Child) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->database->skipRelationships(fn () => $this->database->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case RelationshipType::ManyToMany:
                if (! $related->isEmpty()) {
                    $this->authorizeLink($relatedCollection, $related);
                }

                $this->database->purgeCachedDocument($relatedCollection->getId(), $relationId);

                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $this->database->skipRelationships(fn () => $this->database->createDocument(
                    $junction,
                    $this->junctionDocument($key, $relationId, $twoWayKey, $documentId),
                ));
                break;
        }
    }

    /**
     * Whether relating a document by id writes anything; otherwise it only reads the related document.
     */
    private function writesById(RelationshipType $relationType, bool $twoWay, RelationshipSide $side): bool
    {
        return match ($relationType) {
            RelationshipType::OneToOne => $twoWay,
            RelationshipType::OneToMany => $side === RelationshipSide::Parent,
            RelationshipType::ManyToOne => $side === RelationshipSide::Child,
            RelationshipType::ManyToMany => true,
        };
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, RelationshipSide $side): string
    {
        return $side === RelationshipSide::Parent
            ? '_'.$collection->getSequence().'_'.$relatedCollection->getSequence()
            : '_'.$relatedCollection->getSequence().'_'.$collection->getSequence();
    }

    private function isLinkedElsewhere(Document $collection, string $key, string $relatedId, Document $document): bool
    {
        return ! $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(fn () => $this->database->findOne($collection->getId(), [
            Query::select([Document::ID]),
            Query::equal($key, [$relatedId]),
            Query::notEqual(Document::ID, $document->getId()),
        ])))->isEmpty();
    }

    /**
     * @param  array<string>  $existingIds
     * @return array<string|Document>
     */
    private function applyRelationshipOperator(Operator $operator, array $existingIds): array
    {
        $method = $operator->getMethod();
        $values = $operator->getValues();

        $valueIds = \array_filter(\array_map(fn ($item) => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null), $values));

        switch ($method) {
            case OperatorType::ArrayAppend:
                return \array_values(\array_merge($existingIds, $valueIds));

            case OperatorType::ArrayPrepend:
                return \array_values(\array_merge($valueIds, $existingIds));

            case OperatorType::ArrayInsert:
                /** @var int $index */
                $index = $values[0] ?? 0;
                $item = $values[1] ?? null;
                $itemId = $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null);
                if ($itemId !== null) {
                    \array_splice($existingIds, (int) $index, 0, [$itemId]);
                }

                return \array_values($existingIds);

            case OperatorType::ArrayRemove:
                $toRemove = $values[0] ?? null;
                if (\is_array($toRemove)) {
                    $toRemoveIds = \array_filter(\array_map(fn ($item) => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : null), $toRemove));

                    return \array_values(\array_diff($existingIds, $toRemoveIds));
                }
                $toRemoveId = $toRemove instanceof Document ? $toRemove->getId() : (\is_string($toRemove) ? $toRemove : null);
                if ($toRemoveId !== null) {
                    return \array_values(\array_diff($existingIds, [$toRemoveId]));
                }

                return $existingIds;

            case OperatorType::ArrayUnique:
                return \array_values(\array_unique($existingIds));

            case OperatorType::ArrayIntersect:
                return \array_values(\array_intersect($existingIds, $valueIds));

            case OperatorType::ArrayDiff:
                return \array_values(\array_diff($existingIds, $valueIds));

            default:
                return $existingIds;
        }
    }

    /**
     * @param  array<Document>  $documents  Documents of $collection
     * @param  string  $key  The key $relationship is stored under on $collection
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateSingleRelationshipBatch(array $documents, Document $collection, string $key, Relationship $relationship, RelationshipSide $side, array $queries): array
    {
        return match ($relationship->type) {
            RelationshipType::OneToOne => $this->populateOneToOneRelationshipsBatch($documents, $key, $relationship, $queries),
            RelationshipType::OneToMany => $this->populateOneToManyRelationshipsBatch($documents, $key, $relationship, $side, $queries),
            RelationshipType::ManyToOne => $this->populateManyToOneRelationshipsBatch($documents, $key, $relationship, $side, $queries),
            RelationshipType::ManyToMany => $this->populateManyToManyRelationshipsBatch($documents, $collection, $key, $relationship, $side, $queries),
        };
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateOneToOneRelationshipsBatch(array $documents, string $key, Relationship $relationship, array $queries): array
    {
        $relatedCollection = $this->database->getCollection($relationship->relatedCollection);

        $relatedIds = [];
        $documentsByRelatedId = [];

        foreach ($documents as $document) {
            $value = $document->getAttribute($key);
            if ($value !== null) {
                if ($value instanceof Document) {
                    continue;
                }

                /** @var string $relId */
                $relId = $value;
                $relatedIds[] = $relId;
                if (! isset($documentsByRelatedId[$relId])) {
                    $documentsByRelatedId[$relId] = [];
                }
                $documentsByRelatedId[$relId][] = $document;
            }
        }

        if (empty($relatedIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        /** @var array<string> $uniqueRelatedIds */
        $uniqueRelatedIds = \array_unique($relatedIds);
        $collectionId = $relatedCollection->getId();
        $relatedDocuments = $this->readChunks(\array_map(
            fn (array $chunk): Closure => fn (): array => $this->database->find($collectionId, [
                Query::equal(Document::ID, $chunk),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]),
            \array_chunk($uniqueRelatedIds, $this->relationQueryChunkSize()),
        ));

        $relatedById = [];
        foreach ($relatedDocuments as $related) {
            $relatedById[$related->getId()] = $related;
        }

        $this->database->applySelectFiltersToDocuments($relatedDocuments, $selectQueries);

        foreach ($documentsByRelatedId as $relatedId => $docs) {
            if (isset($relatedById[$relatedId])) {
                foreach ($docs as $document) {
                    $document->setAttribute($key, $relatedById[$relatedId]);
                }
            } else {
                foreach ($docs as $document) {
                    $document->setAttribute($key, new Document());
                }
            }
        }

        return $relatedDocuments;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateOneToManyRelationshipsBatch(array $documents, string $key, Relationship $relationship, RelationshipSide $side, array $queries): array
    {
        $twoWayKey = $relationship->twoWayKey ?? '';
        $relatedCollection = $this->database->getCollection($relationship->relatedCollection);

        if ($side === RelationshipSide::Child) {
            if (! $relationship->twoWay) {
                foreach ($documents as $document) {
                    $document->removeAttribute($key);
                }

                return [];
            }

            return $this->populateOneToOneRelationshipsBatch($documents, $key, $relationship, $queries);
        }

        $parentIds = [];
        foreach ($documents as $document) {
            $parentId = $document->getId();
            $parentIds[] = $parentId;
        }

        $parentIds = \array_unique($parentIds);

        if (empty($parentIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $collectionId = $relatedCollection->getId();
        $relatedDocuments = $this->readChunks(\array_map(
            fn (array $chunk): Closure => fn (): array => $this->database->find($collectionId, [
                Query::equal($twoWayKey, $chunk),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]),
            \array_chunk($parentIds, $this->relationQueryChunkSize()),
        ));

        $relatedByParentId = [];
        foreach ($relatedDocuments as $related) {
            $parentId = $related->getAttribute($twoWayKey);
            if ($parentId instanceof Document) {
                $parentKey = $parentId->getId();
            } elseif (\is_string($parentId)) {
                $parentKey = $parentId;
            } else {
                continue;
            }

            if (! isset($relatedByParentId[$parentKey])) {
                $relatedByParentId[$parentKey] = [];
            }
            $relatedByParentId[$parentKey][] = $related;
        }

        $this->database->applySelectFiltersToDocuments($relatedDocuments, $selectQueries);

        foreach ($documents as $document) {
            $parentId = $document->getId();
            $relatedDocs = $relatedByParentId[$parentId] ?? [];
            $document->setAttribute($key, $relatedDocs);
        }

        return $relatedDocuments;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateManyToOneRelationshipsBatch(array $documents, string $key, Relationship $relationship, RelationshipSide $side, array $queries): array
    {
        $twoWayKey = $relationship->twoWayKey ?? '';
        $relatedCollection = $this->database->getCollection($relationship->relatedCollection);

        if ($side === RelationshipSide::Parent) {
            return $this->populateOneToOneRelationshipsBatch($documents, $key, $relationship, $queries);
        }

        if (! $relationship->twoWay) {
            foreach ($documents as $document) {
                $document->removeAttribute($key);
            }

            return [];
        }

        $childIds = [];
        foreach ($documents as $document) {
            $childId = $document->getId();
            $childIds[] = $childId;
        }

        $childIds = array_unique($childIds);

        if (empty($childIds)) {
            return [];
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $collectionId = $relatedCollection->getId();
        $relatedDocuments = $this->readChunks(\array_map(
            fn (array $chunk): Closure => fn (): array => $this->database->find($collectionId, [
                Query::equal($twoWayKey, $chunk),
                Query::limit(PHP_INT_MAX),
                ...$otherQueries,
            ]),
            \array_chunk($childIds, $this->relationQueryChunkSize()),
        ));

        $relatedByChildId = [];
        foreach ($relatedDocuments as $related) {
            $childId = $related->getAttribute($twoWayKey);
            if ($childId instanceof Document) {
                $childKey = $childId->getId();
            } elseif (\is_string($childId)) {
                $childKey = $childId;
            } else {
                continue;
            }

            if (! isset($relatedByChildId[$childKey])) {
                $relatedByChildId[$childKey] = [];
            }
            $relatedByChildId[$childKey][] = $related;
        }

        $this->database->applySelectFiltersToDocuments($relatedDocuments, $selectQueries);

        foreach ($documents as $document) {
            $childId = $document->getId();
            $document->setAttribute($key, $relatedByChildId[$childId] ?? []);
        }

        return $relatedDocuments;
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function populateManyToManyRelationshipsBatch(array $documents, Document $collection, string $key, Relationship $relationship, RelationshipSide $side, array $queries): array
    {
        $twoWayKey = $relationship->twoWayKey ?? '';
        $relatedCollection = $this->database->getCollection($relationship->relatedCollection);

        if (! $relationship->twoWay && $side === RelationshipSide::Child) {
            return [];
        }

        $documentIds = [];
        foreach ($documents as $document) {
            $documentId = $document->getId();
            $documentIds[] = $documentId;
        }

        $documentIds = array_unique($documentIds);

        if (empty($documentIds)) {
            return [];
        }

        $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

        $junctions = $this->readChunks(\array_map(
            fn (array $chunk): Closure => fn (): array => $this->database->skipRelationships(fn (): array => $this->database->find($junction, [
                Query::equal($twoWayKey, $chunk),
                Query::limit(PHP_INT_MAX),
            ])),
            \array_chunk($documentIds, $this->relationQueryChunkSize()),
        ));

        /** @var array<string> $relatedIds */
        $relatedIds = [];
        /** @var array<string, array<string>> $junctionsByDocumentId */
        $junctionsByDocumentId = [];

        foreach ($junctions as $junctionDoc) {
            $documentId = $junctionDoc->getAttribute($twoWayKey);
            $relatedId = $junctionDoc->getAttribute($key);

            if ($documentId !== null && $relatedId !== null) {
                $documentIdStr = $documentId instanceof Document ? $documentId->getId() : (\is_string($documentId) ? $documentId : null);
                $relatedIdStr = $relatedId instanceof Document ? $relatedId->getId() : (\is_string($relatedId) ? $relatedId : null);
                if ($documentIdStr === null || $relatedIdStr === null) {
                    continue;
                }
                if (! isset($junctionsByDocumentId[$documentIdStr])) {
                    $junctionsByDocumentId[$documentIdStr] = [];
                }
                $junctionsByDocumentId[$documentIdStr][] = $relatedIdStr;
                $relatedIds[] = $relatedIdStr;
            }
        }

        $selectQueries = [];
        $otherQueries = [];
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select) {
                $selectQueries[] = $query;
            } else {
                $otherQueries[] = $query;
            }
        }

        $related = [];
        $allRelatedDocs = [];
        if (! empty($relatedIds)) {
            $uniqueRelatedIds = array_unique($relatedIds);
            $relatedCollectionId = $relatedCollection->getId();
            $foundRelated = $this->readChunks(\array_map(
                fn (array $chunk): Closure => fn (): array => $this->database->find($relatedCollectionId, [
                    Query::equal(Document::ID, $chunk),
                    Query::limit(PHP_INT_MAX),
                    ...$otherQueries,
                ]),
                \array_chunk($uniqueRelatedIds, $this->relationQueryChunkSize()),
            ));

            $allRelatedDocs = $foundRelated;

            $relatedById = [];
            foreach ($foundRelated as $doc) {
                $relatedById[$doc->getId()] = $doc;
            }

            $this->database->applySelectFiltersToDocuments($allRelatedDocs, $selectQueries);

            foreach ($junctionsByDocumentId as $documentId => $relatedDocIds) {
                $documentRelated = [];
                foreach ($relatedDocIds as $relatedId) {
                    if (isset($relatedById[$relatedId])) {
                        $documentRelated[] = $relatedById[$relatedId];
                    }
                }
                $related[$documentId] = $documentRelated;
            }
        }

        foreach ($documents as $document) {
            $documentId = $document->getId();
            $document->setAttribute($key, $related[$documentId] ?? []);
        }

        return $allRelatedDocs;
    }

    private function deleteRestrict(
        Document $collection,
        Document $relatedCollection,
        Document $document,
        string $key,
        RelationshipType $relationType,
        bool $twoWay,
        string $twoWayKey,
        RelationshipSide $side
    ): void {
        if (
            $relationType !== RelationshipType::ManyToOne
            && $side === RelationshipSide::Parent
            && $this->hasRelatedDocument($collection, $relatedCollection, $document, $key, $relationType, $twoWay, $twoWayKey, $side)
        ) {
            throw new RestrictedException('Cannot delete document because it has at least one related document.');
        }

        if (
            $relationType === RelationshipType::OneToOne
            && $side === RelationshipSide::Child
            && ! $twoWay
        ) {
            $this->database->getAuthorization()->skip(function () use ($document, $relatedCollection, $twoWayKey) {
                $related = $this->database->findOne($relatedCollection->getId(), [
                    Query::select([Document::ID]),
                    Query::equal($twoWayKey, [$document->getId()]),
                ]);

                if ($related->isEmpty()) {
                    return;
                }

                $this->database->skipRelationships(fn () => $this->database->updateDocument(
                    $relatedCollection->getId(),
                    $related->getId(),
                    new Document([
                        $twoWayKey => null,
                    ])
                ));
            });
        }

        if (
            $relationType === RelationshipType::ManyToOne
            && $side === RelationshipSide::Child
        ) {
            $related = $this->database->getAuthorization()->skip(fn () => $this->database->findOne($relatedCollection->getId(), [
                Query::select([Document::ID]),
                Query::equal($twoWayKey, [$document->getId()]),
            ]));

            if (! $related->isEmpty()) {
                throw new RestrictedException('Cannot delete document because it has at least one related document.');
            }
        }
    }

    private function hasRelatedDocument(Document $collection, Document $relatedCollection, Document $document, string $key, RelationshipType $relationType, bool $twoWay, string $twoWayKey, RelationshipSide $side): bool
    {
        $authorization = $this->database->getAuthorization();

        if ($relationType === RelationshipType::OneToMany) {
            return ! $authorization->skip(fn () => $this->database->findOne($relatedCollection->getId(), [
                Query::select([Document::ID]),
                Query::equal($twoWayKey, [$document->getId()]),
            ]))->isEmpty();
        }

        $relatedIds = $this->findRelatedIds($collection, $relatedCollection, $document, $key, $relationType, $twoWay, $twoWayKey, $side);

        foreach (\array_chunk($relatedIds, $this->relationQueryChunkSize()) as $chunk) {
            $related = $authorization->skip(fn () => $this->database->findOne($relatedCollection->getId(), [
                Query::select([Document::ID]),
                Query::equal(Document::ID, $chunk),
            ]));

            if (! $related->isEmpty()) {
                return true;
            }
        }

        return false;
    }

    /**
     * The IDs of the documents on the other side of the relationship that a delete of $document
     * reaches, read from storage with permissions and relationships skipped. The relationship
     * value on $document cannot be used: it holds only what the caller could read, and nothing
     * at all when deleteDocuments() read the batch with a select.
     *
     * One-to-one and many-to-many IDs come from a stored reference, so the document they name
     * may already be gone.
     *
     * @return list<string>
     */
    private function findRelatedIds(Document $collection, Document $relatedCollection, Document $document, string $key, RelationshipType $relationType, bool $twoWay, string $twoWayKey, RelationshipSide $side): array
    {
        return match ($relationType) {
            RelationshipType::OneToOne => $side === RelationshipSide::Parent || $twoWay
                ? $this->findStoredRelatedIds($collection, $document, $key)
                : [],
            RelationshipType::OneToMany => $side === RelationshipSide::Parent
                ? $this->findReferencingIds($relatedCollection, $document, $twoWayKey)
                : [],
            RelationshipType::ManyToOne => $side === RelationshipSide::Child
                ? $this->findReferencingIds($relatedCollection, $document, $twoWayKey)
                : [],
            RelationshipType::ManyToMany => $this->findJunctionRelatedIds($collection, $relatedCollection, $document, $key, $twoWayKey, $side),
        };
    }

    /**
     * @return list<string>
     */
    private function findStoredRelatedIds(Document $collection, Document $document, string $key): array
    {
        $stored = $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(
            fn () => $this->database->getDocument($collection->getId(), $document->getId(), forUpdate: true)
        ));
        $relatedId = $stored->getAttribute($key);

        return \is_string($relatedId) && $relatedId !== '' ? [$relatedId] : [];
    }

    /**
     * @return list<string>
     */
    private function findReferencingIds(Document $relatedCollection, Document $document, string $twoWayKey): array
    {
        return \array_values(\array_map(
            fn (Document $related) => $related->getId(),
            $this->findReferencingDocuments($relatedCollection, $document, $twoWayKey),
        ));
    }

    /**
     * @return list<string>
     */
    private function findJunctionRelatedIds(Document $collection, Document $relatedCollection, Document $document, string $key, string $twoWayKey, RelationshipSide $side): array
    {
        $junctions = $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(fn () => $this->database->find(
            $this->getJunctionCollection($collection, $relatedCollection, $side),
            [
                Query::select([$key]),
                Query::equal($twoWayKey, [$document->getId()]),
                Query::limit(PHP_INT_MAX),
            ],
        )));

        $relatedIds = [];
        foreach ($junctions as $junction) {
            $relatedId = $junction->getAttribute($key);
            if (\is_string($relatedId) && $relatedId !== '') {
                $relatedIds[] = $relatedId;
            }
        }

        return \array_values(\array_unique($relatedIds));
    }

    /**
     * Find every document in $relatedCollection whose $twoWayKey points at $document.
     *
     * A delete can start from a document fetched without its relationships
     * populated, because deleteDocuments passes the caller's queries to find()
     * and a select turns population off, so the value carried on the document
     * cannot be trusted to list the referencing rows.
     *
     * Permissions are skipped: a referencing document the caller cannot read
     * still has to have its foreign key cleared, or it is left pointing at a
     * row that no longer exists.
     *
     * @return array<Document>
     */
    private function findReferencingDocuments(Document $relatedCollection, Document $document, string $twoWayKey): array
    {
        return $this->database->getAuthorization()->skip(fn () => $this->database->find($relatedCollection->getId(), [
            Query::select([Document::ID]),
            Query::equal($twoWayKey, [$document->getId()]),
            Query::limit(PHP_INT_MAX),
        ]));
    }

    /**
     * Clear the foreign key on every document referencing $document.
     *
     * @return list<Document> The documents as the write left them, or none unless $report is set
     */
    private function clearReferences(Document $relatedCollection, Document $document, string $twoWayKey, bool $report): array
    {
        $relations = $this->findReferencingDocuments($relatedCollection, $document, $twoWayKey);

        if (empty($relations)) {
            return [];
        }

        $relationIds = \array_map(fn (Document $relation) => $relation->getId(), $relations);

        $cleared = [];
        $collect = function (Document $updated) use (&$cleared): void {
            $cleared[] = $updated;
        };

        foreach (\array_chunk($relationIds, $this->relationQueryChunkSize()) as $chunk) {
            $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(fn () => $this->database->updateDocuments(
                $relatedCollection->getId(),
                new Document([$twoWayKey => null]),
                [Query::equal(Document::ID, $chunk)],
                onNext: $report ? $collect : null,
            )));
        }

        return $cleared;
    }

    /**
     * @return list<Document> The documents the delete wrote, as the write left them, or none unless $report is set
     */
    private function deleteSetNull(Document $collection, Document $relatedCollection, Document $document, RelationshipType $relationType, bool $twoWay, string $twoWayKey, RelationshipSide $side, bool $report): array
    {
        switch ($relationType) {
            case RelationshipType::OneToOne:
                if (! $twoWay && $side === RelationshipSide::Parent) {
                    return [];
                }

                $written = $this->database->getAuthorization()->skip(function () use ($document, $relatedCollection, $twoWayKey): ?Document {
                    $related = $this->database->findOne($relatedCollection->getId(), [
                        Query::select([Document::ID]),
                        Query::equal($twoWayKey, [$document->getId()]),
                    ]);

                    if ($related->isEmpty()) {
                        return null;
                    }

                    return $this->database->skipRelationships(fn () => $this->database->updateDocument(
                        $relatedCollection->getId(),
                        $related->getId(),
                        new Document([
                            $twoWayKey => null,
                        ])
                    ));
                });

                return ! $report || $written === null || $written->isEmpty() ? [] : [$written];

            case RelationshipType::OneToMany:
                if ($side === RelationshipSide::Child) {
                    return [];
                }

                return $this->clearReferences($relatedCollection, $document, $twoWayKey, $report);

            case RelationshipType::ManyToOne:
                if ($side === RelationshipSide::Parent) {
                    return [];
                }

                return $this->clearReferences($relatedCollection, $document, $twoWayKey, $report);

            case RelationshipType::ManyToMany:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->database->find($junction, [
                    Query::select([Document::ID]),
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX),
                ]);

                $junctionIds = \array_map(fn (Document $junctionDocument) => $junctionDocument->getId(), $junctions);
                $this->database->skipRelationships(fn () => $this->deleteRelatedDocuments($junction, $junctionIds));
                break;
        }

        return [];
    }

    private function deleteCascade(Document $collection, Document $relatedCollection, Document $document, string $key, RelationshipType $relationType, bool $twoWay, string $twoWayKey, RelationshipSide $side, Cascade $cascade): void
    {
        switch ($relationType) {
            case RelationshipType::OneToOne:
            case RelationshipType::OneToMany:
            case RelationshipType::ManyToOne:
                $relatedIds = $this->findRelatedIds($collection, $relatedCollection, $document, $key, $relationType, $twoWay, $twoWayKey, $side);

                if ($relatedIds !== []) {
                    $this->cascade($cascade, fn () => $this->deleteRelatedDocuments($relatedCollection->getId(), $relatedIds));
                }

                break;
            case RelationshipType::ManyToMany:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->database->skipRelationships(fn () => $this->database->find($junction, [
                    Query::select([Document::ID, $key]),
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX),
                ]));

                $junctionIds = [];
                $relatedIds = [];
                foreach ($junctions as $junctionDocument) {
                    $junctionIds[] = $junctionDocument->getId();
                    if ($side === RelationshipSide::Parent) {
                        $relatedAttribute = $junctionDocument->getAttribute($key);
                        $relatedId = $relatedAttribute instanceof Document ? $relatedAttribute->getId() : (\is_string($relatedAttribute) ? $relatedAttribute : null);
                        if ($relatedId !== null) {
                            $relatedIds[] = $relatedId;
                        }
                    }
                }

                $this->cascade($cascade, function () use ($relatedCollection, $relatedIds, $junction, $junctionIds): void {
                    $this->deleteRelatedDocuments($relatedCollection->getId(), $relatedIds);
                    $this->deleteRelatedDocuments($junction, $junctionIds);
                });
                break;
        }
    }

    /**
     * @param  callable(): mixed  $callback
     */
    private function cascade(Cascade $cascade, callable $callback): void
    {
        $coroutine = $this->coroutine();
        $this->deleteStacks[$coroutine][] = $cascade;

        try {
            $callback();
        } finally {
            \array_pop($this->deleteStacks[$coroutine]);
            if ($this->deleteStacks[$coroutine] === []) {
                unset($this->deleteStacks[$coroutine]);
            }
        }
    }

    private function leaveWrite(int $coroutine): void
    {
        \array_pop($this->writeStacks[$coroutine]);
        if ($this->writeStacks[$coroutine] === []) {
            unset($this->writeStacks[$coroutine]);
        }
    }

    private function coroutine(): int
    {
        /** @var int $coroutine */
        $coroutine = \extension_loaded('swoole') ? Coroutine::getCid() : -1;

        return $coroutine;
    }

    /**
     * Delete the related documents with the given IDs, one at a time through
     * deleteDocument(). A bulk delete selects its batch under the caller's
     * read permission, so a related document the caller may not read is
     * left behind, together with everything below it, and its own
     * relationships are never checked. deleteDocument() loads the document
     * without reading it, checks only the caller's delete permission, and
     * cascades below it, so it throws AuthorizationException or
     * RestrictedException for a document that cannot go, which rolls back
     * the delete that started the cascade, and skips one that is already
     * gone.
     *
     * @param  array<string>  $ids
     */
    private function deleteRelatedDocuments(string $collection, array $ids): void
    {
        foreach (\array_values(\array_unique($ids)) as $id) {
            $this->database->deleteDocument($collection, $id);
        }
    }

    /**
     * Point $twoWayKey on the related documents with the given IDs at $documentId.
     *
     * updateDocuments() leaves out every document the caller may not update,
     * so a chunk that comes back short is finished one document at a time
     * through linkRelatedDocument().
     *
     * @param  array<string>  $ids
     */
    private function linkRelatedDocuments(Document $collection, string $twoWayKey, string $documentId, array $ids): void
    {
        foreach (\array_chunk(\array_values(\array_unique($ids)), $this->relationQueryChunkSize()) as $chunk) {
            $linked = $this->database->skipRelationships(fn () => $this->database->updateDocuments(
                $collection->getId(),
                new Document([$twoWayKey => $documentId]),
                [Query::equal(Document::ID, $chunk)],
            ));

            if ($linked === \count($chunk)) {
                continue;
            }

            $unlinked = $this->database->getAuthorization()->skip(fn () => $this->database->skipRelationships(fn () => $this->database->find($collection->getId(), [
                Query::select([Document::ID]),
                Query::equal(Document::ID, $chunk),
                $this->notReferencing($twoWayKey, $documentId),
                Query::limit(\count($chunk)),
            ])));

            foreach ($unlinked as $related) {
                $this->linkRelatedDocument($collection, $related->getId(), $twoWayKey, $documentId);
            }
        }
    }

    /**
     * Link one related document that updateDocuments() left out. A document that is already gone
     * or already linked is skipped.
     *
     * @throws AuthorizationException
     */
    private function linkRelatedDocument(Document $collection, string $id, string $twoWayKey, string $documentId): void
    {
        $authorization = $this->database->getAuthorization();

        $related = $authorization->skip(fn () => $this->database->skipRelationships(
            fn () => $this->database->getDocument($collection->getId(), $id, forUpdate: true)
        ));

        if ($related->isEmpty() || $related->getAttribute($twoWayKey) === $documentId) {
            return;
        }

        $this->authorizeLink($collection, $related);

        $this->database->skipRelationships(fn () => $this->database->updateDocument(
            $collection->getId(),
            $id,
            new Document([$twoWayKey => $documentId]),
        ));
    }

    /**
     * Linking an existing document to another one needs update permission on it, whichever
     * relationship type holds the link.
     *
     * @throws AuthorizationException
     */
    private function authorizeLink(Document $collection, Document $related): void
    {
        $authorization = $this->database->getAuthorization();

        if (! $authorization->isValid(new Input(PermissionType::Update, [
            ...$collection->getUpdate(),
            ...($collection->getAttribute('documentSecurity', false) ? $related->getUpdate() : []),
        ]))) {
            throw new AuthorizationException($authorization->getDescription());
        }
    }

    private function notReferencing(string $twoWayKey, string $documentId): Query
    {
        return Query::or([
            Query::isNull($twoWayKey),
            Query::notEqual($twoWayKey, $documentId),
        ]);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<string>|null
     */
    private function processNestedRelationshipPath(string $startCollection, array $queries): ?array
    {
        $pathGroups = [];
        foreach ($queries as $query) {
            $attribute = $query->getAttribute();
            if (\str_contains($attribute, '.')) {
                $parts = \explode('.', $attribute);
                $pathKey = \implode('.', \array_slice($parts, 0, -1));
                if (! isset($pathGroups[$pathKey])) {
                    $pathGroups[$pathKey] = [];
                }
                $pathGroups[$pathKey][] = [
                    'method' => $query->getMethod(),
                    'attribute' => \end($parts),
                    'values' => $query->getValues(),
                ];
            }
        }

        /** @var array<string> $allMatchingIds */
        $allMatchingIds = [];
        foreach ($pathGroups as $path => $queryGroup) {
            $pathParts = \explode('.', $path);
            $currentCollection = $startCollection;
            /** @var list<array{string, string, Relationship, RelationshipSide}> $relationshipChain Each link's key, the collection it starts from, its relationship and side */
            $relationshipChain = [];

            foreach ($pathParts as $relationshipKey) {
                $definition = $this->database->silent(fn () => $this->database->findCollection($currentCollection));
                if ($definition === null) {
                    return null;
                }

                $link = null;
                foreach (self::attributes($definition) as $attribute) {
                    if ($attribute->key === $relationshipKey && $attribute->relationship !== null && $attribute->side !== null) {
                        $link = [$relationshipKey, $currentCollection, $attribute->relationship, $attribute->side];
                        break;
                    }
                }

                if ($link === null) {
                    return null;
                }

                $relationshipChain[] = $link;
                $currentCollection = $link[2]->relatedCollection;
            }

            $leafQueries = [];
            foreach ($queryGroup as $q) {
                $leafQueries[] = new Query($q['method'], $q['attribute'], $q['values']);
            }

            /** @var array<Document> $matchingDocs */
            $matchingDocs = $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find(
                $currentCollection,
                \array_merge($leafQueries, [
                    Query::select([Document::ID]),
                    Query::limit(PHP_INT_MAX),
                ])
            )));

            /** @var array<string> $matchingIds */
            $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $matchingDocs);

            if (empty($matchingIds)) {
                return null;
            }

            for ($i = \count($relationshipChain) - 1; $i >= 0; $i--) {
                [$linkKey, $linkFromCollection, $linkRelationship, $side] = $relationshipChain[$i];
                $relationType = $linkRelationship->type;
                $linkToCollection = $linkRelationship->relatedCollection;
                $linkTwoWayKey = $linkRelationship->twoWayKey ?? '';

                $needsReverseLookup = (
                    ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Parent) ||
                    ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Child) ||
                    ($relationType === RelationshipType::ManyToMany)
                );

                if ($needsReverseLookup) {
                    if ($relationType === RelationshipType::ManyToMany) {
                        $fromCollectionDoc = $this->database->silent(fn () => $this->database->getCollection($linkFromCollection));
                        $toCollectionDoc = $this->database->silent(fn () => $this->database->getCollection($linkToCollection));
                        $junction = $this->getJunctionCollection($fromCollectionDoc, $toCollectionDoc, $side);

                        $junctionDocs = $this->readByIds($matchingIds, fn (array $chunk): array => $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find($junction, [
                            Query::equal($linkKey, $chunk),
                            Query::limit(PHP_INT_MAX),
                        ]))));

                        /** @var array<string> $parentIds */
                        $parentIds = [];
                        foreach ($junctionDocs as $jDoc) {
                            $pIdRaw = $jDoc->getAttribute($linkTwoWayKey);
                            $pId = $pIdRaw instanceof Document ? $pIdRaw->getId() : (\is_string($pIdRaw) ? $pIdRaw : null);
                            if ($pId && ! \in_array($pId, $parentIds)) {
                                $parentIds[] = $pId;
                            }
                        }
                    } else {
                        $childDocs = $this->readByIds($matchingIds, fn (array $chunk): array => $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find(
                            $linkToCollection,
                            [
                                Query::equal(Document::ID, $chunk),
                                Query::limit(PHP_INT_MAX),
                            ]
                        ))));

                        /** @var array<string> $parentIds */
                        $parentIds = [];
                        foreach ($childDocs as $doc) {
                            $parentValue = $doc->getAttribute($linkTwoWayKey);
                            if (\is_array($parentValue)) {
                                foreach ($parentValue as $pId) {
                                    if ($pId instanceof Document) {
                                        $pId = $pId->getId();
                                    }
                                    if (\is_string($pId) && $pId && ! \in_array($pId, $parentIds)) {
                                        $parentIds[] = $pId;
                                    }
                                }
                            } else {
                                if ($parentValue instanceof Document) {
                                    $parentValue = $parentValue->getId();
                                }
                                if (\is_string($parentValue) && $parentValue && ! \in_array($parentValue, $parentIds)) {
                                    $parentIds[] = $parentValue;
                                }
                            }
                        }
                    }
                    $matchingIds = $parentIds;
                } else {
                    $parentDocs = $this->readByIds($matchingIds, fn (array $chunk): array => $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find(
                        $linkFromCollection,
                        [
                            Query::equal($linkKey, $chunk),
                            Query::select([Document::ID]),
                            Query::limit(PHP_INT_MAX),
                        ]
                    ))));
                    $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $parentDocs);
                }

                if (empty($matchingIds)) {
                    return null;
                }
            }

            $allMatchingIds = \array_merge($allMatchingIds, $matchingIds);
        }

        return \array_unique($allMatchingIds);
    }

    /**
     * @param  array<Query>  $relatedQueries
     * @return array{attribute: string, ids: string[]}|null
     */
    private function resolveRelationshipGroupToIds(
        Attribute $attribute,
        array $relatedQueries,
        ?Document $collection = null,
    ): ?array {
        $relationship = $attribute->relationship;
        $side = $attribute->side;
        if ($relationship === null || $side === null) {
            return null;
        }

        $relatedCollection = $relationship->relatedCollection;
        $relationType = $relationship->type;
        $twoWayKey = $relationship->twoWayKey ?? '';
        $relationshipKey = $attribute->key;

        $hasNestedPaths = false;
        foreach ($relatedQueries as $relatedQuery) {
            if (\str_contains($relatedQuery->getAttribute(), '.')) {
                $hasNestedPaths = true;
                break;
            }
        }

        $pathIds = null;

        if ($hasNestedPaths) {
            $pathIds = $this->processNestedRelationshipPath(
                $relatedCollection,
                $relatedQueries
            );

            if ($pathIds === null || empty($pathIds)) {
                return null;
            }

            $relatedQueries = \array_values(\array_filter($relatedQueries, fn (Query $q) => ! \str_contains($q->getAttribute(), '.')));
        }

        $needsParentResolution = (
            ($relationType === RelationshipType::OneToMany && $side === RelationshipSide::Parent) ||
            ($relationType === RelationshipType::ManyToOne && $side === RelationshipSide::Child) ||
            ($relationType === RelationshipType::ManyToMany)
        );

        if ($relationType === RelationshipType::ManyToMany && $needsParentResolution && $collection !== null) {
            $matchingDocs = $this->findRelated($relatedCollection, $relatedQueries, $pathIds, [
                Query::select([Document::ID]),
                Query::limit(PHP_INT_MAX),
            ]);

            $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $matchingDocs);

            if (empty($matchingIds)) {
                return null;
            }

            /** @var Document $relatedCollectionDoc */
            $relatedCollectionDoc = $this->database->silent(fn () => $this->database->getCollection($relatedCollection));
            $junction = $this->getJunctionCollection($collection, $relatedCollectionDoc, $side);

            $junctionDocs = $this->readByIds($matchingIds, fn (array $chunk): array => $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find($junction, [
                Query::equal($relationshipKey, $chunk),
                Query::limit(PHP_INT_MAX),
            ]))));

            /** @var array<string> $parentIds */
            $parentIds = [];
            foreach ($junctionDocs as $jDoc) {
                $pIdRaw = $jDoc->getAttribute($twoWayKey);
                $pId = $pIdRaw instanceof Document ? $pIdRaw->getId() : (\is_string($pIdRaw) ? $pIdRaw : null);
                if ($pId && ! \in_array($pId, $parentIds)) {
                    $parentIds[] = $pId;
                }
            }

            return empty($parentIds) ? null : ['attribute' => Document::ID, 'ids' => $parentIds];
        } elseif ($needsParentResolution) {
            $matchingDocs = $this->findRelated($relatedCollection, $relatedQueries, $pathIds, [Query::limit(PHP_INT_MAX)]);

            /** @var array<string> $parentIds */
            $parentIds = [];

            foreach ($matchingDocs as $doc) {
                $parentId = $doc->getAttribute($twoWayKey);

                if (\is_array($parentId)) {
                    foreach ($parentId as $id) {
                        if ($id instanceof Document) {
                            $id = $id->getId();
                        }
                        if (\is_string($id) && $id && ! \in_array($id, $parentIds)) {
                            $parentIds[] = $id;
                        }
                    }
                } else {
                    if ($parentId instanceof Document) {
                        $parentId = $parentId->getId();
                    }
                    if (\is_string($parentId) && $parentId && ! \in_array($parentId, $parentIds)) {
                        $parentIds[] = $parentId;
                    }
                }
            }

            return empty($parentIds) ? null : ['attribute' => Document::ID, 'ids' => $parentIds];
        } else {
            $matchingDocs = $this->findRelated($relatedCollection, $relatedQueries, $pathIds, [
                Query::select([Document::ID]),
                Query::limit(PHP_INT_MAX),
            ]);

            /** @var array<string> $matchingIds */
            $matchingIds = \array_map(fn (Document $doc) => $doc->getId(), $matchingDocs);

            return empty($matchingIds) ? null : ['attribute' => $relationshipKey, 'ids' => $matchingIds];
        }
    }

    /**
     * Read the related documents matching $relatedQueries, limited to $pathIds when a nested path resolved them,
     * without populating their relationships.
     *
     * @param  array<Query>  $relatedQueries
     * @param  array<string>|null  $pathIds
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function findRelated(string $relatedCollection, array $relatedQueries, ?array $pathIds, array $queries): array
    {
        if ($pathIds === null) {
            return $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find($relatedCollection, \array_merge($relatedQueries, $queries))));
        }

        return $this->readByIds($pathIds, fn (array $chunk): array => $this->database->silent(fn () => $this->database->skipRelationships(fn () => $this->database->find(
            $relatedCollection,
            \array_merge($relatedQueries, [Query::equal(Document::ID, $chunk)], $queries)
        ))));
    }
}
