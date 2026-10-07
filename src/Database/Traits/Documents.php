<?php

namespace Utopia\Database\Traits;

use Closure;
use DateTime as PhpDateTime;
use Exception;
use Generator;
use RuntimeException;
use Throwable;
use Utopia\Console;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Epoch;
use Utopia\Database\Cache\Owners;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Index as IndexModel;
use Utopia\Database\Operator;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Database\Validator\BigInt;
use Utopia\Database\Validator\PartialStructure;
use Utopia\Database\Validator\Permissions;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Queries\Bounds;
use Utopia\Database\Validator\Queries\Document as DocumentValidator;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;
use Utopia\Database\Validator\Queries\Narrow;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Join as JoinValidator;
use Utopia\Database\Validator\Query\JoinedCollection;
use Utopia\Database\Validator\Structure;
use Utopia\Database\Validator\UID;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
use WeakMap;

/**
 * Provides document CRUD operations including find, create, update, upsert, delete, and cache management.
 */
trait Documents
{
    private const string DOCUMENT_CACHE_ACTIVE_PREFIX = 'active:';

    private const string DOCUMENT_CACHE_BLOCKED_PREFIX = 'blocked:';

    private const string DOCUMENT_CACHE_LAPSED_PREFIX = 'lapsed:';

    private const string DOCUMENT_CACHE_SEPARATOR = '@';

    private const string DOCUMENT_CACHE_TOKEN_SEPARATOR = '.';

    private const int DOCUMENT_CACHE_PERMANENT = \PHP_INT_MAX;

    private const int DOCUMENT_CACHE_RECHECK = 60;

    private const string DOCUMENT_CACHE_EPOCH = 'epoch';

    private const string DOCUMENT_CACHE_FIELD = 'field';

    private const string DOCUMENT_CACHE_VALUE = 'document';

    private const string DOCUMENT_CACHE_COLLECTION_EPOCH = 'collectionEpoch';

    private const string DOCUMENT_CACHE_BLOCKED_AT = 'blockedAt';

    private const string DOCUMENT_CACHE_CHECKED_AT = 'checkedAt';

    private int $cacheWriterTimeout = 3600;

    /** @var array<int, array<string, string>> Definition keys of the collections the open invalidation scope wrote, by coroutine id and collection key. */
    private array $documentCacheDefinitions = [];

    /** @var array<string, array{source: array<string, mixed>, model: Document}> The model built from each definition's cached copy, by its cache key. */
    private static array $definitionModels = [];

    private const int DEFINITION_MODELS_LIMIT = 256;

    /** @var WeakMap<Document, string>|null The document-cache epoch each collection definition was read with, until the definition is let go. */
    private static ?WeakMap $collectionCacheEpochs = null;

    /**
     * Seconds after which an invalidation that has not finished is treated as abandoned (a worker killed
     * mid-transaction): readers stop waiting for it, and the next write re-enables the collection's cache.
     */
    public function setCacheWriterTimeout(int $seconds): static
    {
        $this->cacheWriterTimeout = \max(0, $seconds);

        return $this;
    }

    public function getCacheWriterTimeout(): int
    {
        return $this->cacheWriterTimeout;
    }

    private function getNumericResult(Attribute $attribute, mixed $current, int|float|string $value, bool $increase): int|float|string
    {
        $current ??= 0;

        if (Attribute::isIntegerType($attribute->getType())) {
            if (! $attribute->isSigned()
                && $attribute->getType() === ColumnType::BigInteger
                && ! $this->adapter->supports(Capability::UnsignedBigInt)) {
                throw new TypeException('Unsigned 64-bit arithmetic is not supported by this adapter.');
            }
            if ((! \is_int($current) && ! \is_string($current)) || ! BigInt::isIntegerString((string) $current)) {
                throw new TypeException('Attribute value must be an integer.');
            }
            if ((! \is_int($value) && ! \is_string($value)) || ! BigInt::isIntegerString((string) $value)) {
                throw new TypeException('Change value must be an integer.');
            }

            $result = $increase
                ? BigInt::add($current, $value)
                : BigInt::subtract($current, $value);
            $bounds = Attribute::getNumericBounds($attribute->getType(), $attribute->isSigned());
            if ($bounds === null) {
                throw new TypeException('Attribute value must be numeric.');
            }
            if (BigInt::compare($result, $bounds['max']) > 0) {
                throw new LimitException('Attribute value exceeds maximum limit: '.$bounds['max']);
            }
            if (BigInt::compare($result, $bounds['min']) < 0) {
                throw new LimitException('Attribute value exceeds minimum limit: '.$bounds['min']);
            }

            return $result;
        }

        if (! \is_numeric($current)) {
            throw new TypeException('Attribute value must be numeric.');
        }

        $current = $this->getNativeNumber($current);
        $value = $this->getNativeNumber($value);
        $bounds = Attribute::getNumericBounds($attribute->getType(), $attribute->isSigned());

        if ($bounds === null || (\is_float($current) && ! \is_finite($current))) {
            throw new TypeException('Attribute value must be a finite numeric value.');
        }
        $maximum = $this->getNativeNumber($bounds['max']);
        $minimum = $this->getNativeNumber($bounds['min']);

        if ($current > $maximum) {
            throw new LimitException('Attribute value exceeds maximum limit: '.$maximum);
        }

        if ($current < $minimum) {
            throw new LimitException('Attribute value exceeds minimum limit: '.$minimum);
        }

        $overflows = $increase
            ? ($value > 0 && $current > $maximum - $value)
            : ($value < 0 && $current > $maximum + $value);
        if ($overflows) {
            throw new LimitException('Attribute value exceeds maximum limit: '.$maximum);
        }

        $underflows = $increase
            ? ($value < 0 && $current < $minimum - $value)
            : ($value > 0 && $current < $minimum + $value);
        if ($underflows) {
            throw new LimitException('Attribute value exceeds minimum limit: '.$minimum);
        }

        $result = $increase ? $current + $value : $current - $value;
        if (\is_float($result) && ! \is_finite($result)) {
            throw new TypeException('Attribute value must be a finite numeric value.');
        }

        return $result;
    }

    private function getNativeNumber(int|float|string $value): int|float
    {
        if (\is_int($value) || \is_float($value)) {
            return $value;
        }
        if (! \is_numeric($value)) {
            throw new TypeException('Value must be numeric.');
        }

        return \str_contains(\strtolower($value), '.') || \str_contains(\strtolower($value), 'e')
            ? (float) $value
            : (int) $value;
    }

    private function declaredAttribute(Collection $collection, string $key): ?Attribute
    {
        foreach ($collection->getDeclaredAttributes() as $attribute) {
            if ($attribute->getKey() === $key) {
                return $attribute;
            }
        }

        return null;
    }

    private function isDeclaredInteger(?Attribute $attribute): bool
    {
        return $attribute !== null && ! $attribute->isArray() && Attribute::isIntegerType($attribute->getType());
    }

    private function assertIntegerChange(int|float|string $value): void
    {
        if ((! \is_int($value) && ! \is_string($value)) || ! BigInt::isIntegerString((string) $value)) {
            throw new TypeException('Change value must be an integer.');
        }
    }

    private function integerBound(int|float|string $bound, string $name): int|string
    {
        return BigInt::integralValue($bound) ?? throw new TypeException($name.' must be an integer.');
    }

    /**
     * Cached validator instances keyed by context and a
     * stable schema/authorization fingerprint.
     *
     * Building DocumentsValidator deep-copies every collection attribute via
     * Attribute::getArrayCopy(), which is expensive on the find/count/sum
     * hot path. The composite key keeps the cache coherent when the same
     * Database instance is reused across namespaces, tenants, or with a
     * different max-query-values cap. Fresh collection metadata contributes
     * a stable schema and authorization fingerprint to each key.
     *
     * @var array<string, DocumentsValidator>
     */
    private array $documentsValidatorCache = [];

    private const int DOCUMENTS_VALIDATOR_CACHE_LIMIT = 256;

    private ?Bounds $queryBounds = null;

    /** @var array<string, Aggregate> Aggregate validators of sums of declared attributes, by collection schema. */
    private array $sumValidatorCache = [];

    /**
     * Return a DocumentsValidator for the given collection, building it on
     * first request and caching the instance for subsequent calls. The cache
     * is purged when the collection's schema changes. Queries that join other
     * collections get a fresh validator every time: the cache key describes
     * only this collection, never the joined ones.
     *
     * @param  array<Document>  $joinedCollections
     */
    protected function getDocumentsValidator(Document $collection, array $joinedCollections = []): DocumentsValidator
    {
        $supportForJoins = $this->adapter->supports(Capability::Joins);
        $supportForAggregations = $this->adapter->supports(Capability::Aggregations);

        if ($joinedCollections !== []) {
            return $this->createDocumentsValidator($collection, $supportForJoins, $supportForAggregations);
        }

        $context = $this->getCollectionMetadataCacheKey($collection->getId());
        $key = $this->documentsValidatorCacheKey($collection, $context, $supportForJoins, $supportForAggregations);

        if (isset($this->documentsValidatorCache[$key])) {
            return $this->documentsValidatorCache[$key];
        }

        $validator = $this->createDocumentsValidator($collection, $supportForJoins, $supportForAggregations);

        if (\count($this->documentsValidatorCache) >= self::DOCUMENTS_VALIDATOR_CACHE_LIMIT) {
            $this->documentsValidatorCache = [];
        }
        $this->documentsValidatorCache[$key] = $validator;

        return $validator;
    }

    /**
     * The validator of a query list. A narrow list, of plain filters, limits, offsets, cursors and
     * orders on the collection's own top-level attributes, is checked by those validators built
     * from only the attributes it names, from the collection as it is passed; any other list by the
     * collection's documents validator. Both accept the same narrow lists with the same messages.
     *
     * @param  array<mixed>  $queries
     * @param  array<Document>  $joinedCollections
     */
    protected function getQueriesValidator(Document $collection, array $queries, array $joinedCollections = []): Queries
    {
        if ($joinedCollections === [] && Narrow::accepts($queries)) {
            $attributes = $collection->getAttribute('attributes', []);
            $narrow = \is_array($attributes) ? Narrow::of(
                $queries,
                $attributes,
                $this->getQueryBounds(),
                $this->maxQueryValues,
                $this->adapter->supports(Capability::DefinedAttributes),
                $this->adapter->supports(Capability::UnsignedBigInt),
                $this->adapter->supports(Capability::OrderRandom),
            ) : null;

            if ($narrow !== null) {
                return $narrow;
            }
        }

        return $this->getDocumentsValidator($collection, $joinedCollections);
    }

    private function getQueryBounds(): Bounds
    {
        return $this->queryBounds ??= new Bounds(
            $this->adapter->getIdAttributeType(),
            $this->adapter->getMaxUIDLength(),
            $this->adapter->getMinDateTime(),
            $this->adapter->getMaxDateTime(),
        );
    }

    private function createDocumentsValidator(Document $collection, bool $supportForJoins, bool $supportForAggregations): DocumentsValidator
    {
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        return new DocumentsValidator(
            $attributes,
            $indexes,
            $this->adapter->getIdAttributeType(),
            $this->maxQueryValues,
            $this->adapter->getMaxUIDLength(),
            $this->adapter->getMinDateTime(),
            $this->adapter->getMaxDateTime(),
            $this->adapter->supports(Capability::DefinedAttributes),
            $this->adapter->supports(Capability::UnsignedBigInt),
            $supportForJoins,
            $supportForAggregations,
            $this->adapter->getSharedTables(),
            $this->adapter->supports(Capability::OrderRandom),
        );
    }

    /**
     * Build the composite cache key for the DocumentsValidator cache. Scoping
     * by namespace + tenant + max-query-values + the join and aggregation
     * grammar keeps two collections that share an id (different tenant
     * schemas, different namespace prefixes, different per-request limits or
     * adapters with different capabilities) from aliasing onto the same
     * validator.
     */
    private function documentsValidatorCacheKey(Document $collection, string $context, bool $supportForJoins, bool $supportForAggregations): string
    {
        return $context.'::'.$this->maxQueryValues.'::'.(int) $supportForJoins.(int) $supportForAggregations.(int) $this->adapter->getSharedTables().'::'.$this->collectionFingerprint($collection);
    }

    /**
     * A hash of everything a query validator is built from: the collection's attributes, indexes,
     * permissions and document security.
     */
    private function collectionFingerprint(Document $collection): string
    {
        return \hash('xxh128', \serialize([
            'attributes' => $collection->getAttribute('attributes', []),
            'indexes' => $collection->getAttribute('indexes', []),
            'permissions' => $collection->getAttribute(Document::PERMISSIONS, []),
            'documentSecurity' => (bool) $collection->getAttribute('documentSecurity', false),
        ]));
    }

    /**
     * @param  array<mixed>  $queries
     *
     * @throws QueryException
     */
    private function rejectJoins(array $queries, string $message): void
    {
        foreach ($queries as $query) {
            if ($query instanceof Query && $query->getMethod()->isJoin()) {
                throw new QueryException($message);
            }
        }
    }

    /**
     * @param  array<Document>  $documents
     * @param  array<Query>  $selections
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    protected function refetchDocuments(Document $collection, array $documents, array $selections = []): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $sequences = \array_map(function (Document $document): string {
            $sequence = $document->getSequence();
            if ($sequence === null) {
                throw new DatabaseException('Cannot refetch document without a $sequence: '.$document->getId());
            }

            return $sequence;
        }, $documents);

        $refetchedMap = [];
        foreach (\array_chunk($sequences, \max(1, $this->maxQueryValues)) as $chunk) {
            $refetched = $this->getAuthorization()->skip(fn () => $this->silent(
                fn () => $this->find(
                    $collection->getId(),
                    \array_merge([
                        Query::equal(Document::SEQUENCE, $chunk),
                        Query::limit(\count($chunk)),
                    ], $selections)
                )
            ));

            foreach ($refetched as $document) {
                $sequence = $document->getSequence();
                if ($sequence === null) {
                    throw new DatabaseException('Cannot index refetched document without a $sequence: '.$document->getId());
                }

                $refetchedMap[$sequence] = $document;
            }
        }

        $result = [];
        foreach ($documents as $index => $document) {
            $result[$index] = $refetchedMap[$sequences[$index]] ?? $document;
        }

        return $result;
    }

    /**
     * Get Document
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The document identifier
     * @param  array<Query>  $queries  Optional select/filter queries
     * @param  bool  $forUpdate  Whether to lock the document for update
     * @return Document The document, or an empty Document if not found
     *
     * @throws DatabaseException
     * @throws QueryException
     */
    public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        if ($collection === self::METADATA && $id === self::METADATA) {
            return self::collectionDefinition();
        }

        if (empty($collection)) {
            throw new NotFoundException('Collection not found');
        }

        if (empty($id)) {
            return $this->createDocumentInstance($collection, []);
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        $this->checkQueryTypes($queries);

        $joinedCollections = null;

        if ($this->validation()->get() && $queries !== []) {
            $joinedCollections = $this->resolveJoinedCollections($queries);
            $supportForAttributes = $this->adapter->supports(Capability::DefinedAttributes);
            $supportForJoins = $this->adapter->supports(Capability::Joins);
            $validator = $joinedCollections === []
                ? new DocumentValidator($attributes, $supportForAttributes, sharedTables: $this->adapter->getSharedTables(), supportForJoins: $supportForJoins)
                : new DocumentValidator(
                    attributes: $attributes,
                    supportForAttributes: $supportForAttributes,
                    idAttributeType: $this->adapter->getIdAttributeType(),
                    maxValuesCount: $this->maxQueryValues,
                    minAllowedDate: $this->adapter->getMinDateTime(),
                    maxAllowedDate: $this->adapter->getMaxDateTime(),
                    supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt),
                    sharedTables: $this->adapter->getSharedTables(),
                    supportForJoins: $supportForJoins,
                );
            $validator->setJoinedCollections($joinedCollections);
            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        /** @var array<Document> $allAttributes */
        $allAttributes = $collection->getAttribute('attributes', []);
        $relationships = \array_filter(
            $allAttributes,
            fn (Attribute|Document $attribute) => Attribute::isRelationship($attribute)
        );

        $grouped = Query::groupForDatabase($queries);
        $selects = $grouped['selections'];
        $joins = $grouped['joins'];

        if (! empty($joins) && ! $this->adapter->supports(Capability::Joins)) {
            throw new QueryException('Join queries are not supported by this adapter');
        }

        $this->assertJoinCount($joins);

        $joinedCollections ??= $this->resolveJoinedCollections($joins);
        $joinDocumentSecurity = [];
        $joinedByAlias = [];
        if (! empty($joins)) {
            $joinDocumentSecurity = $this->authorizeJoins($joins, PermissionType::Read, $joinedCollections);
            $joinedByAlias = $this->joinedCollectionsByAlias($joins, $joinedCollections);
            $queries = $this->convertQueries($collection, $queries, $joinedByAlias);
        }

        $outerJoinIds = $this->outerJoinIdSelections($selects, $joins, $joinedByAlias);
        if ($outerJoinIds !== []) {
            $queries[] = Query::select($outerJoinIds);
        }

        $selections = $this->validateSelections($collection, $selects);
        $nestedSelections = $this->relationshipHook?->processQueries($relationships, $queries) ?? [];

        $documentSecurity = $collection->getAttribute('documentSecurity', false);

        [$collectionKey, $documentKey, $hashKey] = $this->getCacheKeys(
            $collection->getId(),
            $id,
            $selections
        );
        $definition = $collection->getId() === self::METADATA;
        // The cache lower-cases keys; the hash key keeps the id's case, so casings an adapter tells apart keep separate fields.
        // A global collection's definition has one key for every tenant, and each tenant has its own collection epoch.
        $field = \md5($definition ? $hashKey.':'.\json_encode($this->adapter->getTenant()) : $hashKey);

        // Collection definitions are cacheable because every schema mutation
        // persists the definition through updateMetadata(), which writes the
        // row via the METADATA collection's own document path and therefore
        // purges that definition's slot; a cached definition is served without
        // an epoch check. Any new schema mutator must keep writing through that
        // path, or its readers will serve a stale schema.
        $inTransaction = $this->adapter->inTransaction();
        $cacheable = ! $forUpdate
            && empty($joins)
            && (! $inTransaction || $this->isCachedInTransaction($documentKey))
            && ! isset($this->documentCacheMutations[$this->getEventContext()][$collectionKey]);
        $epoch = $cacheable && ! $definition ? $this->getCollectionCacheEpoch($collection) : null;
        // A transaction reads its own snapshot, which can predate another writer's commit and purge.
        $fillEpoch = $inTransaction ? null : $epoch;
        $fillDefinition = $cacheable && $definition && ! $inTransaction;
        $cached = null;
        $collectionEpoch = null;
        try {
            if ($cacheable && $definition) {
                $entry = $this->loadCachedDefinition($documentKey, $field);
                if ($entry !== null) {
                    $cached = $entry[self::DOCUMENT_CACHE_VALUE];
                    $collectionEpoch = $entry[self::DOCUMENT_CACHE_COLLECTION_EPOCH] ?? null;
                }
            } elseif ($epoch !== null) {
                $cached = $this->loadCachedDocument($documentKey, $field, $epoch);
            }
        } catch (Exception $e) {
            Console::warning('Warning: Failed to get document from cache: '.$e->getMessage());
        }

        if (\is_array($cached) && isset($cached[self::CACHE_EMPTY_MARKER])) {
            return $this->createDocumentInstance($collection->getId(), []);
        }

        if ($cached) {
            /** @var array<string, mixed> $cached */
            $document = $definition
                ? $this->createDefinitionInstance($documentKey, $cached)
                : $this->createDocumentInstance($collection->getId(), $cached);
            $document = $this->casting($collection, $document);

            if ($collection->getId() !== self::METADATA) {

                if (! $this->authorization->isValid(new Input(PermissionType::Read, [
                    ...$collection->getRead(),
                    ...($documentSecurity ? $document->getRead() : []),
                ]))) {
                    return $this->createDocumentInstance($collection->getId(), []);
                }
            }

            $document = $this->decorateDocument(Event::DocumentRead, $collection, $document);

            $this->trigger(Event::DocumentRead, $document);

            if ($this->isTtlExpired($collection, $document)) {
                return $this->createDocumentInstance($collection->getId(), []);
            }

            $this->attachCollectionCacheEpoch($document, \is_string($collectionEpoch) ? $collectionEpoch : null);

            return $document;
        }

        $generation = '0';
        if ($fillEpoch !== null || $fillDefinition) {
            try {
                $generation = $this->cache->getGeneration($documentKey);
            } catch (Exception $e) {
                Console::warning('Warning: Failed to get cache generation: '.$e->getMessage());
            }
        }

        $transactionDefinition = $cacheable && $definition && $inTransaction && $queries === [];
        $transactionDefinitionKey = \strtolower($documentKey);
        $readInTransaction = $transactionDefinition
            ? ($this->transactionDefinitions[$this->getEventContext()][$transactionDefinitionKey][$field] ?? null)
            : null;
        if ($readInTransaction !== null) {
            $collectionState = $this->loadDocumentCacheState($this->getCacheBaseKeys($id)[0]);
            $document = $this->decorateDocument(Event::DocumentRead, $collection, clone $readInTransaction);
            $this->trigger(Event::DocumentRead, $document);
            $this->attachCollectionCacheEpoch($document, $collectionState->value);

            return $document;
        }

        $collectionGranted = $this->authorization->isValid(new Input(PermissionType::Read, $collection->getRead()));
        $skipAuth = empty($joins)
            && $collection->getId() !== self::METADATA
            && $collectionGranted;

        $getDocument = fn () => $this->adapter->getDocument(
            $this->withJoinAttributes($this->withJoinAuthorization($collection, $joinDocumentSecurity, $collectionGranted || $collection->getId() === self::METADATA), $joins, $joinedCollections),
            $id,
            $queries,
            $forUpdate
        );

        $document = $skipAuth ? $this->authorization->skip($getDocument) : $getDocument();
        $fillEpoch = $this->isReadFromReplica() ? null : $fillEpoch;
        $fillDefinition = $fillDefinition && ! $this->isReadFromReplica();

        if ($document->isEmpty()) {
            // The marker is shared by every reader, so a miss observed with authorization
            // enabled only proves absence once an unfiltered read agrees: an adapter may have
            // filtered the row out by the caller's permissions. Collection definitions are
            // never filtered that way, as every write resolves its collection through them.
            $missing = true;
            if ($fillEpoch !== null && empty($relationships) && ! $skipAuth && $collection->getId() !== self::METADATA) {
                $missing = $this->authorization->skip($getDocument)->isEmpty();
                $fillEpoch = $this->isReadFromReplica() ? null : $fillEpoch;
            }

            try {
                if ($fillEpoch !== null && empty($relationships) && $missing) {
                    $this->saveCachedDocument($documentKey, $field, $fillEpoch, [self::CACHE_EMPTY_MARKER => true], $generation);
                } elseif ($fillDefinition) {
                    $this->saveCachedDefinition($documentKey, $field, [self::CACHE_EMPTY_MARKER => true], [], $generation);
                }
            } catch (Exception $e) {
                Console::warning('Failed to save empty document to cache: '.$e->getMessage());
            }

            return $this->createDocumentInstance($collection->getId(), []);
        }

        if ($this->isTtlExpired($collection, $document)) {
            return $this->createDocumentInstance($collection->getId(), []);
        }

        $collectionState = $cacheable && $definition
            ? $this->loadDocumentCacheState($this->getCacheBaseKeys($id)[0])
            : new Epoch();

        $document = $this->castingAfter($collection, $document);

        // Convert to custom document type if mapped
        if (isset($this->documentTypes[$collection->getId()])) {
            $document = $this->createDocumentInstance($collection->getId(), $document->getArrayCopy());
        }

        $document->setAttribute(Document::COLLECTION, $collection->getId());

        if ($collection->getId() !== self::METADATA) {
            if (! $this->authorization->isValid(new Input(PermissionType::Read, [
                ...$collection->getRead(),
                ...($documentSecurity ? $document->getRead() : []),
            ]))) {
                return $this->createDocumentInstance($collection->getId(), []);
            }
        }

        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document, $selections);
        if (! empty($joins)) {
            $document = $this->decodeJoins($document, $joinedByAlias);
            foreach ($outerJoinIds as $outerJoinId) {
                $document->removeAttribute($outerJoinId);
            }
        }

        // Skip relationship population if we're in batch mode (relationships will be populated later)
        if ($this->relationshipHook !== null && ! $this->relationshipHook->isInBatchPopulation() && $this->relationshipHook->isEnabled() && ! empty($relationships) && (empty($selects) || ! empty($nestedSelections))) {
            $documents = $this->silent(fn () => $this->relationshipHook->populateDocuments([$document], $collection, $this->relationshipHook->getFetchDepth(), $nestedSelections));
            $document = $documents[0];
        }

        /** @var array<Document> $cacheCheckAttrs */
        $cacheCheckAttrs = $collection->getAttribute('attributes', []);
        $relationships = \array_filter(
            $cacheCheckAttrs,
            fn (Attribute|Document $attribute) => Attribute::isRelationship($attribute)
        );

        try {
            if ($fillEpoch !== null && empty($relationships)) {
                $this->saveCachedDocument($documentKey, $field, $fillEpoch, $document->getArrayCopy(), $generation);
            } elseif ($fillDefinition) {
                $this->saveCachedDefinition(
                    $documentKey,
                    $field,
                    $document->getArrayCopy(),
                    [
                        self::DOCUMENT_CACHE_COLLECTION_EPOCH => $collectionState->value,
                        self::DOCUMENT_CACHE_BLOCKED_AT => $collectionState->blockedAt,
                        self::DOCUMENT_CACHE_CHECKED_AT => \time(),
                    ],
                    $generation,
                    fn (): bool => $this->loadDocumentCacheState($this->getCacheBaseKeys($id)[0])->value === $collectionState->value,
                );
            }
        } catch (Exception $e) {
            Console::warning('Failed to save document to cache: '.$e->getMessage());
        }

        if ($transactionDefinition) {
            $this->transactionDefinitions[$this->getEventContext()][$transactionDefinitionKey][$field] = clone $document;
        }

        $document = $this->decorateDocument(Event::DocumentRead, $collection, $document);

        $this->trigger(Event::DocumentRead, $document);

        $this->attachCollectionCacheEpoch($document, $collectionState->value);

        return $document;
    }

    /**
     * Whether a read inside a transaction may serve the document's cached copy: only when this
     * context's invalidation scope started the transaction and has not written the document. Ids
     * compare case-insensitively, as an adapter may match any casing of a written id.
     */
    private function isCachedInTransaction(string $documentKey): bool
    {
        $written = $this->transactionWrites[$this->getEventContext()] ?? null;

        return $written !== null && ! isset($written[\strtolower($documentKey)]);
    }

    /**
     * A replica may lag the primary, so what it served must not be cached for other readers.
     */
    private function isReadFromReplica(): bool
    {
        return $this->adapter instanceof ReadWritePool && $this->adapter->servedByReplica();
    }

    /**
     * What a read under $epoch may serve from a document's cache slot: its copy, the absence
     * marker, or null when the slot holds nothing for this read.
     *
     * @return array<mixed>|null
     */
    private function loadCachedDocument(string $documentKey, string $field, string $epoch): ?array
    {
        $entry = $this->cache->load($documentKey, self::TTL, $field);
        if (! \is_array($entry)) {
            return null;
        }

        $document = $entry[self::DOCUMENT_CACHE_VALUE] ?? null;
        if (
            ! \is_array($document)
            || ($entry[self::DOCUMENT_CACHE_EPOCH] ?? null) !== $epoch
            || ($entry[self::DOCUMENT_CACHE_FIELD] ?? null) !== $field
        ) {
            return null;
        }

        return $document;
    }

    /**
     * @param  array<mixed>  $document
     */
    private function saveCachedDocument(string $documentKey, string $field, string $epoch, array $document, string $generation): void
    {
        $this->cache->saveWithLease($documentKey, [
            self::DOCUMENT_CACHE_EPOCH => $epoch,
            self::DOCUMENT_CACHE_FIELD => $field,
            self::DOCUMENT_CACHE_VALUE => $document,
        ], $field, $generation);
    }

    /**
     * The Collection model of a collection definition's cached copy, as a deep clone of the one built
     * the last time this copy was read: it is built again whenever the copy read differs in any value.
     * The kept model has its permissions parsed, so its clones start with the parse, which each one
     * checks against its own permissions before using it.
     * A custom document type for the metadata collection is built on every read, as its constructor
     * may do more than copy the data.
     *
     * @param  array<string, mixed>  $cached
     */
    private function createDefinitionInstance(string $documentKey, array $cached): Document
    {
        if (($this->documentTypes[self::METADATA] ?? null) !== Collection::class) {
            return $this->createDocumentInstance(self::METADATA, $cached);
        }

        $entry = self::$definitionModels[$documentKey] ?? null;
        if ($entry !== null && $entry['source'] === $cached) {
            return clone $entry['model'];
        }

        $model = $this->createDocumentInstance(self::METADATA, $cached);

        if (\count(self::$definitionModels) >= self::DEFINITION_MODELS_LIMIT) {
            self::$definitionModels = [];
        }
        $kept = clone $model;
        try {
            $kept->getPermissions();
        } catch (StructureException) {
            // Permissions that do not parse fail where a clone's are read, as they would unparsed.
        }
        self::$definitionModels[$documentKey] = ['source' => $cached, 'model' => $kept];

        return $model;
    }

    /**
     * What a read may serve from a collection definition's cache slot: its entry, carrying the epoch
     * the collection's documents are cached under, or null when the slot holds nothing for this read
     * or holds a blocked collection due for another look.
     *
     * @return array{document: array<mixed>, collectionEpoch?: mixed}|null
     */
    private function loadCachedDefinition(string $documentKey, string $field): ?array
    {
        $entry = $this->cache->load($documentKey, self::TTL, $field);
        if (
            ! \is_array($entry)
            || ! \is_array($entry[self::DOCUMENT_CACHE_VALUE] ?? null)
            || ($entry[self::DOCUMENT_CACHE_FIELD] ?? null) !== $field
        ) {
            return null;
        }

        if (isset($entry[self::DOCUMENT_CACHE_VALUE][self::CACHE_EMPTY_MARKER])) {
            return [self::DOCUMENT_CACHE_VALUE => $entry[self::DOCUMENT_CACHE_VALUE]];
        }

        $collectionEpoch = $entry[self::DOCUMENT_CACHE_COLLECTION_EPOCH] ?? null;
        if ($collectionEpoch === null) {
            $now = \time();
            $blockedAt = $entry[self::DOCUMENT_CACHE_BLOCKED_AT] ?? null;
            $checkedAt = $entry[self::DOCUMENT_CACHE_CHECKED_AT] ?? null;
            if (
                ! \is_int($blockedAt)
                || ! \is_int($checkedAt)
                || $blockedAt + $this->cacheWriterTimeout <= $now
                || $checkedAt + self::DOCUMENT_CACHE_RECHECK <= $now
            ) {
                return null;
            }
        }

        return [
            self::DOCUMENT_CACHE_VALUE => $entry[self::DOCUMENT_CACHE_VALUE],
            self::DOCUMENT_CACHE_COLLECTION_EPOCH => $collectionEpoch,
        ];
    }

    /**
     * Without generations a fill can land after the purge that should have removed it, so the state it
     * was filled under is read again and the fill is dropped when that state has moved on.
     *
     * @param  array<mixed>  $document
     * @param  array<string, mixed>  $validity
     * @param  (Closure(): bool)|null  $isCurrent
     */
    private function saveCachedDefinition(string $documentKey, string $field, array $document, array $validity, string $generation, ?Closure $isCurrent = null): void
    {
        $saved = $this->cache->saveWithLease($documentKey, [
            ...$validity,
            self::DOCUMENT_CACHE_FIELD => $field,
            self::DOCUMENT_CACHE_VALUE => $document,
        ], $field, $generation);

        if ($saved !== false && $generation === '0' && $isCurrent !== null && ! $isCurrent()) {
            $this->cache->purge($documentKey);
        }
    }

    private function attachCollectionCacheEpoch(Document $definition, ?string $epoch): void
    {
        self::$collectionCacheEpochs ??= new WeakMap();
        if ($epoch === null) {
            unset(self::$collectionCacheEpochs[$definition]);

            return;
        }

        self::$collectionCacheEpochs[$definition] = $epoch;
    }

    /**
     * The epoch a collection's documents may be cached under, as read with its definition; null when
     * they must not be.
     */
    private function getCollectionCacheEpoch(Document $definition): ?string
    {
        $epochs = self::$collectionCacheEpochs;

        return $epochs !== null && isset($epochs[$definition]) ? $epochs[$definition] : null;
    }

    private function isTtlExpired(Document $collection, Document $document): bool
    {
        if (! $this->adapter->supports(Capability::TTLIndexes)) {
            return false;
        }
        /** @var array<IndexModel> $indexes */
        $indexes = $collection->getAttribute('indexes', []);
        foreach ($indexes as $index) {
            if ($index->getType() !== IndexType::Ttl) {
                continue;
            }
            $ttlSeconds = $index->getTtl();
            $ttlAttr = $index->getIndexedAttributes()[0] ?? null;
            if ($ttlSeconds <= 0 || ! $ttlAttr) {
                return false;
            }
            /** @var string $ttlAttrStr */
            $ttlAttrStr = $ttlAttr;
            $val = $document->getAttribute($ttlAttrStr);
            if (is_string($val)) {
                try {
                    $start = new PhpDateTime($val);

                    return (new PhpDateTime()) > (clone $start)->modify("+{$ttlSeconds} seconds");
                } catch (Throwable) {
                    return false;
                }
            }
        }

        return false;
    }

    /**
     * Strip non-selected attributes from documents based on select queries.
     *
     * @param  array<Document>  $documents
     * @param  array<Query>  $selectQueries
     */
    public function applySelectFiltersToDocuments(array $documents, array $selectQueries): void
    {
        if (empty($selectQueries) || empty($documents)) {
            return;
        }

        // Collect all attributes to keep from select queries
        $attributesToKeep = [];
        foreach ($selectQueries as $selectQuery) {
            foreach ($selectQuery->getValues() as $value) {
                /** @var string $strValue */
                $strValue = $value;
                $attributesToKeep[$strValue] = true;
            }
        }

        // Early return if wildcard selector present
        if (isset($attributesToKeep['*'])) {
            return;
        }

        // Always preserve internal attributes (use hashmap for O(1) lookup)
        $internalKeys = \array_map(static fn (Attribute $attribute): string => $attribute->key, $this->internalAttributes());
        foreach ($internalKeys as $key) {
            /** @var string $key */
            $attributesToKeep[$key] = true;
        }

        foreach ($documents as $doc) {
            $allKeys = \array_keys($doc->getArrayCopy());
            foreach ($allKeys as $attrKey) {
                // Keep if: explicitly selected OR is internal attribute ($ prefix)
                if (! isset($attributesToKeep[$attrKey]) && ! \str_starts_with($attrKey, '$')) {
                    $doc->removeAttribute($attrKey);
                }
            }
        }
    }

    /**
     * Create Document
     *
     * @param  string  $collection  The collection identifier
     * @param  Document  $document  The document to create
     * @return Document The created document with generated ID and timestamps
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $this->assertCreateTenancy($collection);

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $document = $this->prepareDocument($collection, $document);

        /** @var array<int, array{Document, array<string, mixed>}> $copies */
        $copies = [];
        try {
            $document = $this->withMutation(Event::DocumentCreate, $document, function () use ($collection, $document, &$copies) {
                $hook = $this->relationshipHook;
                if ($hook?->isEnabled()) {
                    if ($copies !== [] && $copies !== null) {
                        $hook->restore($copies);
                    }
                    $document = $this->silent(function () use ($hook, $collection, $document, &$copies): Document {
                        return $hook->afterDocumentCreate($collection, $document, $copies);
                    });
                }

                $document = $this->adapter->createDocument($collection, $document);
                $this->withDocumentTenant(
                    $document,
                    fn () => $this->purgeCachedDocumentInternal($collection->getId(), $document->getId())
                );

                return $document;
            });
        } catch (Throwable $error) {
            if ($copies !== [] && $copies !== null) {
                $this->relationshipHook?->restore($copies);
            }

            throw $error;
        }

        $hook = $this->relationshipHook;
        if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled()) {
            $fetchDepth = $hook->getWriteStackCount();
            $documents = $this->silent(fn () => $hook->populateDocuments([$document], $collection, $fetchDepth));
            $document = $documents[0];
        }

        $document = $this->castingAfter($collection, $document);
        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document);

        if (isset($this->documentTypes[$collection->getId()])) {
            $document = $this->createDocumentInstance($collection->getId(), $document->getArrayCopy());
        }

        $document = $this->decorateDocument(Event::DocumentCreate, $collection, $document);

        $this->triggerHooks(Event::DocumentCreate, $document);

        return $document;
    }

    /**
     * Apply to a document everything createDocument() does before it writes the document: the tenancy and
     * permission checks, the generated attributes, encoding and validation. The relationship hook prepares
     * the related documents of a write this way and writes them through createPrepared().
     *
     * @internal
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function prepareCreate(Document $collection, Document $document): Document
    {
        $this->assertCreateTenancy($collection->getId());

        return $this->prepareDocument($collection, $document);
    }

    /**
     * Write documents prepared by prepareCreate() one at a time in the order given, each the way
     * createDocument() writes it, under one invalidation scope.
     *
     * @internal
     *
     * @param  list<array{Document, Document}>  $documents  Each prepared document after its collection
     *
     * @throws DuplicateException
     * @throws DatabaseException
     */
    public function createPrepared(array $documents): void
    {
        $this->withInvalidationScope(function () use ($documents): void {
            $collections = [];
            foreach ($documents as [$collection, $document]) {
                $collections[$collection->getId()][] = $document;
            }

            foreach ($collections as $created) {
                $this->blockMutation(Event::DocumentCreate, $created);
            }

            foreach ($documents as [$collection, $document]) {
                $document = $this->adapter->createDocument($collection, $document);
                $this->withDocumentTenant(
                    $document,
                    fn () => $this->purgeCachedDocumentInternal($collection->getId(), $document->getId())
                );
            }
        });
    }

    /**
     * @throws DatabaseException
     */
    private function assertCreateTenancy(string $collection): void
    {
        if (
            $collection !== self::METADATA
            && $this->adapter->getSharedTables()
            && ! $this->adapter->getTenantPerDocument()
            && empty($this->adapter->getTenant())
        ) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (
            ! $this->adapter->getSharedTables()
            && $this->adapter->getTenantPerDocument()
        ) {
            throw new DatabaseException('Shared tables must be enabled if tenant per document is enabled.');
        }
    }

    /**
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws StructureException
     */
    private function prepareDocument(Document $collection, Document $document): Document
    {
        if ($collection->getId() !== self::METADATA) {
            $isValid = $this->authorization->isValid(new Input(PermissionType::Create, $collection->getCreate()));
            if (! $isValid) {
                throw new AuthorizationException($this->authorization->getDescription());
            }
        }

        $time = DateTime::now();

        $createdAt = $document->getCreatedAt();
        $updatedAt = $document->getUpdatedAt();

        $id = $document->getId();
        $document
            ->setAttribute(Document::ID, empty($id) ? ID::unique() : $id)
            ->setAttribute(Document::COLLECTION, $collection->getId())
            ->setAttribute(Document::CREATED_AT, ($createdAt === null || ! $this->datePreservation()->get()) ? $time : $createdAt)
            ->setAttribute(Document::UPDATED_AT, ($updatedAt === null || ! $this->datePreservation()->get()) ? $time : $updatedAt);

        if (empty($document->getPermissions())) {
            $document->setAttribute(Document::PERMISSIONS, []);
        }

        if ($this->adapter->getSharedTables()) {
            if ($this->adapter->getTenantPerDocument()) {
                if (
                    $collection->getId() !== static::METADATA
                    && $document->getTenant() === null
                ) {
                    throw new DatabaseException('Missing tenant. Tenant must be set when tenant per document is enabled.');
                }
            } else {
                $document->setAttribute(Document::TENANT, $this->adapter->getTenant());
            }
        }

        $document = $this->encode($collection, $document);

        if ($this->validation()->get()) {
            $validator = new Permissions();
            if (! $validator->isValid($document->getPermissions())) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        if ($this->validation()->get()) {
            $structure = new Structure(
                collection: $collection,
                idAttributeType: $this->adapter->getIdAttributeType(),
                minAllowedDate: $this->adapter->getMinDateTime(),
                maxAllowedDate: $this->adapter->getMaxDateTime(),
                supportForAttributes: $this->adapter->supports(Capability::DefinedAttributes),
                supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt)
            );
            if (! $structure->isValid($document)) {
                throw new StructureException($structure->getDescription());
            }
        }

        return $this->castingBefore($collection, $document);
    }

    /**
     * Create Documents in a batch
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Document>  $documents  The documents to create
     * @param  int  $batchSize  Number of documents per batch insert
     * @param  (callable(Document): void)|null  $onNext  Callback given each created document once its batch is written
     * @param  (callable(Throwable): void)|null  $onError  Given an error $onNext throws; the write continues. Without it the
     *                                                    error is rethrown. Write errors are always thrown.
     * @return int The number of documents created
     *
     * @throws AuthorizationException
     * @throws StructureException
     * @throws Throwable
     * @throws Exception
     */
    public function createDocuments(
        string $collection,
        array $documents,
        int $batchSize = self::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        if (
            $this->adapter->getSharedTables()
            && ! $this->adapter->getTenantPerDocument()
            && empty($this->adapter->getTenant())
        ) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (! $this->adapter->getSharedTables() && $this->adapter->getTenantPerDocument()) {
            throw new DatabaseException('Shared tables must be enabled if tenant per document is enabled.');
        }

        if (empty($documents)) {
            return 0;
        }

        $batchSize = \min(Database::INSERT_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($collection->getId() !== self::METADATA) {
            if (! $this->authorization->isValid(new Input(PermissionType::Create, $collection->getCreate()))) {
                throw new AuthorizationException($this->authorization->getDescription());
            }
        }

        $time = DateTime::now();
        $modified = 0;
        $hasRelationships = ! empty(\array_filter(
            $collection->getDeclaredAttributes(),
            static fn (Attribute $attribute): bool => $attribute->getType() === ColumnType::Relationship,
        ));

        // Hoisted: validator only depends on the collection + adapter properties,
        // both stable for this call. Allocating once and reusing across all
        // documents avoids per-document construction and (with the in-class
        // memo) per-document `array_merge` of the attribute list.
        $validator = $this->validation()->get()
            ? new Structure(
                collection: $collection,
                idAttributeType: $this->adapter->getIdAttributeType(),
                minAllowedDate: $this->adapter->getMinDateTime(),
                maxAllowedDate: $this->adapter->getMaxDateTime(),
                supportForAttributes: $this->adapter->supports(Capability::DefinedAttributes),
                supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt)
            )
            : null;

        foreach ($documents as $document) {
            $createdAt = $document->getCreatedAt();
            $updatedAt = $document->getUpdatedAt();

            $document
                ->setAttribute(Document::ID, empty($document->getId()) ? ID::unique() : $document->getId())
                ->setAttribute(Document::COLLECTION, $collection->getId())
                ->setAttribute(Document::CREATED_AT, ($createdAt === null || ! $this->datePreservation()->get()) ? $time : $createdAt)
                ->setAttribute(Document::UPDATED_AT, ($updatedAt === null || ! $this->datePreservation()->get()) ? $time : $updatedAt);

            if (empty($document->getPermissions())) {
                $document->setAttribute(Document::PERMISSIONS, []);
            }

            if ($this->adapter->getSharedTables()) {
                if ($this->adapter->getTenantPerDocument()) {
                    if ($document->getTenant() === null) {
                        throw new DatabaseException('Missing tenant. Tenant must be set when tenant per document is enabled.');
                    }
                } else {
                    $document->setAttribute(Document::TENANT, $this->adapter->getTenant());
                }
            }

            $document = $this->encode($collection, $document);

            if ($validator !== null) {
                if (! $validator->isValid($document)) {
                    throw new StructureException($validator->getDescription());
                }
            }

            if ($this->relationshipHook?->isEnabled()) {
                $document = $this->silent(fn () => $this->relationshipHook->afterDocumentCreate($collection, $document));
            }

            $document = $this->castingBefore($collection, $document);
        }

        foreach (\array_chunk($documents, $batchSize) as $chunk) {
            $insert = fn () => $this->withMutation(
                Event::DocumentsCreate,
                $chunk,
                function () use ($collection, $chunk): array {
                    $batch = $this->adapter->createDocuments($collection, $chunk);

                    foreach ($chunk as $document) {
                        $this->withDocumentTenant(
                            $document,
                            fn () => $this->advanceCollectionCacheEpoch($collection->getId(), $document->getId())
                        );
                    }

                    return $batch;
                }
            );
            $batch = $this->duplicateSkipping()->get()
                ? $this->adapter->skipDuplicates($insert)
                : $insert();

            if ($onNext !== null || $hasRelationships) {
                $batch = $this->adapter->getSequences($collection->getId(), $batch);
            }

            $hook = $this->relationshipHook;
            if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled()) {
                $batch = $this->silent(fn () => $hook->populateDocuments($batch, $collection, $hook->getFetchDepth()));
            }

            /** @var array<Document> $batch */
            $batch = \array_map(
                fn (Document $document) => $this->decode($collection, $this->casting($collection, $document)),
                $this->castingAfterDocuments($collection, $batch)
            );

            $batch = $this->decorateDocuments(Event::DocumentsCreate, $collection, $batch);

            foreach ($batch as $document) {
                try {
                    $onNext && $onNext($document);
                } catch (Throwable $e) {
                    $onError ? $onError($e) : throw $e;
                }

                $modified++;
            }
        }

        $this->triggerHooks(Event::DocumentsCreate, new Document([
            Document::COLLECTION => $collection->getId(),
            'modified' => $modified,
        ]));

        return $modified;
    }

    /**
     * Update Document
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The document identifier
     * @param  Document  $document  The document with updated fields
     * @return Document The updated document
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        if (! $id) {
            throw new DatabaseException('Must define $id attribute');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $newUpdatedAt = $document->getUpdatedAt();
        $hasOperators = false;
        $cacheTarget = $collection->getId() === self::METADATA
            ? new Document([Document::ID => $id, Document::COLLECTION => self::METADATA])
            : $collection->getId();
        $document = $this->withMutation(Event::DocumentUpdate, $cacheTarget, function () use ($collection, $id, $document, $newUpdatedAt, &$hasOperators) {
            $old = $this->authorization->skip(fn () => $this->silent(
                fn () => $this->getDocument($collection->getId(), $id, forUpdate: true)
            ));
            if ($old->isEmpty()) {
                return new Document();
            }
            $time = DateTime::nowAfter($old->getUpdatedAt() ?: null);

            $skipPermissionsUpdate = true;

            if ($document->offsetExists(Document::PERMISSIONS)) {
                $originalPermissions = $old->getPermissions();
                $currentPermissions = $document->getPermissions();

                sort($originalPermissions);
                sort($currentPermissions);

                $skipPermissionsUpdate = ($originalPermissions === $currentPermissions);
            }
            $createdAt = $document->getCreatedAt();

            $document = \array_merge($old->getArrayCopy(), $document->getArrayCopy());
            $document[Document::COLLECTION] = $old->getAttribute(Document::COLLECTION); // Make sure user doesn't switch collection ID
            $document[Document::SEQUENCE] = $old->getSequence(); // Sequence is immutable, and adapters key the UPDATE on it
            if ($document[Document::ID] !== $old->getId()) {
                $skipPermissionsUpdate = false;
            }
            $document[Document::CREATED_AT] = ($createdAt === null || ! $this->datePreservation()->get()) ? $old->getCreatedAt() : $createdAt;

            if ($this->adapter->getSharedTables()) {
                $document[Document::TENANT] = $old->getTenant(); // Make sure user doesn't switch tenant
            }
            $document = new Document($document);

            // Ahead of change detection: a dropped attribute is never persisted, so
            // counting it as a change would bump $updatedAt and fire an update event
            // for a write that leaves the stored document identical.
            $document = $this->removeUnknownAttributes($collection, $document);

            /** @var array<Document> $updateAttrs */
            $updateAttrs = $collection->getAttribute('attributes', []);
            $relationships = \array_filter($updateAttrs, function (Attribute|Document $attribute) {
                return Attribute::isRelationship($attribute);
            });

            $shouldUpdate = false;

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                foreach ($relationships as $relationship) {
                    $relationships[$relationship->getId()] = $relationship;
                }

                foreach ($document as $key => $value) {
                    if (Operator::isOperator($value)) {
                        $shouldUpdate = true;
                        break;
                    }
                }

                $internalKeys = [Document::INTERNAL_ID, Document::COLLECTION, Document::TENANT, Document::SEQUENCE];

                // Compare if the document has any changes
                foreach ($document as $key => $value) {
                    if (\in_array($key, $internalKeys, true)) {
                        continue;
                    }

                    if (\array_key_exists($key, $relationships)) {
                        $rel = Relationship::fromArray(['collection' => $collection->getId()] + $relationships[$key]->getArrayCopy());
                        $relationType = $rel->getType();
                        $side = $rel->getSide();
                        $storesKey = $relationType === RelationType::OneToOne
                            || ($relationType === RelationType::ManyToOne && $side === RelationSide::Parent)
                            || ($relationType === RelationType::OneToMany && $side === RelationSide::Child);

                        if (! $storesKey && $this->relationshipHook !== null && $this->relationshipHook->getWriteStackCount() >= Database::RELATION_MAX_DEPTH - 1) {
                            continue;
                        }

                        switch ($relationType) {
                            case RelationType::OneToOne:
                                $oldValue = $old->getAttribute($key) instanceof Document
                                    ? $old->getAttribute($key)->getId()
                                    : $old->getAttribute($key);

                                if ((\is_null($value) !== \is_null($oldValue))
                                    || (\is_string($value) && $value !== $oldValue)
                                    || ($value instanceof Document && $value->getId() !== $oldValue)
                                ) {
                                    $shouldUpdate = true;
                                }
                                break;
                            case RelationType::OneToMany:
                            case RelationType::ManyToOne:
                            case RelationType::ManyToMany:
                                if (
                                    ($relationType === RelationType::ManyToOne && $side === RelationSide::Parent) ||
                                    ($relationType === RelationType::OneToMany && $side === RelationSide::Child)
                                ) {
                                    $oldValue = $old->getAttribute($key) instanceof Document
                                        ? $old->getAttribute($key)->getId()
                                        : $old->getAttribute($key);

                                    if ((\is_null($value) !== \is_null($oldValue))
                                        || (\is_string($value) && $value !== $oldValue)
                                        || ($value instanceof Document && $value->getId() !== $oldValue)
                                    ) {
                                        $shouldUpdate = true;
                                    }
                                    break;
                                }

                                if (Operator::isOperator($value)) {
                                    $shouldUpdate = true;
                                    break;
                                }

                                if (! \is_array($value) || ! \array_is_list($value)) {
                                    throw new RelationshipException('Invalid relationship value. Must be either an array of documents or document IDs, '.\gettype($value).' given.');
                                }

                                /** @var array<mixed> $oldRelValues */
                                $oldRelValues = $old->getAttribute($key);
                                if (\count($oldRelValues) !== \count($value)) {
                                    $shouldUpdate = true;
                                    break;
                                }

                                foreach ($value as $index => $relation) {
                                    $oldValue = $oldRelValues[$index] instanceof Document
                                        ? $oldRelValues[$index]->getId()
                                        : $oldRelValues[$index];

                                    if (
                                        (\is_string($relation) && $relation !== $oldValue) ||
                                        ($relation instanceof Document && $relation->getId() !== $oldValue)
                                    ) {
                                        $shouldUpdate = true;
                                        break;
                                    }
                                }
                                break;
                        }

                        if ($shouldUpdate) {
                            break;
                        }

                        continue;
                    }

                    $oldValue = $old->getAttribute($key);

                    // If values are not equal we need to update document.
                    if (! self::valuesEqual($value, $oldValue)) {
                        $shouldUpdate = true;
                        break;
                    }
                }

                $updatePermissions = [
                    ...$collection->getUpdate(),
                    ...($documentSecurity ? $old->getUpdate() : []),
                ];

                $readPermissions = [
                    ...$collection->getRead(),
                    ...($documentSecurity ? $old->getRead() : []),
                ];

                if ($shouldUpdate) {
                    if (! $this->authorization->isValid(new Input(PermissionType::Update, $updatePermissions))) {
                        throw new AuthorizationException($this->authorization->getDescription());
                    }
                } else {
                    if (! $this->authorization->isValid(new Input(PermissionType::Read, $readPermissions))) {
                        throw new AuthorizationException($this->authorization->getDescription());
                    }
                }
            }

            if ($shouldUpdate) {
                $document->setAttribute(Document::UPDATED_AT, ($newUpdatedAt === null || ! $this->datePreservation()->get()) ? $time : $newUpdatedAt);
            }

            // Check if document was updated after the request timestamp
            $oldUpdatedAt = new PhpDateTime($old->getUpdatedAt() ?? 'now');
            $requestTimestamp = $this->requestTimestamp()->get();
            if ($requestTimestamp !== null && $oldUpdatedAt > $requestTimestamp) {
                throw new ConflictException('Document was updated after the request timestamp');
            }

            $storedAttributes = [];
            if ($this->validation()->get() && $collection->getId() !== self::METADATA) {
                foreach ($document as $key => $value) {
                    if ($old->offsetExists($key) && self::valuesEqual($value, $old->getAttribute($key))) {
                        $storedAttributes[] = $key;
                    }
                }
            }

            $document = $this->encode($collection, $document);

            if ($this->validation()->get()) {
                $structureValidator = new Structure(
                    collection: $collection,
                    idAttributeType: $this->adapter->getIdAttributeType(),
                    minAllowedDate: $this->adapter->getMinDateTime(),
                    maxAllowedDate: $this->adapter->getMaxDateTime(),
                    supportForAttributes: $this->adapter->supports(Capability::DefinedAttributes),
                    supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt),
                    currentDocument: $old,
                    storedAttributes: $storedAttributes,
                );
                if (! $structureValidator->isValid($document)) { // Make sure updated structure still apply collection rules (if any)
                    throw new StructureException($structureValidator->getDescription());
                }
            }

            if ($this->relationshipHook?->isEnabled()) {
                $document = $this->silent(fn () => $this->relationshipHook->afterDocumentUpdate($collection, $old, $document));
            }

            foreach ($document->getArrayCopy() as $value) {
                if (Operator::isOperator($value)) {
                    $hasOperators = true;
                    break;
                }
            }

            $document = $this->castingBefore($collection, $document);

            $this->authorization->skip(fn () => $this->adapter->updateDocument($collection, $old->getId(), $document, $skipPermissionsUpdate));

            $document = $this->castingAfter($collection, $document);

            $purgedIds = \array_values(\array_unique([$id, $old->getId(), $document->getId()]));

            foreach ($purgedIds as $purgedId) {
                $this->purgeCachedDocumentInternal($collection->getId(), $purgedId);
            }

            foreach ($purgedIds as $purgedId) {
                $this->queueDocumentPurge($collection->getId(), $purgedId);
            }

            if ($hasOperators) {
                $refetched = $this->refetchDocuments($collection, [$document]);
                $document = $refetched[0];
            }

            return $document;
        });

        if ($document->isEmpty()) {
            return $document;
        }

        $hook = $this->relationshipHook;
        if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled()) {
            $documents = $this->silent(fn () => $hook->populateDocuments([$document], $collection, $hook->getFetchDepth()));
            $document = $documents[0];
        }

        if (! $hasOperators) {
            $document = $this->decode($collection, $document);
        }

        // Convert to custom document type if mapped
        if (isset($this->documentTypes[$collection->getId()])) {
            $document = $this->createDocumentInstance($collection->getId(), $document->getArrayCopy());
        }

        $document = $this->decorateDocument(Event::DocumentUpdate, $collection, $document);

        $this->triggerHooks(Event::DocumentUpdate, $document);

        return $document;
    }

    /**
     * Update documents
     *
     * Updates all documents which match the given queries.
     *
     * @param  string  $collection  The collection identifier
     * @param  Document  $updates  The document containing fields to update
     * @param  array<Query>  $queries  Queries to filter documents for update
     * @param  int  $batchSize  Number of documents per batch update
     * @param  (callable(Document $updated, Document $old): void)|null  $onNext  Callback given each updated document once its batch is written, with a copy of the document as it was read before the update
     * @param  (callable(Throwable): void)|null  $onError  Given an error $onNext throws; the write continues. Without it the
     *                                                    error is rethrown. Write errors are always thrown.
     * @return int The number of documents updated
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DuplicateException
     * @throws QueryException
     * @throws StructureException
     * @throws TimeoutException
     * @throws Throwable
     * @throws Exception
     */
    public function updateDocuments(
        string $collection,
        Document $updates,
        array $queries = [],
        int $batchSize = self::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        $this->rejectJoins($queries, 'Join queries are not supported for bulk updates');

        if ($updates->isEmpty()) {
            return 0;
        }

        $batchSize = \min(Database::INSERT_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Update, $collection->getUpdate()));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validation()->get()) {
            $validator = $this->getQueriesValidator($collection, $queries);

            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $grouped = Query::groupForDatabase($queries);
        $limit = $grouped['limit'];
        $cursor = $grouped['cursor'];

        if (! empty($cursor) && $cursor->getCollection() !== $collection->getId()) {
            throw new DatabaseException('Cursor document must be from the same Collection.');
        }

        unset($updates[Document::ID]);
        unset($updates[Document::TENANT]);

        if (($updates->getCreatedAt() === null || ! $this->datePreservation()->get())) {
            unset($updates[Document::CREATED_AT]);
        } else {
            $updates[Document::CREATED_AT] = $updates->getCreatedAt();
        }

        if ($this->adapter->getSharedTables()) {
            $updates[Document::TENANT] = $this->adapter->getTenant();
        }

        $updatedAt = $updates->getUpdatedAt();
        $updates[Document::UPDATED_AT] = ($updatedAt === null || ! $this->datePreservation()->get()) ? DateTime::now() : $updatedAt;

        $decodedUpdates = clone $updates;
        $updates = $this->encode(
            $collection,
            $updates,
            applyDefaults: false
        );

        if ($this->validation()->get()) {
            $validator = new PartialStructure(
                collection: $collection,
                idAttributeType: $this->adapter->getIdAttributeType(),
                minAllowedDate: $this->adapter->getMinDateTime(),
                maxAllowedDate: $this->adapter->getMaxDateTime(),
                supportForAttributes: $this->adapter->supports(Capability::DefinedAttributes),
                supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt),
                currentDocument: null
            );

            if (! $validator->isValid($updates)) {
                throw new StructureException($validator->getDescription());
            }
        }

        $hasOperators = false;
        $adapterData = [];
        foreach ($updates->getArrayCopy() as $key => $value) {
            if ($value instanceof Operator) {
                $hasOperators = true;
                $value = clone $value;
            }
            $adapterData[$key] = $value;
        }
        $selections = $this->validateSelections($collection, $grouped['selections']);
        $decodedKeys = $selections === []
            ? []
            : \array_values(\array_unique([...$selections, ...\array_map(\strval(...), \array_keys($adapterData))]));
        $adapterUpdates = $this->castingBefore($collection, new Document($adapterData));

        $originalLimit = $limit;
        $last = $cursor;
        $modified = 0;

        while (true) {
            if ($limit && $limit < $batchSize) {
                $batchSize = $limit;
            } elseif (! empty($limit)) {
                $limit -= $batchSize;
            }

            $new = [
                Query::limit($batchSize),
            ];

            if (! empty($last)) {
                $new[] = Query::cursorAfter($last);
            }

            $batch = $this->silent(fn () => $this->find(
                $collection->getId(),
                array_merge($new, $queries),
                forPermission: PermissionType::Update
            ));

            if (empty($batch)) {
                break;
            }

            $old = array_map(fn ($doc) => clone $doc, $batch);
            $currentPermissions = $updates->getPermissions();
            sort($currentPermissions);

            $cacheTarget = $collection->getId() === self::METADATA ? $batch : $collection->getId();
            $found = $batch;
            $this->withMutation(Event::DocumentsUpdate, $cacheTarget, function () use ($collection, $updates, $decodedUpdates, $adapterUpdates, &$batch, $found, $currentPermissions) {
                foreach ($found as $index => $document) {
                    $skipPermissionsUpdate = true;

                    if ($updates->offsetExists(Document::PERMISSIONS)) {
                        if (! $document->offsetExists(Document::PERMISSIONS)) {
                            throw new QueryException('Permission document missing in select');
                        }

                        $originalPermissions = $document->getPermissions();

                        \sort($originalPermissions);

                        $skipPermissionsUpdate = ($originalPermissions === $currentPermissions);
                    }

                    $document->setAttribute(Document::SKIP_PERMISSIONS_UPDATE, $skipPermissionsUpdate);

                    $updateData = [];
                    foreach ($decodedUpdates->getArrayCopy() as $key => $value) {
                        $updateData[$key] = $value instanceof Operator ? clone $value : $value;
                    }
                    $new = new Document(\array_merge($document->getArrayCopy(), $updateData));

                    $hook = $this->relationshipHook;
                    if ($hook?->isEnabled()) {
                        $this->silent(fn () => $hook->afterDocumentUpdate($collection, $document, $new));
                    }

                    try {
                        $oldUpdatedAt = new PhpDateTime($document->getUpdatedAt() ?? 'now');
                    } catch (Exception $e) {
                        throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                    }

                    $requestTimestamp = $this->requestTimestamp()->get();
                    if ($requestTimestamp !== null && $oldUpdatedAt > $requestTimestamp) {
                        throw new ConflictException('Document was updated after the request timestamp');
                    }

                    $document = $new;

                    $encoded = $this->encode($collection, $document);
                    $batch[$index] = $this->castingBefore($collection, $encoded);
                }

                $this->adapter->updateDocuments(
                    $collection,
                    $adapterUpdates,
                    $batch
                );

                foreach ($batch as $document) {
                    $this->withDocumentTenant(
                        $document,
                        fn () => $this->advanceCollectionCacheEpoch($collection->getId(), $document->getId())
                    );
                }

                $this->queueDocumentPurges($collection->getId(), $batch);
            });

            if ($hasOperators) {
                $batch = $this->refetchDocuments($collection, $batch, $grouped['selections']);
            }

            // The operator refetch goes through find(), which already decoded every document;
            // decoding again would run each decode filter twice.
            /** @var array<Document> $batch */
            $batch = $this->castingAfterDocuments($collection, $batch);
            if (! $hasOperators) {
                $batch = \array_map(
                    fn (Document $doc) => $this->decode($collection, $doc, $decodedKeys),
                    $batch
                );
            }

            $batch = $this->decorateDocuments(Event::DocumentsUpdate, $collection, $batch);

            foreach ($batch as $index => $doc) {
                $doc->removeAttribute(Document::SKIP_PERMISSIONS_UPDATE);
                try {
                    $onNext && $onNext($doc, $old[$index]);
                } catch (Throwable $th) {
                    $onError ? $onError($th) : throw $th;
                }
                $modified++;
            }

            if (count($batch) < $batchSize) {
                break;
            } elseif ($originalLimit && $modified == $originalLimit) {
                break;
            }

            /** @var Document|false $last */
            $last = \end($batch);
        }

        $this->triggerHooks(Event::DocumentsUpdate, new Document([
            Document::COLLECTION => $collection->getId(),
            'modified' => $modified,
        ]));

        return $modified;
    }

    /**
     * Create or update a single document.
     *
     * @param  string  $collection  The collection identifier
     * @param  Document  $document  The document to create or update
     * @return Document The created or updated document
     *
     * @throws StructureException
     * @throws Throwable
     */
    public function upsertDocument(
        string $collection,
        Document $document,
    ): Document {
        $result = null;

        $this->upsertDocumentsWithIncrease(
            $collection,
            '',
            [$document],
            function (Document $doc, ?Document $_old = null) use (&$result) {
                $result = $doc;
            }
        );

        if ($result === null) {
            // No-op (unchanged): return the current persisted doc
            $result = $this->getDocument($collection, $document->getId());
        }

        return $result;
    }

    /**
     * Create or update documents.
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Document>  $documents  The documents to create or update
     * @param  int  $batchSize  Number of documents per batch
     * @param  (callable(Document $upserted, ?Document $old): void)|null  $onNext  Callback given each upserted document once its batch is written, with the stored document it updated, or null when it was created
     * @param  (callable(Throwable): void)|null  $onError  Given an error $onNext throws; the write continues. Without it the
     *                                                    error is rethrown. Write errors are always thrown.
     * @return int The number of documents created or updated
     *
     * @throws StructureException
     * @throws Throwable
     */
    public function upsertDocuments(
        string $collection,
        array $documents,
        int $batchSize = self::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null
    ): int {
        return $this->upsertDocumentsWithIncrease(
            $collection,
            '',
            $documents,
            $onNext,
            $onError,
            $batchSize
        );
    }

    /**
     * Create or update documents, increasing the value of the given attribute by the value in each document.
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $attribute  The attribute to increment on update
     * @param  array<Document>  $documents  The documents to create or update
     * @param  (callable(Document $upserted, ?Document $old): void)|null  $onNext  Callback given each upserted document once its batch is written, with the stored document it updated, or null when it was created
     * @param  (callable(Throwable): void)|null  $onError  Given an error $onNext throws; the write continues. Without it the
     *                                                    error is rethrown. Write errors are always thrown.
     * @param  int  $batchSize  Number of documents per batch
     * @return int The number of documents created or updated
     *
     * @throws StructureException
     * @throws Throwable
     * @throws Exception
     */
    public function upsertDocumentsWithIncrease(
        string $collection,
        string $attribute,
        array $documents,
        ?callable $onNext = null,
        ?callable $onError = null,
        int $batchSize = self::INSERT_BATCH_SIZE
    ): int {
        if (! $this->adapterHasFeature(Feature\Upserts::class)) {
            throw new DatabaseException('Adapter does not support upserts');
        }

        if (
            $this->adapter->getSharedTables()
            && ! $this->adapter->getTenantPerDocument()
            && empty($this->adapter->getTenant())
        ) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (! $this->adapter->getSharedTables() && $this->adapter->getTenantPerDocument()) {
            throw new DatabaseException('Shared tables must be enabled if tenant per document is enabled.');
        }

        if (empty($documents)) {
            return 0;
        }

        $batchSize = \min(Database::INSERT_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        /** @var array<Document> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $time = DateTime::now();
        $created = 0;
        $updated = 0;
        $operatorIds = [];
        $seenIds = [];
        $hasRelationships = ! empty(\array_filter(
            $collection->getDeclaredAttributes(),
            static fn (Attribute $attribute): bool => $attribute->getType() === ColumnType::Relationship,
        ));
        $existing = $this->findDocumentsToUpsert($collection->getId(), $documents);

        foreach ($documents as $key => $document) {
            $old = $existing[$this->upsertKey($document)] ?? new Document();

            $document = $this->removeUnknownAttributes($collection, $document);

            // Extract operators early to avoid comparison issues
            $documentArray = $document->getArrayCopy();
            $extracted = Operator::extractOperators($documentArray);
            $operators = $extracted['operators'];
            $regularUpdates = $extracted['updates'];

            $internalKeys = \array_map(
                fn (Attribute $attr) => $attr->getKey(),
                self::internalAttributesFor(true)
            );

            $regularUpdatesUserOnly = \array_diff_key($regularUpdates, \array_flip($internalKeys));

            $skipPermissionsUpdate = true;

            if ($document->offsetExists(Document::PERMISSIONS)) {
                $originalPermissions = $old->getPermissions();
                $currentPermissions = $document->getPermissions();

                sort($originalPermissions);
                sort($currentPermissions);

                $skipPermissionsUpdate = ($originalPermissions === $currentPermissions);
            }

            // Only skip if no operators and regular attributes haven't changed
            $hasChanges = false;
            if (! empty($operators)) {
                $hasChanges = true;
            } elseif (! empty($attribute)) {
                $hasChanges = true;
            } elseif (! $skipPermissionsUpdate) {
                $hasChanges = true;
            } else {
                // Check if any of the provided attributes differ from old document
                $oldAttributes = $old->getAttributes();
                foreach ($regularUpdatesUserOnly as $attrKey => $value) {
                    $oldValue = $oldAttributes[$attrKey] ?? null;
                    if ($oldValue != $value) {
                        $hasChanges = true;
                        break;
                    }
                }

                // Also check if old document has attributes that new document doesn't
                if (! $hasChanges) {
                    $internalKeys = \array_map(
                        fn (Attribute $attr) => $attr->getKey(),
                        self::internalAttributesFor(true)
                    );

                    $oldUserAttributes = array_diff_key($oldAttributes, array_flip($internalKeys));

                    foreach (array_keys($oldUserAttributes) as $oldAttrKey) {
                        if (! array_key_exists($oldAttrKey, $regularUpdatesUserOnly)) {
                            // Old document has an attribute that new document doesn't
                            $hasChanges = true;
                            break;
                        }
                    }
                }
            }

            if (! $hasChanges) {
                // If not updating a single attribute and the document is the same as the old one, skip it
                unset($documents[$key]);

                continue;
            }

            // If old is empty, check if user has create permission on the collection
            // If old is not empty, check if user has update permission on the collection
            // If old is not empty AND documentSecurity is enabled, check if user has update permission on the collection or document

            if ($old->isEmpty()) {
                if (! $this->authorization->isValid(new Input(PermissionType::Create, $collection->getCreate()))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            } elseif (! $this->authorization->isValid(new Input(PermissionType::Update, \array_merge(
                $collection->getUpdate(),
                ((bool) $documentSecurity ? $old->getUpdate() : [])
            )))) {
                throw new AuthorizationException($this->authorization->getDescription());
            }

            $updatedAt = $document->getUpdatedAt();

            $document
                ->setAttribute(Document::ID, empty($document->getId()) ? ID::unique() : $document->getId())
                ->setAttribute(Document::COLLECTION, $collection->getId())
                ->setAttribute(Document::UPDATED_AT, ($updatedAt === null || ! $this->datePreservation()->get()) ? $time : $updatedAt);

            if (! $this->sequencePreservation()->get()) {
                $document->removeAttribute(Document::SEQUENCE);
            }

            $createdAt = $document->getCreatedAt();
            if ($createdAt === null || ! $this->datePreservation()->get()) {
                $document->setAttribute(Document::CREATED_AT, $old->isEmpty() ? $time : $old->getCreatedAt());
            } else {
                $document->setAttribute(Document::CREATED_AT, $createdAt);
            }

            // Force matching optional parameter sets
            // Doesn't use decode as that intentionally skips null defaults to reduce payload size
            foreach ($collectionAttributes as $attr) {
                /** @var string $attrId */
                $attrId = $attr[Document::ID];
                if (! $attr->getAttribute('required') && ! \array_key_exists($attrId, (array) $document)) {
                    $document->setAttribute(
                        $attrId,
                        $old->getAttribute($attrId, ($attr['default'] ?? null))
                    );
                }
            }

            if ($skipPermissionsUpdate) {
                $document->setAttribute(Document::PERMISSIONS, $old->getPermissions());
            }

            if ($this->adapter->getSharedTables()) {
                if ($this->adapter->getTenantPerDocument()) {
                    if ($document->getTenant() === null) {
                        throw new DatabaseException('Missing tenant. Tenant must be set when tenant per document is enabled.');
                    }
                    if (! $old->isEmpty() && $old->getTenant() !== $document->getTenant()) {
                        throw new DatabaseException('Tenant cannot be changed.');
                    }
                } else {
                    $document->setAttribute(Document::TENANT, $this->adapter->getTenant());
                }
            }

            $document = $this->encode($collection, $document);

            if ($this->validation()->get()) {
                $validator = new Structure(
                    collection: $collection,
                    idAttributeType: $this->adapter->getIdAttributeType(),
                    minAllowedDate: $this->adapter->getMinDateTime(),
                    maxAllowedDate: $this->adapter->getMaxDateTime(),
                    supportForAttributes: $this->adapter->supports(Capability::DefinedAttributes),
                    supportUnsignedBigInt: $this->adapter->supports(Capability::UnsignedBigInt),
                    currentDocument: $old->isEmpty() ? null : $old
                );

                if (! $validator->isValid($document)) {
                    throw new StructureException($validator->getDescription());
                }
            }

            if (! $old->isEmpty()) {
                // Check if document was updated after the request timestamp
                try {
                    $oldUpdatedAt = new PhpDateTime($old->getUpdatedAt() ?? 'now');
                } catch (Exception $e) {
                    throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                }

                $requestTimestamp = $this->requestTimestamp()->get();
                if ($requestTimestamp !== null && $oldUpdatedAt > $requestTimestamp) {
                    throw new ConflictException('Document was updated after the request timestamp');
                }
            }

            $hook = $this->relationshipHook;
            if ($hook?->isEnabled()) {
                $document = $this->silent(fn () => $hook->afterDocumentCreate($collection, $document));
            }

            $identity = $this->getDocumentIdentity($document);
            $seenIds[] = $identity;
            if (! empty($operators)) {
                $operatorIds[$identity] = true;
            }
            $old = $this->castingBefore($collection, $old);
            $document = $this->castingBefore($collection, $document);

            $documents[$key] = new Change(
                old: $old,
                new: $document
            );
        }

        // Required because *some* DBs will allow duplicate IDs for upsert
        if (\count($seenIds) !== \count(\array_unique($seenIds))) {
            throw new DuplicateException('Duplicate document IDs found in the input array.');
        }

        foreach (\array_chunk($documents, $batchSize) as $chunk) {
            /**
             * @var array<Change> $chunk
             */
            $hasOperators = false;
            foreach ($chunk as $change) {
                if (isset($operatorIds[$this->getDocumentIdentity($change->getNew())])) {
                    $hasOperators = true;
                    break;
                }
            }

            $batch = $this->withMutation(
                Event::DocumentsUpsert,
                \array_map(static fn (Change $change): Document => $change->getNew(), $chunk),
                function () use ($collection, $attribute, $chunk): array {
                    if (! $this->adapterHasFeature(Feature\Upserts::class)) {
                        throw new DatabaseException('Adapter does not support upserts');
                    }

                    $adapter = $this->adapter;
                    $batch = $this->authorization->skip(fn () => $adapter->upsertDocuments(
                        $collection,
                        $attribute,
                        $chunk
                    ));

                    foreach ($batch as $document) {
                        $this->withDocumentTenant(
                            $document,
                            fn () => $this->advanceCollectionCacheEpoch($collection->getId(), $document->getId())
                        );
                    }

                    $this->queueDocumentPurges($collection->getId(), $batch);

                    return $batch;
                }
            );

            foreach ($batch as $index => $document) {
                if (empty($document->getSequence()) && ! empty($chunk[$index]->getOld()->getSequence())) {
                    $document->setAttribute(Document::SEQUENCE, $chunk[$index]->getOld()->getSequence());
                }
            }

            if ($onNext !== null || $hasRelationships) {
                $batch = $this->adapter->getSequences($collection->getId(), $batch);
            }

            foreach ($chunk as $change) {
                if ($change->getOld()->isEmpty()) {
                    $created++;
                } else {
                    $updated++;
                }
            }

            $hook = $this->relationshipHook;
            if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled()) {
                $batch = $this->silent(fn () => $hook->populateDocuments($batch, $collection, $hook->getFetchDepth()));
            }

            if ($hasOperators && $onNext !== null) {
                $batch = $this->refetchDocuments($collection, $batch);
            }

            /** @var array<Document> $batch */
            $batch = $this->castingAfterDocuments($collection, $batch);
            if (! $hasOperators) {
                $batch = \array_map(
                    fn (Document $doc) => $this->decode($collection, $doc),
                    $batch
                );
            }

            $batch = $this->decorateDocuments(Event::DocumentsUpsert, $collection, $batch);

            foreach ($batch as $index => $doc) {
                $old = $chunk[$index]->getOld();

                if (! $old->isEmpty()) {
                    $old = $this->castingAfter($collection, $old);
                }

                try {
                    $onNext && $onNext($doc, $old->isEmpty() ? null : $old);
                } catch (Throwable $th) {
                    $onError ? $onError($th) : throw $th;
                }
            }
        }

        $this->triggerHooks(Event::DocumentsUpsert, new Document([
            Document::COLLECTION => $collection->getId(),
            'created' => $created,
            'updated' => $updated,
        ]));

        return $created + $updated;
    }

    /**
     * Load the stored documents an upsert batch will be compared against, in one
     * read per tenant instead of one per document. A batch of N documents costs a
     * bounded number of round trips; getDocument() per document cost 2N, which
     * doubled the stats-resources sweep and is what this replaces.
     *
     * @param  array<Document>  $documents
     * @return array<string, Document>
     *
     * @throws Throwable
     */
    private function findDocumentsToUpsert(string $collection, array $documents): array
    {
        $perTenant = $this->getSharedTables() && $this->getTenantPerDocument();

        $batches = [];
        foreach ($documents as $document) {
            if ($document->getId() === '') {
                continue;
            }

            $tenant = $perTenant ? $document->getTenant() : null;
            $key = $tenant === null ? '' : (string) $tenant;

            if (! isset($batches[$key])) {
                $batches[$key] = ['tenant' => $tenant, 'ids' => []];
            }

            $batches[$key]['ids'][] = $document->getId();
        }

        $existing = [];
        foreach ($batches as $batch) {
            foreach (\array_chunk(\array_values(\array_unique($batch['ids'])), \max(1, $this->maxQueryValues)) as $chunk) {
                $read = fn (): array => $this->authorization->skip(fn () => $this->silent(fn () => $this->find($collection, [
                    Query::equal(Document::ID, $chunk),
                    Query::limit($this->maxQueryValues),
                ], forPermission: PermissionType::Update)));

                $found = $perTenant
                    ? $this->withTenant($batch['tenant'], $read)
                    : $read();

                foreach ($found as $document) {
                    $existing[$this->upsertKey($document)] = $document;
                }
            }
        }

        return $existing;
    }

    /**
     * Identity of a document within one upsert batch. Two tenants may hold the
     * same document id, so the tenant is part of the key whenever a batch can
     * span tenants.
     */
    private function upsertKey(Document $document): string
    {
        return $this->getSharedTables() && $this->getTenantPerDocument()
            ? $document->getTenant().':'.$document->getId()
            : $document->getId();
    }

    /**
     * Increase a document attribute by a value
     *
     * @param  string  $collection  The collection ID
     * @param  string  $id  The document ID
     * @param  string  $attribute  The attribute to increase
     * @param  int|float|string  $value  The value to increase the attribute by, a number greater than 0
     * @param  int|float|string|null  $max  The maximum value the attribute can reach after the increase, null means no limit
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws LimitException
     * @throws NotFoundException
     * @throws TypeException
     * @throws Throwable
     */
    public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float|string $value = 1,
        int|float|string|null $max = null
    ): Document {
        $this->assertPositiveChange($value);

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $numericAttribute = null;
        if ($this->adapter->supports(Capability::DefinedAttributes)) {
            /** @var array<Attribute> $allAttrs */
            $allAttrs = $collection->getAttribute('attributes', []);
            $matchedAttrs = \array_filter($allAttrs, function (Attribute $a) use ($attribute) {
                return $a->getKey() === $attribute;
            });

            if (empty($matchedAttrs)) {
                throw new NotFoundException('Attribute not found');
            }

            /** @var Attribute $matchedAttr */
            $matchedAttr = \end($matchedAttrs);
            if (! Attribute::isNumericType($matchedAttr->getType()) || $matchedAttr->isArray()) {
                throw new TypeException('Attribute must be an integer or float and can not be an array.');
            }
            $numericAttribute = $matchedAttr;
        }

        if ($this->isDeclaredInteger($numericAttribute ?? $this->declaredAttribute($collection, $attribute))) {
            $this->assertIntegerChange($value);
            if ($max !== null) {
                $max = $this->integerBound($max, 'Max');
            }
        }

        $cacheTarget = $collection->getId() === self::METADATA
            ? new Document([Document::ID => $id, Document::COLLECTION => self::METADATA])
            : $collection->getId();
        $document = $this->withMutation(Event::DocumentIncrease, $cacheTarget, function () use ($collection, $id, $attribute, $value, $max, $numericAttribute) {
            /** @var Document $document */
            $document = $this->authorization->skip(fn () => $this->silent(fn () => $this->getDocument($collection->getId(), $id, forUpdate: true))); // Skip ensures user does not need read permission for this

            if ($document->isEmpty()) {
                throw new NotFoundException('Document not found');
            }

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                if (! $this->authorization->isValid(new Input(PermissionType::Update, \array_merge(
                    $collection->getUpdate(),
                    ((bool) $documentSecurity ? $document->getUpdate() : [])
                )))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            }

            $attributeExists = $document->offsetExists($attribute);
            $currentVal = $document->getAttribute($attribute);
            if ($numericAttribute instanceof Attribute) {
                $result = $this->getNumericResult($numericAttribute, $currentVal, $value, true);
            } else {
                if (! $attributeExists) {
                    $currentVal = 0;
                }
                if (! \is_int($currentVal) && ! \is_float($currentVal)) {
                    throw new TypeException('Attribute value must be numeric.');
                }
                $result = $currentVal + $this->getNativeNumber($value);
            }
            $exceedsMaximum = ! \is_null($max) && (
                $numericAttribute instanceof Attribute && Attribute::isIntegerType($numericAttribute->getType())
                    ? BigInt::compare($result, $max) > 0
                    : $result > $max
            );
            if ($exceedsMaximum) {
                throw new LimitException('Attribute value exceeds maximum limit: '.$max);
            }

            $time = DateTime::nowAfter($document->getUpdatedAt());
            $updatedAt = $document->getUpdatedAt();
            $updatedAt = (empty($updatedAt) || ! $this->datePreservation()->get()) ? $time : DateTime::setTimezone($updatedAt);
            if ($max !== null) {
                $max = $numericAttribute instanceof Attribute && Attribute::isIntegerType($numericAttribute->getType())
                    ? BigInt::subtract($max, $value)
                    : $this->getNativeNumber($max) - $this->getNativeNumber($value);
            }

            $this->adapter->increaseDocumentAttribute(
                $collection->getId(),
                $id,
                $attribute,
                $numericAttribute instanceof Attribute && Attribute::isIntegerType($numericAttribute->getType())
                    ? BigInt::toNative($value)
                    : $this->getNativeNumber($value),
                $updatedAt,
                max: $max
            );

            $this->purgeCachedDocumentInternal($collection->getId(), $id);
            $this->queueDocumentPurge($collection->getId(), $id);

            return $document->setAttribute($attribute, $result);
        });

        $this->triggerHooks(Event::DocumentIncrease, $document);

        return $document;
    }

    /**
     * @throws TypeException
     */
    private function assertPositiveChange(int|float|string $value): void
    {
        if (! \is_numeric($value) || (\is_string($value) && BigInt::isIntegerString($value)
            ? BigInt::compare($value, 0) <= 0
            : (float) $value <= 0)) {
            throw new TypeException('Value must be numeric and greater than 0');
        }
    }

    /**
     * Decrease a document attribute by a value.
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The document identifier
     * @param  string  $attribute  The attribute to decrease
     * @param  int|float|string  $value  The value to decrease the attribute by, a number greater than 0
     * @param  int|float|string|null  $min  The minimum value the attribute can reach, null means no limit
     * @return Document The updated document
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws TypeException When $value is not a number greater than 0
     */
    public function decreaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float|string $value = 1,
        int|float|string|null $min = null
    ): Document {
        $this->assertPositiveChange($value);

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $numericAttribute = null;
        if ($this->adapter->supports(Capability::DefinedAttributes)) {
            /** @var array<Attribute> $decAllAttrs */
            $decAllAttrs = $collection->getAttribute('attributes', []);
            $matchedDecAttrs = \array_filter($decAllAttrs, function (Attribute $a) use ($attribute) {
                return $a->getKey() === $attribute;
            });

            if (empty($matchedDecAttrs)) {
                throw new NotFoundException('Attribute not found');
            }

            /** @var Attribute $matchedDecAttr */
            $matchedDecAttr = \end($matchedDecAttrs);
            if (! Attribute::isNumericType($matchedDecAttr->getType()) || $matchedDecAttr->isArray()) {
                throw new TypeException('Attribute must be an integer or float and can not be an array.');
            }
            $numericAttribute = $matchedDecAttr;
        }

        if ($this->isDeclaredInteger($numericAttribute ?? $this->declaredAttribute($collection, $attribute))) {
            $this->assertIntegerChange($value);
            if ($min !== null) {
                $min = $this->integerBound($min, 'Min');
            }
        }

        $cacheTarget = $collection->getId() === self::METADATA
            ? new Document([Document::ID => $id, Document::COLLECTION => self::METADATA])
            : $collection->getId();
        $document = $this->withMutation(Event::DocumentDecrease, $cacheTarget, function () use ($collection, $id, $attribute, $value, $min, $numericAttribute) {
            /** @var Document $document */
            $document = $this->authorization->skip(fn () => $this->silent(fn () => $this->getDocument($collection->getId(), $id, forUpdate: true))); // Skip ensures user does not need read permission for this

            if ($document->isEmpty()) {
                throw new NotFoundException('Document not found');
            }

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                if (! $this->authorization->isValid(new Input(PermissionType::Update, \array_merge(
                    $collection->getUpdate(),
                    ((bool) $documentSecurity ? $document->getUpdate() : [])
                )))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            }

            $attributeExists = $document->offsetExists($attribute);
            $currentDecVal = $document->getAttribute($attribute);
            if ($numericAttribute instanceof Attribute) {
                $result = $this->getNumericResult($numericAttribute, $currentDecVal, $value, false);
            } else {
                if (! $attributeExists) {
                    $currentDecVal = 0;
                }
                if (! \is_int($currentDecVal) && ! \is_float($currentDecVal)) {
                    throw new TypeException('Attribute value must be numeric.');
                }
                $result = $currentDecVal - $this->getNativeNumber($value);
            }
            $belowMinimum = ! \is_null($min) && (
                $numericAttribute instanceof Attribute && Attribute::isIntegerType($numericAttribute->getType())
                    ? BigInt::compare($result, $min) < 0
                    : $result < $min
            );
            if ($belowMinimum) {
                throw new LimitException('Attribute value exceeds minimum limit: '.$min);
            }

            $time = DateTime::nowAfter($document->getUpdatedAt());
            $updatedAt = $document->getUpdatedAt();
            $updatedAt = (empty($updatedAt) || ! $this->datePreservation()->get()) ? $time : DateTime::setTimezone($updatedAt);
            if ($min !== null) {
                $min = $numericAttribute instanceof Attribute && Attribute::isIntegerType($numericAttribute->getType())
                    ? BigInt::add($min, $value)
                    : $this->getNativeNumber($min) + $this->getNativeNumber($value);
            }

            $this->adapter->increaseDocumentAttribute(
                $collection->getId(),
                $id,
                $attribute,
                $numericAttribute instanceof Attribute && Attribute::isIntegerType($numericAttribute->getType())
                    ? BigInt::negate($value)
                    : $this->getNativeNumber($value) * -1,
                $updatedAt,
                min: $min
            );

            $this->purgeCachedDocumentInternal($collection->getId(), $id);
            $this->queueDocumentPurge($collection->getId(), $id);

            return $document->setAttribute($attribute, $result);
        });

        $this->triggerHooks(Event::DocumentDecrease, $document);

        return $document;
    }

    /**
     * Delete Document
     *
     * Also fires Event::DocumentUpdate for each document on the other side of a two-way
     * relationship that the delete changed, after Event::DocumentDelete. See
     * Hook\Relationships::beforeDocumentDelete() for which documents those are, their shape
     * and their trust level.
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $id  The document identifier
     * @return bool True if the document was deleted successfully
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        $cacheTarget = $collection->getId() === self::METADATA
            ? new Document([Document::ID => $id, Document::COLLECTION => self::METADATA])
            : $collection->getId();
        $report = $this->getActiveLifecycleHooks(Event::DocumentUpdate) !== [];
        $changed = [];
        $deleted = $this->withMutation(Event::DocumentDelete, $cacheTarget, function () use ($collection, $id, $report, &$changed): ?Document {
            $changed = [];
            $document = $this->authorization->skip(fn () => $this->silent(
                fn () => $this->getDocument($collection->getId(), $id, forUpdate: true)
            ));

            if ($document->isEmpty()) {
                return null;
            }

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                if (! $this->authorization->isValid(new Input(PermissionType::Delete, [
                    ...$collection->getDelete(),
                    ...($documentSecurity ? $document->getDelete() : []),
                ]))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            }

            try {
                $oldUpdatedAt = new PhpDateTime($document->getUpdatedAt() ?? 'now');
            } catch (Exception $e) {
                throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
            }

            $requestTimestamp = $this->requestTimestamp()->get();
            if ($requestTimestamp !== null && $oldUpdatedAt > $requestTimestamp) {
                throw new ConflictException('Document was updated after the request timestamp');
            }

            if ($this->relationshipHook?->isEnabled()) {
                $changed = $this->silent(fn () => $this->relationshipHook->beforeDocumentDelete($collection, $document, $report));
            }

            $result = $this->authorization->skip(fn () => $this->adapter->deleteDocument($collection->getId(), $id));

            $this->purgeCachedDocumentInternal($collection->getId(), $id);

            if ($result) {
                $this->queueDocumentPurge($collection->getId(), $id);
            }

            return $result ? $document : null;
        });

        if ($deleted === null) {
            return false;
        }

        $this->triggerDeleteHooks($deleted, $changed);

        return true;
    }

    /**
     * The delete's transaction has returned, so a failing hook cannot undo it: every event still
     * fires, and the first failure reaches the caller once they have.
     *
     * @param  list<Document>  $changed
     */
    private function triggerDeleteHooks(Document $document, array $changed): void
    {
        $failure = null;

        try {
            $this->triggerHooks(Event::DocumentDelete, $document);
        } catch (Throwable $error) {
            $failure = $error;
        }

        foreach ($changed as $related) {
            try {
                $this->triggerHooks(Event::DocumentUpdate, $related);
            } catch (Throwable $error) {
                $failure ??= $error;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * Delete Documents
     *
     * Deletes all documents which match the given queries, respecting relationship onDelete options.
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Query>  $queries  Queries to filter documents for deletion
     * @param  int  $batchSize  Number of documents per batch deletion
     * @param  (callable(Document $deleted, Document $copy): void)|null  $onNext  Callback given each deleted document once its batch is deleted, and a copy of that same document taken before the delete, not a separately stored version
     * @param  (callable(Throwable): void)|null  $onError  Given an error $onNext throws; the write continues. Without it the
     *                                                    error is rethrown. Write errors are always thrown.
     * @return int The number of documents deleted
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws QueryException
     * @throws RestrictedException
     * @throws Throwable
     */
    public function deleteDocuments(
        string $collection,
        array $queries = [],
        int $batchSize = self::DELETE_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        $this->rejectJoins($queries, 'Join queries are not supported for bulk deletes');

        if ($this->adapter->getSharedTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $batchSize = \min(Database::DELETE_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Delete, $collection->getDelete()));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validation()->get()) {
            $validator = $this->getQueriesValidator($collection, $queries);

            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $grouped = Query::groupForDatabase($queries);
        $limit = $grouped['limit'];
        $cursor = $grouped['cursor'];

        if (! empty($cursor) && $cursor->getCollection() !== $collection->getId()) {
            throw new DatabaseException('Cursor document must be from the same Collection.');
        }

        $originalLimit = $limit;
        $last = $cursor;
        $modified = 0;

        while (true) {
            if ($limit && $limit < $batchSize && $limit > 0) {
                $batchSize = $limit;
            } elseif (! empty($limit)) {
                $limit -= $batchSize;
            }

            $new = [
                Query::limit($batchSize),
            ];

            if (! empty($last)) {
                $new[] = Query::cursorAfter($last);
            }

            /**
             * @var array<Document> $batch
             */
            $batch = $this->silent(fn () => $this->find(
                $collection->getId(),
                array_merge($new, $queries),
                forPermission: PermissionType::Delete
            ));

            if (empty($batch)) {
                break;
            }

            $old = array_map(fn ($doc) => clone $doc, $batch);
            $sequences = [];
            $permissionIds = [];

            $cacheTarget = $collection->getId() === self::METADATA ? $batch : $collection->getId();
            $this->withMutation(Event::DocumentsDelete, $cacheTarget, function () use ($collection, $sequences, $permissionIds, $batch) {
                foreach ($batch as $document) {
                    $seq = $document->getSequence();
                    if ($seq !== null) {
                        $sequences[] = $seq;
                    }
                    if (! empty($document->getPermissions())) {
                        $permissionIds[] = $document->getId();
                    }

                    if ($this->relationshipHook?->isEnabled()) {
                        $this->silent(fn () => $this->relationshipHook->beforeDocumentDelete(
                            $collection,
                            $document
                        ));
                    }

                    // Check if document was updated after the request timestamp
                    try {
                        $oldUpdatedAt = new PhpDateTime($document->getUpdatedAt() ?? 'now');
                    } catch (Exception $e) {
                        throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                    }

                    $requestTimestamp = $this->requestTimestamp()->get();
                    if ($requestTimestamp !== null && $oldUpdatedAt > $requestTimestamp) {
                        throw new ConflictException('Document was updated after the request timestamp');
                    }
                }

                $this->adapter->deleteDocuments(
                    $collection->getId(),
                    $sequences,
                    $permissionIds
                );

                foreach ($batch as $document) {
                    $this->withDocumentTenant(
                        $document,
                        fn () => $this->advanceCollectionCacheEpoch($collection->getId(), $document->getId())
                    );
                }

                $this->queueDocumentPurges($collection->getId(), $batch);
            });

            foreach ($batch as $index => $document) {
                try {
                    $onNext && $onNext($document, $old[$index]);
                } catch (Throwable $th) {
                    $onError ? $onError($th) : throw $th;
                }
                $modified++;
            }

            if (count($batch) < $batchSize) {
                break;
            } elseif ($originalLimit && $modified >= $originalLimit) {
                break;
            }

            $last = \end($batch);
        }

        $this->triggerHooks(Event::DocumentsDelete, new Document([
            Document::COLLECTION => $collection->getId(),
            'modified' => $modified,
        ]));

        return $modified;
    }

    /**
     * Cleans all of the collection's documents from the cache and all related cached documents.
     *
     * @param  string  $collectionId  The collection identifier
     * @return bool True if the cache was purged successfully
     */
    public function purgeCachedCollection(string $collectionId): bool
    {
        if ($collectionId === self::METADATA) {
            $this->purgeCachedDefinitions();
            $this->queryCache?->invalidateCollection($this->getQueryCacheScope(), $collectionId);

            return true;
        }

        [$collectionKey] = $this->getCacheKeys($collectionId);

        $purged = $this->advanceDocumentCacheEpoch($collectionKey, $this->getDefinitionCacheKey($collectionId));
        $this->queryCache?->invalidateCollection($this->getQueryCacheScope(), $collectionId);

        return $purged;
    }

    /**
     * Purge a document's cache slot, and once more after the open invalidation scope ends.
     *
     * @throws Exception
     */
    protected function purgeCachedDocumentInternal(string $collectionId, ?string $id): bool
    {
        if ($id === null) {
            return true;
        }

        [$collectionKey, $documentKey] = $this->getCacheBaseKeys($collectionId, $id);

        $context = $this->getEventContext();
        if (isset($this->documentCachePurges[$context])) {
            $this->documentCachePurges[$context][$documentKey] = $collectionKey;
            if ($collectionId !== self::METADATA) {
                $this->documentCacheDefinitions[$context][$collectionKey] = $this->getDefinitionCacheKey($collectionId);
            }
        }
        if (isset($this->transactionWrites[$context])) {
            $this->transactionWrites[$context][\strtolower($documentKey)] = true;
            unset($this->transactionDefinitions[$context][\strtolower($documentKey)]);
        }

        $this->cache->purge($documentKey);

        return true;
    }

    /**
     * A batch write retires the documents of their collection at once; a batch write to `_metadata`
     * purges each definition it wrote instead, as a cached definition is checked against no epoch.
     */
    private function advanceCollectionCacheEpoch(string $collectionId, string $documentId): bool
    {
        if ($collectionId === self::METADATA) {
            return $this->purgeCachedDocumentInternal(self::METADATA, $documentId);
        }

        [$collectionKey] = $this->getCacheBaseKeys($collectionId);

        return $this->advanceDocumentCacheEpoch($collectionKey, $this->getDefinitionCacheKey($collectionId));
    }

    /**
     * The cache key of a collection's definition, which carries the epoch its documents are cached under.
     */
    private function getDefinitionCacheKey(string $collectionId): string
    {
        return $this->getCacheBaseKeys(self::METADATA, $collectionId)[1];
    }

    /**
     * Cached definitions are checked against no epoch, so each one the database lists is purged.
     */
    private function purgeCachedDefinitions(): void
    {
        $this->silent(fn () => $this->authorization->skip(fn () => $this->foreach(
            self::METADATA,
            function (Document $definition): void {
                $this->cache->purge($this->getDefinitionCacheKey($definition->getId()));
            },
        )));
    }

    /**
     * Purge the documents a transaction wrote once it has committed or rolled back. A reader
     * outside the transaction may have cached the pre-commit row after the purge inside it, so
     * when this purge fails the collection's epoch is retired instead, which no such fill survives.
     * A collection definition has no epoch of its own: its purge is tried once more.
     *
     * @param  array<string, string>  $documents  Collection keys by document key
     */
    protected function purgeWrittenDocuments(array $documents): void
    {
        $definitions = $this->documentCacheDefinitions[$this->getEventContext()] ?? [];
        $failure = null;
        $retired = [];
        foreach ($documents as $documentKey => $collectionKey) {
            try {
                $this->cache->purge($documentKey);
            } catch (Throwable $error) {
                $failure ??= $error;
                try {
                    if (! isset($definitions[$collectionKey])) {
                        $this->cache->purge($documentKey);
                    } elseif (! isset($retired[$collectionKey])) {
                        $retired[$collectionKey] = true;
                        $this->advanceDocumentCacheEpoch($collectionKey, $definitions[$collectionKey]);
                    }
                } catch (Throwable) {
                    // The purge failure below reaches the caller either way.
                }
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * The epoch a collection's documents may be cached under, or null while a write to the collection
     * is in flight, with the time that write blocked it. A tombstone older than the writer timeout
     * lapses into an epoch of its own, which a later activation changes.
     */
    private function loadDocumentCacheState(string $collectionKey): Epoch
    {
        $now = \time();

        try {
            $record = $this->cache->load($collectionKey.'#epoch', self::DOCUMENT_CACHE_PERMANENT);
            if (! \is_string($record) || $record === '') {
                return $this->restoreDocumentCacheEpoch($collectionKey, $now);
            }

            $separator = \strrpos($record, self::DOCUMENT_CACHE_SEPARATOR);
            $marker = $separator === false ? $record : \substr($record, 0, $separator);
            $stamp = $separator === false ? '' : \substr($record, $separator + 1);

            if (\str_starts_with($record, self::DOCUMENT_CACHE_BLOCKED_PREFIX)) {
                $blockedAt = \ctype_digit($stamp) ? (int) $stamp : 0;
                if ($blockedAt + $this->cacheWriterTimeout > $now) {
                    return new Epoch(blockedAt: $blockedAt);
                }

                $tombstone = \substr($record, \strlen(self::DOCUMENT_CACHE_BLOCKED_PREFIX));
                $finished = $this->cache->getGeneration($collectionKey.'#finished');

                return new Epoch(self::DOCUMENT_CACHE_LAPSED_PREFIX.$tombstone.self::DOCUMENT_CACHE_SEPARATOR.$finished);
            }

            $started = $this->cache->getGeneration($collectionKey.'#started');
            if (
                ($separator !== false && $started === $stamp)
                || $started === $this->cache->getGeneration($collectionKey.'#finished')
            ) {
                return new Epoch($marker);
            }

            return $this->restoreDocumentCacheEpoch($collectionKey, $now);
        } catch (Throwable $error) {
            Console::warning('Warning: Failed to load document cache epoch: '.$error->getMessage());

            return new Epoch(blockedAt: $now);
        }
    }

    /**
     * Replace a missing or unusable epoch: with a fresh one when no write is counted in flight, or
     * else with a tombstone of its own, so the collection lapses back into the cache after the writer
     * timeout even when the write that blocked it left no tombstone behind.
     */
    private function restoreDocumentCacheEpoch(string $collectionKey, int $now): Epoch
    {
        $started = $this->cache->getGeneration($collectionKey.'#started');
        if ($started !== $this->cache->getGeneration($collectionKey.'#finished')) {
            $this->cache->save($collectionKey.'#epoch', self::DOCUMENT_CACHE_BLOCKED_PREFIX.$this->createDocumentCacheToken().self::DOCUMENT_CACHE_SEPARATOR.$now);

            return new Epoch(blockedAt: $now);
        }

        $epoch = self::DOCUMENT_CACHE_ACTIVE_PREFIX.\bin2hex(\random_bytes(16));
        if ($this->cache->save($collectionKey.'#epoch', $epoch.self::DOCUMENT_CACHE_SEPARATOR.$started) === false) {
            return new Epoch(blockedAt: $now);
        }

        return new Epoch($epoch);
    }

    /**
     * A write's token, which records when it was created so a later activation can tell an abandoned write.
     */
    private function createDocumentCacheToken(): string
    {
        return \time().self::DOCUMENT_CACHE_TOKEN_SEPARATOR.\bin2hex(\random_bytes(16));
    }

    private function advanceDocumentCacheEpoch(string $collectionKey, string $definitionKey): bool
    {
        $context = $this->getEventContext();
        if (isset($this->documentCacheMutations[$context][$collectionKey])) {
            return true;
        }

        $token = $this->createDocumentCacheToken();
        if (! $this->blockDocumentCacheEpoch($collectionKey, $token, $definitionKey)) {
            return true;
        }

        if (isset($this->documentCacheMutations[$context])) {
            $this->documentCacheMutations[$context][$collectionKey] = $token;
            $this->documentCacheDefinitions[$context][$collectionKey] = $definitionKey;

            return true;
        }

        $this->activateDocumentCacheEpoch($collectionKey, $token, $definitionKey);

        return true;
    }

    /**
     * Publish a tombstone before the write, then drop the definition that carried the previous
     * epoch, so readers refill it with the tombstone.
     */
    private function blockDocumentCacheEpoch(string $collectionKey, string $token, string $definitionKey): bool
    {
        $epochKey = $collectionKey.'#epoch';
        if (! (new Owners($this->cache))->register($collectionKey, $token)) {
            $epoch = $this->cache->load($epochKey, self::DOCUMENT_CACHE_PERMANENT);
            if ($epoch === false || $epoch === null) {
                return false;
            }

            throw new RuntimeException("Failed to register document cache owner '{$token}' for '{$collectionKey}'");
        }

        if ($this->cache->save($epochKey, self::DOCUMENT_CACHE_BLOCKED_PREFIX.$token.self::DOCUMENT_CACHE_SEPARATOR.\time()) === false) {
            throw new RuntimeException("Failed to block document cache epoch '{$epochKey}'");
        }

        $this->cache->purge($collectionKey.'#started');
        $this->purgeCachedDefinition($definitionKey);

        return true;
    }

    private function purgeCachedDefinition(string $definitionKey): void
    {
        if ($definitionKey !== '') {
            $this->cache->purge($definitionKey);
        }
    }

    /**
     * @param  array<string, string>  $tokens
     */
    protected function activateDocumentInvalidation(array $tokens): void
    {
        $context = $this->getEventContext();
        $definitions = $this->documentCacheDefinitions[$context] ?? [];
        unset($this->documentCacheDefinitions[$context]);

        $failure = null;
        foreach ($tokens as $collectionKey => $token) {
            try {
                $this->activateDocumentCacheEpoch($collectionKey, $token, $definitions[$collectionKey] ?? '');
            } catch (Throwable $error) {
                $failure ??= $error;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * Replace this write's tombstone with a fresh epoch once no other write to the collection is in
     * flight. Writes older than the writer timeout no longer count as in flight: their registrations
     * are released and the epoch is published.
     */
    private function activateDocumentCacheEpoch(string $collectionKey, string $token, string $definitionKey): void
    {
        $registration = (new Owners($this->cache))->find($collectionKey, $token);
        $owner = $this->cache->load($registration->key, self::TTL, $registration->field);
        if ($owner !== false && $owner !== null && $owner !== $token) {
            throw new RuntimeException("Invalid document cache owner '{$token}' for '{$collectionKey}'");
        }
        $owned = $owner === $token;
        if ($owned && ! $this->cache->purge($registration->key, $registration->field)) {
            $owner = $this->cache->load($registration->key, self::TTL, $registration->field);
            if ($owner !== false && $owner !== null) {
                throw new RuntimeException("Failed to release document cache owner '{$token}' for '{$collectionKey}'");
            }
            $owned = false;
        }

        $startedKey = $collectionKey.'#started';
        $finishedKey = $collectionKey.'#finished';
        $started = $this->cache->getGeneration($startedKey);
        $finished = $this->cache->getGeneration($finishedKey);
        $epochKey = $collectionKey.'#epoch';
        $epoch = $this->cache->load($epochKey, self::DOCUMENT_CACHE_PERMANENT);
        $blocked = \is_string($epoch) && \str_starts_with($epoch, self::DOCUMENT_CACHE_BLOCKED_PREFIX);
        $ours = $blocked && \str_starts_with($epoch, self::DOCUMENT_CACHE_BLOCKED_PREFIX.$token.self::DOCUMENT_CACHE_SEPARATOR);

        if ($started === $finished) {
            if (! $blocked || $ours) {
                $this->publishDocumentCacheEpoch($collectionKey, $finished, $definitionKey);
            }

            return;
        }

        if (! $owned && ! $ours) {
            // This token was cleared by a cache flush or released as abandoned. Leave another
            // writer's tombstone fail-closed, but retire whatever readers filled while it ran.
            if (\is_string($epoch) && ! $blocked) {
                $this->publishDocumentCacheEpoch($collectionKey, $started, $definitionKey);
            }

            return;
        }

        $this->cache->purge($finishedKey);
        $nextFinished = $this->cache->getGeneration($finishedKey);

        // A cache flush restarts generations, so an unchanged #finished
        // only proves this purge was lost while the epoch read with it is
        // still in place; publish the new epoch after this check, not before.
        if (
            $nextFinished === $finished
            && \is_string($epoch)
            && $this->cache->load($epochKey, self::DOCUMENT_CACHE_PERMANENT) === $epoch
        ) {
            throw new RuntimeException("Failed to finish document cache invalidation '{$epochKey}'");
        }

        $nextStarted = $this->cache->getGeneration($startedKey);
        if ($nextStarted === $nextFinished) {
            $this->publishDocumentCacheEpoch($collectionKey, $nextFinished, $definitionKey);

            return;
        }

        if ($registration->field !== '' && $this->releaseAbandonedDocumentCacheOwners($registration->key)) {
            $this->publishDocumentCacheEpoch($collectionKey, $nextStarted, $definitionKey);

            return;
        }

        $this->purgeCachedDefinition($definitionKey);
    }

    /**
     * An epoch carries the started generation it was published at: until the next write starts, it is
     * current even while writes judged abandoned still count as unfinished.
     */
    private function publishDocumentCacheEpoch(string $collectionKey, string $started, string $definitionKey): void
    {
        $epochKey = $collectionKey.'#epoch';
        $epoch = self::DOCUMENT_CACHE_ACTIVE_PREFIX.\bin2hex(\random_bytes(16)).self::DOCUMENT_CACHE_SEPARATOR.$started;
        if ($this->cache->save($epochKey, $epoch) === false) {
            throw new RuntimeException("Failed to activate document cache epoch '{$epochKey}'");
        }

        $this->purgeCachedDefinition($definitionKey);
    }

    /**
     * Release every other writer still registered when all of them are older than the writer timeout.
     * A token without a creation time counts as live.
     */
    private function releaseAbandonedDocumentCacheOwners(string $owners): bool
    {
        $now = \time();
        $abandoned = [];
        foreach ($this->cache->list($owners) as $token) {
            $separator = \strpos($token, self::DOCUMENT_CACHE_TOKEN_SEPARATOR);
            $created = $separator === false ? '' : \substr($token, 0, $separator);
            if (! \ctype_digit($created) || (int) $created + $this->cacheWriterTimeout > $now) {
                return false;
            }

            $abandoned[] = $token;
        }

        foreach ($abandoned as $token) {
            $this->cache->purge($owners, $token);
        }

        return true;
    }

    /**
     * Run a cache operation under the document's tenant when tenant-per-document is enabled.
     *
     * @param  callable(): mixed  $callback
     */
    private function withDocumentTenant(Document $document, callable $callback): void
    {
        $tenant = $document->getTenant();

        // A tenant of null is not a tenant to switch to. Collection definitions
        // are the one kind of row createDocument() lets through without one
        // under tenant-per-document, and readers still resolve their cache key
        // under the adapter's tenant, so borrowing the document's null here
        // purged an epoch no reader ever looks at and left every cached
        // _metadata entry - a negative marker above all - live for its full TTL.
        if ($this->getSharedTables() && $this->getTenantPerDocument() && $tenant !== null) {
            $this->withTenant($tenant, $callback);

            return;
        }

        $callback();
    }

    private function getDocumentIdentity(Document $document): string
    {
        if (! $this->adapter->getTenantPerDocument()) {
            return $document->getId();
        }

        return ($document->getTenant() ?? '').'\0'.$document->getId();
    }

    /**
     * Cleans a specific document from cache and triggers Event::DocumentPurge, whose
     * lifecycle hook exceptions reach the caller.
     *
     * Note: Do not retry this method as it triggers events. Use purgeCachedDocumentInternal() with retry instead.
     *
     * @param  string  $collectionId  The collection identifier
     * @param  string|null  $id  The document identifier, or null to skip
     * @return bool True if the cache was purged successfully
     *
     * @throws Exception
     */
    public function purgeCachedDocument(string $collectionId, ?string $id): bool
    {
        $result = $this->purgeCachedDocumentInternal($collectionId, $id);

        if ($id !== null) {
            $purged = new Document([
                Document::ID => $id,
                Document::COLLECTION => $collectionId,
            ]);
            $this->invalidate(Event::DocumentPurge, $purged);
            $this->triggerPropagatingHooks(Event::DocumentPurge, $purged);
        }

        return $result;
    }

    /**
     * Announce the purge of a written document once the outermost transaction of its
     * invalidation scope has committed, under the tenant and the hook silences in force
     * when it was written. A write the adapter holds no transaction for is already
     * durable and announces at once.
     */
    private function queueDocumentPurge(string $collectionId, string $id): void
    {
        $document = new Document([
            Document::ID => $id,
            Document::COLLECTION => $collectionId,
        ]);

        if (! $this->adapter->inTransaction()) {
            $this->triggerPropagatingHooks(Event::DocumentPurge, $document);

            return;
        }

        if ($this->areEventsSilenced()) {
            return;
        }

        $context = $this->getEventContext();
        $tenant = $this->getTenant();
        $silenced = \array_keys($this->silencedListeners()->get());
        $announce = fn () => $this->triggerPropagatingHooks(Event::DocumentPurge, $document);

        $this->documentPurgeEvents[$context][] = function () use ($tenant, $silenced, $announce): void {
            $this->withTenant(
                $tenant,
                $silenced === [] ? $announce : fn () => $this->silent($announce, $silenced),
            );
        };
    }

    /**
     * @param  array<Document>  $documents
     */
    private function queueDocumentPurges(string $collectionId, array $documents): void
    {
        foreach ($documents as $document) {
            $this->withDocumentTenant(
                $document,
                fn () => $this->queueDocumentPurge($collectionId, $document->getId())
            );
        }
    }

    /**
     * Purge every cached query result of a collection namespace: the find() query
     * cache and the caller-owned withCache() region.
     */
    public function purgeCachedQueries(string $collection, ?string $namespace = null): bool
    {
        $collectionDocument = $this->silent(fn () => $this->getCollection($collection));
        $collection = $collectionDocument->isEmpty() ? $collection : $collectionDocument->getId();
        $epochKey = $this->getQueryCacheKey($collection, $namespace).'#epoch';

        try {
            $existing = $this->cache->load($epochKey, self::TTL);
            $rotated = ($existing === false || $existing === null || $this->cache->purge($epochKey))
                && $this->cache->save($epochKey, \bin2hex(\random_bytes(16))) !== false;
        } catch (Exception $error) {
            Console::warning('Warning: Failed to purge the cached queries: '.$error->getMessage());
            $rotated = false;
        }

        try {
            $this->queryCache?->invalidateCollection($this->getQueryCacheScope($namespace), $collection);
        } catch (Exception $error) {
            Console::warning('Warning: Failed to purge the query cache: '.$error->getMessage());

            return false;
        }

        return $rotated;
    }

    /**
     * Execute a callback behind a generation-protected cache-aside lookup.
     *
     * @template T
     * @param  callable(): T  $callback
     * @return T
     *
     * @throws AuthorizationException
     */
    public function withCache(
        string $key,
        callable $callback,
        ?string $hash = '',
    ): mixed {
        if ($hash === null || $this->adapter->inTransaction()) {
            return $callback();
        }

        $epochKey = $key.'#epoch';
        try {
            $epoch = $this->cache->load($epochKey, self::TTL);
            if ($epoch === false || $epoch === null) {
                $epoch = \bin2hex(\random_bytes(16));
                if ($this->cache->save($epochKey, $epoch) === false) {
                    return $callback();
                }
            }
            if (! \is_string($epoch) || $epoch === '') {
                return $callback();
            }
        } catch (Throwable $error) {
            Console::warning('Warning: Failed to load cache epoch: '.$error->getMessage());

            return $callback();
        }

        $physicalKey = $key.'#'.$epoch.':'.$hash;

        $shouldRefreshCache = false;

        try {
            $cached = $this->cache->load($physicalKey, self::TTL);
        } catch (Throwable $error) {
            Console::warning('Warning: Failed to load cache value: '.$error->getMessage());
            $cached = false;
        }

        if ($cached !== false && $cached !== null) {
            $cachedValue = \is_array($cached) && \array_key_exists('value', $cached)
                ? $cached['value']
                : false;

            if ($cachedValue !== false) {
                $decoded = $cachedValue;
                $collectionId = $cached['collection'] ?? null;

                if (\is_string($collectionId) && $collectionId !== '') {
                    $collection = $this->silent(fn () => $this->getCollection($collectionId));

                    if ($collection->isEmpty()) {
                        $decoded = false;
                    } else {
                        $documentSecurity = $collection->getAttribute('documentSecurity', false);
                        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Read, $collection->getRead()));

                        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
                            throw new AuthorizationException($this->authorization->getDescription());
                        }

                        $type = $cached['type'] ?? null;
                        $payload = $type === 'document' ? [$cachedValue] : $cachedValue;

                        if (! \is_array($payload)) {
                            $decoded = false;
                        } else {
                            $documents = [];

                            foreach ($payload as $item) {
                                if (! \is_array($item)) {
                                    $decoded = false;
                                    break;
                                }

                                /** @var array<string, mixed> $item */
                                $document = $this->createDocumentInstance($collection->getId(), $item);
                                $document = $this->casting($collection, $document);

                                if ($this->isTtlExpired($collection, $document)) {
                                    $decoded = false;
                                    break;
                                }

                                if (! $skipAuth && $documentSecurity && $collection->getId() !== self::METADATA) {
                                    if (! $this->authorization->isValid(new Input(PermissionType::Read, $document->getRead()))) {
                                        if ($type === 'document') {
                                            $decoded = false;
                                            break;
                                        }

                                        continue;
                                    }
                                }

                                $documents[] = $document;
                            }

                            if ($decoded !== false) {
                                $decoded = $type === 'document' ? ($documents[0] ?? false) : $documents;
                            }
                        }
                    }
                }

                if ($decoded !== false) {
                    return $decoded;
                }
            }

            $shouldRefreshCache = true;
        }

        if ($shouldRefreshCache) {
            try {
                $this->cache->purge($physicalKey);
            } catch (Throwable $error) {
                Console::warning('Warning: Failed to purge rejected cache value: '.$error->getMessage());
            }
        }

        $generation = '0';
        try {
            $generation = $this->cache->getGeneration($physicalKey);
        } catch (Throwable $error) {
            Console::warning('Warning: Failed to get cache generation: '.$error->getMessage());
        }

        $callbackValue = $callback();

        if ($callbackValue !== false) {
            try {
                $encoded = $this->encodeCacheValue($callbackValue);

                if ($encoded !== false) {
                    $this->cache->saveWithLease($physicalKey, $encoded, '', $generation);
                }
            } catch (Throwable $error) {
                Console::warning('Warning: Failed to save cache value: '.$error->getMessage());
            }
        }

        /** @var T $callbackValue */
        return $callbackValue;
    }

    /**
     * @return array<string, mixed>|false
     */
    private function encodeCacheValue(mixed $value): array|false
    {
        if ($value instanceof Document) {
            $collection = $value->getCollection();

            return $collection === '' ? false : [
                'collection' => $collection,
                'type' => 'document',
                'value' => $value->getArrayCopy(),
            ];
        }

        if (! \is_array($value)) {
            return ['value' => $value];
        }

        $collection = null;
        $documents = [];
        $hasDocuments = false;
        $hasNonDocuments = false;

        foreach ($value as $item) {
            if (! $item instanceof Document) {
                if ($hasDocuments || $this->containsDocument($item)) {
                    return false;
                }

                $hasNonDocuments = true;
                continue;
            }

            if ($hasNonDocuments) {
                return false;
            }

            $documentCollection = $item->getCollection();
            if ($documentCollection === '' || ($collection !== null && $collection !== $documentCollection)) {
                return false;
            }

            $collection = $documentCollection;
            $hasDocuments = true;
            $documents[] = $item->getArrayCopy();
        }

        return $hasDocuments ? [
            'collection' => $collection,
            'type' => 'documents',
            'value' => $documents,
        ] : ['value' => $value];
    }

    private function containsDocument(mixed $value): bool
    {
        if ($value instanceof Document) {
            return true;
        }

        if (! \is_array($value)) {
            return false;
        }

        foreach ($value as $item) {
            if ($this->containsDocument($item)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Find Documents
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Query>  $queries  Queries for filtering, sorting, pagination, and selection
     * @param  PermissionType  $forPermission  The permission type to check for authorization
     * @return array<Document>
     *
     * @param  array<Query>  $queries
     * @return array<Document>
     *
     * @throws DatabaseException
     * @throws QueryException
     * @throws TimeoutException
     * @throws Exception
     */
    public function find(string $collection, array $queries = [], PermissionType $forPermission = PermissionType::Read): array
    {
        $queryCacheQueries = $queries;

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        $joinedCollectionsById = null;

        if ($this->validation()->get()) {
            $joinedCollectionsById = $this->resolveJoinedCollections($queries);
            $this->validateDocumentsQueries($collection, $queries, $joinedCollectionsById);
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $collectionGranted = $this->authorization->isValid(new Input($forPermission, $collection->getPermissionsByType($forPermission)));

        if (! $collectionGranted && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        /** @var array<Document> $relationships */
        $relationships = \array_filter(
            $attributes,
            fn (Attribute|Document $attribute) => Attribute::isRelationship($attribute)
        );

        $grouped = Query::groupForDatabase($queries);
        $filters = $grouped['filters'];
        $selects = $grouped['selections'];
        $aggregations = $grouped['aggregations'];
        $groupByAttrs = $grouped['groupBy'];
        $having = $grouped['having'];
        $joins = $grouped['joins'];
        // Skipping authorization would also skip the joined collections' permission filters,
        // so with joins the main collection's grant travels to the adapter instead.
        $skipAuth = $collectionGranted && empty($joins);
        $distinct = $grouped['distinct'];
        $limit = $grouped['limit'];
        $offset = $grouped['offset'];
        $orderAttributes = $grouped['orderAttributes'];
        $orderTypes = $grouped['orderTypes'];
        $cursor = $grouped['cursor'];
        $cursorDirection = $grouped['cursorDirection'] ?? CursorDirection::After;

        $isAggregation = ! empty($aggregations) || ! empty($groupByAttrs);

        if ($isAggregation && ! $this->adapter->supports(Capability::Aggregations)) {
            throw new QueryException('Aggregation queries are not supported by this adapter');
        }

        if ($distinct && ! $this->adapter->supports(Capability::Aggregations)) {
            throw new QueryException('Distinct queries are not supported by this adapter');
        }

        foreach ($aggregations as $aggregation) {
            $method = $aggregation->getMethod();
            $capability = match ($method) {
                Method::Stddev, Method::StddevPop, Method::StddevSamp, Method::Variance, Method::VarPop, Method::VarSamp => Capability::StatisticalAggregates,
                Method::BitAnd, Method::BitOr, Method::BitXor => Capability::BitwiseAggregates,
                default => null,
            };

            if ($capability !== null && ! $this->adapter->supports($capability)) {
                throw new QueryException('Aggregate '.$method->value.' is not supported by this adapter');
            }
        }

        if (! empty($joins) && ! $this->adapter->supports(Capability::Joins)) {
            throw new QueryException('Join queries are not supported by this adapter');
        }

        $this->assertJoinCount($joins);

        $joinedCollectionsById ??= $this->resolveJoinedCollections($joins);
        $joinDocumentSecurity = [];
        if (! empty($joins)) {
            $joinDocumentSecurity = $this->authorizeJoins($joins, $forPermission, $joinedCollectionsById);
        }

        $joinedByAlias = $this->joinedCollectionsByAlias($joins, $joinedCollectionsById);
        $joinedCollections = $isAggregation ? [] : $joinedByAlias;

        if ($joinedCollections !== [] && $cursor !== null) {
            [$orderAttributes, $cursor] = $this->qualifyJoinedOrders($collection, $orderAttributes, $cursor, $joinedCollections);
        }

        if (! $isAggregation && ! $distinct) {
            [$orderAttributes, $orderTypes] = $this->addTieBreaks($orderAttributes, $orderTypes, $filters, $joins, $joinedCollections, ! empty($cursor), $selects);
        }

        if (! empty($cursor)) {
            if ($isAggregation) {
                throw new QueryException('Cursor pagination is not supported with aggregation queries');
            }

            if ($joins === [] && ! $distinct && $this->validation()->get() && $cursor->getId() === '') {
                throw new QueryException('Invalid query: Invalid cursor: '.(new UID($this->adapter->getMaxUIDLength()))->getDescription());
            }

            if ($distinct) {
                $this->assertDistinctCursorOrder($selects, $orderAttributes);
            }

            if ($joins !== [] || $distinct) {
                $this->assertCursorHasOrderValues($cursor, $orderAttributes);
            }

            if ($joins === []) {
                foreach ($orderAttributes as $order) {
                    if ($cursor->getAttribute($order) === null) {
                        throw new OrderException(
                            message: "Order attribute '{$order}' is empty",
                            attribute: $order
                        );
                    }
                }
            }
        }

        if (! empty($cursor) && $cursor->getCollection() !== $collection->getId()) {
            throw new DatabaseException('cursor Document must be from the same Collection.');
        }

        if (! empty($cursor)) {
            $cursor = $this->encode($collection, clone $cursor);
            $cursor = $this->castingBefore($collection, $cursor);
            $cursor = $this->encodeJoins($cursor, $joinedCollections);
            $cursor = $cursor->getArrayCopy();
        } else {
            $cursor = [];
        }

        $outerJoinIds = $distinct ? [] : $this->outerJoinIdSelections($selects, $joins, $joinedCollections);

        /** @var array<Query> $queries */
        $queries = \array_merge(
            $selects,
            $outerJoinIds === [] ? [] : [Query::select($outerJoinIds)],
            $this->convertQueries($collection, \array_merge($filters, $aggregations, $having, $joins), $joinedByAlias),
        );

        if (! empty($groupByAttrs)) {
            $queries[] = Query::groupBy($groupByAttrs);
        }

        if ($distinct) {
            $queries[] = Query::distinct();
        }

        $selections = $this->validateSelections($collection, $selects);

        if ($isAggregation) {
            $nestedSelections = [];
        } else {
            $nestedSelections = $this->relationshipHook?->processQueries($relationships, $queries) ?? [];
        }

        // Convert relationship filter queries to SQL-level subqueries
        if (! $isAggregation) {
            $convertedQueries = $this->relationshipHook !== null
                ? $this->relationshipHook->convertQueries($relationships, $queries, $collection)
                : $queries;
        } else {
            $convertedQueries = $queries;
        }

        // If conversion returns null, it means no documents can match (relationship filter found no matches)
        if ($convertedQueries === null) {
            $results = [];
        } else {
            $queries = $convertedQueries;

            $cacheEntry = null;
            $cacheGeneration = '';
            if (
                $this->queryCache !== null
                && $collection->getId() !== self::METADATA
                && $this->adapter->supports(Capability::Caching)
                && ! $this->adapter->inTransaction()
                && empty($joins)
            ) {
                $cacheContext = $skipAuth
                    ? $this->authorization->skip(fn () => $this->getQueryCacheField($collection, $queryCacheQueries, forPermission: $forPermission))
                    : $this->getQueryCacheField($collection, $queryCacheQueries, forPermission: $forPermission);

                if ($cacheContext !== null) {
                    $cacheQueries = [
                        'input' => \array_map(
                            fn (Query $query): array => $this->serializeQueryCacheQuery($query),
                            $queryCacheQueries,
                        ),
                        'queries' => \array_map(
                            fn (Query $query): array => $this->serializeQueryCacheQuery($query),
                            $queries,
                        ),
                        'limit' => $limit ?? 25,
                        'offset' => $offset ?? 0,
                        'orderAttributes' => $orderAttributes,
                        'orderTypes' => \array_map(
                            static fn (\Utopia\Query\OrderDirection $direction): string => $direction->value,
                            $orderTypes,
                        ),
                        'cursor' => $this->normalizeQueryCacheQueryValue($cursor),
                        'cursorDirection' => $cursorDirection->value,
                    ];

                    try {
                        $cacheEntry = $this->queryCache->getEntry(
                            $this->getQueryCacheScope(),
                            $collection->getId(),
                            $cacheQueries,
                            $cacheContext,
                        );

                        if ($cacheEntry !== null) {
                            $cached = $this->queryCache->get($cacheEntry);
                            if ($cached !== null) {
                                $results = $cached;
                                $cacheEntry = null;
                            } else {
                                $cacheGeneration = $this->queryCache->getGeneration($cacheEntry);
                            }
                        }
                    } catch (Exception $error) {
                        Console::warning('Warning: Failed to get query results from cache: '.$error->getMessage());
                        $cacheEntry = null;
                    }
                }
            }

            if (! isset($results)) {
                $adapterCollection = $this->withJoinAttributes($this->withJoinAuthorization($collection, $joinDocumentSecurity, $collectionGranted), $joins, $joinedCollectionsById);

                $find = fn (): array => $this->adapter->find(
                    $adapterCollection,
                    $queries,
                    $limit ?? 25,
                    $offset ?? 0,
                    $orderAttributes,
                    $orderTypes,
                    $cursor,
                    $cursorDirection,
                    $forPermission
                );
                $results = $skipAuth ? $this->authorization->skip($find) : $find();

                if ($cacheEntry !== null && $this->queryCache !== null) {
                    try {
                        if (! $this->isReadFromReplica()) {
                            $this->queryCache->set($cacheEntry, $results, $cacheGeneration);
                        }
                    } catch (Exception $error) {
                        Console::warning('Failed to save query results to cache: '.$error->getMessage());
                    }
                }
            }
        }

        if ($isAggregation) {
            $this->trigger(Event::DocumentFind, $results);

            return $results;
        }

        $hook = $this->relationshipHook;
        if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled() && ! empty($relationships) && (empty($selects) || ! empty($nestedSelections))) {
            if (count($results) > 0) {
                $results = $this->silent(fn () => $hook->populateDocuments($results, $collection, $hook->getFetchDepth(), $nestedSelections));
            }
        }

        $collectionId = $collection->getId();
        $hasCustomType = isset($this->documentTypes[$collectionId]);

        foreach ($this->castingAfterDocuments($collection, $results) as $index => $node) {
            $node = $this->casting($collection, $node);
            $node = $this->decode($collection, $node, $selections);
            if ($joinedCollections !== []) {
                $node = $this->decodeJoins($node, $joinedCollections);
                foreach ($outerJoinIds as $outerJoinId) {
                    $node->removeAttribute($outerJoinId);
                }
            }

            if ($hasCustomType) {
                $node = $this->createDocumentInstance($collectionId, $node->getArrayCopy());
            }

            if (! $node->isEmpty()) {
                $node->setAttribute(Document::COLLECTION, $collectionId);
            }

            $results[$index] = $node;
        }

        $results = $this->decorateDocuments(Event::DocumentFind, $collection, $results);

        if ($collection->getId() === self::METADATA) {
            foreach ($results as $index => $node) {
                $results[$index] = $this->toCollection($node);
            }
        }

        $this->trigger(Event::DocumentFind, $results);

        return $results;
    }

    /**
     * Execute a raw query bypassing the query builder. The statement runs as written, with no
     * permission or tenant scope, so like from() and execute() it runs only while authorization is
     * disabled: inside getAuthorization()->skip().
     *
     * @param string $query The raw query string
     * @param array<mixed> $bindings Parameter bindings
     * @return array<Document>
     *
     * @throws AuthorizationException While authorization is enabled
     * @throws DatabaseException
     */
    public function rawQuery(string $query, array $bindings = []): array
    {
        $this->requireSkippedAuthorization();

        if (! $this->adapterHasFeature(Feature\RawQuery::class)) {
            throw new DatabaseException('Raw queries are not supported by this adapter');
        }

        return $this->adapter->rawQuery($query, $bindings);
    }

    /**
     * Iterate documents in collection using a callback pattern.
     *
     * @param  string  $collection  The collection identifier
     * @param  callable(Document): void  $callback  Callback invoked for each matching document
     * @param  array<Query>  $queries  Queries for filtering, sorting, and pagination
     * @param  PermissionType  $forPermission  The permission type to check for authorization
     *
     * @throws DatabaseException
     */
    public function foreach(string $collection, callable $callback, array $queries = [], PermissionType $forPermission = PermissionType::Read): void
    {
        foreach ($this->iterate($collection, $queries, $forPermission) as $document) {
            $callback($document);
        }
    }

    /**
     * Return a generator yielding each document of the given collection that matches the given queries.
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Query>  $queries  Queries for filtering, sorting, and pagination
     * @param  PermissionType  $forPermission  The permission type to check for authorization
     * @return Generator<Document>
     *
     * @throws DatabaseException
     */
    public function iterate(string $collection, array $queries = [], PermissionType $forPermission = PermissionType::Read): Generator
    {
        $grouped = Query::groupForDatabase($queries);
        $limitExists = $grouped['limit'] !== null;
        $limit = $grouped['limit'] ?? 25;
        $offset = $grouped['offset'];

        $cursor = $grouped['cursor'];
        $cursorDirection = $grouped['cursorDirection'];

        // Cursor before is not supported
        if ($cursor !== null && $cursorDirection === CursorDirection::Before) {
            throw new DatabaseException('Cursor '.CursorDirection::Before->value.' not supported in this method.');
        }

        $sum = $limit;
        $latestDocument = null;
        $check = null;

        while ($sum === $limit) {
            $newQueries = $queries;
            if ($latestDocument !== null) {
                // reset offset and cursor as groupByType ignores same type query after first one is encountered
                if ($offset !== null) {
                    array_unshift($newQueries, Query::offset(0));
                }

                array_unshift($newQueries, Query::cursorAfter($latestDocument));
            }
            if (! $limitExists) {
                $newQueries[] = Query::limit($limit);
            }
            $results = $this->find($collection, $newQueries, $forPermission);

            if (empty($results)) {
                return;
            }

            $sum = count($results);
            $latestDocument = $results[array_key_last($results)];

            if ($sum === $limit) {
                $check ??= $this->nextPageCheck($collection, $queries);
                $check($latestDocument);
            }

            foreach ($results as $document) {
                yield $document;
            }
        }
    }

    /**
     * Find a single document matching the given queries.
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Query>  $queries  Queries for filtering
     * @return Document The matching document, or an empty Document if none found, which fires no event
     *
     * @throws DatabaseException
     */
    public function findOne(string $collection, array $queries = []): Document
    {
        $results = $this->silent(fn () => $this->find($collection, \array_merge([
            Query::limit(1),
        ], $queries)));

        $found = \reset($results);

        if ($found === false) {
            return new Document();
        }

        $this->trigger(Event::DocumentFind, $found);

        return $found;
    }

    /**
     * Count Documents
     *
     * Count the number of documents matching the given queries.
     *
     * @param  string  $collection  The collection identifier
     * @param  array<Query>  $queries  Queries for filtering
     * @param  int|null  $max  The most documents to count, greater than 0, or null for every match
     * @return int The document count
     *
     * @throws DatabaseException
     * @throws QueryException When $max is not greater than 0
     */
    public function count(string $collection, array $queries = [], ?int $max = null): int
    {
        $this->assertMax($max);

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        $joinedCollections = null;

        if ($this->validation()->get() && $queries !== []) {
            $joinedCollections = $this->resolveJoinedCollections($queries);
            $this->validateDocumentsQueries($collection, $queries, $joinedCollections);
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $collectionGranted = $this->authorization->isValid(new Input(PermissionType::Read, $collection->getRead()));

        if (! $collectionGranted && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        /** @var array<Document> $relationships */
        $relationships = \array_filter(
            $attributes,
            fn (Attribute|Document $attribute) => Attribute::isRelationship($attribute)
        );

        $prepared = $this->prepareFilterJoinQueries($collection, $queries, $relationships, $collectionGranted, $joinedCollections);
        if ($prepared === null) {
            return 0;
        }

        [$collection, $queries, $skipAuth] = $prepared;

        $getCount = fn () => $this->adapter->count($collection, $queries, $max);
        $count = $skipAuth ? $this->authorization->skip($getCount) : $getCount();

        $this->trigger(Event::DocumentCount, $count);

        return $count;
    }

    /**
     * Sum an attribute
     *
     * Sum an attribute for all matching documents.
     *
     * @param  string  $collection  The collection identifier
     * @param  string  $attribute  The attribute to sum
     * @param  array<Query>  $queries  Queries for filtering
     * @param  int|null  $max  The most documents to include in the sum, greater than 0, or null for every match
     * @return float|int The sum of the attribute values
     *
     * @throws DatabaseException
     * @throws QueryException When $max is not greater than 0
     */
    public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $this->assertMax($max);

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $indexes */
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        $joinedCollections = null;

        if ($this->validation()->get()) {
            $joinedCollections = $this->resolveJoinedCollections($queries);
            if ($queries !== []) {
                $this->validateDocumentsQueries($collection, $queries, $joinedCollections);
            }
            $this->validateSumAttribute($collection, $attribute, $queries, $joinedCollections);
        }

        if (! \str_contains($attribute, '.') && ! $this->declaresSumAttribute($collection, $attribute)) {
            $joinedCollections ??= $this->resolveJoinedCollections($queries);
            $attribute = $this->resolveSumAttribute($collection, $attribute, $queries, $joinedCollections);
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $collectionGranted = $this->authorization->isValid(new Input(PermissionType::Read, $collection->getRead()));

        if (! $collectionGranted && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        /** @var array<Document> $relationships */
        $relationships = \array_filter(
            $attributes,
            fn (Attribute|Document $attribute) => Attribute::isRelationship($attribute)
        );

        $prepared = $this->prepareFilterJoinQueries($collection, $queries, $relationships, $collectionGranted, $joinedCollections);
        if ($prepared === null) {
            return 0;
        }

        [$collection, $queries, $skipAuth] = $prepared;

        $getSum = fn () => $this->adapter->sum($collection, $attribute, $queries, $max);
        $sum = $skipAuth ? $this->authorization->skip($getSum) : $getSum();

        $this->trigger(Event::DocumentSum, $sum);

        return $sum;
    }

    /**
     * @throws QueryException
     */
    private function assertMax(?int $max): void
    {
        if ($max !== null && $max <= 0) {
            throw new QueryException('Max must be greater than 0');
        }
    }

    /**
     * A bare name the main collection does not declare reads the attribute of the one aliased join whose
     * collection declares it, as an aggregate in find() does. Any other name is returned as given, for the
     * validator to accept or refuse.
     *
     * @param  array<Query>  $queries
     * @param  array<string, Document>  $joinedCollections  The collection each join names, by its id
     *
     * @throws QueryException
     */
    private function resolveSumAttribute(Document $collection, string $attribute, array $queries, array $joinedCollections): string
    {
        try {
            return $this->qualifyJoinedAttribute($collection, $attribute, $this->aliasedJoinCollections($queries, $joinedCollections));
        } catch (QueryException $exception) {
            throw new QueryException('Invalid query: '.$exception->getMessage(), previous: $exception);
        }
    }

    /**
     * The collection each join given an alias reads, by that alias.
     *
     * @param  array<Query>  $queries
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array<string, Document>
     */
    private function aliasedJoinCollections(array $queries, ?array $joinedCollections = null): array
    {
        $joins = \array_values(\array_filter(
            $queries,
            static fn (Query $query): bool => $query->getMethod()->isJoin() && $query->getJoinAlias() !== '',
        ));

        return $this->joinedCollectionsByAlias($joins, $joinedCollections);
    }

    /**
     * `alias.name` for a bare name the main collection does not declare and exactly one of the joined collections
     * declares as a non-relationship attribute; any other name as given.
     *
     * @param  array<string, Document>  $joinedCollections  The collection each join reads, by its alias
     *
     * @throws QueryException  when several joined collections declare the name
     */
    private function qualifyJoinedAttribute(Document $collection, string $attribute, array $joinedCollections): string
    {
        if (\str_contains($attribute, '.') || $this->declaresSumAttribute($collection, $attribute)) {
            return $attribute;
        }

        $aliases = [];
        foreach ($joinedCollections as $alias => $joined) {
            /** @var array<Attribute|Document> $joinedAttributes */
            $joinedAttributes = $joined->getAttribute('attributes', []);
            foreach ($joinedAttributes as $declared) {
                if ($declared->getId() === $attribute && ! Attribute::isRelationship($declared)) {
                    $aliases[] = $alias;
                    break;
                }
            }
        }

        if (\count($aliases) > 1) {
            throw new QueryException('Attribute "'.$attribute.'" is ambiguous across joins; qualify it with a join alias');
        }

        return $aliases === [] ? $attribute : $aliases[0].'.'.$attribute;
    }

    private function declaresSumAttribute(Document $collection, string $attribute): bool
    {
        foreach (self::internalAttributesFor(true) as $internal) {
            if ($internal->getKey() === $attribute) {
                return true;
            }
        }

        /** @var array<Attribute|Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $declared) {
            if ($declared->getId() === $attribute) {
                return true;
            }
        }

        return false;
    }

    /**
     * sum() adds up what a sum aggregate adds up: a numeric attribute that is not an array, of the
     * main collection or, under a join alias, of the collection that join reads.
     *
     * @param  array<Query>  $queries
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     *
     * @throws QueryException
     */
    private function validateSumAttribute(Document $collection, string $attribute, array $queries, ?array $joinedCollections = null): void
    {
        /** @var array<Document> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $declared) {
            if ($declared->getAttribute('key', $declared->getId()) === $attribute) {
                if (isset(Aggregate::numericTypes([$declared])[$attribute])) {
                    return;
                }
                break;
            }
        }

        $supportForAttributes = $this->adapter->supports(Capability::DefinedAttributes);

        if (! \str_contains($attribute, '.') && $this->declaresSumAttribute($collection, $attribute)) {
            $validator = $this->getSumValidator($collection, $attributes, $supportForAttributes);
        } else {
            $validator = new Aggregate($attributes, $supportForAttributes, $this->adapter->getSharedTables());
            $joins = [];
            foreach ($this->aliasedJoinCollections($queries, $joinedCollections) as $alias => $joined) {
                $joins[] = JoinedCollection::of($alias, $joined);
            }
            $validator->allowJoins($joins);
        }

        if (! $validator->isValid(Query::sum($attribute))) {
            throw new QueryException('Invalid query: '.$validator->getDescription());
        }
    }

    /**
     * The aggregate validator of a sum of an attribute the collection declares, built once per
     * collection schema like the documents validators. It is never handed joins, so no state of
     * one sum reaches the next.
     *
     * @param  array<Document>  $attributes
     */
    private function getSumValidator(Document $collection, array $attributes, bool $supportForAttributes): Aggregate
    {
        $key = $this->getCollectionMetadataCacheKey($collection->getId())
            .'::'.(int) $supportForAttributes.(int) $this->adapter->getSharedTables()
            .'::'.$this->collectionFingerprint($collection);

        if (isset($this->sumValidatorCache[$key])) {
            return $this->sumValidatorCache[$key];
        }

        if (\count($this->sumValidatorCache) >= self::DOCUMENTS_VALIDATOR_CACHE_LIMIT) {
            $this->sumValidatorCache = [];
        }

        return $this->sumValidatorCache[$key] = new Aggregate($attributes, $supportForAttributes, $this->adapter->getSharedTables());
    }

    /**
     * Yield each document matching the queries, read in batches of $batchSize. A limit in the queries caps the
     * iteration, and an offset or a cursorAfter in them positions the first batch only.
     *
     * @param  array<Query>  $queries
     * @return Generator<int, Document>
     *
     * @throws DatabaseException
     */
    public function cursor(string $collection, array $queries = [], int $batchSize = 100): Generator
    {
        $grouped = Query::groupForDatabase($queries);
        $remaining = $grouped['limit'];
        $offset = $grouped['offset'];
        $cursor = $grouped['cursor'];

        if ($cursor !== null && $grouped['cursorDirection'] === CursorDirection::Before) {
            throw new DatabaseException('Cursor '.CursorDirection::Before->value.' not supported in this method.');
        }

        $queries = \array_values(\array_filter(
            $queries,
            static fn (Query $query): bool => ! \in_array($query->getMethod(), [Method::Limit, Method::Offset, Method::CursorAfter, Method::CursorBefore], true),
        ));
        $check = null;

        while ($remaining === null || $remaining > 0) {
            $size = $remaining === null ? $batchSize : \min($batchSize, $remaining);
            $page = [Query::limit($size)];
            if ($offset !== null) {
                $page[] = Query::offset($offset);
            }
            if ($cursor !== null) {
                $page[] = Query::cursorAfter($cursor);
            }

            $documents = $this->find($collection, [...$page, ...$queries]);
            $last = \end($documents);
            $pages = $last !== false && \count($documents) === $size && ($remaining === null || $remaining > $size);

            if ($pages) {
                $check ??= $this->nextPageCheck($collection, $queries);
                $check($last);
            }

            foreach ($documents as $document) {
                yield $document;
            }

            if (! $pages) {
                return;
            }

            if ($remaining !== null) {
                $remaining -= \count($documents);
            }
            $offset = null;
            $cursor = $last;
        }
    }

    /**
     * Execute aggregation queries (count, sum, avg, min, max, groupBy) and return results.
     *
     * @param  array<Query>  $queries  Must include at least one aggregation query (Query::count(), Query::sum(), etc.)
     * @return array<Document>
     */
    public function aggregate(string $collection, array $queries): array
    {
        return $this->find($collection, $queries);
    }

    /**
     * A vector index answers exactly one sort key, the distance: a tie break behind it makes the ordering unanswerable
     * from the index, so tie breaks are only added when a cursor needs a stable page boundary.
     *
     * A joined `$id` behind the main `$sequence` costs MariaDB and MySQL a sort of the whole join instead of reading
     * the main table in index order up to the limit, so it is added only where the rows show the join or a cursor
     * pages them (see showsJoinedRows()).
     *
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<Query>  $filters
     * @param  array<Query>  $joins
     * @param  array<string, Document>  $joinedCollections
     * @param  array<Query>  $selects
     * @return array{array<string>, array<OrderDirection>}
     */
    private function addTieBreaks(array $orderAttributes, array $orderTypes, array $filters, array $joins, array $joinedCollections, bool $paged, array $selects): array
    {
        $uniqueOrderBy = \in_array(Document::ID, $orderAttributes, true) || \in_array(Document::SEQUENCE, $orderAttributes, true);

        $vectorSearch = false;
        foreach ($filters as $filter) {
            if (\in_array($filter->getMethod(), [Method::VectorCosine, Method::VectorDot, Method::VectorEuclidean], true)) {
                $vectorSearch = true;
                break;
            }
        }

        if ($vectorSearch && ! $paged) {
            return [$orderAttributes, $orderTypes];
        }

        if (! $uniqueOrderBy) {
            $leadingAttribute = $orderAttributes[0] ?? null;
            $leadingOrderType = $orderTypes[0] ?? OrderDirection::Asc;

            $orderAttributes[] = Document::SEQUENCE;
            $orderTypes[] = \in_array($leadingAttribute, [Document::CREATED_AT, Document::UPDATED_AT], true)
                ? $leadingOrderType
                : OrderDirection::Asc;
        }

        if ($joins === [] || (! $paged && ! $this->showsJoinedRows($selects, $joinedCollections))) {
            return [$orderAttributes, $orderTypes];
        }

        $aliases = \array_keys($joinedCollections);
        if (\count($aliases) === \count($joins)) {
            foreach (\array_values($joins) as $position => $join) {
                $alias = $aliases[$position];
                $joinedId = $alias.'.'.Document::ID;
                if (
                    ! $this->joinMatchesAtMostOneRow($join, $alias)
                    && ! \in_array($joinedId, $orderAttributes, true)
                    && ! \in_array($alias.'.'.Document::SEQUENCE, $orderAttributes, true)
                ) {
                    $orderAttributes[] = $joinedId;
                    $orderTypes[] = OrderDirection::Asc;
                }
            }
        }

        return [$orderAttributes, $orderTypes];
    }

    /**
     * What find() checks of the cursor it is given for a joined or distinct read, run on a page's last row before the
     * page is yielded, so a read that cannot be paged fails before its caller acts on any row.
     *
     * @param  array<Query>  $queries
     * @return Closure(Document): void
     */
    private function nextPageCheck(string $collection, array $queries): Closure
    {
        $grouped = Query::groupForDatabase($queries);
        $joins = $grouped['joins'];
        $distinct = $grouped['distinct'];
        if (($joins === [] && ! $distinct) || $grouped['aggregations'] !== [] || $grouped['groupBy'] !== []) {
            return static function (Document $cursor): void {
            };
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $joinedCollections = $this->joinedCollectionsByAlias($joins, $this->resolveJoinedCollections($joins));
        $selects = $grouped['selections'];
        $filters = $grouped['filters'];
        $orderAttributes = $grouped['orderAttributes'];
        $orderTypes = $grouped['orderTypes'];

        return function (Document $cursor) use ($collection, $joins, $distinct, $joinedCollections, $selects, $filters, $orderAttributes, $orderTypes): void {
            $orders = $orderAttributes;
            if ($joinedCollections !== []) {
                [$orders, $cursor] = $this->qualifyJoinedOrders($collection, $orders, $cursor, $joinedCollections);
            }

            if ($distinct) {
                $this->assertDistinctCursorOrder($selects, $orders);
            } else {
                [$orders] = $this->addTieBreaks($orders, $orderTypes, $filters, $joins, $joinedCollections, true, $selects);
            }

            $this->assertCursorHasOrderValues($cursor, $orders);
        };
    }

    /**
     * Whether a read's rows show anything of its joins. Rows that select only main attributes are equal for every
     * joined row they pair the same main document with, so their order is not observable, and they lack the joined
     * `$id` a cursor over the join has to carry.
     *
     * @param  array<Query>  $selects
     * @param  array<string, Document>  $joinedCollections
     */
    private function showsJoinedRows(array $selects, array $joinedCollections): bool
    {
        if ($selects === []) {
            return true;
        }

        foreach ($selects as $select) {
            foreach ($select->getValues() as $value) {
                if (! \is_string($value)) {
                    continue;
                }
                if ($value === '*') {
                    return true;
                }
                $dot = \strpos($value, '.');
                if ($dot !== false && isset($joinedCollections[\substr($value, 0, $dot)])) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * An inner or left join on the joined `$id` pairs each row it joins onto with at most one joined row, so the rows
     * of the read are told apart without the joined id, and ordering by it would only cost the engine a sort.
     */
    private function joinMatchesAtMostOneRow(Query $join, string $alias): bool
    {
        if (! \in_array($join->getMethod(), [Method::Join, Method::LeftJoin], true) || $join->isNestedJoin()) {
            return false;
        }

        [$left, $operator, $right] = \array_pad($join->getValues(), 3, null);
        if ($operator !== '=' || ! \is_string($left) || ! \is_string($right)) {
            return false;
        }

        return ($right === Document::ID || $right === $alias.'.'.Document::ID) && ! \str_starts_with($left, $alias.'.');
    }

    /**
     * A distinct row has no id: only its order values tell it from the next one, so the order has to name every
     * attribute the read selects.
     *
     * @param  array<Query>  $selects
     * @param  array<string>  $orderAttributes
     *
     * @throws QueryException
     */
    private function assertDistinctCursorOrder(array $selects, array $orderAttributes): void
    {
        $selected = [];
        foreach ($selects as $select) {
            foreach ($select->getValues() as $value) {
                if (\is_string($value)) {
                    $selected[] = $value;
                }
            }
        }

        foreach ($selected as $attribute) {
            if (\str_ends_with($attribute, '*')) {
                $selected = [];
                break;
            }
        }

        if ($selected === [] || $orderAttributes === []) {
            throw new QueryException('A cursor on a distinct() read pages along its orders, so the read needs a select() of named attributes and an order on each of them');
        }

        foreach ($selected as $attribute) {
            if (! \in_array($attribute, $orderAttributes, true)) {
                throw new QueryException("A cursor on a distinct() read pages along its orders, so the read must order by every selected attribute, and '{$attribute}' is not ordered");
            }
        }
    }

    /**
     * A cursor on a bare order name only one join declares carries the value under `alias.name`, the
     * name the adapter orders by (SQL::qualifyJoinedOrders()), so the order is qualified here, before
     * the tie keys and the cursor checks read it. A cursor value under the bare name follows it. A
     * name several joins declare is refused rather than read from one of them.
     *
     * @param  array<string>  $orderAttributes
     * @param  array<string, Document>  $joinedCollections
     * @return array{array<string>, Document}
     *
     * @throws QueryException
     */
    private function qualifyJoinedOrders(Document $collection, array $orderAttributes, Document $cursor, array $joinedCollections): array
    {
        foreach ($orderAttributes as $index => $attribute) {
            $qualified = $this->qualifyJoinedAttribute($collection, $attribute, $joinedCollections);
            if ($qualified === $attribute) {
                continue;
            }

            $orderAttributes[$index] = $qualified;
            if ($cursor->offsetExists($attribute) && ! $cursor->offsetExists($qualified)) {
                $cursor = clone $cursor;
                $cursor->setAttribute($qualified, $cursor->getAttribute($attribute));
            }
        }

        return [$orderAttributes, $cursor];
    }

    /**
     * A cursor names the row it was read from by the values of the read's order, each under the name the read orders
     * by. A value it lacks is never taken from an attribute of the same name elsewhere in the document.
     *
     * @param  array<string>  $orderAttributes
     *
     * @throws OrderException
     */
    private function assertCursorHasOrderValues(Document $cursor, array $orderAttributes): void
    {
        $values = $cursor->getArrayCopy();
        foreach ($orderAttributes as $order) {
            if (! \array_key_exists($order, $values)) {
                throw new OrderException(
                    message: "Cursor has no value for order attribute '{$order}'. Use a row this read returned as the cursor, and select '{$order}' when the read selects attributes.",
                    attribute: $order,
                );
            }
        }
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     *
     * @throws QueryException
     */
    private function validateDocumentsQueries(Document $collection, array $queries, ?array $joinedCollections = null): void
    {
        $joinedCollections ??= $this->resolveJoinedCollections($queries);
        $validator = $this->getQueriesValidator($collection, $queries, $joinedCollections);

        if ($joinedCollections !== []) {
            $validator->setJoinedCollections($joinedCollections);
        }

        if (! $validator->isValid($queries)) {
            throw new QueryException($validator->getDescription());
        }
    }

    /**
     * The collections the join queries name, each loaded once, by the id the join names it with. A
     * read resolves them once and hands them to validation, authorization, the adapter and decoding.
     *
     * @param  array<Query>  $queries
     * @return array<string, Document>
     *
     * @throws QueryException
     */
    private function resolveJoinedCollections(array $queries): array
    {
        $collections = [];

        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $id = $query->getAttribute();
            if ($id === '' || isset($collections[$id])) {
                continue;
            }

            $collection = $this->silent(fn () => $this->getCollection($id));
            if ($collection->isEmpty()) {
                throw new QueryException("Joined collection '{$id}' not found");
            }

            $collections[$id] = $collection;
        }

        return $collections;
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<Document>  $relationships
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array{0: Document, 1: array<Query>, 2: bool}|null
     */
    private function prepareFilterJoinQueries(
        Document $collection,
        array $queries,
        array $relationships,
        bool $collectionGranted,
        ?array $joinedCollections = null,
    ): ?array {
        $grouped = Query::groupForDatabase($queries);
        $filters = $grouped['filters'];
        $joins = $grouped['joins'];

        if (! empty($joins)) {
            if (! $this->adapter->supports(Capability::Joins)) {
                throw new QueryException('Join queries are not supported by this adapter');
            }

            $this->assertJoinCount($joins);

            $joinedCollections ??= $this->resolveJoinedCollections($joins);
            $collection = $this->withJoinAuthorization(
                $collection,
                $this->authorizeJoins($joins, PermissionType::Read, $joinedCollections),
                $collectionGranted,
            );
        }

        $queries = \array_merge($filters, $joins);
        if ($queries !== []) {
            $queries = $this->convertQueries($collection, $queries, $this->joinedCollectionsByAlias($joins, $joinedCollections));
        }

        $convertedQueries = $this->relationshipHook !== null
            ? $this->relationshipHook->convertQueries($relationships, $queries, $collection)
            : $queries;

        if ($convertedQueries === null) {
            return null;
        }

        return [$collection, $convertedQueries, $collectionGranted && empty($joins)];
    }

    /**
     * The join cap the Join validator enforces holds without validation too: every join is one
     * more table the engine plans and the permission filters check.
     *
     * @param  array<Query>  $joins
     *
     * @throws QueryException
     */
    private function assertJoinCount(array $joins): void
    {
        $validator = new JoinValidator();
        if (! $validator->isValidCount(\count($joins))) {
            throw new QueryException($validator->getDescription());
        }
    }

    /**
     * Set on the collection handed to the adapter for a join read when the caller
     * holds the collection-level permission, so its rows are not filtered per document.
     */
    public const string COLLECTION_GRANTED = 'collectionGranted';

    /**
     * Maps each joined table to whether the adapter filters its rows per document.
     */
    public const string JOIN_DOCUMENT_SECURITY = 'joinDocumentSecurity';

    /**
     * Each joined collection is authorized once, however many joins read it.
     *
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array<string, bool>
     */
    private function authorizeJoins(array $joins, PermissionType $forPermission, ?array $joinedCollections = null): array
    {
        $joinedCollections ??= $this->resolveJoinedCollections($joins);
        $joinDocumentSecurity = [];
        $authorized = [];

        foreach ($joins as $joinQuery) {
            $joinCollectionId = $joinQuery->getAttribute();
            if (isset($authorized[$joinCollectionId])) {
                continue;
            }
            $authorized[$joinCollectionId] = true;

            $joinCollection = $joinedCollections[$joinCollectionId] ?? new Document();

            if ($joinCollection->isEmpty()) {
                throw new QueryException("Joined collection '{$joinCollectionId}' not found");
            }

            $granted = $this->authorization->isValid(new Input($forPermission, $joinCollection->getPermissionsByType($forPermission)));
            $documentSecurity = (bool) $joinCollection->getAttribute('documentSecurity', false);

            if (! $granted && ! $documentSecurity) {
                throw new AuthorizationException("Unauthorized access to joined collection '{$joinCollectionId}'");
            }

            foreach ($this->joinDocumentSecurityKeys($joinCollectionId, $joinCollection) as $key) {
                $joinDocumentSecurity[$key] = ! $granted;
            }
        }

        return $joinDocumentSecurity;
    }

    /**
     * @return list<string>
     */
    private function joinDocumentSecurityKeys(string $joinCollectionId, Document $joinCollection): array
    {
        $keys = [
            $joinCollectionId,
            $joinCollection->getId(),
            $this->adapter->filter($joinCollectionId),
            $this->adapter->filter($joinCollection->getId()),
        ];

        return \array_values(\array_unique(\array_filter(
            $keys,
            static fn (string $key): bool => $key !== '',
        )));
    }

    /**
     * @param  array<string, bool>  $joinDocumentSecurity
     */
    private function withJoinAuthorization(Document $collection, array $joinDocumentSecurity, bool $collectionGranted): Document
    {
        if ($joinDocumentSecurity === []) {
            return $collection;
        }

        $adapterCollection = clone $collection;
        $adapterCollection->setAttribute(self::COLLECTION_GRANTED, $collectionGranted);
        $adapterCollection->setAttribute(self::JOIN_DOCUMENT_SECURITY, $joinDocumentSecurity);

        return $adapterCollection;
    }

    /**
     * Maps each joined collection, as its join query names it, to the attributes a join without a
     * select returns under the join's alias.
     */
    public const string JOIN_ATTRIBUTES = 'joinAttributes';

    /**
     * Relationship attributes are left out: only some sides of a relationship have a column, and a
     * join does not populate related documents.
     *
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     */
    private function withJoinAttributes(Document $collection, array $joins, ?array $joinedCollections = null): Document
    {
        if ($joins === []) {
            return $collection;
        }

        $joinedCollections ??= $this->resolveJoinedCollections($joins);

        $joinAttributes = [];
        foreach ($joins as $join) {
            $joinCollectionId = $join->getAttribute();
            if (isset($joinAttributes[$joinCollectionId])) {
                continue;
            }

            $joinCollection = $joinedCollections[$joinCollectionId] ?? new Document();
            /** @var array<Attribute|Document> $attributes */
            $attributes = $joinCollection->getAttribute('attributes', []);
            $keys = [];
            foreach ($attributes as $attribute) {
                if (! Attribute::isRelationship($attribute)) {
                    $keys[] = $attribute->getId();
                }
            }
            $joinAttributes[$joinCollectionId] = $keys;
        }

        $adapterCollection = clone $collection;
        $adapterCollection->setAttribute(self::JOIN_ATTRIBUTES, $joinAttributes);

        return $adapterCollection;
    }

    /**
     * The collection each join reads, by the alias its values come back under: the alias the join
     * declares, or the one generated for it.
     *
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array<string, Document>
     */
    private function joinedCollectionsByAlias(array $joins, ?array $joinedCollections = null): array
    {
        if ($joins === []) {
            return [];
        }

        $joinedCollections ??= $this->resolveJoinedCollections($joins);
        $taken = [];
        foreach ($joins as $join) {
            $alias = $join->getJoinAlias();
            if ($alias !== '') {
                $taken[\strtolower($alias)] = true;
            }
        }

        $collections = [];
        foreach (\array_values($joins) as $position => $join) {
            $alias = $join->getJoinAlias();
            if ($alias === '') {
                $alias = Storage::joinAlias($position, $taken);
            }

            $collections[$alias] = $joinedCollections[$join->getAttribute()] ?? new Document();
        }

        return $collections;
    }

    /**
     * The `alias.$id` of each join whose attributes a select names without it or `alias.*`, when an
     * outer join can leave a joined row unmatched: the joined `$id` is what tells an unmatched row
     * from a matched one when the row is decoded, so it is selected for that and left out of the
     * result.
     *
     * @param  array<Query>  $selects
     * @param  array<Query>  $joins
     * @param  array<string, Document>  $joinedCollections  The collection each join alias reads
     * @return list<string>
     */
    private function outerJoinIdSelections(array $selects, array $joins, array $joinedCollections): array
    {
        if ($selects === [] || $joinedCollections === []) {
            return [];
        }

        $outer = false;
        foreach ($joins as $join) {
            if (\in_array($join->getMethod(), [Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin], true)) {
                $outer = true;
                break;
            }
        }
        if (! $outer) {
            return [];
        }

        $selectedAliases = [];
        foreach ($selects as $select) {
            foreach ($select->getValues() as $value) {
                if (! \is_string($value)) {
                    continue;
                }
                if ($value === '*') {
                    return [];
                }
                $dot = \strpos($value, '.');
                if ($dot !== false) {
                    $selectedAliases[\substr($value, 0, $dot)][\substr($value, $dot + 1)] = true;
                }
            }
        }

        $ids = [];
        foreach ($selectedAliases as $alias => $attributes) {
            if (isset($joinedCollections[$alias]) && ! isset($attributes[Document::ID]) && ! isset($attributes['*'])) {
                $ids[] = $alias.'.'.Document::ID;
            }
        }

        return $ids;
    }

    /**
     * @param  array<Query>  $queries
     * @return array<string>
     */
    private function validateSelections(Document $collection, array $queries): array
    {
        if (empty($queries)) {
            return [];
        }

        /** @var array<string> $selections */
        $selections = [];
        /** @var array<string> $relationshipSelections */
        $relationshipSelections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() == Method::Select) {
                foreach ($query->getValues() as $value) {
                    if (! \is_string($value)) {
                        throw new QueryException('Select queries must contain only string attributes.');
                    }

                    $strVal = $value;
                    if (\str_contains($strVal, '.')) {
                        $relationshipSelections[] = $strVal;

                        continue;
                    }
                    $selections[] = $strVal;
                }
            }
        }

        // Allow querying internal attributes
        /** @var array<string> $keys */
        $keys = \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $this->internalAttributes()
        );

        /** @var array<Document> $collAttrs */
        $collAttrs = $collection->getAttribute('attributes', []);
        foreach ($collAttrs as $attribute) {
            if (Attribute::isRelationship($attribute)) {
                continue;
            }
            /** @var string $attrKey */
            $attrKey = $attribute->getAttribute('key', $attribute->getId());
            $keys[] = $attrKey;
        }
        if ($this->adapter->supports(Capability::DefinedAttributes)) {
            $invalid = \array_diff($selections, $keys);
            if (! empty($invalid) && ! \in_array('*', $invalid)) {
                throw new QueryException('Cannot select attributes: '.\implode(', ', $invalid));
            }
        }

        $selections = \array_merge($selections, $relationshipSelections);

        $selections[] = Document::ID;
        $selections[] = Document::SEQUENCE;
        $selections[] = Document::COLLECTION;
        $selections[] = Document::CREATED_AT;
        $selections[] = Document::UPDATED_AT;
        $selections[] = Document::PERMISSIONS;

        return \array_values(\array_unique($selections));
    }

    /**
     * @param  array<mixed>  $queries
     *
     * @throws QueryException
     */
    private function checkQueryTypes(array $queries): void
    {
        foreach ($queries as $query) {
            if (! $query instanceof Query) {
                throw new QueryException('Invalid query type: "'.\gettype($query).'". Expected instances of "'.Query::class.'"');
            }

            if ($query->isNested()) {
                $this->checkQueryTypes($query->getValues());
            }
        }
    }

    private function castingBefore(Document $collection, Document $document): Document
    {
        if ($this->adapter->hasFeature(Feature\InternalCasting::class)) {
            /** @var Adapter&Feature\InternalCasting $adapter */
            $adapter = $this->adapter;

            return $adapter->castingBefore($collection, $document);
        }

        return $document;
    }

    private function castingAfter(Document $collection, Document $document): Document
    {
        if ($this->adapter->hasFeature(Feature\InternalCasting::class)) {
            /** @var Adapter&Feature\InternalCasting $adapter */
            $adapter = $this->adapter;

            return $adapter->castingAfter($collection, $document);
        }

        return $document;
    }

    /**
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    private function castingAfterDocuments(Document $collection, array $documents): array
    {
        if ($documents !== [] && $this->adapter->hasFeature(Feature\InternalCasting::class)) {
            /** @var Adapter&Feature\InternalCasting $adapter */
            $adapter = $this->adapter;

            return $adapter->castingAfterDocuments($collection, $documents);
        }

        return $documents;
    }
}
