<?php

namespace Utopia\Database\Traits;

use Exception;
use Throwable;
use Utopia\CLI\Console;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\CursorDirection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
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
use Utopia\Database\Operator;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Database\Validator\PartialStructure;
use Utopia\Database\Validator\Permissions;
use Utopia\Database\Validator\Queries\Document as DocumentValidator;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait Documents
{
    /**
     * @param  array<Document>  $documents
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    protected function refetchDocuments(Document $collection, array $documents): array
    {
        if (empty($documents)) {
            return $documents;
        }

        $docIds = array_map(fn ($doc) => $doc->getId(), $documents);

        // Fetch fresh copies with computed operator values
        $refetched = $this->getAuthorization()->skip(fn () => $this->silent(
            fn () => $this->find($collection->getId(), [Query::equal('$id', $docIds)])
        ));

        $refetchedMap = [];
        foreach ($refetched as $doc) {
            $refetchedMap[$doc->getId()] = $doc;
        }

        $result = [];
        foreach ($documents as $doc) {
            $result[] = $refetchedMap[$doc->getId()] ?? $doc;
        }

        return $result;
    }

    /**
     * Get Document
     *
     * @param  array<Query>  $queries
     *
     * @throws DatabaseException
     * @throws QueryException
     */
    public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        if ($collection === self::METADATA && $id === self::METADATA) {
            return new Document(self::COLLECTION);
        }

        if (empty($collection)) {
            throw new NotFoundException('Collection not found');
        }

        if (empty($id)) {
            return new Document;
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $attributes = $collection->getAttribute('attributes', []);

        $this->checkQueryTypes($queries);

        if ($this->validate) {
            $validator = new DocumentValidator($attributes, $this->adapter->supports(Capability::DefinedAttributes));
            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('type') === ColumnType::Relationship->value
        );

        $selects = Query::groupForDatabase($queries)['selections'];
        $selections = $this->validateSelections($collection, $selects);
        $nestedSelections = $this->relationshipHook?->processQueries($relationships, $queries) ?? [];

        $documentSecurity = $collection->getAttribute('documentSecurity', false);

        [$collectionKey, $documentKey, $hashKey] = $this->getCacheKeys(
            $collection->getId(),
            $id,
            $selections
        );

        try {
            $cached = $this->cache->load($documentKey, self::TTL, $hashKey);
        } catch (Exception $e) {
            Console::warning('Warning: Failed to get document from cache: '.$e->getMessage());
            $cached = null;
        }

        if ($cached) {
            $document = $this->createDocumentInstance($collection->getId(), $cached);

            if ($collection->getId() !== self::METADATA) {

                if (! $this->authorization->isValid(new Input(PermissionType::Read->value, [
                    ...$collection->getRead(),
                    ...($documentSecurity ? $document->getRead() : []),
                ]))) {
                    return $this->createDocumentInstance($collection->getId(), []);
                }
            }

            $this->trigger(self::EVENT_DOCUMENT_READ, $document);

            if ($this->isTtlExpired($collection, $document)) {
                return $this->createDocumentInstance($collection->getId(), []);
            }

            return $document;
        }

        $skipAuth = $collection->getId() !== self::METADATA
            && $this->authorization->isValid(new Input(PermissionType::Read->value, $collection->getRead()));

        $getDocument = fn () => $this->adapter->getDocument(
            $collection,
            $id,
            $queries,
            $forUpdate
        );

        $document = $skipAuth ? $this->authorization->skip($getDocument) : $getDocument();

        if ($document->isEmpty()) {
            return $this->createDocumentInstance($collection->getId(), []);
        }

        if ($this->isTtlExpired($collection, $document)) {
            return $this->createDocumentInstance($collection->getId(), []);
        }

        $document = $this->adapter->castingAfter($collection, $document);

        // Convert to custom document type if mapped
        if (isset($this->documentTypes[$collection->getId()])) {
            $document = $this->createDocumentInstance($collection->getId(), $document->getArrayCopy());
        }

        $document->setAttribute('$collection', $collection->getId());

        if ($collection->getId() !== self::METADATA) {
            if (! $this->authorization->isValid(new Input(PermissionType::Read->value, [
                ...$collection->getRead(),
                ...($documentSecurity ? $document->getRead() : []),
            ]))) {
                return $this->createDocumentInstance($collection->getId(), []);
            }
        }

        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document, $selections);

        // Skip relationship population if we're in batch mode (relationships will be populated later)
        if ($this->relationshipHook !== null && ! $this->relationshipHook->isInBatchPopulation() && $this->relationshipHook->isEnabled() && ! empty($relationships) && (empty($selects) || ! empty($nestedSelections))) {
            $documents = $this->silent(fn () => $this->relationshipHook->populateDocuments([$document], $collection, $this->relationshipHook->getFetchDepth(), $nestedSelections));
            $document = $documents[0];
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn ($attribute) => $attribute['type'] === ColumnType::Relationship->value
        );

        // Don't save to cache if it's part of a relationship
        if (empty($relationships)) {
            try {
                $this->cache->save($documentKey, $document->getArrayCopy(), $hashKey);
                $this->cache->save($collectionKey, 'empty', $documentKey);
            } catch (Exception $e) {
                Console::warning('Failed to save document to cache: '.$e->getMessage());
            }
        }

        $this->trigger(self::EVENT_DOCUMENT_READ, $document);

        return $document;
    }

    private function isTtlExpired(Document $collection, Document $document): bool
    {
        if (! $this->adapter->supports(Capability::TTLIndexes)) {
            return false;
        }
        foreach ($collection->getAttribute('indexes', []) as $index) {
            if ($index->getAttribute('type') !== IndexType::Ttl->value) {
                continue;
            }
            $ttlSeconds = (int) $index->getAttribute('ttl', 0);
            $ttlAttr = $index->getAttribute('attributes')[0] ?? null;
            if ($ttlSeconds <= 0 || ! $ttlAttr) {
                return false;
            }
            $val = $document->getAttribute($ttlAttr);
            if (is_string($val)) {
                try {
                    $start = new \DateTime($val);

                    return (new \DateTime) > (clone $start)->modify("+{$ttlSeconds} seconds");
                } catch (\Throwable) {
                    return false;
                }
            }
        }

        return false;
    }

    /**
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
                $attributesToKeep[$value] = true;
            }
        }

        // Early return if wildcard selector present
        if (isset($attributesToKeep['*'])) {
            return;
        }

        // Always preserve internal attributes (use hashmap for O(1) lookup)
        $internalKeys = \array_map(fn ($attr) => $attr['$id'], $this->getInternalAttributes());
        foreach ($internalKeys as $key) {
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
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function createDocument(string $collection, Document $document): Document
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

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() !== self::METADATA) {
            $isValid = $this->authorization->isValid(new Input(PermissionType::Create->value, $collection->getCreate()));
            if (! $isValid) {
                throw new AuthorizationException($this->authorization->getDescription());
            }
        }

        $time = DateTime::now();

        $createdAt = $document->getCreatedAt();
        $updatedAt = $document->getUpdatedAt();

        $document
            ->setAttribute('$id', empty($document->getId()) ? ID::unique() : $document->getId())
            ->setAttribute('$collection', $collection->getId())
            ->setAttribute('$createdAt', ($createdAt === null || ! $this->preserveDates) ? $time : $createdAt)
            ->setAttribute('$updatedAt', ($updatedAt === null || ! $this->preserveDates) ? $time : $updatedAt);

        if (empty($document->getPermissions())) {
            $document->setAttribute('$permissions', []);
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
                $document->setAttribute('$tenant', $this->adapter->getTenant());
            }
        }

        $document = $this->encode($collection, $document);

        if ($this->validate) {
            $validator = new Permissions;
            if (! $validator->isValid($document->getPermissions())) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        if ($this->validate) {
            $structure = new Structure(
                $collection,
                $this->adapter->getIdAttributeType(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes)
            );
            if (! $structure->isValid($document)) {
                throw new StructureException($structure->getDescription());
            }
        }

        $document = $this->adapter->castingBefore($collection, $document);

        $document = $this->withTransaction(function () use ($collection, $document) {
            $hook = $this->relationshipHook;
            if ($hook?->isEnabled()) {
                $document = $this->silent(fn () => $hook->afterDocumentCreate($collection, $document));
            }

            return $this->adapter->createDocument($collection, $document);
        });

        $hook = $this->relationshipHook;
        if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled()) {
            $fetchDepth = $hook->getWriteStackCount();
            $documents = $this->silent(fn () => $hook->populateDocuments([$document], $collection, $fetchDepth));
            $document = $this->adapter->castingAfter($collection, $documents[0]);
        }

        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document);

        // Convert to custom document type if mapped
        if (isset($this->documentTypes[$collection->getId()])) {
            $document = $this->createDocumentInstance($collection->getId(), $document->getArrayCopy());
        }

        $this->trigger(self::EVENT_DOCUMENT_CREATE, $document);

        return $document;
    }

    /**
     * Create Documents in a batch
     *
     * @param  array<Document>  $documents
     * @param  (callable(Document): void)|null  $onNext
     * @param  (callable(Throwable): void)|null  $onError
     *
     * @throws AuthorizationException
     * @throws StructureException
     * @throws \Throwable
     * @throws Exception
     */
    public function createDocuments(
        string $collection,
        array $documents,
        int $batchSize = self::INSERT_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        if (! $this->adapter->getSharedTables() && $this->adapter->getTenantPerDocument()) {
            throw new DatabaseException('Shared tables must be enabled if tenant per document is enabled.');
        }

        if (empty($documents)) {
            return 0;
        }

        $batchSize = \min(Database::INSERT_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($collection->getId() !== self::METADATA) {
            if (! $this->authorization->isValid(new Input(PermissionType::Create->value, $collection->getCreate()))) {
                throw new AuthorizationException($this->authorization->getDescription());
            }
        }

        $time = DateTime::now();
        $modified = 0;

        foreach ($documents as $document) {
            $createdAt = $document->getCreatedAt();
            $updatedAt = $document->getUpdatedAt();

            $document
                ->setAttribute('$id', empty($document->getId()) ? ID::unique() : $document->getId())
                ->setAttribute('$collection', $collection->getId())
                ->setAttribute('$createdAt', ($createdAt === null || ! $this->preserveDates) ? $time : $createdAt)
                ->setAttribute('$updatedAt', ($updatedAt === null || ! $this->preserveDates) ? $time : $updatedAt);

            if (empty($document->getPermissions())) {
                $document->setAttribute('$permissions', []);
            }

            if ($this->adapter->getSharedTables()) {
                if ($this->adapter->getTenantPerDocument()) {
                    if ($document->getTenant() === null) {
                        throw new DatabaseException('Missing tenant. Tenant must be set when tenant per document is enabled.');
                    }
                } else {
                    $document->setAttribute('$tenant', $this->adapter->getTenant());
                }
            }

            $document = $this->encode($collection, $document);

            if ($this->validate) {
                $validator = new Structure(
                    $collection,
                    $this->adapter->getIdAttributeType(),
                    $this->adapter->getMinDateTime(),
                    $this->adapter->getMaxDateTime(),
                    $this->adapter->supports(Capability::DefinedAttributes)
                );
                if (! $validator->isValid($document)) {
                    throw new StructureException($validator->getDescription());
                }
            }

            if ($this->relationshipHook?->isEnabled()) {
                $document = $this->silent(fn () => $this->relationshipHook->afterDocumentCreate($collection, $document));
            }

            $document = $this->adapter->castingBefore($collection, $document);
        }

        foreach (\array_chunk($documents, $batchSize) as $chunk) {
            $batch = $this->withTransaction(function () use ($collection, $chunk) {
                return $this->adapter->createDocuments($collection, $chunk);
            });

            $batch = $this->adapter->getSequences($collection->getId(), $batch);

            $hook = $this->relationshipHook;
            if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled()) {
                $batch = $this->silent(fn () => $hook->populateDocuments($batch, $collection, $hook->getFetchDepth()));
            }

            foreach ($batch as $document) {
                $document = $this->adapter->castingAfter($collection, $document);
                $document = $this->casting($collection, $document);
                $document = $this->decode($collection, $document);

                try {
                    $onNext && $onNext($document);
                } catch (\Throwable $e) {
                    $onError ? $onError($e) : throw $e;
                }

                $modified++;
            }
        }

        $this->trigger(self::EVENT_DOCUMENTS_CREATE, new Document([
            '$collection' => $collection->getId(),
            'modified' => $modified,
        ]));

        return $modified;
    }

    /**
     * Update Document
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
        $document = $this->withTransaction(function () use ($collection, $id, $document, $newUpdatedAt) {
            $time = DateTime::now();
            $old = $this->authorization->skip(fn () => $this->silent(
                fn () => $this->getDocument($collection->getId(), $id, forUpdate: true)
            ));
            if ($old->isEmpty()) {
                return new Document;
            }

            $skipPermissionsUpdate = true;

            if ($document->offsetExists('$permissions')) {
                $originalPermissions = $old->getPermissions();
                $currentPermissions = $document->getPermissions();

                sort($originalPermissions);
                sort($currentPermissions);

                $skipPermissionsUpdate = ($originalPermissions === $currentPermissions);
            }
            $createdAt = $document->getCreatedAt();

            $document = \array_merge($old->getArrayCopy(), $document->getArrayCopy());
            $document['$collection'] = $old->getAttribute('$collection'); // Make sure user doesn't switch collection ID
            $document['$createdAt'] = ($createdAt === null || ! $this->preserveDates) ? $old->getCreatedAt() : $createdAt;

            if ($this->adapter->getSharedTables()) {
                $document['$tenant'] = $old->getTenant(); // Make sure user doesn't switch tenant
            }
            $document = new Document($document);

            $relationships = \array_filter($collection->getAttribute('attributes', []), function ($attribute) {
                return $attribute['type'] === ColumnType::Relationship->value;
            });

            $shouldUpdate = false;

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                foreach ($relationships as $relationship) {
                    $relationships[$relationship->getAttribute('key')] = $relationship;
                }

                foreach ($document as $key => $value) {
                    if (Operator::isOperator($value)) {
                        $shouldUpdate = true;
                        break;
                    }
                }

                // Compare if the document has any changes
                foreach ($document as $key => $value) {
                    if (\array_key_exists($key, $relationships)) {
                        if ($this->relationshipHook !== null && $this->relationshipHook->getWriteStackCount() >= Database::RELATION_MAX_DEPTH - 1) {
                            continue;
                        }

                        $relationType = (string) $relationships[$key]['options']['relationType'];
                        $side = (string) $relationships[$key]['options']['side'];
                        switch ($relationType) {
                            case RelationType::OneToOne->value:
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
                            case RelationType::OneToMany->value:
                            case RelationType::ManyToOne->value:
                            case RelationType::ManyToMany->value:
                                if (
                                    ($relationType === RelationType::ManyToOne->value && $side === RelationSide::Parent->value) ||
                                    ($relationType === RelationType::OneToMany->value && $side === RelationSide::Child->value)
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

                                if (\count($old->getAttribute($key)) !== \count($value)) {
                                    $shouldUpdate = true;
                                    break;
                                }

                                foreach ($value as $index => $relation) {
                                    $oldValue = $old->getAttribute($key)[$index] instanceof Document
                                        ? $old->getAttribute($key)[$index]->getId()
                                        : $old->getAttribute($key)[$index];

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
                    if ($value !== $oldValue) {
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
                    if (! $this->authorization->isValid(new Input(PermissionType::Update->value, $updatePermissions))) {
                        throw new AuthorizationException($this->authorization->getDescription());
                    }
                } else {
                    if (! $this->authorization->isValid(new Input(PermissionType::Read->value, $readPermissions))) {
                        throw new AuthorizationException($this->authorization->getDescription());
                    }
                }
            }

            if ($shouldUpdate) {
                $document->setAttribute('$updatedAt', ($newUpdatedAt === null || ! $this->preserveDates) ? $time : $newUpdatedAt);
            }

            // Check if document was updated after the request timestamp
            $oldUpdatedAt = new \DateTime($old->getUpdatedAt());
            if (! is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
                throw new ConflictException('Document was updated after the request timestamp');
            }

            $document = $this->encode($collection, $document);

            if ($this->validate) {
                $structureValidator = new Structure(
                    $collection,
                    $this->adapter->getIdAttributeType(),
                    $this->adapter->getMinDateTime(),
                    $this->adapter->getMaxDateTime(),
                    $this->adapter->supports(Capability::DefinedAttributes),
                    $old
                );
                if (! $structureValidator->isValid($document)) { // Make sure updated structure still apply collection rules (if any)
                    throw new StructureException($structureValidator->getDescription());
                }
            }

            if ($this->relationshipHook?->isEnabled()) {
                $document = $this->silent(fn () => $this->relationshipHook->afterDocumentUpdate($collection, $old, $document));
            }

            $document = $this->adapter->castingBefore($collection, $document);

            $this->authorization->skip(fn () => $this->adapter->updateDocument($collection, $id, $document, $skipPermissionsUpdate));

            $document = $this->adapter->castingAfter($collection, $document);

            $this->purgeCachedDocument($collection->getId(), $id);

            if ($document->getId() !== $id) {
                $this->purgeCachedDocument($collection->getId(), $document->getId());
            }

            // If operators were used, refetch document to get computed values
            $hasOperators = false;
            foreach ($document->getArrayCopy() as $value) {
                if (Operator::isOperator($value)) {
                    $hasOperators = true;
                    break;
                }
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

        $document = $this->decode($collection, $document);

        // Convert to custom document type if mapped
        if (isset($this->documentTypes[$collection->getId()])) {
            $document = $this->createDocumentInstance($collection->getId(), $document->getArrayCopy());
        }

        $this->trigger(self::EVENT_DOCUMENT_UPDATE, $document);

        return $document;
    }

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param  array<Query>  $queries
     * @param  (callable(Document $updated, Document $old): void)|null  $onNext
     * @param  (callable(Throwable): void)|null  $onError
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DuplicateException
     * @throws QueryException
     * @throws StructureException
     * @throws TimeoutException
     * @throws \Throwable
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
        if ($updates->isEmpty()) {
            return 0;
        }

        $batchSize = \min(Database::INSERT_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Update->value, $collection->getUpdate()));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validate) {
            $validator = new DocumentsValidator(
                $attributes,
                $indexes,
                $this->adapter->getIdAttributeType(),
                $this->maxQueryValues,
                $this->adapter->getMaxUIDLength(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes)
            );

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

        unset($updates['$id']);
        unset($updates['$tenant']);

        if (($updates->getCreatedAt() === null || ! $this->preserveDates)) {
            unset($updates['$createdAt']);
        } else {
            $updates['$createdAt'] = $updates->getCreatedAt();
        }

        if ($this->adapter->getSharedTables()) {
            $updates['$tenant'] = $this->adapter->getTenant();
        }

        $updatedAt = $updates->getUpdatedAt();
        $updates['$updatedAt'] = ($updatedAt === null || ! $this->preserveDates) ? DateTime::now() : $updatedAt;

        $updates = $this->encode(
            $collection,
            $updates,
            applyDefaults: false
        );

        if ($this->validate) {
            $validator = new PartialStructure(
                $collection,
                $this->adapter->getIdAttributeType(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes),
                null // No old document available in bulk updates
            );

            if (! $validator->isValid($updates)) {
                throw new StructureException($validator->getDescription());
            }
        }

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
                forPermission: PermissionType::Update->value
            ));

            if (empty($batch)) {
                break;
            }

            $old = array_map(fn ($doc) => clone $doc, $batch);
            $currentPermissions = $updates->getPermissions();
            sort($currentPermissions);

            $this->withTransaction(function () use ($collection, $updates, &$batch, $currentPermissions) {
                foreach ($batch as $index => $document) {
                    $skipPermissionsUpdate = true;

                    if ($updates->offsetExists('$permissions')) {
                        if (! $document->offsetExists('$permissions')) {
                            throw new QueryException('Permission document missing in select');
                        }

                        $originalPermissions = $document->getPermissions();

                        \sort($originalPermissions);

                        $skipPermissionsUpdate = ($originalPermissions === $currentPermissions);
                    }

                    $document->setAttribute('$skipPermissionsUpdate', $skipPermissionsUpdate);

                    $new = new Document(\array_merge($document->getArrayCopy(), $updates->getArrayCopy()));

                    $hook = $this->relationshipHook;
                    if ($hook?->isEnabled()) {
                        $this->silent(fn () => $hook->afterDocumentUpdate($collection, $document, $new));
                    }

                    $document = $new;

                    // Check if document was updated after the request timestamp
                    try {
                        $oldUpdatedAt = new \DateTime($document->getUpdatedAt());
                    } catch (Exception $e) {
                        throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                    }

                    if (! is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
                        throw new ConflictException('Document was updated after the request timestamp');
                    }
                    $encoded = $this->encode($collection, $document);
                    $batch[$index] = $this->adapter->castingBefore($collection, $encoded);
                }

                $this->adapter->updateDocuments(
                    $collection,
                    $updates,
                    $batch
                );
            });

            $updates = $this->adapter->castingBefore($collection, $updates);

            $hasOperators = false;
            foreach ($updates->getArrayCopy() as $value) {
                if (Operator::isOperator($value)) {
                    $hasOperators = true;
                    break;
                }
            }

            if ($hasOperators) {
                $batch = $this->refetchDocuments($collection, $batch);
            }

            foreach ($batch as $index => $doc) {
                $doc = $this->adapter->castingAfter($collection, $doc);
                $doc->removeAttribute('$skipPermissionsUpdate');
                $this->purgeCachedDocument($collection->getId(), $doc->getId());
                $doc = $this->decode($collection, $doc);
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

            $last = \end($batch);
        }

        $this->trigger(self::EVENT_DOCUMENTS_UPDATE, new Document([
            '$collection' => $collection->getId(),
            'modified' => $modified,
        ]));

        return $modified;
    }

    /**
     * Create or update a single document.
     *
     * @throws StructureException
     * @throws \Throwable
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
     * @param  array<Document>  $documents
     * @param  (callable(Document, ?Document): void)|null  $onNext
     * @param  (callable(Throwable): void)|null  $onError
     *
     * @throws StructureException
     * @throws \Throwable
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
     * @param  array<Document>  $documents
     * @param  (callable(Document, ?Document): void)|null  $onNext
     * @param  (callable(Throwable): void)|null  $onError
     *
     * @throws StructureException
     * @throws \Throwable
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
        if (empty($documents)) {
            return 0;
        }

        $batchSize = \min(Database::INSERT_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $collectionAttributes = $collection->getAttribute('attributes', []);
        $time = DateTime::now();
        $created = 0;
        $updated = 0;
        $seenIds = [];
        foreach ($documents as $key => $document) {
            if ($this->getSharedTables() && $this->getTenantPerDocument()) {
                $old = $this->authorization->skip(fn () => $this->withTenant($document->getTenant(), fn () => $this->silent(fn () => $this->getDocument(
                    $collection->getId(),
                    $document->getId(),
                ))));
            } else {
                $old = $this->authorization->skip(fn () => $this->silent(fn () => $this->getDocument(
                    $collection->getId(),
                    $document->getId(),
                )));
            }

            // Extract operators early to avoid comparison issues
            $documentArray = $document->getArrayCopy();
            $extracted = Operator::extractOperators($documentArray);
            $operators = $extracted['operators'];
            $regularUpdates = $extracted['updates'];

            $internalKeys = \array_map(
                fn ($attr) => $attr['$id'],
                self::INTERNAL_ATTRIBUTES
            );

            $regularUpdatesUserOnly = \array_diff_key($regularUpdates, \array_flip($internalKeys));

            $skipPermissionsUpdate = true;

            if ($document->offsetExists('$permissions')) {
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
                        fn ($attr) => $attr['$id'],
                        self::INTERNAL_ATTRIBUTES
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
                if (! $this->authorization->isValid(new Input(PermissionType::Create->value, $collection->getCreate()))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            } elseif (! $this->authorization->isValid(new Input(PermissionType::Update->value, [
                ...$collection->getUpdate(),
                ...($documentSecurity ? $old->getUpdate() : []),
            ]))) {
                throw new AuthorizationException($this->authorization->getDescription());
            }

            $updatedAt = $document->getUpdatedAt();

            $document
                ->setAttribute('$id', empty($document->getId()) ? ID::unique() : $document->getId())
                ->setAttribute('$collection', $collection->getId())
                ->setAttribute('$updatedAt', ($updatedAt === null || ! $this->preserveDates) ? $time : $updatedAt);

            if (! $this->preserveSequence) {
                $document->removeAttribute('$sequence');
            }

            $createdAt = $document->getCreatedAt();
            if ($createdAt === null || ! $this->preserveDates) {
                $document->setAttribute('$createdAt', $old->isEmpty() ? $time : $old->getCreatedAt());
            } else {
                $document->setAttribute('$createdAt', $createdAt);
            }

            // Force matching optional parameter sets
            // Doesn't use decode as that intentionally skips null defaults to reduce payload size
            foreach ($collectionAttributes as $attr) {
                if (! $attr->getAttribute('required') && ! \array_key_exists($attr['$id'], (array) $document)) {
                    $document->setAttribute(
                        $attr['$id'],
                        $old->getAttribute($attr['$id'], ($attr['default'] ?? null))
                    );
                }
            }

            if ($skipPermissionsUpdate) {
                $document->setAttribute('$permissions', $old->getPermissions());
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
                    $document->setAttribute('$tenant', $this->adapter->getTenant());
                }
            }

            $document = $this->encode($collection, $document);

            if ($this->validate) {
                $validator = new Structure(
                    $collection,
                    $this->adapter->getIdAttributeType(),
                    $this->adapter->getMinDateTime(),
                    $this->adapter->getMaxDateTime(),
                    $this->adapter->supports(Capability::DefinedAttributes),
                    $old->isEmpty() ? null : $old
                );

                if (! $validator->isValid($document)) {
                    throw new StructureException($validator->getDescription());
                }
            }

            if (! $old->isEmpty()) {
                // Check if document was updated after the request timestamp
                try {
                    $oldUpdatedAt = new \DateTime($old->getUpdatedAt());
                } catch (Exception $e) {
                    throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                }

                if (! \is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
                    throw new ConflictException('Document was updated after the request timestamp');
                }
            }

            $hook = $this->relationshipHook;
            if ($hook?->isEnabled()) {
                $document = $this->silent(fn () => $hook->afterDocumentCreate($collection, $document));
            }

            $seenIds[] = $document->getId();
            $old = $this->adapter->castingBefore($collection, $old);
            $document = $this->adapter->castingBefore($collection, $document);

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
            $batch = $this->withTransaction(fn () => $this->authorization->skip(fn () => $this->adapter->upsertDocuments(
                $collection,
                $attribute,
                $chunk
            )));

            $batch = $this->adapter->getSequences($collection->getId(), $batch);

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

            // Check if any document in the batch contains operators
            $hasOperators = false;
            foreach ($batch as $doc) {
                $extracted = Operator::extractOperators($doc->getArrayCopy());
                if (! empty($extracted['operators'])) {
                    $hasOperators = true;
                    break;
                }
            }

            if ($hasOperators) {
                $batch = $this->refetchDocuments($collection, $batch);
            }

            foreach ($batch as $index => $doc) {
                $doc = $this->adapter->castingAfter($collection, $doc);
                if (! $hasOperators) {
                    $doc = $this->decode($collection, $doc);
                }

                if ($this->getSharedTables() && $this->getTenantPerDocument()) {
                    $this->withTenant($doc->getTenant(), function () use ($collection, $doc) {
                        $this->purgeCachedDocument($collection->getId(), $doc->getId());
                    });
                } else {
                    $this->purgeCachedDocument($collection->getId(), $doc->getId());
                }

                $old = $chunk[$index]->getOld();

                if (! $old->isEmpty()) {
                    $old = $this->adapter->castingAfter($collection, $old);
                }

                try {
                    $onNext && $onNext($doc, $old->isEmpty() ? null : $old);
                } catch (\Throwable $th) {
                    $onError ? $onError($th) : throw $th;
                }
            }
        }

        $this->trigger(self::EVENT_DOCUMENTS_UPSERT, new Document([
            '$collection' => $collection->getId(),
            'created' => $created,
            'updated' => $updated,
        ]));

        return $created + $updated;
    }

    /**
     * Increase a document attribute by a value
     *
     * @param  string  $collection  The collection ID
     * @param  string  $id  The document ID
     * @param  string  $attribute  The attribute to increase
     * @param  int|float  $value  The value to increase the attribute by, can be a float
     * @param  int|float|null  $max  The maximum value the attribute can reach after the increase, null means no limit
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws LimitException
     * @throws NotFoundException
     * @throws TypeException
     * @throws \Throwable
     */
    public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value = 1,
        int|float|null $max = null
    ): Document {
        if ($value <= 0) { // Can be a float
            throw new \InvalidArgumentException('Value must be numeric and greater than 0');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($this->adapter->supports(Capability::DefinedAttributes)) {
            $attr = \array_filter($collection->getAttribute('attributes', []), function ($a) use ($attribute) {
                return $a['$id'] === $attribute;
            });

            if (empty($attr)) {
                throw new NotFoundException('Attribute not found');
            }

            $whiteList = [
                ColumnType::Integer->value,
                ColumnType::Double->value,
            ];

            /** @var Document $attr */
            $attr = \end($attr);
            if (! \in_array($attr->getAttribute('type'), $whiteList) || $attr->getAttribute('array')) {
                throw new TypeException('Attribute must be an integer or float and can not be an array.');
            }
        }

        $document = $this->withTransaction(function () use ($collection, $id, $attribute, $value, $max) {
            /* @var $document Document */
            $document = $this->authorization->skip(fn () => $this->silent(fn () => $this->getDocument($collection->getId(), $id, forUpdate: true))); // Skip ensures user does not need read permission for this

            if ($document->isEmpty()) {
                throw new NotFoundException('Document not found');
            }

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                if (! $this->authorization->isValid(new Input(PermissionType::Update->value, [
                    ...$collection->getUpdate(),
                    ...($documentSecurity ? $document->getUpdate() : []),
                ]))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            }

            if (! \is_null($max) && ($document->getAttribute($attribute) + $value > $max)) {
                throw new LimitException('Attribute value exceeds maximum limit: '.$max);
            }

            $time = DateTime::now();
            $updatedAt = $document->getUpdatedAt();
            $updatedAt = (empty($updatedAt) || ! $this->preserveDates) ? $time : $updatedAt;
            $max = $max ? $max - $value : null;

            $this->adapter->increaseDocumentAttribute(
                $collection->getId(),
                $id,
                $attribute,
                $value,
                $updatedAt,
                max: $max
            );

            return $document->setAttribute(
                $attribute,
                $document->getAttribute($attribute) + $value
            );
        });

        $this->purgeCachedDocument($collection->getId(), $id);

        $this->trigger(self::EVENT_DOCUMENT_INCREASE, $document);

        return $document;
    }

    /**
     * Decrease a document attribute by a value
     *
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     */
    public function decreaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value = 1,
        int|float|null $min = null
    ): Document {
        if ($value <= 0) { // Can be a float
            throw new \InvalidArgumentException('Value must be numeric and greater than 0');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($this->adapter->supports(Capability::DefinedAttributes)) {
            $attr = \array_filter($collection->getAttribute('attributes', []), function ($a) use ($attribute) {
                return $a['$id'] === $attribute;
            });

            if (empty($attr)) {
                throw new NotFoundException('Attribute not found');
            }

            $whiteList = [
                ColumnType::Integer->value,
                ColumnType::Double->value,
            ];

            /**
             * @var Document $attr
             */
            $attr = \end($attr);
            if (! \in_array($attr->getAttribute('type'), $whiteList) || $attr->getAttribute('array')) {
                throw new TypeException('Attribute must be an integer or float and can not be an array.');
            }
        }

        $document = $this->withTransaction(function () use ($collection, $id, $attribute, $value, $min) {
            /* @var $document Document */
            $document = $this->authorization->skip(fn () => $this->silent(fn () => $this->getDocument($collection->getId(), $id, forUpdate: true))); // Skip ensures user does not need read permission for this

            if ($document->isEmpty()) {
                throw new NotFoundException('Document not found');
            }

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                if (! $this->authorization->isValid(new Input(PermissionType::Update->value, [
                    ...$collection->getUpdate(),
                    ...($documentSecurity ? $document->getUpdate() : []),
                ]))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            }

            if (! \is_null($min) && ($document->getAttribute($attribute) - $value < $min)) {
                throw new LimitException('Attribute value exceeds minimum limit: '.$min);
            }

            $time = DateTime::now();
            $updatedAt = $document->getUpdatedAt();
            $updatedAt = (empty($updatedAt) || ! $this->preserveDates) ? $time : $updatedAt;
            $min = $min ? $min + $value : null;

            $this->adapter->increaseDocumentAttribute(
                $collection->getId(),
                $id,
                $attribute,
                $value * -1,
                $updatedAt,
                min: $min
            );

            return $document->setAttribute(
                $attribute,
                $document->getAttribute($attribute) - $value
            );
        });

        $this->purgeCachedDocument($collection->getId(), $id);

        $this->trigger(self::EVENT_DOCUMENT_DECREASE, $document);

        return $document;
    }

    /**
     * Delete Document
     *
     *
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        $deleted = $this->withTransaction(function () use ($collection, $id, &$document) {
            $document = $this->authorization->skip(fn () => $this->silent(
                fn () => $this->getDocument($collection->getId(), $id, forUpdate: true)
            ));

            if ($document->isEmpty()) {
                return false;
            }

            if ($collection->getId() !== self::METADATA) {
                $documentSecurity = $collection->getAttribute('documentSecurity', false);

                if (! $this->authorization->isValid(new Input(PermissionType::Delete->value, [
                    ...$collection->getDelete(),
                    ...($documentSecurity ? $document->getDelete() : []),
                ]))) {
                    throw new AuthorizationException($this->authorization->getDescription());
                }
            }

            // Check if document was updated after the request timestamp
            try {
                $oldUpdatedAt = new \DateTime($document->getUpdatedAt());
            } catch (Exception $e) {
                throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
            }

            if (! \is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
                throw new ConflictException('Document was updated after the request timestamp');
            }

            if ($this->relationshipHook?->isEnabled()) {
                $document = $this->silent(fn () => $this->relationshipHook->beforeDocumentDelete($collection, $document));
            }

            $result = $this->authorization->skip(fn () => $this->adapter->deleteDocument($collection->getId(), $id));

            $this->purgeCachedDocument($collection->getId(), $id);

            return $result;
        });

        if ($deleted) {
            $this->trigger(self::EVENT_DOCUMENT_DELETE, $document);
        }

        return $deleted;
    }

    /**
     * Delete Documents
     *
     * Deletes all documents which match the given query, will respect the relationship's onDelete optin.
     *
     * @param  array<Query>  $queries
     * @param  (callable(Document, Document): void)|null  $onNext
     * @param  (callable(Throwable): void)|null  $onError
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws \Throwable
     */
    public function deleteDocuments(
        string $collection,
        array $queries = [],
        int $batchSize = self::DELETE_BATCH_SIZE,
        ?callable $onNext = null,
        ?callable $onError = null,
    ): int {
        if ($this->adapter->getSharedTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $batchSize = \min(Database::DELETE_BATCH_SIZE, \max(1, $batchSize));
        $collection = $this->silent(fn () => $this->getCollection($collection));
        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Delete->value, $collection->getDelete()));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validate) {
            $validator = new DocumentsValidator(
                $attributes,
                $indexes,
                $this->adapter->getIdAttributeType(),
                $this->maxQueryValues,
                $this->adapter->getMaxUIDLength(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes)
            );

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
                forPermission: PermissionType::Delete->value
            ));

            if (empty($batch)) {
                break;
            }

            $old = array_map(fn ($doc) => clone $doc, $batch);
            $sequences = [];
            $permissionIds = [];

            $this->withTransaction(function () use ($collection, $sequences, $permissionIds, $batch) {
                foreach ($batch as $document) {
                    $sequences[] = $document->getSequence();
                    if (! empty($document->getPermissions())) {
                        $permissionIds[] = $document->getId();
                    }

                    if ($this->relationshipHook?->isEnabled()) {
                        $document = $this->silent(fn () => $this->relationshipHook->beforeDocumentDelete(
                            $collection,
                            $document
                        ));
                    }

                    // Check if document was updated after the request timestamp
                    try {
                        $oldUpdatedAt = new \DateTime($document->getUpdatedAt());
                    } catch (Exception $e) {
                        throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
                    }

                    if (! \is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
                        throw new ConflictException('Document was updated after the request timestamp');
                    }
                }

                $this->adapter->deleteDocuments(
                    $collection->getId(),
                    $sequences,
                    $permissionIds
                );
            });

            foreach ($batch as $index => $document) {
                if ($this->getSharedTables() && $this->getTenantPerDocument()) {
                    $this->withTenant($document->getTenant(), function () use ($collection, $document) {
                        $this->purgeCachedDocument($collection->getId(), $document->getId());
                    });
                } else {
                    $this->purgeCachedDocument($collection->getId(), $document->getId());
                }
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

        $this->trigger(self::EVENT_DOCUMENTS_DELETE, new Document([
            '$collection' => $collection->getId(),
            'modified' => $modified,
        ]));

        return $modified;
    }

    /**
     * Cleans the all the collection's documents from the cache
     * And the all related cached documents.
     */
    public function purgeCachedCollection(string $collectionId): bool
    {
        [$collectionKey] = $this->getCacheKeys($collectionId);

        $documentKeys = $this->cache->list($collectionKey);
        foreach ($documentKeys as $documentKey) {
            $this->cache->purge($documentKey);
        }

        $this->cache->purge($collectionKey);

        return true;
    }

    /**
     * Cleans a specific document from cache
     * And related document reference in the collection cache.
     *
     * @throws Exception
     */
    protected function purgeCachedDocumentInternal(string $collectionId, ?string $id): bool
    {
        if ($id === null) {
            return true;
        }

        [$collectionKey, $documentKey] = $this->getCacheKeys($collectionId, $id);

        $this->cache->purge($collectionKey, $documentKey);
        $this->cache->purge($documentKey);

        return true;
    }

    /**
     * Cleans a specific document from cache and triggers EVENT_DOCUMENT_PURGE.
     * And related document reference in the collection cache.
     *
     * Note: Do not retry this method as it triggers events. Use purgeCachedDocumentInternal() with retry instead.
     *
     * @throws Exception
     */
    public function purgeCachedDocument(string $collectionId, ?string $id): bool
    {
        $result = $this->purgeCachedDocumentInternal($collectionId, $id);

        if ($id !== null) {
            $this->trigger(self::EVENT_DOCUMENT_PURGE, new Document([
                '$id' => $id,
                '$collection' => $collectionId,
            ]));
        }

        return $result;
    }

    /**
     * Find Documents
     *
     * @param  array<Query>  $queries
     * @return array<Document>
     *
     * @throws DatabaseException
     * @throws QueryException
     * @throws TimeoutException
     * @throws Exception
     */
    public function find(string $collection, array $queries = [], string $forPermission = PermissionType::Read->value): array
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new NotFoundException('Collection not found');
        }

        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validate) {
            $validator = new DocumentsValidator(
                $attributes,
                $indexes,
                $this->adapter->getIdAttributeType(),
                $this->maxQueryValues,
                $this->adapter->getMaxUIDLength(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes)
            );
            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input($forPermission, $collection->getPermissionsByType($forPermission)));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('type') === ColumnType::Relationship->value
        );

        $grouped = Query::groupForDatabase($queries);
        $filters = $grouped['filters'];
        $selects = $grouped['selections'];
        $limit = $grouped['limit'];
        $offset = $grouped['offset'];
        $orderAttributes = $grouped['orderAttributes'];
        $orderTypes = $grouped['orderTypes'];
        $cursor = $grouped['cursor'];
        $cursorDirection = $grouped['cursorDirection'] ?? CursorDirection::After->value;

        $uniqueOrderBy = false;
        foreach ($orderAttributes as $order) {
            if ($order === '$id' || $order === '$sequence') {
                $uniqueOrderBy = true;
            }
        }

        if ($uniqueOrderBy === false) {
            $orderAttributes[] = '$sequence';
        }

        if (! empty($cursor)) {
            foreach ($orderAttributes as $order) {
                if ($cursor->getAttribute($order) === null) {
                    throw new OrderException(
                        message: "Order attribute '{$order}' is empty",
                        attribute: $order
                    );
                }
            }
        }

        if (! empty($cursor) && $cursor->getCollection() !== $collection->getId()) {
            throw new DatabaseException('cursor Document must be from the same Collection.');
        }

        if (! empty($cursor)) {
            $cursor = $this->encode($collection, $cursor);
            $cursor = $this->adapter->castingBefore($collection, $cursor);
            $cursor = $cursor->getArrayCopy();
        } else {
            $cursor = [];
        }

        /** @var array<Query> $queries */
        $queries = \array_merge(
            $selects,
            $this->convertQueries($collection, $filters)
        );

        $selections = $this->validateSelections($collection, $selects);
        $nestedSelections = $this->relationshipHook?->processQueries($relationships, $queries) ?? [];

        // Convert relationship filter queries to SQL-level subqueries
        $convertedQueries = $this->relationshipHook !== null
            ? $this->relationshipHook->convertQueries($relationships, $queries, $collection)
            : $queries;

        // If conversion returns null, it means no documents can match (relationship filter found no matches)
        if ($convertedQueries === null) {
            $results = [];
        } else {
            $queries = $convertedQueries;

            $getResults = fn () => $this->adapter->find(
                $collection,
                $queries,
                $limit ?? 25,
                $offset ?? 0,
                $orderAttributes,
                $orderTypes,
                $cursor,
                $cursorDirection,
                $forPermission
            );

            $results = $skipAuth ? $this->authorization->skip($getResults) : $getResults();
        }

        $hook = $this->relationshipHook;
        if ($hook !== null && ! $hook->isInBatchPopulation() && $hook->isEnabled() && ! empty($relationships) && (empty($selects) || ! empty($nestedSelections))) {
            if (count($results) > 0) {
                $results = $this->silent(fn () => $hook->populateDocuments($results, $collection, $hook->getFetchDepth(), $nestedSelections));
            }
        }

        foreach ($results as $index => $node) {
            $node = $this->adapter->castingAfter($collection, $node);
            $node = $this->casting($collection, $node);
            $node = $this->decode($collection, $node, $selections);

            // Convert to custom document type if mapped
            if (isset($this->documentTypes[$collection->getId()])) {
                $node = $this->createDocumentInstance($collection->getId(), $node->getArrayCopy());
            }

            if (! $node->isEmpty()) {
                $node->setAttribute('$collection', $collection->getId());
            }

            $results[$index] = $node;
        }

        $this->trigger(self::EVENT_DOCUMENT_FIND, $results);

        return $results;
    }

    /**
     * Helper method to iterate documents in collection using callback pattern
     * Alterative is
     *
     * @param  array<Query>  $queries
     *
     * @throws \Utopia\Database\Exception
     */
    public function foreach(string $collection, callable $callback, array $queries = [], string $forPermission = PermissionType::Read->value): void
    {
        foreach ($this->iterate($collection, $queries, $forPermission) as $document) {
            $callback($document);
        }
    }

    /**
     * Return each document of the given collection
     * that matches the given queries
     *
     * @param  array<Query>  $queries
     *
     * @throws \Utopia\Database\Exception
     */
    public function iterate(string $collection, array $queries = [], string $forPermission = PermissionType::Read->value): \Generator
    {
        $grouped = Query::groupForDatabase($queries);
        $limitExists = $grouped['limit'] !== null;
        $limit = $grouped['limit'] ?? 25;
        $offset = $grouped['offset'];

        $cursor = $grouped['cursor'];
        $cursorDirection = $grouped['cursorDirection'];

        // Cursor before is not supported
        if ($cursor !== null && $cursorDirection === CursorDirection::Before->value) {
            throw new DatabaseException('Cursor '.CursorDirection::Before->value.' not supported in this method.');
        }

        $sum = $limit;
        $latestDocument = null;

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

            foreach ($results as $document) {
                yield $document;
            }

            $latestDocument = $results[array_key_last($results)];
        }
    }

    /**
     * @param  array<Query>  $queries
     *
     * @throws DatabaseException
     */
    public function findOne(string $collection, array $queries = []): Document
    {
        $results = $this->silent(fn () => $this->find($collection, \array_merge([
            Query::limit(1),
        ], $queries)));

        $found = \reset($results);

        $this->trigger(self::EVENT_DOCUMENT_FIND, $found);

        if (! $found) {
            return new Document;
        }

        return $found;
    }

    /**
     * Count Documents
     *
     * Count the number of documents.
     *
     * @param  array<Query>  $queries
     *
     * @throws DatabaseException
     */
    public function count(string $collection, array $queries = [], ?int $max = null): int
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validate) {
            $validator = new DocumentsValidator(
                $attributes,
                $indexes,
                $this->adapter->getIdAttributeType(),
                $this->maxQueryValues,
                $this->adapter->getMaxUIDLength(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes)
            );
            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Read->value, $collection->getRead()));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('type') === ColumnType::Relationship->value
        );

        $queries = Query::groupForDatabase($queries)['filters'];
        $queries = $this->convertQueries($collection, $queries);

        $convertedQueries = $this->relationshipHook !== null
            ? $this->relationshipHook->convertQueries($relationships, $queries, $collection)
            : $queries;

        if ($convertedQueries === null) {
            return 0;
        }

        $queries = $convertedQueries;

        $getCount = fn () => $this->adapter->count($collection, $queries, $max);
        $count = $skipAuth ? $this->authorization->skip($getCount) : $getCount();

        $this->trigger(self::EVENT_DOCUMENT_COUNT, $count);

        return $count;
    }

    /**
     * Sum an attribute
     *
     * Sum an attribute for all the documents. Pass $max=0 for unlimited count
     *
     * @param  array<Query>  $queries
     *
     * @throws DatabaseException
     */
    public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $this->checkQueryTypes($queries);

        if ($this->validate) {
            $validator = new DocumentsValidator(
                $attributes,
                $indexes,
                $this->adapter->getIdAttributeType(),
                $this->maxQueryValues,
                $this->adapter->getMaxUIDLength(),
                $this->adapter->getMinDateTime(),
                $this->adapter->getMaxDateTime(),
                $this->adapter->supports(Capability::DefinedAttributes)
            );
            if (! $validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $this->authorization->isValid(new Input(PermissionType::Read->value, $collection->getRead()));

        if (! $skipAuth && ! $documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($this->authorization->getDescription());
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('type') === ColumnType::Relationship->value
        );

        $queries = $this->convertQueries($collection, $queries);
        $convertedQueries = $this->relationshipHook !== null
            ? $this->relationshipHook->convertQueries($relationships, $queries, $collection)
            : $queries;

        // If conversion returns null, it means no documents can match (relationship filter found no matches)
        if ($convertedQueries === null) {
            return 0;
        }

        $queries = $convertedQueries;

        $getSum = fn () => $this->adapter->sum($collection, $attribute, $queries, $max);
        $sum = $skipAuth ? $this->authorization->skip($getSum) : $getSum();

        $this->trigger(self::EVENT_DOCUMENT_SUM, $sum);

        return $sum;
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

        $selections = [];
        $relationshipSelections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() == Query::TYPE_SELECT) {
                foreach ($query->getValues() as $value) {
                    if (\str_contains($value, '.')) {
                        $relationshipSelections[] = $value;

                        continue;
                    }
                    $selections[] = $value;
                }
            }
        }

        // Allow querying internal attributes
        $keys = \array_map(
            fn ($attribute) => $attribute['$id'],
            $this->getInternalAttributes()
        );

        foreach ($collection->getAttribute('attributes', []) as $attribute) {
            if ($attribute['type'] !== ColumnType::Relationship->value) {
                // Fallback to $id when key property is not present in metadata table for some tables such as Indexes or Attributes
                $keys[] = $attribute['key'] ?? $attribute['$id'];
            }
        }
        if ($this->adapter->supports(Capability::DefinedAttributes)) {
            $invalid = \array_diff($selections, $keys);
            if (! empty($invalid) && ! \in_array('*', $invalid)) {
                throw new QueryException('Cannot select attributes: '.\implode(', ', $invalid));
            }
        }

        $selections = \array_merge($selections, $relationshipSelections);

        $selections[] = '$id';
        $selections[] = '$sequence';
        $selections[] = '$collection';
        $selections[] = '$createdAt';
        $selections[] = '$updatedAt';
        $selections[] = '$permissions';

        return \array_values(\array_unique($selections));
    }

    /**
     * @param  array<Query>  $queries
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
}
