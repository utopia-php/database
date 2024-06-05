<?php

namespace Utopia\Database;

use Utopia\Cache\Cache;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Mirroring\Filter;
use Utopia\Database\Validator\Authorization;

class Mirror extends Database
{
    protected Database $destination;

    /**
     * Filters to apply to documents before writing to the destination database
     *
     * @var array<Filter>
     */
    protected array $writeFilters = [];

    /**
     * Collections that should only be present in the source database
     */
    protected const SOURCE_ONLY_COLLECTIONS = [
        'upgrades',
    ];

    /**
     * Callbacks to run when an error occurs on the destination database
     *
     * @var array<callable(string, \Throwable): void>
     */
    protected array $errorCallbacks = [];

    /**
     * @param Adapter $adapter
     * @param Cache $cache
     * @param Database $destination
     * @param array<Filter> $filters
     */
    public function __construct(
        Adapter $adapter,
        Cache $cache,
        Database $destination,
        array $filters = [],
    ) {
        parent::__construct($adapter, $cache);
        $this->destination = $destination;
        $this->writeFilters = $filters;
    }

    public function getDestination(): ?Database
    {
        return $this->destination;
    }

    public function getSource(): Database
    {
        return $this;
    }

    /**
     * @param callable(string, \Throwable): void $callback
     * @return void
     */
    public function onError(callable $callback): void
    {
        $this->errorCallbacks[] = $callback;
    }

    /**
     * @param string $method
     * @param array<mixed> $args
     * @return mixed
     */
    protected function delegate(string $method, array $args = []): mixed
    {
        $result = parent::{$method}(...$args);

        try {
            $result = $this->destination->{$method}(...$args);
        } catch (\Throwable $err) {
            $this->logError($method, $err);
        }

        return $result;
    }

    public function enableValidation(): self
    {
        return $this->delegate('enableValidation');
    }

    public function disableValidation(): self
    {
        return $this->delegate('disableValidation');
    }

    public function delete(?string $database = null): bool
    {
        return $this->delegate('delete', [$database]);
    }

    public function create(?string $database = null): bool
    {
        return $this->delegate('create', [$database]);
    }

    public function createCollection(string $id, array $attributes = [], array $indexes = [], array $permissions = null, bool $documentSecurity = true): Document
    {
        $result = parent::createCollection(
            $id,
            $attributes,
            $indexes,
            $permissions,
            $documentSecurity
        );

        try {
            $this->destination->createCollection(
                $id,
                $attributes,
                $indexes,
                $permissions,
                $documentSecurity
            );

            $this->createUpgrades();

            parent::createDocument('upgrades', new Document([
                '$id' => $id,
                'collectionId' => $id,
                'status' => 'upgraded'
            ]));
        } catch (\Throwable $err) {
            $this->logError('createCollection', $err);
        }
        return $result;
    }

    public function updateCollection(string $id, array $permissions, bool $documentSecurity): Document
    {
        return $this->delegate('updateCollection', [$id, $permissions, $documentSecurity]);
    }

    public function deleteCollection(string $id): bool
    {
        return $this->delegate('deleteCollection', [$id]);
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $required, $default = null, bool $signed = true, bool $array = false, string $format = null, array $formatOptions = [], array $filters = []): bool
    {
        return $this->delegate('createAttribute', [$collection, $id, $type, $size, $required, $default, $signed, $array, $format, $formatOptions, $filters]);
    }

    public function updateAttribute(string $collection, string $id, string $type = null, int $size = null, bool $required = null, mixed $default = null, bool $signed = null, bool $array = null, string $format = null, ?array $formatOptions = null, ?array $filters = null): Document
    {
        return $this->delegate('updateAttribute', [$collection, $id, $type, $size, $required, $default, $signed, $array, $format, $formatOptions, $filters]);
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        return $this->delegate('deleteAttribute', [$collection, $id]);
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths = [], array $orders = []): bool
    {
        return $this->delegate('createIndex', [$collection, $id, $type, $attributes, $lengths, $orders]);
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        return $this->delegate('deleteIndex', [$collection, $id]);
    }

    public function createDocument(string $collection, Document $document): Document
    {
        $document = parent::createDocument($collection, $document);

        if (\in_array($collection, self::SOURCE_ONLY_COLLECTIONS)) {
            return $document;
        }

        $upgrade = $this->getUpgradeStatus($collection);
        if ($upgrade->getAttribute('status', '') !== 'upgraded') {
            return $document;
        }

        try {
            $clone = clone $document;

            foreach ($this->writeFilters as $filter) {
                $clone = $filter->onCreateDocument(
                    source: $this,
                    destination: $this->destination,
                    collection: $collection,
                    document: $clone,
                );
            }

            $this->destination->setPreserveDates(true);
            $this->destination->createDocument($collection, $clone);
            $this->destination->setPreserveDates(false);
        } catch (\Throwable $err) {
            $this->logError('createDocument', $err);
        }

        return $document;
    }

    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        $document = parent::updateDocument($collection, $id, $document);

        $upgrade = $this->getUpgradeStatus($collection);
        if ($upgrade->getAttribute('status', '') !== 'upgraded') {
            return $document;
        }

        try {
            $clone = clone $document;

            foreach ($this->writeFilters as $filter) {
                $clone = $filter->onUpdateDocument(
                    source: $this,
                    destination: $this->destination,
                    collection: $collection,
                    document: $clone,
                );
            }

            if (!$this->destination->getDocument($collection, $id)->isEmpty()) {
                $this->destination->setPreserveDates(true);
                $this->destination->updateDocument($collection, $id, $clone);
                $this->destination->setPreserveDates(false);
            }
        } catch (\Throwable $err) {
            $this->logError('updateDocument', $err);
        }

        return $document;
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        $result = parent::deleteDocument($collection, $id);

        $upgrade = $this->getUpgradeStatus($collection);
        if ($upgrade->getAttribute('status', '') !== 'upgraded') {
            return $result;
        }

        try {
            $this->destination->deleteDocument($collection, $id);

            foreach ($this->writeFilters as $filter) {
                $filter->onDeleteDocument(
                    source: $this,
                    destination: $this->destination,
                    collection: $collection,
                );
            }
        } catch (\Throwable $err) {
            $this->logError('deleteDocument', $err);
        }

        return $result;
    }

    public function updateAttributeRequired(string $collection, string $id, bool $required): Document
    {
        return $this->delegate('updateAttributeRequired', [$collection, $id, $required]);
    }

    public function updateAttributeFormat(string $collection, string $id, string $format): Document
    {
        return $this->delegate('updateAttributeFormat', [$collection, $id, $format]);
    }

    public function updateAttributeFormatOptions(string $collection, string $id, array $formatOptions): Document
    {
        return $this->delegate('updateAttributeFormatOptions', [$collection, $id, $formatOptions]);
    }

    public function updateAttributeFilters(string $collection, string $id, array $filters): Document
    {
        return $this->delegate('updateAttributeFilters', [$collection, $id, $filters]);
    }

    public function updateAttributeDefault(string $collection, string $id, mixed $default = null): Document
    {
        return $this->delegate('updateAttributeDefault', [$collection, $id, $default]);
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        return $this->delegate('renameAttribute', [$collection, $old, $new]);
    }

    public function createRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay = false,
        ?string $id = null,
        ?string $twoWayKey = null,
        string $onDelete = Database::RELATION_MUTATE_RESTRICT
    ): bool {
        return $this->delegate('createRelationship', [$collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey, $onDelete]);
    }

    public function updateRelationship(
        string $collection,
        string $id,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
        ?bool $twoWay = null,
        ?string $onDelete = null
    ): bool {
        return $this->delegate('updateRelationship', [$collection, $id, $newKey, $newTwoWayKey, $twoWay, $onDelete]);
    }

    public function deleteRelationship(string $collection, string $id): bool
    {
        return $this->delegate('deleteRelationship', [$collection, $id]);
    }


    public function renameIndex(string $collection, string $old, string $new): bool
    {
        return $this->delegate('renameIndex', [$collection, $old, $new]);
    }

    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value = 1, int|float|null $max = null): bool
    {
        return $this->delegate('increaseDocumentAttribute', [$collection, $id, $attribute, $value, $max]);
    }

    public function decreaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value = 1, int|float|null $min = null): bool
    {
        return $this->delegate('decreaseDocumentAttribute', [$collection, $id, $attribute, $value, $min]);
    }

    /**
     * @throws Limit
     * @throws DuplicateException
     * @throws Exception
     */
    public function createUpgrades(): void
    {
        try {
            parent::createCollection(
                id: 'upgrades',
                attributes: [
                    new Document([
                        '$id' => ID::custom('collectionId'),
                        'type' => Database::VAR_STRING,
                        'size' => Database::LENGTH_KEY,
                        'required' => true,
                        'signed' => true,
                        'array' => false,
                        'filters' => [],
                        'default' => null,
                        'format' => ''
                    ]),
                    new Document([
                        '$id' => ID::custom('status'),
                        'type' => Database::VAR_STRING,
                        'size' => Database::LENGTH_KEY,
                        'required' => false,
                        'signed' => true,
                        'array' => false,
                        'filters' => [],
                        'default' => null,
                        'format' => ''
                    ]),
                ],
                indexes: [
                    new Document([
                        '$id' => ID::custom('_unique_collection'),
                        'type' => Database::INDEX_UNIQUE,
                        'attributes' => ['collectionId'],
                        'lengths' => [Database::LENGTH_KEY],
                        'orders' => [],
                    ]),
                    new Document([
                        '$id' => ID::custom('_status_index'),
                        'type' => Database::INDEX_KEY,
                        'attributes' => ['status'],
                        'lengths' => [Database::LENGTH_KEY],
                        'orders' => [Database::ORDER_ASC],
                    ]),
                ],
            );
        } catch (DuplicateException) {
            // Ignore
        }
    }

    /**
     * @throws Exception
     */
    protected function getUpgradeStatus(string $collection): Document
    {
        if ($collection === 'upgrades' || $collection === Database::METADATA) {
            return new Document([]);
        }

        return Authorization::skip(function () use ($collection) {
            return $this->getDocument('upgrades', $collection);
        });
    }

    protected function logError(string $action, \Throwable $err): void
    {
        foreach ($this->errorCallbacks as $callback) {
            $callback($action, $err);
        }
    }
}
