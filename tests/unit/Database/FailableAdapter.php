<?php

namespace Tests\Unit\Database;

use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;

/**
 * A test double that extends SQLite and supports injecting failures
 * at specific adapter-level method calls for testing recovery paths.
 */
class FailableAdapter extends SQLite
{
    /**
     * Queue of exceptions to throw on next call to each method.
     * @var array<string, list<\Throwable>>
     */
    private array $failureQueues = [];

    /**
     * Count of calls per method for assertions.
     * @var array<string, int>
     */
    private array $callCounts = [];

    /**
     * When > 0, adapter-level updateDocument for _metadata collection will throw.
     */
    private int $failMetadataUpdates = 0;

    /**
     * When > 0, adapter-level createDocument for _metadata collection will throw.
     */
    private int $failMetadataCreates = 0;

    /**
     * When > 0, adapter-level deleteDocument for _metadata collection will throw.
     */
    private int $failMetadataDeletes = 0;

    /**
     * Enqueue a failure for the next call to $method.
     * Multiple calls queue multiple failures (FIFO).
     */
    public function failOnNext(string $method, \Throwable $exception): void
    {
        $this->failureQueues[$method][] = $exception;
    }

    /**
     * Make metadata updates (updateDocument on _metadata) fail $times times.
     */
    public function failMetadataUpdates(int $times = 10): void
    {
        $this->failMetadataUpdates = $times;
    }

    /**
     * Make metadata creates (createDocument on _metadata) fail $times times.
     */
    public function failMetadataCreates(int $times = 10): void
    {
        $this->failMetadataCreates = $times;
    }

    /**
     * Make metadata deletes (deleteDocument on _metadata) fail $times times.
     */
    public function failMetadataDeletes(int $times = 10): void
    {
        $this->failMetadataDeletes = $times;
    }

    /**
     * Clear all injected failures.
     */
    public function clearFailures(): void
    {
        $this->failureQueues = [];
        $this->failMetadataUpdates = 0;
        $this->failMetadataCreates = 0;
        $this->failMetadataDeletes = 0;
    }

    /**
     * Get number of times $method was called.
     */
    public function getCallCount(string $method): int
    {
        return $this->callCounts[$method] ?? 0;
    }

    /**
     * Reset all call counts.
     */
    public function resetCallCounts(): void
    {
        $this->callCounts = [];
    }

    private function track(string $method): void
    {
        $this->callCounts[$method] = ($this->callCounts[$method] ?? 0) + 1;
    }

    private function checkQueue(string $method): void
    {
        if (!empty($this->failureQueues[$method])) {
            $exception = \array_shift($this->failureQueues[$method]);
            if (empty($this->failureQueues[$method])) {
                unset($this->failureQueues[$method]);
            }
            throw $exception;
        }
    }

    // --- Schema methods ---

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::createCollection($name, $attributes, $indexes);
    }

    public function deleteCollection(string $id): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::deleteCollection($id);
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::createAttribute($collection, $id, $type, $size, $signed, $array, $required);
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::createAttributes($collection, $attributes);
    }

    public function deleteAttribute(string $collection, string $id, bool $array = false): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::deleteAttribute($collection, $id, $array);
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::renameAttribute($collection, $old, $new);
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::createIndex($collection, $id, $type, $attributes, $lengths, $orders, $indexAttributeTypes, $collation, $ttl);
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::deleteIndex($collection, $id);
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::renameIndex($collection, $old, $new);
    }

    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::createRelationship($collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey);
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::deleteRelationship($collection, $relatedCollection, $type, $twoWay, $key, $twoWayKey, $side);
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);
        return parent::updateRelationship($collection, $relatedCollection, $type, $twoWay, $key, $twoWayKey, $side, $newKey, $newTwoWayKey);
    }

    // --- Document methods (with metadata-specific failure support) ---

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);

        if ($this->failMetadataUpdates > 0 && $collection->getId() === '_metadata') {
            $this->failMetadataUpdates--;
            throw new DatabaseException('Simulated metadata update failure');
        }

        return parent::updateDocument($collection, $id, $document, $skipPermissions);
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);

        if ($this->failMetadataCreates > 0 && $collection->getId() === '_metadata') {
            $this->failMetadataCreates--;
            throw new DatabaseException('Simulated metadata create failure');
        }

        return parent::createDocument($collection, $document);
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        $this->track(__FUNCTION__);
        $this->checkQueue(__FUNCTION__);

        if ($this->failMetadataDeletes > 0 && $collection === '_metadata') {
            $this->failMetadataDeletes--;
            throw new DatabaseException('Simulated metadata delete failure');
        }

        return parent::deleteDocument($collection, $id);
    }
}
