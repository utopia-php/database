<?php

namespace Utopia\Database\Traits;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;

/**
 * Provides database-level operations including creation, existence checks, listing, renaming and deletion.
 */
trait Databases
{
    /**
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     */
    public function create(?string $database = null): bool
    {
        $database ??= $this->adapter->getDatabase();

        $this->adapter->create($database);

        $this->silent(fn () => $this->createCollection(self::collectionDefinition()));

        $listeners = $this->listens(Event::DatabaseCreate);
        if ($listeners !== []) {
            $this->dispatch(new Event\Database\Created($database), $listeners);
        }

        return true;
    }

    /**
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     */
    public function exists(?string $database = null): bool
    {
        return $this->adapter->exists($database ?? $this->adapter->getDatabase());
    }

    /**
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     */
    public function collectionExists(string $collection, ?string $database = null): bool
    {
        return $this->adapter->collectionExists($database ?? $this->adapter->getDatabase(), $collection);
    }

    /**
     * @return array<Document>
     */
    public function list(): array
    {
        $databases = $this->adapter->list();

        $listeners = $this->listens(Event::DatabaseList);
        if ($listeners !== []) {
            $this->dispatch(new Event\Database\Listed(\array_values($databases)), $listeners);
        }

        return $databases;
    }

    /**
     * Renames a database. Under shared tables a database holds other tenants' data, so renaming it is refused.
     * The definitions and documents cached under either name are retired, and a database that was current
     * stays current under its new name.
     *
     * @throws DatabaseException
     * @throws DuplicateException when a database is already named $new
     * @throws NotFoundException when no database is named $database
     */
    public function update(string $database, string $new): bool
    {
        if ($this->adapter->hasSharedTables()) {
            throw new DatabaseException('Cannot rename a database while shared tables are enabled');
        }

        $collections = $this->adapter->exists($database)
            ? $this->inDatabase($database, $this->getCollectionIds(...))
            : [];

        $updated = $this->adapter->update($database, $new);

        foreach ([$database, $new] as $name) {
            $this->inDatabase($name, fn () => $this->purgeCachedCollections($collections));
        }

        if ($this->adapter->getDatabase() === $this->adapter->filter($database)) {
            $this->setDatabase($new);
        }

        $listeners = $this->listens(Event::DatabaseUpdate);
        if ($listeners !== []) {
            $this->dispatch(new Event\Database\Updated($database, $new), $listeners);
        }

        return $updated;
    }

    /**
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     *
     * @throws DatabaseException
     */
    public function delete(?string $database = null): bool
    {
        $database ??= $this->adapter->getDatabase();

        $deleted = $this->adapter->delete($database);

        $this->cache->flush();

        $listeners = $this->listens(Event::DatabaseDelete);
        if ($listeners !== []) {
            $this->dispatch(new Event\Database\Deleted($database, $deleted), $listeners);
        }

        return $deleted;
    }

    /**
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    private function inDatabase(string $database, callable $callback): mixed
    {
        $current = $this->adapter->getDatabase();
        if ($current === $this->adapter->filter($database)) {
            return $callback();
        }

        $this->adapter->setDatabase($database);

        try {
            return $callback();
        } finally {
            $this->adapter->setDatabase($current);
        }
    }

    /**
     * @return list<string>
     */
    private function getCollectionIds(): array
    {
        return $this->silent(fn (): array => $this->authorization->skip(function (): array {
            $ids = [];
            foreach ($this->cursor(self::METADATA, batchSize: 25) as $definition) {
                $ids[] = $definition->getId();
            }

            return $ids;
        }));
    }

    /**
     * @param  list<string>  $collections
     */
    private function purgeCachedCollections(array $collections): void
    {
        foreach ($collections as $collection) {
            $this->purgeCachedCollection($collection);
        }

        $this->queryCache?->invalidateCollection($this->getQueryCacheScope(), self::METADATA);
    }
}
