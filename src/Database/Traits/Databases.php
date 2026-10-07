<?php

namespace Utopia\Database\Traits;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;

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

        $this->trigger(Event::DatabaseCreate, $database);

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

        $this->trigger(Event::DatabaseList, $databases);

        return $databases;
    }

    /**
     * Renames a database. Under shared tables a database holds other tenants' data, so renaming it is refused.
     *
     * @throws DatabaseException
     */
    public function update(string $database, string $new): bool
    {
        if ($this->adapter->getSharedTables()) {
            throw new DatabaseException('Cannot rename a database while shared tables are enabled');
        }

        $updated = $this->adapter->update($database, $new);

        $this->cache->flush();

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

        $this->trigger(Event::DatabaseDelete, [
            'name' => $database,
            'deleted' => $deleted,
        ]);

        return $deleted;
    }
}
