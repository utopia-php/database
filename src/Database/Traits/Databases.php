<?php

namespace Utopia\Database\Traits;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;

/**
 * Provides database-level operations including creation, existence checks, listing, and deletion.
 */
trait Databases
{
    /**
     * Create Database
     *
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     * @return bool True if the database was created successfully
     */
    public function create(?string $database = null): bool
    {
        $database ??= $this->adapter->getDatabase();

        $this->adapter->create($database);

        /** @var array<Document> $metaAttributes */
        $metaAttributes = self::collectionMeta()['attributes'];
        $attributes = [];
        foreach ($metaAttributes as $attribute) {
            $attributes[] = Attribute::fromDocument($attribute);
        }

        $this->silent(fn () => $this->createCollection(self::METADATA, $attributes));

        $this->trigger(Event::DatabaseCreate, $database);

        return true;
    }

    /**
     * Check if database exists, and optionally check if a collection exists in the database.
     *
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     * @param  string|null  $collection  Collection name to check for within the database
     * @return bool True if the database (and optionally the collection) exists
     */
    public function exists(?string $database = null, ?string $collection = null): bool
    {
        $database ??= $this->adapter->getDatabase();

        return $this->adapter->exists($database, $collection);
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        $databases = $this->adapter->list();

        $this->trigger(Event::DatabaseList, $databases);

        return $databases;
    }

    /**
     * Delete Database
     *
     * @param  string|null  $database  Database name, defaults to the adapter's configured database
     * @return bool True if the database was deleted successfully
     *
     * @throws DatabaseException
     */
    public function delete(?string $database = null): bool
    {
        $database = $database ?? $this->adapter->getDatabase();

        $deleted = $this->adapter->delete($database);

        $this->trigger(Event::DatabaseDelete, [
            'name' => $database,
            'deleted' => $deleted,
        ]);

        $this->cache->flush();

        // Drop the in-process metadata memo entirely — the entire database
        // is gone, so nothing in there is still valid.
        $this->collectionMetadataCache = [];

        return $deleted;
    }
}
