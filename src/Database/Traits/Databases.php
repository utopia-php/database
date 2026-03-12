<?php

namespace Utopia\Database\Traits;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;

trait Databases
{
    /**
     * Create Database
     *
     * @param string|null $database
     * @return bool
     */
    public function create(?string $database = null): bool
    {
        $database ??= $this->adapter->getDatabase();

        $this->adapter->create($database);

        /** @var array<Attribute> $attributes */
        $attributes = \array_map(function ($attribute) {
            return Attribute::fromArray($attribute);
        }, self::COLLECTION['attributes']);

        $this->silent(fn () => $this->createCollection(self::METADATA, $attributes));

        try {
            $this->trigger(self::EVENT_DATABASE_CREATE, $database);
        } catch (\Throwable $e) {
            // Ignore
        }

        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string|null $database (optional) database name
     * @param string|null $collection (optional) collection name
     *
     * @return bool
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

        try {
            $this->trigger(self::EVENT_DATABASE_LIST, $databases);
        } catch (\Throwable $e) {
            // Ignore
        }

        return $databases;
    }

    /**
     * Delete Database
     *
     * @param string|null $database
     * @return bool
     * @throws DatabaseException
     */
    public function delete(?string $database = null): bool
    {
        $database = $database ?? $this->adapter->getDatabase();

        $deleted = $this->adapter->delete($database);

        try {
            $this->trigger(self::EVENT_DATABASE_DELETE, [
                'name' => $database,
                'deleted' => $deleted
            ]);
        } catch (\Throwable $e) {
            // Ignore
        }

        $this->cache->flush();

        return $deleted;
    }
}
