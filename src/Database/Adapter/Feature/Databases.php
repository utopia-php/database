<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

/**
 * Defines database-level lifecycle operations for a database adapter.
 */
interface Databases
{
    /**
     * Create a new database.
     *
     * @param string $name The database name.
     * @return bool True on success.
     */
    public function create(string $name): bool;

    /**
     * Check whether a database or collection exists.
     *
     * @param string $database The database name.
     * @param string|null $collection Optional collection name to check within the database.
     * @return bool True if the database (or collection) exists.
     */
    public function exists(string $database, ?string $collection = null): bool;

    /**
     * List all databases.
     *
     * @return array<Document> Array of database documents.
     */
    public function list(): array;

    /**
     * Delete a database by name.
     *
     * @param string $name The database name.
     * @return bool True on success.
     */
    public function delete(string $name): bool;
}
