<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

interface Databases
{
    /**
     * @param string $name
     * @return bool
     */
    public function create(string $name): bool;

    /**
     * @param string $database
     * @param string|null $collection
     * @return bool
     */
    public function exists(string $database, ?string $collection = null): bool;

    /**
     * @return array<Document>
     */
    public function list(): array;

    /**
     * @param string $name
     * @return bool
     */
    public function delete(string $name): bool;
}
