<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

interface Databases
{
    public function create(string $name): bool;

    public function exists(string $database, ?string $collection = null): bool;

    /**
     * @return array<Document>
     */
    public function list(): array;

    public function delete(string $name): bool;
}
