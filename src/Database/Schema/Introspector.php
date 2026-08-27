<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Collection;
use Utopia\Database\Database;

class Introspector
{
    public function __construct(
        private Database $db,
    ) {
    }

    public function introspectCollection(string $collectionId): Collection
    {
        $collectionDoc = $this->db->getCollection($collectionId);

        if ($collectionDoc->isEmpty()) {
            throw new \RuntimeException("Collection '{$collectionId}' not found");
        }

        return $collectionDoc;
    }

    /**
     * @return array<Collection>
     */
    public function introspectDatabase(): array
    {
        $collections = $this->db->listCollections();
        $result = [];

        foreach ($collections as $doc) {
            $result[] = $doc;
        }

        return $result;
    }
}
