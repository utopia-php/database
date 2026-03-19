<?php

namespace Utopia\Database\Seeder;

use Utopia\Database\Database;
use Utopia\Database\Document;

class Fixture
{
    /** @var array<array{collection: string, id: string}> */
    private array $created = [];

    /**
     * @param  array<array<string, mixed>>  $documents
     */
    public function load(Database $db, string $collection, array $documents): void
    {
        foreach ($documents as $document) {
            $doc = $db->createDocument($collection, new Document($document));
            $this->created[] = ['collection' => $collection, 'id' => $doc->getId()];
        }
    }

    public function cleanup(Database $db): void
    {
        foreach (\array_reverse($this->created) as $entry) {
            try {
                $db->deleteDocument($entry['collection'], $entry['id']);
            } catch (\Throwable) {
            }
        }

        $this->created = [];
    }

    /**
     * @return array<array{collection: string, id: string}>
     */
    public function getCreated(): array
    {
        return $this->created;
    }
}
