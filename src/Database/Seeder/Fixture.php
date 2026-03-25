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
        if ($documents === []) {
            return;
        }

        $docs = \array_map(fn (array $d) => new Document($d), $documents);

        if (\count($docs) === 1) {
            $created = $db->createDocument($collection, $docs[0]);
            $this->created[] = ['collection' => $collection, 'id' => $created->getId()];
        } else {
            $db->createDocuments($collection, $docs, Database::INSERT_BATCH_SIZE, function (Document $created) use ($collection): void {
                $this->created[] = ['collection' => $collection, 'id' => $created->getId()];
            });
        }
    }

    public function cleanup(Database $db): void
    {
        if ($this->created === []) {
            return;
        }

        $grouped = [];
        foreach (\array_reverse($this->created) as $entry) {
            $grouped[$entry['collection']][] = $entry['id'];
        }

        foreach ($grouped as $collection => $ids) {
            foreach ($ids as $id) {
                try {
                    $db->deleteDocument($collection, $id);
                } catch (\Throwable) {
                }
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
