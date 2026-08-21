<?php

namespace Utopia\Database\Migration;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Query;

class Tracker
{
    private const COLLECTION = '_migrations';

    private Database $db;

    private bool $initialized = false;

    public function __construct(Database $db)
    {
        $this->db = $db;
    }

    public function reset(): void
    {
        $this->initialized = false;
    }

    public function setup(): void
    {
        if ($this->initialized) {
            return;
        }

        if ($this->db->exists($this->db->getAdapter()->getDatabase(), self::COLLECTION)) {
            $this->initialized = true;

            return;
        }

        $this->db->createCollection(new Collection(id: self::COLLECTION, attributes: [
            Attribute::string(key: 'version', required: true),
            Attribute::string(key: 'name', required: true),
            Attribute::integer(key: 'batch', required: true),
            Attribute::datetime(key: 'appliedAt', filters: ['datetime']),
        ]));

        $this->initialized = true;
    }

    /**
     * @return array<Document>
     */
    public function getApplied(): array
    {
        $this->setup();

        return $this->db->find(self::COLLECTION, [
            Query::orderAsc('version'),
        ]);
    }

    /**
     * @return array<string>
     */
    public function getAppliedVersions(): array
    {
        return \array_values(\array_filter(
            \array_map(
                static function (Document $doc): ?string {
                    $version = $doc->getAttribute('version', '');

                    return \is_string($version) ? $version : null;
                },
                $this->getApplied()
            ),
        ));
    }

    public function markApplied(string $version, string $name, int $batch): void
    {
        $this->setup();

        $this->db->createDocument(self::COLLECTION, new Document([
            Document::ID => ID::unique(),
            'version' => $version,
            'name' => $name,
            'batch' => $batch,
            'appliedAt' => \date('Y-m-d H:i:s'),
        ]));
    }

    public function markRolledBack(string $version): void
    {
        $this->setup();

        $docs = $this->db->find(self::COLLECTION, [
            Query::equal('version', [$version]),
            Query::limit(1),
        ]);

        if ($docs !== []) {
            $this->db->deleteDocument(self::COLLECTION, $docs[0]->getId());
        }
    }

    public function getLastBatch(): int
    {
        $this->setup();

        $docs = $this->db->find(self::COLLECTION, [
            Query::orderDesc('batch'),
            Query::limit(1),
        ]);

        if ($docs === []) {
            return 0;
        }

        $batch = $docs[0]->getAttribute('batch', 0);

        return \is_numeric($batch) ? (int) $batch : 0;
    }

    /**
     * @return array<Document>
     */
    public function getByBatch(int $batch): array
    {
        $this->setup();

        return $this->db->find(self::COLLECTION, [
            Query::equal('batch', [$batch]),
            Query::orderDesc('version'),
        ]);
    }
}
