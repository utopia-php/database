<?php

namespace Utopia\Database\Loading;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class BatchLoader
{
    /** @var array<string, array<string, array<LazyProxy>>> */
    private array $pending = [];

    private Database $db;

    public function __construct(Database $db)
    {
        $this->db = $db;
    }

    public function register(LazyProxy $proxy, string $collection, string $id): void
    {
        $this->pending[$collection][$id][] = $proxy;
    }

    public function resolve(string $collection, string $id): ?Document
    {
        if (! isset($this->pending[$collection])) {
            return null;
        }

        $ids = \array_keys($this->pending[$collection]);

        if ($ids === []) {
            return null;
        }

        $documents = $this->db->find($collection, [
            Query::equal('$id', $ids),
            Query::limit(\count($ids)),
        ]);

        $byId = [];
        foreach ($documents as $doc) {
            $byId[$doc->getId()] = $doc;
        }

        foreach ($this->pending[$collection] as $pendingId => $proxies) {
            $doc = $byId[$pendingId] ?? null;
            foreach ($proxies as $proxy) {
                $proxy->resolveWith($doc);
            }
        }

        unset($this->pending[$collection]);

        return $byId[$id] ?? null;
    }
}
