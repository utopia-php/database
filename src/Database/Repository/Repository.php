<?php

namespace Utopia\Database\Repository;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

abstract class Repository
{
    public function __construct(
        protected Database $db,
    ) {
    }

    abstract public function collection(): string;

    public function findById(string $id): Document
    {
        return $this->db->getDocument($this->collection(), $id);
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    public function findAll(array $queries = []): array
    {
        return $this->db->find($this->collection(), $queries);
    }

    public function findOneBy(string $attribute, mixed $value): Document
    {
        $results = $this->db->find($this->collection(), [
            Query::equal($attribute, \is_array($value) ? $value : [$value]),
            Query::limit(1),
        ]);

        return $results[0] ?? new Document();
    }

    /**
     * @param  array<Query>  $queries
     */
    public function count(array $queries = []): int
    {
        return $this->db->count($this->collection(), $queries);
    }

    public function create(Document $document): Document
    {
        return $this->db->createDocument($this->collection(), $document);
    }

    public function update(string $id, Document $document): Document
    {
        return $this->db->updateDocument($this->collection(), $id, $document);
    }

    public function delete(string $id): bool
    {
        return $this->db->deleteDocument($this->collection(), $id);
    }

    /**
     * @param  array<Query>  $baseQueries
     * @return array<Document>
     */
    public function matching(Specification $spec, array $baseQueries = []): array
    {
        return $this->findAll(\array_merge($baseQueries, $spec->toQueries()));
    }
}
