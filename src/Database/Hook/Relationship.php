<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;
use Utopia\Database\Query;

interface Relationship
{
    public function isEnabled(): bool;

    public function setEnabled(bool $enabled): void;

    public function shouldCheckExist(): bool;

    public function setCheckExist(bool $check): void;

    public function getWriteStackCount(): int;

    public function getFetchDepth(): int;

    public function isInBatchPopulation(): bool;

    /**
     * Process relationship attributes after a document is created.
     */
    public function afterDocumentCreate(Document $collection, Document $document): Document;

    /**
     * Process relationship attributes after a document is updated.
     */
    public function afterDocumentUpdate(Document $collection, Document $old, Document $document): Document;

    /**
     * Process relationship attributes before a document is deleted.
     */
    public function beforeDocumentDelete(Document $collection, Document $document): Document;

    /**
     * Populate relationship data for an array of documents.
     *
     * @param array<Document> $documents
     * @param array<string, array<Query>> $selects
     * @return array<Document>
     */
    public function populateDocuments(array $documents, Document $collection, int $fetchDepth, array $selects = []): array;

    /**
     * Extract nested relationship selections from queries.
     *
     * @param array<Document> $relationships
     * @param array<Query> $queries
     * @return array<string, array<Query>>
     */
    public function processQueries(array $relationships, array $queries): array;

    /**
     * Convert relationship filter queries to SQL-safe subqueries.
     *
     * @param array<Document> $relationships
     * @param array<Query> $queries
     * @return array<Query>|null
     */
    public function convertQueries(array $relationships, array $queries, ?Document $collection = null): ?array;
}
