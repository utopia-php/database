<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Contract for handling document relationship operations including creation, updates, deletion, and population.
 */
interface Relationship
{
    /**
     * Check whether relationship processing is enabled.
     *
     * @return bool True if relationship handling is active
     */
    public function isEnabled(): bool;

    /**
     * Enable or disable relationship processing.
     *
     * @param bool $enabled Whether to enable relationship handling
     */
    public function setEnabled(bool $enabled): void;

    /**
     * Check whether existence validation is enabled for related documents.
     *
     * @return bool True if related documents must exist before linking
     */
    public function shouldCheckExist(): bool;

    /**
     * Enable or disable existence validation for related documents.
     *
     * @param bool $check Whether to validate that related documents exist
     */
    public function setCheckExist(bool $check): void;

    /**
     * Get the number of documents currently in the write stack (recursion guard).
     *
     * @return int The current write stack depth
     */
    public function getWriteStackCount(): int;

    /**
     * Get the current relationship fetch depth.
     *
     * @return int The fetch depth level
     */
    public function getFetchDepth(): int;

    /**
     * Check whether documents are currently being populated in batch mode.
     *
     * @return bool True if batch population is in progress
     */
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
     * @param  array<Document>  $documents
     * @param  array<string, array<Query>>  $selects
     * @return array<Document>
     */
    public function populateDocuments(array $documents, Document $collection, int $fetchDepth, array $selects = []): array;

    /**
     * Extract nested relationship selections from queries.
     *
     * @param  array<Document>  $relationships
     * @param  array<Query>  $queries
     * @return array<string, array<Query>>
     */
    public function processQueries(array $relationships, array $queries): array;

    /**
     * Convert relationship filter queries to SQL-safe subqueries.
     *
     * @param  array<Document>  $relationships
     * @param  array<Query>  $queries
     * @return array<Query>|null
     */
    public function convertQueries(array $relationships, array $queries, ?Document $collection = null): ?array;
}
