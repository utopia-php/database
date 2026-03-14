<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;

/**
 * Defines document CRUD, querying, and aggregation operations for a database adapter.
 */
interface Documents
{
    /**
     * Get a single document by its identifier.
     *
     * @param Document $collection The collection document.
     * @param string $id The document identifier.
     * @param array<Query> $queries Optional queries for field selection.
     * @param bool $forUpdate Whether to lock the document for update.
     * @return Document The retrieved document.
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

    /**
     * Create a new document in a collection.
     *
     * @param Document $collection The collection document.
     * @param Document $document The document to create.
     * @return Document The created document.
     */
    public function createDocument(Document $collection, Document $document): Document;

    /**
     * Create multiple documents in a collection at once.
     *
     * @param Document $collection The collection document.
     * @param array<Document> $documents The documents to create.
     * @return array<Document> The created documents.
     */
    public function createDocuments(Document $collection, array $documents): array;

    /**
     * Update an existing document in a collection.
     *
     * @param Document $collection The collection document.
     * @param string $id The document identifier.
     * @param Document $document The document with updated data.
     * @param bool $skipPermissions Whether to skip permission checks.
     * @return Document The updated document.
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document;

    /**
     * Update multiple documents matching the given criteria.
     *
     * @param Document $collection The collection document.
     * @param Document $updates The fields to update.
     * @param array<Document> $documents The documents to update.
     * @return int The number of documents updated.
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int;

    /**
     * Delete a document from a collection.
     *
     * @param string $collection The collection identifier.
     * @param string $id The document identifier.
     * @return bool True on success.
     */
    public function deleteDocument(string $collection, string $id): bool;

    /**
     * Delete multiple documents from a collection.
     *
     * @param string $collection The collection identifier.
     * @param array<string> $sequences The document sequences to delete.
     * @param array<string> $permissionIds The permission identifiers to clean up.
     * @return int The number of documents deleted.
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int;

    /**
     * Find documents in a collection matching the given queries and ordering.
     *
     * @param Document $collection The collection document.
     * @param array<Query> $queries Filter queries.
     * @param int|null $limit Maximum number of documents to return.
     * @param int|null $offset Number of documents to skip.
     * @param array<string> $orderAttributes Attributes to order by.
     * @param array<OrderDirection> $orderTypes Direction for each order attribute.
     * @param array<string, mixed> $cursor Cursor values for pagination.
     * @param CursorDirection $cursorDirection Direction of cursor pagination.
     * @param PermissionType $forPermission The permission type to check.
     * @return array<Document> The matching documents.
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array;

    /**
     * Calculate the sum of an attribute's values across matching documents.
     *
     * @param Document $collection The collection document.
     * @param string $attribute The attribute to sum.
     * @param array<Query> $queries Optional filter queries.
     * @param int|null $max Maximum number of documents to consider.
     * @return float|int The sum result.
     */
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * Count documents matching the given queries.
     *
     * @param Document $collection The collection document.
     * @param array<Query> $queries Optional filter queries.
     * @param int|null $max Maximum count to return.
     * @return int The document count.
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int;

    /**
     * Increase or decrease a numeric attribute value on a document.
     *
     * @param string $collection The collection identifier.
     * @param string $id The document identifier.
     * @param string $attribute The numeric attribute to modify.
     * @param int|float $value The value to add (negative to decrease).
     * @param string $updatedAt The timestamp to set as the updated time.
     * @param int|float|null $min Optional minimum bound for the resulting value.
     * @param int|float|null $max Optional maximum bound for the resulting value.
     * @return bool True on success.
     */
    public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value,
        string $updatedAt,
        int|float|null $min = null,
        int|float|null $max = null
    ): bool;

    /**
     * Retrieve internal sequence values for the given documents.
     *
     * @param string $collection The collection identifier.
     * @param array<Document> $documents The documents to retrieve sequences for.
     * @return array<Document> The documents with populated sequence values.
     */
    public function getSequences(string $collection, array $documents): array;
}
