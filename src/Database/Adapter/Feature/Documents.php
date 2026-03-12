<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Change;
use Utopia\Database\CursorDirection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;

interface Documents
{
    /**
     * @param Document $collection
     * @param string $id
     * @param array<Query> $queries
     * @param bool $forUpdate
     * @return Document
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

    /**
     * @param Document $collection
     * @param Document $document
     * @return Document
     */
    public function createDocument(Document $collection, Document $document): Document;

    /**
     * @param Document $collection
     * @param array<Document> $documents
     * @return array<Document>
     */
    public function createDocuments(Document $collection, array $documents): array;

    /**
     * @param Document $collection
     * @param string $id
     * @param Document $document
     * @param bool $skipPermissions
     * @return Document
     */
    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document;

    /**
     * @param Document $collection
     * @param Document $updates
     * @param array<Document> $documents
     * @return int
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int;

    /**
     * @param string $collection
     * @param string $id
     * @return bool
     */
    public function deleteDocument(string $collection, string $id): bool;

    /**
     * @param string $collection
     * @param array<string> $sequences
     * @param array<string> $permissionIds
     * @return int
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int;

    /**
     * @param Document $collection
     * @param array<Query> $queries
     * @param int|null $limit
     * @param int|null $offset
     * @param array<string> $orderAttributes
     * @param array<string> $orderTypes
     * @param array<string, mixed> $cursor
     * @param string $cursorDirection
     * @param string $forPermission
     * @return array<Document>
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = CursorDirection::After->value, string $forPermission = PermissionType::Read->value): array;

    /**
     * @param Document $collection
     * @param string $attribute
     * @param array<Query> $queries
     * @param int|null $max
     * @return int|float
     */
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * @param Document $collection
     * @param array<Query> $queries
     * @param int|null $max
     * @return int
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int;

    /**
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param string $updatedAt
     * @param int|float|null $min
     * @param int|float|null $max
     * @return bool
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
     * @param string $collection
     * @param array<Document> $documents
     * @return array<Document>
     */
    public function getSequences(string $collection, array $documents): array;
}
