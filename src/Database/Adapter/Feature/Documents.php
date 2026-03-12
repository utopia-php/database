<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\CursorDirection;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;

interface Documents
{
    /**
     * @param  array<Query>  $queries
     */
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document;

    public function createDocument(Document $collection, Document $document): Document;

    /**
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    public function createDocuments(Document $collection, array $documents): array;

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document;

    /**
     * @param  array<Document>  $documents
     */
    public function updateDocuments(Document $collection, Document $updates, array $documents): int;

    public function deleteDocument(string $collection, string $id): bool;

    /**
     * @param  array<string>  $sequences
     * @param  array<string>  $permissionIds
     */
    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int;

    /**
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<string>  $orderTypes
     * @param  array<string, mixed>  $cursor
     * @return array<Document>
     */
    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = CursorDirection::After->value, string $forPermission = PermissionType::Read->value): array;

    /**
     * @param  array<Query>  $queries
     */
    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int;

    /**
     * @param  array<Query>  $queries
     */
    public function count(Document $collection, array $queries = [], ?int $max = null): int;

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
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    public function getSequences(string $collection, array $documents): array;
}
