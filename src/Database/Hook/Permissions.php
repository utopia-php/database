<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\PermissionType;
use Utopia\Database\Storage;
use Utopia\Query\Builder\Feature\InsertOrIgnore as InsertOrIgnoreFeature;
use Utopia\Query\Query;

/**
 * Permission hook that handles both read-side query filtering and write-side side-table management.
 *
 * On reads: The SQL adapter generates permission-checking subquery conditions when this hook is registered.
 * On writes: Manages inserting, updating, and deleting permission entries in the _perms side table.
 */
class Permissions extends Interceptor
{
    private const PERM_TYPES = [
        PermissionType::Create,
        PermissionType::Read,
        PermissionType::Update,
        PermissionType::Delete,
    ];

    /**
     * Insert permission rows for all newly created documents.
     *
     * @param string $collection The collection name
     * @param array<Document> $documents The created documents
     * @param WriteContext $context The write context providing builder and execution closures
     */
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
        $permBuilder = ($context->createBuilder)()->into(($context->getTableRaw)(Storage::permissionsTable($collection)));
        $hasPermissions = false;

        foreach ($documents as $document) {
            foreach ($this->buildPermissionRows($document, $context) as $row) {
                $permBuilder->set($row);
                $hasPermissions = true;
            }
        }

        if ($hasPermissions) {
            if ($context->skipDuplicates) {
                if (! $permBuilder instanceof InsertOrIgnoreFeature) {
                    throw new DatabaseException('Insert-or-ignore is not supported on this dialect');
                }

                $result = $permBuilder->insertOrIgnore();
            } else {
                $result = $permBuilder->insert();
            }
            $stmt = ($context->executeResult)($result, Event::PermissionsCreate);
            ($context->execute)($stmt);
        }
    }

    /**
     * Diff current vs. new permissions and apply additions/removals for a single document.
     *
     * @param string $collection The collection name
     * @param Document $document The updated document with new permissions
     * @param bool $skipPermissions Whether to skip permission syncing
     * @param WriteContext $context The write context providing builder and execution closures
     */
    public function afterDocumentUpdate(string $collection, Document $document, bool $skipPermissions, WriteContext $context): void
    {
        if ($skipPermissions) {
            return;
        }

        [$permissionsMap, $storedIds] = $this->readCurrentPermissionsBatch($collection, [$document], $context);
        $permissions = $this->currentPermissions($permissionsMap, $document->getId());
        $permissionDocumentId = $this->permissionDocumentId($document->getId(), $storedIds);

        /** @var array<string, list<string>> $removals */
        $removals = [];
        /** @var array<string, list<string>> $additions */
        $additions = [];
        foreach (self::PERM_TYPES as $type) {
            $removed = \array_values(\array_diff($permissions[$type->value], $document->getPermissionsByType($type)));
            if (! empty($removed)) {
                $removals[$type->value] = $removed;
            }

            $added = $this->uniqueAdditions($document->getPermissionsByType($type), $permissions[$type->value]);
            if (! empty($added)) {
                $additions[$type->value] = $added;
            }
        }

        $this->deletePermissions($collection, $permissionDocumentId, $removals, $context);
        $this->insertPermissions($collection, $document, $permissionDocumentId, $additions, $context);
    }

    /**
     * Diff and sync permission rows for a batch of updated documents.
     *
     * @param string $collection The collection name
     * @param Document $updates The update document containing new permission values
     * @param array<Document> $documents The documents being updated
     * @param WriteContext $context The write context providing builder and execution closures
     */
    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void
    {
        if (! $updates->offsetExists(Document::PERMISSIONS)) {
            return;
        }

        $removeConditions = [];
        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)(Storage::permissionsTable($collection)));
        $hasAdditions = false;

        $eligible = [];
        foreach ($documents as $document) {
            if ($document->getAttribute(Document::SKIP_PERMISSIONS_UPDATE, false)) {
                continue;
            }
            $eligible[] = $document;
        }

        if (empty($eligible)) {
            return;
        }

        [$permissionsMap, $storedIds] = $this->readCurrentPermissionsBatch($collection, $eligible, $context);
        $updatesByType = [];
        foreach (self::PERM_TYPES as $type) {
            $updatesByType[$type->value] = $updates->getPermissionsByType($type);
        }

        foreach ($eligible as $document) {
            $permissions = $this->currentPermissions($permissionsMap, $document->getId());
            $permissionDocumentId = $this->permissionDocumentId($document->getId(), $storedIds);

            foreach (self::PERM_TYPES as $type) {
                $diff = \array_diff($permissions[$type->value], $updatesByType[$type->value]);
                if (! empty($diff)) {
                    $removeConditions[] = Query::and([
                        Query::equal(Storage::PERM_DOCUMENT, [$permissionDocumentId]),
                        Query::equal(Storage::PERM_TYPE, [$type->value]),
                        Query::equal(Storage::PERM_PERMISSION, \array_values($diff)),
                    ]);
                }
            }

            $metadata = $this->documentMetadata($document);
            foreach (self::PERM_TYPES as $type) {
                $diff = $this->uniqueAdditions($updatesByType[$type->value], $permissions[$type->value]);
                if (! empty($diff)) {
                    foreach ($diff as $permission) {
                        $row = ($context->decorateRow)([
                            Storage::PERM_DOCUMENT => $permissionDocumentId,
                            Storage::PERM_TYPE => $type->value,
                            Storage::PERM_PERMISSION => $permission,
                        ], $metadata);
                        $addBuilder->set($row);
                        $hasAdditions = true;
                    }
                }
            }
        }

        if (! empty($removeConditions)) {
            $removeBuilder = ($context->newBuilder)(Storage::permissionsTable($collection));
            $removeBuilder->filter([Query::or($removeConditions)]);
            $deleteResult = $removeBuilder->delete();
            $deleteStmt = ($context->executeResult)($deleteResult, Event::PermissionsDelete);
            ($context->execute)($deleteStmt);
        }

        if ($hasAdditions) {
            $addResult = $addBuilder->insert();
            $addStmt = ($context->executeResult)($addResult, Event::PermissionsCreate);
            ($context->execute)($addStmt);
        }
    }

    /**
     * Diff old vs. new permissions from upsert change sets and apply additions/removals.
     *
     * @param string $collection The collection name
     * @param array<\Utopia\Database\Change> $changes The upsert change objects containing old and new documents
     * @param WriteContext $context The write context providing builder and execution closures
     */
    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void
    {
        $removeConditions = [];
        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)(Storage::permissionsTable($collection)));
        $hasAdditions = false;

        foreach ($changes as $change) {
            $old = $change->getOld();
            $document = $change->getNew();
            $metadata = $this->documentMetadata($document);

            $current = [];
            foreach (self::PERM_TYPES as $type) {
                $current[$type->value] = $old->getPermissionsByType($type);
            }

            foreach (self::PERM_TYPES as $type) {
                $toRemove = \array_diff($current[$type->value], $document->getPermissionsByType($type));
                if (! empty($toRemove)) {
                    $removeConditions[] = Query::and([
                        Query::equal(Storage::PERM_DOCUMENT, [$document->getId()]),
                        Query::equal(Storage::PERM_TYPE, [$type->value]),
                        Query::equal(Storage::PERM_PERMISSION, \array_values($toRemove)),
                    ]);
                }
            }

            foreach (self::PERM_TYPES as $type) {
                $toAdd = $this->uniqueAdditions($document->getPermissionsByType($type), $current[$type->value]);
                foreach ($toAdd as $permission) {
                    $row = ($context->decorateRow)([
                        Storage::PERM_DOCUMENT => $document->getId(),
                        Storage::PERM_TYPE => $type->value,
                        Storage::PERM_PERMISSION => $permission,
                    ], $metadata);
                    $addBuilder->set($row);
                    $hasAdditions = true;
                }
            }
        }

        if (! empty($removeConditions)) {
            $removeBuilder = ($context->newBuilder)(Storage::permissionsTable($collection));
            $removeBuilder->filter([Query::or($removeConditions)]);
            $deleteResult = $removeBuilder->delete();
            $deleteStmt = ($context->executeResult)($deleteResult, Event::PermissionsDelete);
            ($context->execute)($deleteStmt);
        }

        if ($hasAdditions) {
            $addResult = $addBuilder->insert();
            $addStmt = ($context->executeResult)($addResult, Event::PermissionsCreate);
            ($context->execute)($addStmt);
        }
    }

    /**
     * Delete all permission rows for the given document IDs.
     *
     * @param string $collection The collection name
     * @param list<string> $documentIds The IDs of deleted documents
     * @param WriteContext $context The write context providing builder and execution closures
     * @throws DatabaseException If the permission deletion fails
     */
    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
    {
        if (empty($documentIds)) {
            return;
        }

        $permsBuilder = ($context->newBuilder)(Storage::permissionsTable($collection));
        $permsBuilder->filter([Query::equal(Storage::PERM_DOCUMENT, $documentIds)]);
        $permsResult = $permsBuilder->delete();
        $stmtPermissions = ($context->executeResult)($permsResult, Event::PermissionsDelete);

        if (! ($context->execute)($stmtPermissions)) {
            throw new DatabaseException('Failed to delete permissions');
        }
    }

    /**
     * Batched version of readCurrentPermissions — issues a single SELECT scoped
     * to all document ids and groups rows into the same shape per document.
     *
     * @param  array<Document>  $documents
     * @return array{0: array<string, array<string, list<string>>>, 1: array<string, string>}
     */
    private function readCurrentPermissionsBatch(string $collection, array $documents, WriteContext $context): array
    {
        if (empty($documents)) {
            return [[], []];
        }

        $documentIds = $this->permissionReadIds($documents, $context);
        if ($documentIds === []) {
            return [[], []];
        }

        $readBuilder = ($context->newBuilder)(Storage::permissionsTable($collection));
        $readBuilder->select([Storage::PERM_DOCUMENT, Storage::PERM_TYPE, Storage::PERM_PERMISSION]);
        $readBuilder->filter([Query::equal(Storage::PERM_DOCUMENT, $documentIds)]);

        $readResult = $readBuilder->build();
        $readStmt = ($context->executeResult)($readResult, Event::PermissionsRead);
        ($context->execute)($readStmt);
        /** @var array<array<string, string>> $rows */
        $rows = (array) $readStmt->fetchAll();
        $readStmt->closeCursor();

        return [
            $this->groupPermissionRows($documentIds, $rows),
            $this->storedDocumentIds($documentIds, $rows),
        ];
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function permissionReadIds(array $documents, WriteContext $context): array
    {
        $documentIds = [];
        foreach ($documents as $document) {
            $id = $document->getId();
            if ($id !== '') {
                $documentIds[] = $id;
            }
        }

        if ($context->lookupId !== null && $context->lookupId !== '') {
            $documentIds[] = $context->lookupId;
        }

        return \array_values(\array_unique($documentIds));
    }

    /**
     * @param  list<string>  $documentIds
     * @param  array<array<string, string>>  $rows
     * @return array<string, string>
     */
    private function storedDocumentIds(array $documentIds, array $rows): array
    {
        $stored = [];
        foreach ($rows as $row) {
            $storedId = $row[Storage::PERM_DOCUMENT] ?? null;
            if (! \is_string($storedId) || $storedId === '') {
                continue;
            }

            foreach ($documentIds as $id) {
                if (\strcasecmp($storedId, $id) === 0) {
                    $stored[$id] = $storedId;
                }
            }
        }

        return $stored;
    }

    /**
     * @param  array<string, string>  $storedIds
     */
    private function permissionDocumentId(string $requestedId, array $storedIds): string
    {
        if (isset($storedIds[$requestedId]) && \strcasecmp($storedIds[$requestedId], $requestedId) === 0) {
            return $storedIds[$requestedId];
        }

        foreach ($storedIds as $storedId) {
            if (\strcasecmp($storedId, $requestedId) === 0) {
                return $storedId;
            }
        }

        return $requestedId;
    }

    /**
     * @param  list<string>  $documentIds
     * @param  array<array<string, string>>  $rows
     * @return array<string, array<string, list<string>>>
     */
    private function groupPermissionRows(array $documentIds, array $rows): array
    {
        $result = [];
        $requestedByLower = [];
        foreach ($documentIds as $id) {
            $result[$id] = $this->emptyPermissions();
            $requestedByLower[\strtolower($id)][] = $id;
        }

        foreach ($rows as $row) {
            $storedId = $row[Storage::PERM_DOCUMENT] ?? null;
            $type = $row[Storage::PERM_TYPE] ?? null;
            $permission = $row[Storage::PERM_PERMISSION] ?? null;
            if ($storedId === null || $type === null || $permission === null) {
                continue;
            }

            $targets = $requestedByLower[\strtolower($storedId)] ?? [];
            if ($targets === []) {
                $targets = [$this->resolveStoredDocumentId($storedId, $result, $requestedByLower)];
            }

            foreach ($targets as $key) {
                if (! isset($result[$key])) {
                    $result[$key] = $this->emptyPermissions();
                }
                $result[$key][$type][] = $permission;
            }
        }

        return $result;
    }

    /**
     * @param  array<string, array<string, list<string>>>  $result
     * @param  array<string, list<string>>  $requestedByLower
     */
    private function resolveStoredDocumentId(string $storedId, array $result, array $requestedByLower): string
    {
        if (isset($result[$storedId])) {
            return $storedId;
        }

        $candidates = $requestedByLower[\strtolower($storedId)] ?? [];
        if (\count($candidates) === 1) {
            return $candidates[0];
        }

        return $storedId;
    }

    /**
     * @param  array<string, array<string, list<string>>>  $map
     * @return array<string, list<string>>
     */
    private function currentPermissions(array $map, string $documentId): array
    {
        if (\array_key_exists($documentId, $map)) {
            return $map[$documentId];
        }

        foreach ($map as $storedId => $permissions) {
            if (\strcasecmp((string) $storedId, $documentId) === 0) {
                return $permissions;
            }
        }

        return $this->emptyPermissions();
    }

    /**
     * @param  array<array-key, string>  $desired
     * @param  array<array-key, string>  $current
     * @return list<string>
     */
    private function uniqueAdditions(array $desired, array $current): array
    {
        return \array_values(\array_unique(\array_diff($desired, $current)));
    }

    /**
     * @return array<string, list<string>>
     */
    private function emptyPermissions(): array
    {
        $initial = [];
        foreach (self::PERM_TYPES as $type) {
            $initial[$type->value] = [];
        }

        return $initial;
    }

    /**
     * @param  array<string, list<string>>  $removals
     */
    private function deletePermissions(string $collection, string $documentId, array $removals, WriteContext $context): void
    {
        if (empty($removals)) {
            return;
        }

        $removeConditions = [];
        foreach ($removals as $type => $perms) {
            $removeConditions[] = Query::and([
                Query::equal(Storage::PERM_DOCUMENT, [$documentId]),
                Query::equal(Storage::PERM_TYPE, [$type]),
                Query::equal(Storage::PERM_PERMISSION, $perms),
            ]);
        }

        $removeBuilder = ($context->newBuilder)(Storage::permissionsTable($collection));
        $removeBuilder->filter([Query::or($removeConditions)]);
        $deleteResult = $removeBuilder->delete();
        $deleteStmt = ($context->executeResult)($deleteResult, Event::PermissionsDelete);
        ($context->execute)($deleteStmt);
    }

    /**
     * @param  array<string, list<string>>  $additions
     */
    private function insertPermissions(string $collection, Document $document, string $documentId, array $additions, WriteContext $context): void
    {
        if (empty($additions)) {
            return;
        }

        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)(Storage::permissionsTable($collection)));
        $metadata = $this->documentMetadata($document);

        foreach ($additions as $type => $perms) {
            foreach (\array_values(\array_unique($perms)) as $permission) {
                $row = ($context->decorateRow)([
                    Storage::PERM_DOCUMENT => $documentId,
                    Storage::PERM_TYPE => $type,
                    Storage::PERM_PERMISSION => $permission,
                ], $metadata);
                $addBuilder->set($row);
            }
        }

        $addResult = $addBuilder->insert();
        $addStmt = ($context->executeResult)($addResult, Event::PermissionsCreate);
        ($context->execute)($addStmt);
    }

    /**
     * Build permission rows for a document, applying decorateRow for tenant etc.
     *
     * @return list<array<string, mixed>>
     */
    private function buildPermissionRows(Document $document, WriteContext $context): array
    {
        $rows = [];
        $metadata = $this->documentMetadata($document);

        foreach (self::PERM_TYPES as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $row = [
                    Storage::PERM_DOCUMENT => $document->getId(),
                    Storage::PERM_TYPE => $type->value,
                    Storage::PERM_PERMISSION => \str_replace('"', '', $permission),
                ];
                $rows[] = ($context->decorateRow)($row, $metadata);
            }
        }

        return $rows;
    }

    /**
     * @return array<string, mixed>
     */
    private function documentMetadata(Document $document): array
    {
        return [
            'id' => $document->getId(),
            'tenant' => $document->getTenant(),
        ];
    }
}
