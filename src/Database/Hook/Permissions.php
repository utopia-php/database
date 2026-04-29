<?php

namespace Utopia\Database\Hook;

use PDOStatement;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\PermissionType;
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
        $permBuilder = ($context->createBuilder)()->into(($context->getTableRaw)($collection.'_perms'));
        $hasPermissions = false;

        foreach ($documents as $document) {
            foreach ($this->buildPermissionRows($document, $context) as $row) {
                $permBuilder->set($row);
                $hasPermissions = true;
            }
        }

        if ($hasPermissions) {
            $result = $context->skipDuplicates ? $permBuilder->insertOrIgnore() : $permBuilder->insert();
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

        $permissions = $this->readCurrentPermissions($collection, $document, $context);

        /** @var array<string, list<string>> $removals */
        $removals = [];
        /** @var array<string, list<string>> $additions */
        $additions = [];
        foreach (self::PERM_TYPES as $type) {
            $removed = \array_values(\array_diff($permissions[$type->value], $document->getPermissionsByType($type)));
            if (! empty($removed)) {
                $removals[$type->value] = $removed;
            }

            $added = \array_values(\array_diff($document->getPermissionsByType($type), $permissions[$type->value]));
            if (! empty($added)) {
                $additions[$type->value] = $added;
            }
        }

        $this->deletePermissions($collection, $document, $removals, $context);
        $this->insertPermissions($collection, $document, $additions, $context);
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
        if (! $updates->offsetExists('$permissions')) {
            return;
        }

        $removeConditions = [];
        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)($collection.'_perms'));
        $hasAdditions = false;

        $eligible = [];
        foreach ($documents as $document) {
            if ($document->getAttribute('$skipPermissionsUpdate', false)) {
                continue;
            }
            $eligible[] = $document;
        }

        if (empty($eligible)) {
            return;
        }

        $permissionsMap = $this->readCurrentPermissionsBatch($collection, $eligible, $context);

        foreach ($eligible as $document) {
            $permissions = $permissionsMap[$document->getId()] ?? $this->emptyPermissions();

            foreach (self::PERM_TYPES as $type) {
                $diff = \array_diff($permissions[$type->value], $updates->getPermissionsByType($type));
                if (! empty($diff)) {
                    $removeConditions[] = Query::and([
                        Query::equal('_document', [$document->getId()]),
                        Query::equal('_type', [$type->value]),
                        Query::equal('_permission', \array_values($diff)),
                    ]);
                }
            }

            $metadata = $this->documentMetadata($document);
            foreach (self::PERM_TYPES as $type) {
                $diff = \array_diff($updates->getPermissionsByType($type), $permissions[$type->value]);
                if (! empty($diff)) {
                    foreach ($diff as $permission) {
                        $row = ($context->decorateRow)([
                            '_document' => $document->getId(),
                            '_type' => $type->value,
                            '_permission' => $permission,
                        ], $metadata);
                        $addBuilder->set($row);
                        $hasAdditions = true;
                    }
                }
            }
        }

        if (! empty($removeConditions)) {
            $removeBuilder = ($context->newBuilder)($collection.'_perms');
            $removeBuilder->filter([Query::or($removeConditions)]);
            $deleteResult = $removeBuilder->delete();
            /** @var PDOStatement $deleteStmt */
            $deleteStmt = ($context->executeResult)($deleteResult, Event::PermissionsDelete);
            $deleteStmt->execute();
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
        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)($collection.'_perms'));
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
                        Query::equal('_document', [$document->getId()]),
                        Query::equal('_type', [$type->value]),
                        Query::equal('_permission', \array_values($toRemove)),
                    ]);
                }
            }

            foreach (self::PERM_TYPES as $type) {
                $toAdd = \array_diff($document->getPermissionsByType($type), $current[$type->value]);
                foreach ($toAdd as $permission) {
                    $row = ($context->decorateRow)([
                        '_document' => $document->getId(),
                        '_type' => $type->value,
                        '_permission' => $permission,
                    ], $metadata);
                    $addBuilder->set($row);
                    $hasAdditions = true;
                }
            }
        }

        if (! empty($removeConditions)) {
            $removeBuilder = ($context->newBuilder)($collection.'_perms');
            $removeBuilder->filter([Query::or($removeConditions)]);
            $deleteResult = $removeBuilder->delete();
            /** @var PDOStatement $deleteStmt */
            $deleteStmt = ($context->executeResult)($deleteResult, Event::PermissionsDelete);
            $deleteStmt->execute();
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

        $permsBuilder = ($context->newBuilder)($collection.'_perms');
        $permsBuilder->filter([Query::equal('_document', $documentIds)]);
        $permsResult = $permsBuilder->delete();
        /** @var PDOStatement $stmtPermissions */
        $stmtPermissions = ($context->executeResult)($permsResult, Event::PermissionsDelete);

        if (! $stmtPermissions->execute()) {
            throw new DatabaseException('Failed to delete permissions');
        }
    }

    /**
     * @return array<string, list<string>>
     */
    private function readCurrentPermissions(string $collection, Document $document, WriteContext $context): array
    {
        $map = $this->readCurrentPermissionsBatch($collection, [$document], $context);

        return $map[$document->getId()] ?? $this->emptyPermissions();
    }

    /**
     * Batched version of readCurrentPermissions — issues a single SELECT scoped
     * to all document ids and groups rows into the same shape per document.
     *
     * @param  array<Document>  $documents
     * @return array<string, array<string, list<string>>>
     */
    private function readCurrentPermissionsBatch(string $collection, array $documents, WriteContext $context): array
    {
        if (empty($documents)) {
            return [];
        }

        $documentIds = [];
        foreach ($documents as $document) {
            $documentIds[] = $document->getId();
        }

        $readBuilder = ($context->newBuilder)($collection.'_perms');
        $readBuilder->select(['_document', '_type', '_permission']);
        $readBuilder->filter([Query::equal('_document', $documentIds)]);

        $readResult = $readBuilder->build();
        /** @var PDOStatement $readStmt */
        $readStmt = ($context->executeResult)($readResult, Event::PermissionsRead);
        $readStmt->execute();
        /** @var array<array<string, string>> $rows */
        $rows = (array) $readStmt->fetchAll();
        $readStmt->closeCursor();

        $result = [];
        foreach ($documentIds as $id) {
            $result[$id] = $this->emptyPermissions();
        }

        foreach ($rows as $row) {
            $docId = $row['_document'] ?? null;
            $type = $row['_type'] ?? null;
            $permission = $row['_permission'] ?? null;
            if ($docId === null || $type === null || $permission === null) {
                continue;
            }
            if (! isset($result[$docId])) {
                $result[$docId] = $this->emptyPermissions();
            }
            $result[$docId][$type][] = $permission;
        }

        return $result;
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
    private function deletePermissions(string $collection, Document $document, array $removals, WriteContext $context): void
    {
        if (empty($removals)) {
            return;
        }

        $removeConditions = [];
        foreach ($removals as $type => $perms) {
            $removeConditions[] = Query::and([
                Query::equal('_document', [$document->getId()]),
                Query::equal('_type', [$type]),
                Query::equal('_permission', $perms),
            ]);
        }

        $removeBuilder = ($context->newBuilder)($collection.'_perms');
        $removeBuilder->filter([Query::or($removeConditions)]);
        $deleteResult = $removeBuilder->delete();
        /** @var PDOStatement $deleteStmt */
        $deleteStmt = ($context->executeResult)($deleteResult, Event::PermissionsDelete);
        $deleteStmt->execute();
    }

    /**
     * @param  array<string, list<string>>  $additions
     */
    private function insertPermissions(string $collection, Document $document, array $additions, WriteContext $context): void
    {
        if (empty($additions)) {
            return;
        }

        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)($collection.'_perms'));
        $metadata = $this->documentMetadata($document);

        foreach ($additions as $type => $perms) {
            foreach ($perms as $permission) {
                $row = ($context->decorateRow)([
                    '_document' => $document->getId(),
                    '_type' => $type,
                    '_permission' => $permission,
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
                    '_document' => $document->getId(),
                    '_type' => $type->value,
                    '_permission' => \str_replace('"', '', $permission),
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
