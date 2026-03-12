<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\PermissionType;
use Utopia\Query\Query;

class PermissionWrite implements Write
{
    private const PERM_TYPES = [
        PermissionType::Create,
        PermissionType::Read,
        PermissionType::Update,
        PermissionType::Delete,
    ];

    public function decorateRow(array $row, array $metadata = []): array
    {
        return $row;
    }

    public function afterCreate(string $table, array $metadata, mixed $context): void
    {
    }

    public function afterUpdate(string $table, array $metadata, mixed $context): void
    {
    }

    public function afterBatchUpdate(string $table, array $updateData, array $metadata, mixed $context): void
    {
    }

    public function afterDelete(string $table, array $ids, mixed $context): void
    {
    }

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
            $result = $permBuilder->insert();
            $stmt = ($context->executeResult)($result, Database::EVENT_PERMISSIONS_CREATE);
            ($context->execute)($stmt);
        }
    }

    public function afterDocumentUpdate(string $collection, Document $document, bool $skipPermissions, WriteContext $context): void
    {
        if ($skipPermissions) {
            return;
        }

        $permissions = $this->readCurrentPermissions($collection, $document, $context);

        $removals = [];
        $additions = [];
        foreach (self::PERM_TYPES as $type) {
            $removed = \array_diff($permissions[$type->value], $document->getPermissionsByType($type->value));
            if (! empty($removed)) {
                $removals[$type->value] = $removed;
            }

            $added = \array_diff($document->getPermissionsByType($type->value), $permissions[$type->value]);
            if (! empty($added)) {
                $additions[$type->value] = $added;
            }
        }

        $this->deletePermissions($collection, $document, $removals, $context);
        $this->insertPermissions($collection, $document, $additions, $context);
    }

    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void
    {
        if (! $updates->offsetExists('$permissions')) {
            return;
        }

        $removeConditions = [];
        $addBuilder = ($context->createBuilder)()->into(($context->getTableRaw)($collection.'_perms'));
        $hasAdditions = false;

        foreach ($documents as $document) {
            if ($document->getAttribute('$skipPermissionsUpdate', false)) {
                continue;
            }

            $permissions = $this->readCurrentPermissions($collection, $document, $context);

            foreach (self::PERM_TYPES as $type) {
                $diff = \array_diff($permissions[$type->value], $updates->getPermissionsByType($type->value));
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
                $diff = \array_diff($updates->getPermissionsByType($type->value), $permissions[$type->value]);
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
            $deleteStmt = ($context->executeResult)($deleteResult, Database::EVENT_PERMISSIONS_DELETE);
            $deleteStmt->execute();
        }

        if ($hasAdditions) {
            $addResult = $addBuilder->insert();
            $addStmt = ($context->executeResult)($addResult, Database::EVENT_PERMISSIONS_CREATE);
            ($context->execute)($addStmt);
        }
    }

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
                $current[$type->value] = $old->getPermissionsByType($type->value);
            }

            foreach (self::PERM_TYPES as $type) {
                $toRemove = \array_diff($current[$type->value], $document->getPermissionsByType($type->value));
                if (! empty($toRemove)) {
                    $removeConditions[] = Query::and([
                        Query::equal('_document', [$document->getId()]),
                        Query::equal('_type', [$type->value]),
                        Query::equal('_permission', \array_values($toRemove)),
                    ]);
                }
            }

            foreach (self::PERM_TYPES as $type) {
                $toAdd = \array_diff($document->getPermissionsByType($type->value), $current[$type->value]);
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
            $deleteStmt = ($context->executeResult)($deleteResult, Database::EVENT_PERMISSIONS_DELETE);
            $deleteStmt->execute();
        }

        if ($hasAdditions) {
            $addResult = $addBuilder->insert();
            $addStmt = ($context->executeResult)($addResult, Database::EVENT_PERMISSIONS_CREATE);
            ($context->execute)($addStmt);
        }
    }

    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
    {
        if (empty($documentIds)) {
            return;
        }

        $permsBuilder = ($context->newBuilder)($collection.'_perms');
        $permsBuilder->filter([Query::equal('_document', \array_values($documentIds))]);
        $permsResult = $permsBuilder->delete();
        $stmtPermissions = ($context->executeResult)($permsResult, Database::EVENT_PERMISSIONS_DELETE);

        if (! $stmtPermissions->execute()) {
            throw new \Utopia\Database\Exception('Failed to delete permissions');
        }
    }

    /**
     * @return array<string, list<string>>
     */
    private function readCurrentPermissions(string $collection, Document $document, WriteContext $context): array
    {
        $readBuilder = ($context->newBuilder)($collection.'_perms');
        $readBuilder->select(['_type', '_permission']);
        $readBuilder->filter([Query::equal('_document', [$document->getId()])]);

        $readResult = $readBuilder->build();
        $readStmt = ($context->executeResult)($readResult, Database::EVENT_PERMISSIONS_READ);
        $readStmt->execute();
        $rows = $readStmt->fetchAll();
        $readStmt->closeCursor();

        $initial = [];
        foreach (self::PERM_TYPES as $type) {
            $initial[$type->value] = [];
        }

        return \array_reduce($rows, function (array $carry, array $item) {
            $carry[$item['_type']][] = $item['_permission'];

            return $carry;
        }, $initial);
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
                Query::equal('_permission', \array_values($perms)),
            ]);
        }

        $removeBuilder = ($context->newBuilder)($collection.'_perms');
        $removeBuilder->filter([Query::or($removeConditions)]);
        $deleteResult = $removeBuilder->delete();
        $deleteStmt = ($context->executeResult)($deleteResult, Database::EVENT_PERMISSIONS_DELETE);
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
        $addStmt = ($context->executeResult)($addResult, Database::EVENT_PERMISSIONS_CREATE);
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
            foreach ($document->getPermissionsByType($type->value) as $permission) {
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
