<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Change;
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
    private const array PERMISSION_TYPES = [
        PermissionType::Create,
        PermissionType::Read,
        PermissionType::Update,
        PermissionType::Delete,
    ];

    /**
     * Insert permission rows for all newly created documents.
     *
     * @param array<Document> $documents
     */
    #[\Override]
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
        $permissionsBuilder = $context->builder()->into($context->rawTable(Storage::permissionsTable($collection)));
        $hasPermissions = false;

        foreach ($documents as $document) {
            foreach ($this->buildPermissionRows($document, $context) as $row) {
                $permissionsBuilder->set($row);
                $hasPermissions = true;
            }
        }

        if ($hasPermissions) {
            if ($context->ignoreDuplicates()) {
                if (! $permissionsBuilder instanceof InsertOrIgnoreFeature) {
                    throw new DatabaseException('Insert-or-ignore is not supported on this dialect');
                }

                $result = $permissionsBuilder->insertOrIgnore();
            } else {
                $result = $permissionsBuilder->insert();
            }
            $context->run($result, Event::PermissionsCreate);
        }
    }

    /**
     * Diff current vs. new permissions and apply additions/removals for a single document.
     */
    #[\Override]
    public function afterDocumentUpdate(string $collection, string $id, Document $document, WriteContext $context): void
    {
        if ($context->skipPermissions($document)) {
            return;
        }

        if ($id !== '' && $id !== $document->getId()) {
            $this->movePermissions($collection, $id, $document, $context);

            return;
        }

        [$permissionsMap, $storedIds] = $this->readCurrentPermissionsBatch($collection, [$document], $context);
        $permissions = $this->currentPermissions($permissionsMap, $document->getId());
        $permissionDocumentId = $this->permissionDocumentId($document->getId(), $storedIds);

        /** @var array<string, list<string>> $removals */
        $removals = [];
        /** @var array<string, list<string>> $additions */
        $additions = [];
        foreach (self::PERMISSION_TYPES as $type) {
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
     * @param array<Document> $documents
     */
    #[\Override]
    public function afterDocumentBatchUpdate(string $collection, Document $updates, array $documents, WriteContext $context): void
    {
        if (! $updates->offsetExists(Document::PERMISSIONS)) {
            return;
        }

        $removeConditions = [];
        $addBuilder = $context->builder()->into($context->rawTable(Storage::permissionsTable($collection)));
        $hasAdditions = false;

        $eligible = [];
        foreach ($documents as $document) {
            if ($context->skipPermissions($document)) {
                continue;
            }
            $eligible[] = $document;
        }

        if (empty($eligible)) {
            return;
        }

        [$permissionsMap, $storedIds] = $this->readCurrentPermissionsBatch($collection, $eligible, $context);
        $updatesByType = [];
        foreach (self::PERMISSION_TYPES as $type) {
            $updatesByType[$type->value] = $updates->getPermissionsByType($type);
        }

        foreach ($eligible as $document) {
            $permissions = $this->currentPermissions($permissionsMap, $document->getId());
            $permissionDocumentId = $this->permissionDocumentId($document->getId(), $storedIds);

            foreach (self::PERMISSION_TYPES as $type) {
                $diff = \array_diff($permissions[$type->value], $updatesByType[$type->value]);
                if (! empty($diff)) {
                    $removeConditions[] = Query::and([
                        Query::equal(Storage::PERMISSIONS_DOCUMENT, [$permissionDocumentId]),
                        Query::equal(Storage::PERMISSIONS_TYPE, [$type->value]),
                        Query::equal(Storage::PERMISSIONS_PERMISSION, \array_values($diff)),
                    ]);
                }
            }

            foreach (self::PERMISSION_TYPES as $type) {
                $diff = $this->uniqueAdditions($updatesByType[$type->value], $permissions[$type->value]);
                if (! empty($diff)) {
                    foreach ($diff as $permission) {
                        $row = $context->decorateRow([
                            Storage::PERMISSIONS_DOCUMENT => $permissionDocumentId,
                            Storage::PERMISSIONS_TYPE => $type->value,
                            Storage::PERMISSIONS_PERMISSION => $permission,
                        ], $document);
                        $addBuilder->set($row);
                        $hasAdditions = true;
                    }
                }
            }
        }

        if (! empty($removeConditions)) {
            $removeBuilder = $context->builder()->from(Storage::permissionsTable($collection));
            $removeBuilder->filter([Query::or($removeConditions)]);
            $context->run($removeBuilder->delete(), Event::PermissionsDelete);
        }

        if ($hasAdditions) {
            $context->run($addBuilder->insert(), Event::PermissionsCreate);
        }
    }

    /**
     * Diff old vs. new permissions from upsert change sets and apply additions/removals.
     *
     * @param array<Change> $changes
     */
    #[\Override]
    public function afterDocumentUpsert(string $collection, array $changes, WriteContext $context): void
    {
        $removeConditions = [];
        $addBuilder = $context->builder()->into($context->rawTable(Storage::permissionsTable($collection)));
        $hasAdditions = false;

        foreach ($changes as $change) {
            $old = $change->old;
            $document = $change->new;
            $tenantScope = $this->tenantScope($document, $context);

            $current = [];
            foreach (self::PERMISSION_TYPES as $type) {
                $current[$type->value] = $old->getPermissionsByType($type);
            }

            foreach (self::PERMISSION_TYPES as $type) {
                $toRemove = \array_diff($current[$type->value], $document->getPermissionsByType($type));
                if (! empty($toRemove)) {
                    $removeConditions[] = Query::and([
                        Query::equal(Storage::PERMISSIONS_DOCUMENT, [$document->getId()]),
                        ...$tenantScope,
                        Query::equal(Storage::PERMISSIONS_TYPE, [$type->value]),
                        Query::equal(Storage::PERMISSIONS_PERMISSION, \array_values($toRemove)),
                    ]);
                }
            }

            foreach (self::PERMISSION_TYPES as $type) {
                $toAdd = $this->uniqueAdditions($document->getPermissionsByType($type), $current[$type->value]);
                foreach ($toAdd as $permission) {
                    $row = $context->decorateRow([
                        Storage::PERMISSIONS_DOCUMENT => $document->getId(),
                        Storage::PERMISSIONS_TYPE => $type->value,
                        Storage::PERMISSIONS_PERMISSION => $permission,
                    ], $document);
                    $addBuilder->set($row);
                    $hasAdditions = true;
                }
            }
        }

        if (! empty($removeConditions)) {
            $removeBuilder = $context->builder()->fromTable($context->rawTable(Storage::permissionsTable($collection)));
            $removeBuilder->filter([Query::or($removeConditions)]);
            $context->run($removeBuilder->delete(), Event::PermissionsDelete);
        }

        if ($hasAdditions) {
            $context->run($addBuilder->insert(), Event::PermissionsCreate);
        }
    }

    /**
     * An upsert batch can hold documents of several tenants, none of them the adapter's, so its
     * removals cannot take from()'s filter on the adapter's tenant: each one is scoped to
     * the tenant decorateRow() stores its own document's rows under instead.
     *
     * @return list<Query>
     */
    private function tenantScope(Document $document, WriteContext $context): array
    {
        $row = $context->decorateRow([], $document);
        if (! \array_key_exists(Storage::TENANT, $row)) {
            return [];
        }

        $tenant = $row[Storage::TENANT];

        return [Query::equal(Storage::TENANT, [\is_int($tenant) || \is_string($tenant) ? $tenant : null])];
    }

    /**
     * Delete all permission rows for the given document IDs.
     *
     * @param list<string> $documentIds
     * @throws DatabaseException If the permission deletion fails
     */
    #[\Override]
    public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
    {
        if (empty($documentIds)) {
            return;
        }

        $permissionsBuilder = $context->builder()->from(Storage::permissionsTable($collection));
        $permissionsBuilder->filter([Query::equal(Storage::PERMISSIONS_DOCUMENT, $documentIds)]);

        if (! $context->run($permissionsBuilder->delete(), Event::PermissionsDelete)) {
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

        $documentIds = $this->permissionReadIds($documents);
        if ($documentIds === []) {
            return [[], []];
        }

        $readBuilder = $context->builder()->from(Storage::permissionsTable($collection));
        $readBuilder->select([Storage::PERMISSIONS_DOCUMENT, Storage::PERMISSIONS_TYPE, Storage::PERMISSIONS_PERMISSION]);
        $readBuilder->filter([Query::equal(Storage::PERMISSIONS_DOCUMENT, $documentIds)]);

        /** @var array<array<string, string>> $rows */
        $rows = $context->fetch($readBuilder->build(), Event::PermissionsRead);

        return [
            $this->groupPermissionRows($documentIds, $rows),
            $this->storedDocumentIds($documentIds, $rows),
        ];
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function permissionReadIds(array $documents): array
    {
        $documentIds = [];
        foreach ($documents as $document) {
            $id = $document->getId();
            if ($id !== '') {
                $documentIds[] = $id;
            }
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
            $storedId = $row[Storage::PERMISSIONS_DOCUMENT] ?? null;
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
            $storedId = $row[Storage::PERMISSIONS_DOCUMENT] ?? null;
            $type = $row[Storage::PERMISSIONS_TYPE] ?? null;
            $permission = $row[Storage::PERMISSIONS_PERMISSION] ?? null;
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
        return $map[$documentId] ?? $this->emptyPermissions();
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
        foreach (self::PERMISSION_TYPES as $type) {
            $initial[$type->value] = [];
        }

        return $initial;
    }

    /**
     * A renamed document leaves its rows keyed by the old id, which nothing reads any more, so
     * they are dropped and the full set is written under the new id.
     */
    private function movePermissions(string $collection, string $previousId, Document $document, WriteContext $context): void
    {
        $removeBuilder = $context->builder()->from(Storage::permissionsTable($collection));
        $removeBuilder->filter([Query::equal(Storage::PERMISSIONS_DOCUMENT, [$previousId])]);
        $context->run($removeBuilder->delete(), Event::PermissionsDelete);

        $this->afterDocumentCreate($collection, [$document], $context);
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
        foreach ($removals as $type => $permissions) {
            $removeConditions[] = Query::and([
                Query::equal(Storage::PERMISSIONS_DOCUMENT, [$documentId]),
                Query::equal(Storage::PERMISSIONS_TYPE, [$type]),
                Query::equal(Storage::PERMISSIONS_PERMISSION, $permissions),
            ]);
        }

        $removeBuilder = $context->builder()->from(Storage::permissionsTable($collection));
        $removeBuilder->filter([Query::or($removeConditions)]);
        $context->run($removeBuilder->delete(), Event::PermissionsDelete);
    }

    /**
     * @param  array<string, list<string>>  $additions
     */
    private function insertPermissions(string $collection, Document $document, string $documentId, array $additions, WriteContext $context): void
    {
        if (empty($additions)) {
            return;
        }

        $addBuilder = $context->builder()->into($context->rawTable(Storage::permissionsTable($collection)));

        foreach ($additions as $type => $permissions) {
            foreach (\array_values(\array_unique($permissions)) as $permission) {
                $row = $context->decorateRow([
                    Storage::PERMISSIONS_DOCUMENT => $documentId,
                    Storage::PERMISSIONS_TYPE => $type,
                    Storage::PERMISSIONS_PERMISSION => $permission,
                ], $document);
                $addBuilder->set($row);
            }
        }

        $context->run($addBuilder->insert(), Event::PermissionsCreate);
    }

    /**
     * Build permission rows for a document, applying decorateRow for tenant etc.
     *
     * @return list<array<string, mixed>>
     */
    private function buildPermissionRows(Document $document, WriteContext $context): array
    {
        $rows = [];

        foreach (self::PERMISSION_TYPES as $type) {
            foreach ($document->getPermissionsByType($type) as $permission) {
                $row = [
                    Storage::PERMISSIONS_DOCUMENT => $document->getId(),
                    Storage::PERMISSIONS_TYPE => $type->value,
                    Storage::PERMISSIONS_PERMISSION => \str_replace('"', '', $permission),
                ];
                $rows[] = $context->decorateRow($row, $document);
            }
        }

        return $rows;
    }
}
