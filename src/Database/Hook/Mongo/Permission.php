<?php

namespace Utopia\Database\Hook\Mongo;

use Utopia\Database\PermissionType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

/**
 * Narrows a MongoDB read to the documents the current roles may access through an exact-match `$in` of
 * `type("role")` strings against the embedded `_permissions` array. Permissions live on the document, so no
 * write-side hook exists.
 */
final readonly class Permission implements Read
{
    public function __construct(
        private Authorization $authorization,
    ) {
    }

    /**
     * @param  array<string, mixed>  $filters
     * @return array<string, mixed>
     */
    public function applyFilters(array $filters, string $collection, PermissionType $forPermission): array
    {
        if (! $this->authorization->getStatus()) {
            return $filters;
        }

        $permissions = [];
        foreach ($this->authorization->getRoles() as $role) {
            $permissions[] = $forPermission->value.'("'.$role.'")';
        }

        /** @var array<string, mixed> $permissionsFilter */
        $permissionsFilter = isset($filters[Storage::PERMISSIONS]) && \is_array($filters[Storage::PERMISSIONS])
            ? $filters[Storage::PERMISSIONS]
            : [];
        $permissionsFilter['$in'] = $permissions;
        $filters[Storage::PERMISSIONS] = $permissionsFilter;

        return $filters;
    }
}
