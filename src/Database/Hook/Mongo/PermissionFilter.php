<?php

namespace Utopia\Database\Hook\Mongo;

use Utopia\Database\Database;
use Utopia\Database\Hook\Read;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

/**
 * MongoDB read hook that injects permission-based filters into queries.
 *
 * Unlike SQL adapters which use separate PermissionFilter (read) and Permission (write)
 * hooks, MongoDB stores permissions as an embedded `_permissions` array directly on the
 * document. This means no side-table management is needed on write, so there is no
 * corresponding MongoPermission hook. Read filtering is sufficient because the
 * permissions are part of the document itself.
 */
class PermissionFilter implements Read
{
    /**
     * @param Authorization $authorization The authorization instance providing current user roles
     */
    public function __construct(
        private Authorization $authorization,
    ) {
    }

    /**
     * Inject an exact-match `$in` filter of `type("role")` strings against `_permissions`.
     *
     * @param array<string, mixed> $filters The current MongoDB filter array
     * @param string $collection The collection being queried
     * @param string $forPermission The permission type to filter for (e.g. 'read')
     * @return array<string, mixed> The modified filter array with permission constraints
     */
    public function applyFilters(array $filters, string $collection, string $forPermission = 'read'): array
    {
        if (! $this->authorization->getStatus()) {
            return $filters;
        }

        if ($collection === Database::METADATA) {
            return $filters;
        }

        $permissions = [];
        foreach ($this->authorization->getRoles() as $role) {
            $permissions[] = $forPermission.'("'.$role.'")';
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
