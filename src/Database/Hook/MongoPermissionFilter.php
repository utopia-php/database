<?php

namespace Utopia\Database\Hook;

use MongoDB\BSON\Regex;
use Utopia\Database\Database;
use Utopia\Database\Validator\Authorization;

/**
 * MongoDB read hook that injects permission-based regex filters into queries.
 *
 * Unlike SQL adapters which use separate PermissionFilter (read) and Permission (write)
 * hooks, MongoDB stores permissions as an embedded `_permissions` array directly on the
 * document. This means no side-table management is needed on write, so there is no
 * corresponding MongoPermission hook. Read filtering is sufficient because the
 * permissions are part of the document itself.
 */
class MongoPermissionFilter implements Read
{
    /**
     * @param Authorization $authorization The authorization instance providing current user roles
     */
    public function __construct(
        private Authorization $authorization,
    ) {
    }

    /**
     * Inject a regex filter matching the current user's roles against the _permissions field.
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

        $roles = \implode('|', $this->authorization->getRoles());
        /** @var array<string, mixed> $permissionsFilter */
        $permissionsFilter = isset($filters['_permissions']) && \is_array($filters['_permissions'])
            ? $filters['_permissions']
            : [];
        $permissionsFilter['$in'] = [new Regex("{$forPermission}\\(\"(?:{$roles})\"\\)", 'i')];
        $filters['_permissions'] = $permissionsFilter;

        return $filters;
    }
}
