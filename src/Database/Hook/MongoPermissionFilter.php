<?php

namespace Utopia\Database\Hook;

use MongoDB\BSON\Regex;
use Utopia\Database\Database;
use Utopia\Database\Validator\Authorization;

class MongoPermissionFilter implements Read
{
    public function __construct(
        private Authorization $authorization,
    ) {
    }

    public function applyFilters(array $filters, string $collection, string $forPermission = 'read'): array
    {
        if (!$this->authorization->getStatus()) {
            return $filters;
        }

        if ($collection === Database::METADATA) {
            return $filters;
        }

        $roles = \implode('|', $this->authorization->getRoles());
        $filters['_permissions']['$in'] = [new Regex("{$forPermission}\\(\".*(?:{$roles}).*\"\\)", 'i')];

        return $filters;
    }
}
