<?php

namespace Utopia\Database\ORM;

use Utopia\Database\ORM\Mapping\TableIndex;

class EntityMetadata
{
    /**
     * @param  array<string, ColumnMapping>  $columns
     * @param  array<string, RelationshipMapping>  $relationships
     * @param  array<TableIndex>  $indexes
     * @param  array<string>  $permissions
     */
    public function __construct(
        public readonly string $className,
        public readonly string $collection,
        public readonly bool $documentSecurity,
        public readonly array $permissions,
        public readonly ?string $idProperty,
        public readonly ?string $versionProperty,
        public readonly ?string $createdAtProperty,
        public readonly ?string $updatedAtProperty,
        public readonly ?string $tenantProperty,
        public readonly ?string $permissionsProperty,
        public readonly array $columns,
        public readonly array $relationships,
        public readonly array $indexes,
    ) {
    }
}
