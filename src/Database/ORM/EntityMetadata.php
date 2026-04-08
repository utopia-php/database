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
     * @param  array<string, EmbeddableMapping>  $embeddables
     * @param  array<string>  $prePersistCallbacks
     * @param  array<string>  $postPersistCallbacks
     * @param  array<string>  $preUpdateCallbacks
     * @param  array<string>  $postUpdateCallbacks
     * @param  array<string>  $preRemoveCallbacks
     * @param  array<string>  $postRemoveCallbacks
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
        public readonly array $embeddables = [],
        public readonly ?string $softDeleteColumn = null,
        public readonly array $prePersistCallbacks = [],
        public readonly array $postPersistCallbacks = [],
        public readonly array $preUpdateCallbacks = [],
        public readonly array $postUpdateCallbacks = [],
        public readonly array $preRemoveCallbacks = [],
        public readonly array $postRemoveCallbacks = [],
    ) {
    }
}
