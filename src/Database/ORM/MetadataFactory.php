<?php

namespace Utopia\Database\ORM;

use ReflectionClass;
use Utopia\Database\ORM\Mapping\BelongsTo;
use Utopia\Database\ORM\Mapping\BelongsToMany;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\CreatedAt;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\HasMany;
use Utopia\Database\ORM\Mapping\HasOne;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\Permissions;
use Utopia\Database\ORM\Mapping\TableIndex;
use Utopia\Database\ORM\Mapping\Tenant;
use Utopia\Database\ORM\Mapping\UpdatedAt;
use Utopia\Database\ORM\Mapping\Version;
use Utopia\Database\RelationType;

class MetadataFactory
{
    /** @var array<string, EntityMetadata> */
    private static array $cache = [];

    public function getMetadata(string $className): EntityMetadata
    {
        if (isset(self::$cache[$className])) {
            return self::$cache[$className];
        }

        $ref = new ReflectionClass($className);
        $entityAttrs = $ref->getAttributes(Entity::class);

        if ($entityAttrs === []) {
            throw new \RuntimeException("Class {$className} is not annotated with #[Entity]");
        }

        /** @var Entity $entity */
        $entity = $entityAttrs[0]->newInstance();

        $idProperty = null;
        $versionProperty = null;
        $createdAtProperty = null;
        $updatedAtProperty = null;
        $tenantProperty = null;
        $permissionsProperty = null;
        $columns = [];
        $relationships = [];

        foreach ($ref->getProperties() as $prop) {
            $name = $prop->getName();

            if ($prop->getAttributes(Id::class)) {
                $idProperty = $name;

                continue;
            }

            if ($prop->getAttributes(Version::class)) {
                $versionProperty = $name;

                continue;
            }

            if ($prop->getAttributes(CreatedAt::class)) {
                $createdAtProperty = $name;

                continue;
            }

            if ($prop->getAttributes(UpdatedAt::class)) {
                $updatedAtProperty = $name;

                continue;
            }

            if ($prop->getAttributes(Tenant::class)) {
                $tenantProperty = $name;

                continue;
            }

            if ($prop->getAttributes(Permissions::class)) {
                $permissionsProperty = $name;

                continue;
            }

            $columnAttrs = $prop->getAttributes(Column::class);
            if ($columnAttrs !== []) {
                /** @var Column $col */
                $col = $columnAttrs[0]->newInstance();
                $docKey = $col->key ?? $name;
                $columns[$name] = new ColumnMapping($name, $docKey, $col);

                continue;
            }

            $rel = $this->parseRelationship($prop, $name);
            if ($rel !== null) {
                $relationships[$name] = $rel;
            }
        }

        $indexes = [];
        foreach ($ref->getAttributes(TableIndex::class) as $idxAttr) {
            $indexes[] = $idxAttr->newInstance();
        }

        $metadata = new EntityMetadata(
            className: $className,
            collection: $entity->collection,
            documentSecurity: $entity->documentSecurity,
            permissions: $entity->permissions,
            idProperty: $idProperty,
            versionProperty: $versionProperty,
            createdAtProperty: $createdAtProperty,
            updatedAtProperty: $updatedAtProperty,
            tenantProperty: $tenantProperty,
            permissionsProperty: $permissionsProperty,
            columns: $columns,
            relationships: $relationships,
            indexes: $indexes,
        );

        self::$cache[$className] = $metadata;

        return $metadata;
    }

    /**
     * Get the collection name for an entity class.
     */
    public function getCollection(string $className): string
    {
        return $this->getMetadata($className)->collection;
    }

    /**
     * Clear the metadata cache (useful for testing).
     */
    public static function clearCache(): void
    {
        self::$cache = [];
    }

    private function parseRelationship(\ReflectionProperty $prop, string $name): ?RelationshipMapping
    {
        $hasOne = $prop->getAttributes(HasOne::class);
        if ($hasOne !== []) {
            /** @var HasOne $attr */
            $attr = $hasOne[0]->newInstance();

            return new RelationshipMapping(
                propertyName: $name,
                documentKey: $attr->key ?: $name,
                type: RelationType::OneToOne,
                targetClass: $attr->target,
                twoWayKey: $attr->twoWayKey,
                twoWay: $attr->twoWay,
                onDelete: $attr->onDelete,
            );
        }

        $belongsTo = $prop->getAttributes(BelongsTo::class);
        if ($belongsTo !== []) {
            /** @var BelongsTo $attr */
            $attr = $belongsTo[0]->newInstance();

            return new RelationshipMapping(
                propertyName: $name,
                documentKey: $attr->key ?: $name,
                type: RelationType::ManyToOne,
                targetClass: $attr->target,
                twoWayKey: $attr->twoWayKey,
                twoWay: $attr->twoWay,
                onDelete: $attr->onDelete,
            );
        }

        $hasMany = $prop->getAttributes(HasMany::class);
        if ($hasMany !== []) {
            /** @var HasMany $attr */
            $attr = $hasMany[0]->newInstance();

            return new RelationshipMapping(
                propertyName: $name,
                documentKey: $attr->key ?: $name,
                type: RelationType::OneToMany,
                targetClass: $attr->target,
                twoWayKey: $attr->twoWayKey,
                twoWay: $attr->twoWay,
                onDelete: $attr->onDelete,
            );
        }

        $belongsToMany = $prop->getAttributes(BelongsToMany::class);
        if ($belongsToMany !== []) {
            /** @var BelongsToMany $attr */
            $attr = $belongsToMany[0]->newInstance();

            return new RelationshipMapping(
                propertyName: $name,
                documentKey: $attr->key ?: $name,
                type: RelationType::ManyToMany,
                targetClass: $attr->target,
                twoWayKey: $attr->twoWayKey,
                twoWay: $attr->twoWay,
                onDelete: $attr->onDelete,
            );
        }

        return null;
    }
}
