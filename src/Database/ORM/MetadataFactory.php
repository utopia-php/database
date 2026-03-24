<?php

namespace Utopia\Database\ORM;

use ReflectionClass;
use Utopia\Database\ORM\Mapping\BelongsTo;
use Utopia\Database\ORM\Mapping\BelongsToMany;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\CreatedAt;
use Utopia\Database\ORM\Mapping\Embedded;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\HasMany;
use Utopia\Database\ORM\Mapping\HasOne;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\Permissions;
use Utopia\Database\ORM\Mapping\PostPersist;
use Utopia\Database\ORM\Mapping\PostRemove;
use Utopia\Database\ORM\Mapping\PostUpdate;
use Utopia\Database\ORM\Mapping\PrePersist;
use Utopia\Database\ORM\Mapping\PreRemove;
use Utopia\Database\ORM\Mapping\PreUpdate;
use Utopia\Database\ORM\Mapping\SoftDelete;
use Utopia\Database\ORM\Mapping\TableIndex;
use Utopia\Database\ORM\Mapping\Tenant;
use Utopia\Database\ORM\Mapping\UpdatedAt;
use Utopia\Database\ORM\Mapping\Version;
use Utopia\Database\RelationType;
use Utopia\Database\Type\TypeRegistry;

class MetadataFactory
{
    /** @var array<string, EntityMetadata> */
    private static array $cache = [];

    private ?TypeRegistry $typeRegistry = null;

    public function setTypeRegistry(?TypeRegistry $typeRegistry): void
    {
        $this->typeRegistry = $typeRegistry;
    }

    public function getTypeRegistry(): ?TypeRegistry
    {
        return $this->typeRegistry;
    }

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

        $softDeleteAttrs = $ref->getAttributes(SoftDelete::class);
        $softDeleteColumn = null;
        if ($softDeleteAttrs !== []) {
            /** @var SoftDelete $sd */
            $sd = $softDeleteAttrs[0]->newInstance();
            $softDeleteColumn = $sd->column;
        }

        $idProperty = null;
        $versionProperty = null;
        $createdAtProperty = null;
        $updatedAtProperty = null;
        $tenantProperty = null;
        $permissionsProperty = null;
        $columns = [];
        $relationships = [];
        $embeddables = [];

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

            $embeddedAttrs = $prop->getAttributes(Embedded::class);
            if ($embeddedAttrs !== []) {
                /** @var Embedded $emb */
                $emb = $embeddedAttrs[0]->newInstance();
                $embeddables[$name] = new EmbeddableMapping($name, $emb->type, $emb->prefix ?: $name . '_');

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

        $lifecycleCallbacks = $this->parseLifecycleCallbacks($ref);

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
            embeddables: $embeddables,
            softDeleteColumn: $softDeleteColumn,
            prePersistCallbacks: $lifecycleCallbacks['prePersist'],
            postPersistCallbacks: $lifecycleCallbacks['postPersist'],
            preUpdateCallbacks: $lifecycleCallbacks['preUpdate'],
            postUpdateCallbacks: $lifecycleCallbacks['postUpdate'],
            preRemoveCallbacks: $lifecycleCallbacks['preRemove'],
            postRemoveCallbacks: $lifecycleCallbacks['postRemove'],
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

    /**
     * @return array{prePersist: array<string>, postPersist: array<string>, preUpdate: array<string>, postUpdate: array<string>, preRemove: array<string>, postRemove: array<string>}
     */
    private function parseLifecycleCallbacks(ReflectionClass $ref): array
    {
        $callbacks = [
            'prePersist' => [],
            'postPersist' => [],
            'preUpdate' => [],
            'postUpdate' => [],
            'preRemove' => [],
            'postRemove' => [],
        ];

        foreach ($ref->getMethods() as $method) {
            $name = $method->getName();

            if ($method->getAttributes(PrePersist::class)) {
                $callbacks['prePersist'][] = $name;
            }

            if ($method->getAttributes(PostPersist::class)) {
                $callbacks['postPersist'][] = $name;
            }

            if ($method->getAttributes(PreUpdate::class)) {
                $callbacks['preUpdate'][] = $name;
            }

            if ($method->getAttributes(PostUpdate::class)) {
                $callbacks['postUpdate'][] = $name;
            }

            if ($method->getAttributes(PreRemove::class)) {
                $callbacks['preRemove'][] = $name;
            }

            if ($method->getAttributes(PostRemove::class)) {
                $callbacks['postRemove'][] = $name;
            }
        }

        return $callbacks;
    }
}
