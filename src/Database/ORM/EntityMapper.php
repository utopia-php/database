<?php

namespace Utopia\Database\ORM;

use ReflectionClass;
use ReflectionProperty;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Relationship as RelationshipModel;
use Utopia\Database\RelationSide;

class EntityMapper
{
    private MetadataFactory $metadataFactory;

    public function __construct(MetadataFactory $metadataFactory)
    {
        $this->metadataFactory = $metadataFactory;
    }

    public function toDocument(object $entity, EntityMetadata $metadata): Document
    {
        $data = [];

        if ($metadata->idProperty !== null) {
            $data['$id'] = $this->getPropertyValue($entity, $metadata->idProperty);
        }

        if ($metadata->versionProperty !== null) {
            $data['$version'] = $this->getPropertyValue($entity, $metadata->versionProperty);
        }

        if ($metadata->createdAtProperty !== null) {
            $data['$createdAt'] = $this->getPropertyValue($entity, $metadata->createdAtProperty);
        }

        if ($metadata->updatedAtProperty !== null) {
            $data['$updatedAt'] = $this->getPropertyValue($entity, $metadata->updatedAtProperty);
        }

        if ($metadata->tenantProperty !== null) {
            $data['$tenant'] = $this->getPropertyValue($entity, $metadata->tenantProperty);
        }

        if ($metadata->permissionsProperty !== null) {
            $data['$permissions'] = $this->getPropertyValue($entity, $metadata->permissionsProperty) ?? [];
        }

        foreach ($metadata->columns as $mapping) {
            $value = $this->getPropertyValue($entity, $mapping->propertyName);
            $data[$mapping->documentKey] = $value;
        }

        foreach ($metadata->relationships as $mapping) {
            $value = $this->getPropertyValue($entity, $mapping->propertyName);

            if ($value === null) {
                $data[$mapping->documentKey] = null;

                continue;
            }

            if (\is_array($value)) {
                $data[$mapping->documentKey] = \array_map(function (mixed $item) use ($mapping): mixed {
                    if (\is_object($item)) {
                        $relMeta = $this->metadataFactory->getMetadata($mapping->targetClass);

                        return $this->toDocument($item, $relMeta);
                    }

                    return $item;
                }, $value);
            } elseif (\is_object($value)) {
                $relMeta = $this->metadataFactory->getMetadata($mapping->targetClass);
                $data[$mapping->documentKey] = $this->toDocument($value, $relMeta);
            } else {
                $data[$mapping->documentKey] = $value;
            }
        }

        return new Document($data);
    }

    public function toEntity(Document $document, EntityMetadata $metadata, IdentityMap $identityMap): object
    {
        $id = $document->getId();

        if ($id !== '' && $identityMap->has($metadata->collection, $id)) {
            /** @var object $existing */
            $existing = $identityMap->get($metadata->collection, $id);

            return $existing;
        }

        $ref = new ReflectionClass($metadata->className);
        $entity = $ref->newInstanceWithoutConstructor();

        if ($id !== '') {
            $identityMap->put($metadata->collection, $id, $entity);
        }

        if ($metadata->idProperty !== null) {
            $this->setPropertyValue($entity, $metadata->idProperty, $id);
        }

        if ($metadata->versionProperty !== null) {
            $this->setPropertyValue($entity, $metadata->versionProperty, $document->getAttribute('$version'));
        }

        if ($metadata->createdAtProperty !== null) {
            $this->setPropertyValue($entity, $metadata->createdAtProperty, $document->getAttribute('$createdAt'));
        }

        if ($metadata->updatedAtProperty !== null) {
            $this->setPropertyValue($entity, $metadata->updatedAtProperty, $document->getAttribute('$updatedAt'));
        }

        if ($metadata->tenantProperty !== null) {
            $this->setPropertyValue($entity, $metadata->tenantProperty, $document->getAttribute('$tenant'));
        }

        if ($metadata->permissionsProperty !== null) {
            $this->setPropertyValue($entity, $metadata->permissionsProperty, $document->getPermissions());
        }

        foreach ($metadata->columns as $mapping) {
            $value = $document->getAttribute($mapping->documentKey, $mapping->column->default);
            $this->setPropertyValue($entity, $mapping->propertyName, $value);
        }

        foreach ($metadata->relationships as $mapping) {
            $value = $document->getAttribute($mapping->documentKey);

            if ($value === null) {
                $isArray = $mapping->type === \Utopia\Database\RelationType::OneToMany
                    || $mapping->type === \Utopia\Database\RelationType::ManyToMany;
                $this->setPropertyValue($entity, $mapping->propertyName, $isArray ? [] : null);

                continue;
            }

            $relMeta = $this->metadataFactory->getMetadata($mapping->targetClass);

            if (\is_array($value)) {
                $related = \array_map(function (mixed $item) use ($relMeta, $identityMap): mixed {
                    if ($item instanceof Document && ! $item->isEmpty()) {
                        return $this->toEntity($item, $relMeta, $identityMap);
                    }

                    return $item;
                }, $value);
                $this->setPropertyValue($entity, $mapping->propertyName, $related);
            } elseif ($value instanceof Document && ! $value->isEmpty()) {
                $this->setPropertyValue($entity, $mapping->propertyName, $this->toEntity($value, $relMeta, $identityMap));
            } else {
                $this->setPropertyValue($entity, $mapping->propertyName, $value);
            }
        }

        return $entity;
    }

    public function applyDocumentToEntity(Document $document, object $entity, EntityMetadata $metadata): void
    {
        if ($metadata->idProperty !== null) {
            $this->setPropertyValue($entity, $metadata->idProperty, $document->getId());
        }

        if ($metadata->versionProperty !== null) {
            $this->setPropertyValue($entity, $metadata->versionProperty, $document->getAttribute('$version'));
        }

        if ($metadata->createdAtProperty !== null) {
            $this->setPropertyValue($entity, $metadata->createdAtProperty, $document->getAttribute('$createdAt'));
        }

        if ($metadata->updatedAtProperty !== null) {
            $this->setPropertyValue($entity, $metadata->updatedAtProperty, $document->getAttribute('$updatedAt'));
        }
    }

    /**
     * @return array<string, mixed>
     */
    public function takeSnapshot(object $entity, EntityMetadata $metadata): array
    {
        $snapshot = [];

        if ($metadata->idProperty !== null) {
            $snapshot['$id'] = $this->getPropertyValue($entity, $metadata->idProperty);
        }

        foreach ($metadata->columns as $mapping) {
            $snapshot[$mapping->documentKey] = $this->getPropertyValue($entity, $mapping->propertyName);
        }

        foreach ($metadata->relationships as $mapping) {
            $value = $this->getPropertyValue($entity, $mapping->propertyName);

            if (\is_array($value)) {
                $snapshot[$mapping->documentKey] = \array_map(function (mixed $item) use ($mapping): mixed {
                    if (\is_object($item)) {
                        $relMeta = $this->metadataFactory->getMetadata($mapping->targetClass);

                        return $this->getId($item, $relMeta);
                    }

                    return $item;
                }, $value);
            } elseif (\is_object($value)) {
                $relMeta = $this->metadataFactory->getMetadata($mapping->targetClass);
                $snapshot[$mapping->documentKey] = $this->getId($value, $relMeta);
            } else {
                $snapshot[$mapping->documentKey] = $value;
            }
        }

        return $snapshot;
    }

    public function getId(object $entity, EntityMetadata $metadata): ?string
    {
        if ($metadata->idProperty === null) {
            return null;
        }

        /** @var string|null $value */
        $value = $this->getPropertyValue($entity, $metadata->idProperty);

        return $value;
    }

    /**
     * @return array{collection: Collection, relationships: array<RelationshipModel>}
     */
    public function toCollectionDefinitions(EntityMetadata $metadata): array
    {
        $attributes = [];
        foreach ($metadata->columns as $mapping) {
            $col = $mapping->column;
            $attributes[] = new Attribute(
                key: $mapping->documentKey,
                type: $col->type,
                size: $col->size,
                required: $col->required,
                default: $col->default,
                signed: $col->signed,
                array: $col->array,
                format: $col->format,
                formatOptions: $col->formatOptions,
                filters: $col->filters,
            );
        }

        $indexes = [];
        foreach ($metadata->indexes as $tableIndex) {
            $indexes[] = new Index(
                key: $tableIndex->key,
                type: $tableIndex->type,
                attributes: $tableIndex->attributes,
                lengths: $tableIndex->lengths,
                orders: $tableIndex->orders,
            );
        }

        $collection = new Collection(
            id: $metadata->collection,
            name: $metadata->collection,
            attributes: $attributes,
            indexes: $indexes,
            permissions: $metadata->permissions,
            documentSecurity: $metadata->documentSecurity,
        );

        $relationships = [];
        foreach ($metadata->relationships as $mapping) {
            $relMeta = $this->metadataFactory->getMetadata($mapping->targetClass);

            $relationships[] = new RelationshipModel(
                collection: $metadata->collection,
                relatedCollection: $relMeta->collection,
                type: $mapping->type,
                twoWay: $mapping->twoWay,
                key: $mapping->documentKey,
                twoWayKey: $mapping->twoWayKey,
                onDelete: $mapping->onDelete,
                side: RelationSide::Parent,
            );
        }

        return [
            'collection' => $collection,
            'relationships' => $relationships,
        ];
    }

    private function getPropertyValue(object $entity, string $property): mixed
    {
        $ref = new ReflectionProperty($entity, $property);

        if (! $ref->isInitialized($entity)) {
            return null;
        }

        return $ref->getValue($entity);
    }

    private function setPropertyValue(object $entity, string $property, mixed $value): void
    {
        $ref = new ReflectionProperty($entity, $property);
        $ref->setValue($entity, $value);
    }
}
