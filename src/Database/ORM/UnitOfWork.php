<?php

namespace Utopia\Database\ORM;

use SplObjectStorage;
use Utopia\Database\Database;
use Utopia\Database\Document;

class UnitOfWork
{
    /** @var SplObjectStorage<object, EntityState> */
    private SplObjectStorage $entityStates;

    /** @var SplObjectStorage<object, array<string, mixed>> */
    private SplObjectStorage $originalSnapshots;

    /** @var array<int, object> */
    private array $scheduledInsertions = [];

    /** @var array<int, object> */
    private array $scheduledDeletions = [];

    private IdentityMap $identityMap;

    private MetadataFactory $metadataFactory;

    private EntityMapper $entityMapper;

    public function __construct(
        IdentityMap $identityMap,
        MetadataFactory $metadataFactory,
        EntityMapper $entityMapper,
    ) {
        $this->identityMap = $identityMap;
        $this->metadataFactory = $metadataFactory;
        $this->entityMapper = $entityMapper;
        $this->entityStates = new SplObjectStorage();
        $this->originalSnapshots = new SplObjectStorage();
    }

    public function persist(object $entity): void
    {
        if ($this->entityStates->contains($entity)) {
            $state = $this->entityStates[$entity];

            if ($state === EntityState::Managed) {
                return;
            }

            if ($state === EntityState::Removed) {
                $this->entityStates[$entity] = EntityState::Managed;
                $key = \array_search($entity, $this->scheduledDeletions, true);
                if ($key !== false) {
                    unset($this->scheduledDeletions[$key]);
                }

                return;
            }
        }

        $this->entityStates[$entity] = EntityState::New;
        $this->scheduledInsertions[] = $entity;

        $this->cascadePersist($entity);
    }

    public function remove(object $entity): void
    {
        if (! $this->entityStates->contains($entity)) {
            return;
        }

        $state = $this->entityStates[$entity];

        if ($state === EntityState::New) {
            unset($this->entityStates[$entity]);
            $key = \array_search($entity, $this->scheduledInsertions, true);
            if ($key !== false) {
                unset($this->scheduledInsertions[$key]);
            }

            return;
        }

        if ($state === EntityState::Managed) {
            $metadata = $this->metadataFactory->getMetadata($entity::class);
            if ($metadata->softDeleteColumn !== null) {
                $ref = new \ReflectionProperty($entity, $metadata->softDeleteColumn);
                $ref->setValue($entity, \date('Y-m-d H:i:s'));

                return;
            }

            $this->entityStates[$entity] = EntityState::Removed;
            $this->scheduledDeletions[] = $entity;
        }
    }

    public function forceRemove(object $entity): void
    {
        if (! $this->entityStates->contains($entity)) {
            return;
        }

        $state = $this->entityStates[$entity];

        if ($state === EntityState::New) {
            unset($this->entityStates[$entity]);
            $key = \array_search($entity, $this->scheduledInsertions, true);
            if ($key !== false) {
                unset($this->scheduledInsertions[$key]);
            }

            return;
        }

        if ($state === EntityState::Managed) {
            $this->entityStates[$entity] = EntityState::Removed;
            $this->scheduledDeletions[] = $entity;
        }
    }

    public function restore(object $entity): void
    {
        $metadata = $this->metadataFactory->getMetadata($entity::class);
        if ($metadata->softDeleteColumn === null) {
            return;
        }

        $ref = new \ReflectionProperty($entity, $metadata->softDeleteColumn);
        $ref->setValue($entity, null);
    }

    public function registerManaged(object $entity, EntityMetadata $metadata): void
    {
        $this->entityStates[$entity] = EntityState::Managed;
        $this->originalSnapshots[$entity] = $this->entityMapper->takeSnapshot($entity, $metadata);
    }

    public function flush(Database $db): void
    {
        /** @var array<string, array<object>> $inserts */
        $inserts = [];
        /** @var array<string, array<object>> $updates */
        $updates = [];
        /** @var array<string, array<object>> $deletes */
        $deletes = [];

        foreach ($this->scheduledInsertions as $entity) {
            $metadata = $this->metadataFactory->getMetadata($entity::class);
            $inserts[$metadata->collection][] = $entity;
        }

        foreach ($this->identityMap->all() as $entity) {
            if (! $this->entityStates->contains($entity)) {
                continue;
            }

            if ($this->entityStates[$entity] !== EntityState::Managed) {
                continue;
            }

            $metadata = $this->metadataFactory->getMetadata($entity::class);
            $currentSnapshot = $this->entityMapper->takeSnapshot($entity, $metadata);
            $originalSnapshot = $this->originalSnapshots->contains($entity)
                ? $this->originalSnapshots[$entity]
                : [];

            if ($currentSnapshot !== $originalSnapshot) {
                $updates[$metadata->collection][] = $entity;
            }
        }

        foreach ($this->scheduledDeletions as $entity) {
            $metadata = $this->metadataFactory->getMetadata($entity::class);
            $deletes[$metadata->collection][] = $entity;
        }

        if ($inserts === [] && $updates === [] && $deletes === []) {
            return;
        }

        $db->withTransaction(function () use ($db, $inserts, $updates, $deletes): void {
            foreach ($inserts as $collection => $entities) {
                $documents = [];
                $entityMap = [];

                foreach ($entities as $entity) {
                    $metadata = $this->metadataFactory->getMetadata($entity::class);
                    $this->invokeCallbacks($entity, $metadata->prePersistCallbacks);
                    $doc = $this->entityMapper->toDocument($entity, $metadata);
                    $documents[] = $doc;
                    $entityMap[] = $entity;
                }

                if (\count($documents) === 1) {
                    $created = $db->createDocument($collection, $documents[0]);
                    $metadata = $this->metadataFactory->getMetadata($entityMap[0]::class);
                    $this->entityMapper->applyDocumentToEntity($created, $entityMap[0], $metadata);
                    $this->identityMap->put($collection, $created->getId(), $entityMap[0]);
                    $this->entityStates[$entityMap[0]] = EntityState::Managed;
                    $this->originalSnapshots[$entityMap[0]] = $this->entityMapper->takeSnapshot($entityMap[0], $metadata);
                    $this->invokeCallbacks($entityMap[0], $metadata->postPersistCallbacks);
                } else {
                    $idx = 0;
                    $db->createDocuments($collection, $documents, Database::INSERT_BATCH_SIZE, function (Document $created) use (&$entityMap, &$idx, $collection): void {
                        if (! isset($entityMap[$idx])) {
                            return;
                        }
                        $entity = $entityMap[$idx];
                        $metadata = $this->metadataFactory->getMetadata($entity::class);
                        $this->entityMapper->applyDocumentToEntity($created, $entity, $metadata);
                        $this->identityMap->put($collection, $created->getId(), $entity);
                        $this->entityStates[$entity] = EntityState::Managed;
                        $this->originalSnapshots[$entity] = $this->entityMapper->takeSnapshot($entity, $metadata);
                        $this->invokeCallbacks($entity, $metadata->postPersistCallbacks);
                        $idx++;
                    });
                }
            }

            foreach ($updates as $collection => $entities) {
                foreach ($entities as $entity) {
                    $metadata = $this->metadataFactory->getMetadata($entity::class);
                    $this->invokeCallbacks($entity, $metadata->preUpdateCallbacks);
                    $document = $this->entityMapper->toDocument($entity, $metadata);
                    $id = $this->entityMapper->getId($entity, $metadata);

                    if ($id === null) {
                        continue;
                    }

                    $updated = $db->updateDocument($collection, $id, $document);
                    $this->entityMapper->applyDocumentToEntity($updated, $entity, $metadata);
                    $this->originalSnapshots[$entity] = $this->entityMapper->takeSnapshot($entity, $metadata);
                    $this->invokeCallbacks($entity, $metadata->postUpdateCallbacks);
                }
            }

            foreach ($deletes as $collection => $entities) {
                foreach ($entities as $entity) {
                    $metadata = $this->metadataFactory->getMetadata($entity::class);
                    $id = $this->entityMapper->getId($entity, $metadata);

                    if ($id === null) {
                        continue;
                    }

                    $this->invokeCallbacks($entity, $metadata->preRemoveCallbacks);
                    $db->deleteDocument($collection, $id);
                    $this->identityMap->remove($collection, $id);
                    $this->entityStates->detach($entity);

                    if ($this->originalSnapshots->contains($entity)) {
                        $this->originalSnapshots->detach($entity);
                    }

                    $this->invokeCallbacks($entity, $metadata->postRemoveCallbacks);
                }
            }
        });

        $this->scheduledInsertions = [];
        $this->scheduledDeletions = [];
    }

    public function detach(object $entity): void
    {
        if ($this->entityStates->contains($entity)) {
            $this->entityStates->detach($entity);
        }

        if ($this->originalSnapshots->contains($entity)) {
            $this->originalSnapshots->detach($entity);
        }

        $key = \array_search($entity, $this->scheduledInsertions, true);
        if ($key !== false) {
            unset($this->scheduledInsertions[$key]);
        }

        $key = \array_search($entity, $this->scheduledDeletions, true);
        if ($key !== false) {
            unset($this->scheduledDeletions[$key]);
        }

        $metadata = $this->metadataFactory->getMetadata($entity::class);
        $id = $this->entityMapper->getId($entity, $metadata);

        if ($id !== null) {
            $this->identityMap->remove($metadata->collection, $id);
        }
    }

    public function clear(): void
    {
        $this->entityStates = new SplObjectStorage();
        $this->originalSnapshots = new SplObjectStorage();
        $this->scheduledInsertions = [];
        $this->scheduledDeletions = [];
        $this->identityMap->clear();
    }

    public function getState(object $entity): ?EntityState
    {
        if (! $this->entityStates->contains($entity)) {
            return null;
        }

        return $this->entityStates[$entity];
    }

    public function getIdentityMap(): IdentityMap
    {
        return $this->identityMap;
    }

    private function cascadePersist(object $entity): void
    {
        $metadata = $this->metadataFactory->getMetadata($entity::class);

        foreach ($metadata->relationships as $mapping) {
            $ref = new \ReflectionProperty($entity, $mapping->propertyName);

            if (! $ref->isInitialized($entity)) {
                continue;
            }

            $value = $ref->getValue($entity);

            if ($value === null) {
                continue;
            }

            if (\is_array($value)) {
                foreach ($value as $related) {
                    if (\is_object($related) && ! $this->entityStates->contains($related)) {
                        $this->persist($related);
                    }
                }
            } elseif (\is_object($value) && ! $this->entityStates->contains($value)) {
                $this->persist($value);
            }
        }
    }

    /**
     * @param  array<string>  $methods
     */
    private function invokeCallbacks(object $entity, array $methods): void
    {
        foreach ($methods as $method) {
            $entity->{$method}();
        }
    }
}
