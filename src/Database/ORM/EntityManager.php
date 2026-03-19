<?php

namespace Utopia\Database\ORM;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class EntityManager
{
    private UnitOfWork $unitOfWork;

    private IdentityMap $identityMap;

    private MetadataFactory $metadataFactory;

    private EntityMapper $entityMapper;

    private Database $db;

    public function __construct(Database $db)
    {
        $this->db = $db;
        $this->identityMap = new IdentityMap();
        $this->metadataFactory = new MetadataFactory();
        $this->entityMapper = new EntityMapper($this->metadataFactory);
        $this->unitOfWork = new UnitOfWork(
            $this->identityMap,
            $this->metadataFactory,
            $this->entityMapper,
        );
    }

    public function persist(object $entity): void
    {
        $this->unitOfWork->persist($entity);
    }

    public function remove(object $entity): void
    {
        $this->unitOfWork->remove($entity);
    }

    public function flush(): void
    {
        $this->unitOfWork->flush($this->db);
    }

    /**
     * @template T of object
     * @param  class-string<T>  $className
     * @return T|null
     */
    public function find(string $className, string $id): ?object
    {
        $metadata = $this->metadataFactory->getMetadata($className);

        $existing = $this->identityMap->get($metadata->collection, $id);
        if ($existing !== null) {
            /** @var T $existing */
            return $existing;
        }

        $document = $this->db->getDocument($metadata->collection, $id);

        if ($document->isEmpty()) {
            return null;
        }

        /** @var T $entity */
        $entity = $this->entityMapper->toEntity($document, $metadata, $this->identityMap);
        $this->unitOfWork->registerManaged($entity, $metadata);

        return $entity;
    }

    /**
     * @template T of object
     * @param  class-string<T>  $className
     * @param  array<Query>  $queries
     * @return array<T>
     */
    public function findMany(string $className, array $queries = []): array
    {
        $metadata = $this->metadataFactory->getMetadata($className);
        $documents = $this->db->find($metadata->collection, $queries);
        $entities = [];

        foreach ($documents as $document) {
            /** @var T $entity */
            $entity = $this->entityMapper->toEntity($document, $metadata, $this->identityMap);
            $this->unitOfWork->registerManaged($entity, $metadata);
            $entities[] = $entity;
        }

        return $entities;
    }

    /**
     * @template T of object
     * @param  class-string<T>  $className
     * @param  array<Query>  $queries
     * @return T|null
     */
    public function findOne(string $className, array $queries = []): ?object
    {
        $queries[] = Query::limit(1);
        $results = $this->findMany($className, $queries);

        if ($results === []) {
            return null;
        }

        /** @var T */
        return $results[0];
    }

    public function createCollectionFromEntity(string $className): Document
    {
        $metadata = $this->metadataFactory->getMetadata($className);
        $defs = $this->entityMapper->toCollectionDefinitions($metadata);

        /** @var \Utopia\Database\Collection $collection */
        $collection = $defs['collection'];
        /** @var array<\Utopia\Database\Relationship> $relationships */
        $relationships = $defs['relationships'];

        $doc = $this->db->createCollection(
            id: $collection->id,
            attributes: $collection->attributes,
            indexes: $collection->indexes,
            permissions: $collection->permissions !== [] ? $collection->permissions : null,
            documentSecurity: $collection->documentSecurity,
        );

        foreach ($relationships as $relationship) {
            $this->db->createRelationship($relationship);
        }

        return $doc;
    }

    public function detach(object $entity): void
    {
        $this->unitOfWork->detach($entity);
    }

    public function clear(): void
    {
        $this->unitOfWork->clear();
    }

    public function getUnitOfWork(): UnitOfWork
    {
        return $this->unitOfWork;
    }

    public function getIdentityMap(): IdentityMap
    {
        return $this->identityMap;
    }

    public function getMetadataFactory(): MetadataFactory
    {
        return $this->metadataFactory;
    }

    public function getEntityMapper(): EntityMapper
    {
        return $this->entityMapper;
    }
}
