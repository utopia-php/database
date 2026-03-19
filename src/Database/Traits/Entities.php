<?php

namespace Utopia\Database\Traits;

use Utopia\Database\Document;
use Utopia\Database\ORM\EntityManager;
use Utopia\Database\Query;

trait Entities
{
    protected ?EntityManager $entityManager = null;

    public function getEntityManager(): EntityManager
    {
        if ($this->entityManager === null) {
            $this->entityManager = new EntityManager($this);
        }

        return $this->entityManager;
    }

    public function persistEntity(object $entity): void
    {
        $this->getEntityManager()->persist($entity);
    }

    public function removeEntity(object $entity): void
    {
        $this->getEntityManager()->remove($entity);
    }

    /**
     * Flush all pending entity changes to the database.
     */
    public function flushEntities(): void
    {
        $this->getEntityManager()->flush();
    }

    /**
     * @template T of object
     * @param  class-string<T>  $className
     * @return T|null
     */
    public function findEntity(string $className, string $id): ?object
    {
        return $this->getEntityManager()->find($className, $id);
    }

    /**
     * @template T of object
     * @param  class-string<T>  $className
     * @param  array<Query>  $queries
     * @return array<T>
     */
    public function findEntities(string $className, array $queries = []): array
    {
        return $this->getEntityManager()->findMany($className, $queries);
    }

    /**
     * @template T of object
     * @param  class-string<T>  $className
     * @param  array<Query>  $queries
     * @return T|null
     */
    public function findOneEntity(string $className, array $queries = []): ?object
    {
        return $this->getEntityManager()->findOne($className, $queries);
    }

    public function createCollectionFromEntity(string $className): Document
    {
        return $this->getEntityManager()->createCollectionFromEntity($className);
    }

    public function detachEntity(object $entity): void
    {
        $this->getEntityManager()->detach($entity);
    }

    public function clearEntityManager(): void
    {
        $this->getEntityManager()->clear();
    }
}
