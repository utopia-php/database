<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\EntityMapper;
use Utopia\Database\ORM\EntityState;
use Utopia\Database\ORM\IdentityMap;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\ORM\UnitOfWork;

class UnitOfWorkTest extends TestCase
{
    private UnitOfWork $uow;

    private IdentityMap $identityMap;

    private MetadataFactory $metadataFactory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->identityMap = new IdentityMap();
        $this->metadataFactory = new MetadataFactory();
        $mapper = new EntityMapper($this->metadataFactory);
        $this->uow = new UnitOfWork($this->identityMap, $this->metadataFactory, $mapper);
    }

    public function testPersistNewEntity(): void
    {
        $entity = new TestEntity();
        $entity->id = 'new-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->uow->persist($entity);

        $this->assertEquals(EntityState::New, $this->uow->getState($entity));
    }

    public function testPersistIdempotent(): void
    {
        $entity = new TestEntity();
        $entity->id = 'new-2';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->uow->persist($entity);
        $this->uow->persist($entity);

        $this->assertEquals(EntityState::New, $this->uow->getState($entity));
    }

    public function testRemoveNewEntityUnracks(): void
    {
        $entity = new TestEntity();
        $entity->id = 'new-3';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->uow->persist($entity);
        $this->uow->remove($entity);

        $this->assertNull($this->uow->getState($entity));
    }

    public function testRemoveManagedEntitySchedulesDeletion(): void
    {
        $entity = new TestEntity();
        $entity->id = 'managed-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'managed-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->assertEquals(EntityState::Managed, $this->uow->getState($entity));

        $this->uow->remove($entity);

        $this->assertEquals(EntityState::Removed, $this->uow->getState($entity));
    }

    public function testPersistRemovedEntityRestoresManaged(): void
    {
        $entity = new TestEntity();
        $entity->id = 'managed-2';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'managed-2', $entity);
        $this->uow->registerManaged($entity, $metadata);
        $this->uow->remove($entity);
        $this->uow->persist($entity);

        $this->assertEquals(EntityState::Managed, $this->uow->getState($entity));
    }

    public function testDetach(): void
    {
        $entity = new TestEntity();
        $entity->id = 'detach-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->uow->persist($entity);
        $this->uow->detach($entity);

        $this->assertNull($this->uow->getState($entity));
    }

    public function testClear(): void
    {
        $e1 = new TestEntity();
        $e1->id = 'clear-1';
        $e1->name = 'A';
        $e1->email = 'a@example.com';

        $e2 = new TestEntity();
        $e2->id = 'clear-2';
        $e2->name = 'B';
        $e2->email = 'b@example.com';

        $this->uow->persist($e1);
        $this->uow->persist($e2);
        $this->uow->clear();

        $this->assertNull($this->uow->getState($e1));
        $this->assertNull($this->uow->getState($e2));
        $this->assertEmpty(\iterator_to_array($this->identityMap->all()));
    }

    public function testGetStateReturnsNullForUntracked(): void
    {
        $entity = new TestEntity();
        $this->assertNull($this->uow->getState($entity));
    }

    public function testCascadePersistRelatedEntities(): void
    {
        $post = new TestPost();
        $post->id = 'post-1';
        $post->title = 'My Post';
        $post->content = 'Content';

        $user = new TestEntity();
        $user->id = 'cascade-1';
        $user->name = 'User';
        $user->email = 'user@example.com';
        $user->posts = [$post];

        $this->uow->persist($user);

        $this->assertEquals(EntityState::New, $this->uow->getState($user));
        $this->assertEquals(EntityState::New, $this->uow->getState($post));
    }
}
