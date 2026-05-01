<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\ORM\EntityMapper;
use Utopia\Database\ORM\EntityState;
use Utopia\Database\ORM\IdentityMap;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\ORM\UnitOfWork;

class UnitOfWorkAdvancedTest extends TestCase
{
    private UnitOfWork $uow;

    private IdentityMap $identityMap;

    private MetadataFactory $metadataFactory;

    private EntityMapper $mapper;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->identityMap = new IdentityMap();
        $this->metadataFactory = new MetadataFactory();
        $this->mapper = new EntityMapper($this->metadataFactory);
        $this->uow = new UnitOfWork($this->identityMap, $this->metadataFactory, $this->mapper);
    }

    public function testFlushWithNoChangesDoesNothing(): void
    {
        $db = $this->createMock(Database::class);

        $db->expects($this->never())
            ->method('withTransaction');

        $this->uow->flush($db);
    }

    public function testFlushProcessesInsertsBeforeUpdatesBeforeDeletes(): void
    {
        $insertEntity = new TestEntity();
        $insertEntity->id = 'insert-1';
        $insertEntity->name = 'Insert';
        $insertEntity->email = 'insert@example.com';
        $insertEntity->age = 20;
        $insertEntity->active = true;

        $updateEntity = new TestEntity();
        $updateEntity->id = 'update-1';
        $updateEntity->name = 'Before';
        $updateEntity->email = 'update@example.com';
        $updateEntity->age = 25;
        $updateEntity->active = true;

        $deleteEntity = new TestEntity();
        $deleteEntity->id = 'delete-1';
        $deleteEntity->name = 'Delete';
        $deleteEntity->email = 'delete@example.com';
        $deleteEntity->age = 30;
        $deleteEntity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);

        $this->identityMap->put('users', 'update-1', $updateEntity);
        $this->uow->registerManaged($updateEntity, $metadata);
        $updateEntity->name = 'After';

        $this->identityMap->put('users', 'delete-1', $deleteEntity);
        $this->uow->registerManaged($deleteEntity, $metadata);
        $this->uow->remove($deleteEntity);

        $this->uow->persist($insertEntity);

        $callOrder = [];
        $db = $this->createMock(Database::class);

        $db->expects($this->once())
            ->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $db->method('createDocument')
            ->willReturnCallback(function (string $collection, Document $doc) use (&$callOrder) {
                $callOrder[] = 'insert';

                return $doc;
            });

        $db->method('updateDocument')
            ->willReturnCallback(function (string $collection, string $id, Document $doc) use (&$callOrder) {
                $callOrder[] = 'update';

                return $doc;
            });

        $db->method('deleteDocument')
            ->willReturnCallback(function (string $collection, string $id) use (&$callOrder) {
                $callOrder[] = 'delete';

                return true;
            });

        $this->uow->flush($db);

        $this->assertEquals(['insert', 'update', 'delete'], $callOrder);
    }

    public function testRegisterManagedSetsStateAndTakesSnapshot(): void
    {
        $entity = new TestEntity();
        $entity->id = 'reg-1';
        $entity->name = 'Registered';
        $entity->email = 'reg@example.com';
        $entity->age = 30;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->uow->registerManaged($entity, $metadata);

        $this->assertEquals(EntityState::Managed, $this->uow->getState($entity));
    }

    public function testDirtyDetectionUnchangedEntityNotQueuedForUpdate(): void
    {
        $entity = new TestEntity();
        $entity->id = 'dirty-no-1';
        $entity->name = 'Clean';
        $entity->email = 'clean@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'dirty-no-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $db = $this->createMock(Database::class);

        $db->expects($this->never())
            ->method('withTransaction');

        $db->expects($this->never())
            ->method('updateDocument');

        $this->uow->flush($db);
    }

    public function testDirtyDetectionChangedColumnQueuedForUpdate(): void
    {
        $entity = new TestEntity();
        $entity->id = 'dirty-col-1';
        $entity->name = 'Before';
        $entity->email = 'dirty@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'dirty-col-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $entity->name = 'After';

        $db = $this->createMock(Database::class);

        $db->expects($this->once())
            ->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $updatedDoc = new Document([
            '$id' => 'dirty-col-1',
            '$version' => 2,
            '$createdAt' => '2024-01-01 00:00:00',
            '$updatedAt' => '2024-01-02 00:00:00',
            'name' => 'After',
        ]);

        $db->expects($this->once())
            ->method('updateDocument')
            ->with('users', 'dirty-col-1', $this->isInstanceOf(Document::class))
            ->willReturn($updatedDoc);

        $this->uow->flush($db);
    }

    public function testDirtyDetectionChangedRelationshipQueuedForUpdate(): void
    {
        $entity = new TestEntity();
        $entity->id = 'dirty-rel-1';
        $entity->name = 'User';
        $entity->email = 'user@example.com';
        $entity->age = 25;
        $entity->active = true;
        $entity->posts = [];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'dirty-rel-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $post = new TestPost();
        $post->id = 'new-post-1';
        $post->title = 'New Post';
        $post->content = 'Content';
        $entity->posts = [$post];

        $db = $this->createMock(Database::class);

        $db->expects($this->once())
            ->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $db->expects($this->once())
            ->method('updateDocument')
            ->willReturn(new Document(['$id' => 'dirty-rel-1']));

        $this->uow->flush($db);
    }

    public function testDetachRemovesFromIdentityMap(): void
    {
        $entity = new TestEntity();
        $entity->id = 'detach-map-1';
        $entity->name = 'Detach';
        $entity->email = 'detach@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'detach-map-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->uow->detach($entity);

        $this->assertFalse($this->identityMap->has('users', 'detach-map-1'));
    }

    public function testDetachRemovesFromScheduledInsertions(): void
    {
        $entity = new TestEntity();
        $entity->id = 'detach-ins-1';
        $entity->name = 'DetachIns';
        $entity->email = 'detachins@example.com';
        $entity->age = 20;
        $entity->active = true;

        $this->uow->persist($entity);
        $this->assertEquals(EntityState::New, $this->uow->getState($entity));

        $this->uow->detach($entity);

        $this->assertNull($this->uow->getState($entity));

        $db = $this->createMock(Database::class);
        $db->expects($this->never())->method('withTransaction');
        $db->expects($this->never())->method('createDocument');

        $this->uow->flush($db);
    }

    public function testDetachRemovesFromScheduledDeletions(): void
    {
        $entity = new TestEntity();
        $entity->id = 'detach-del-1';
        $entity->name = 'DetachDel';
        $entity->email = 'detachdel@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'detach-del-1', $entity);
        $this->uow->registerManaged($entity, $metadata);
        $this->uow->remove($entity);
        $this->assertEquals(EntityState::Removed, $this->uow->getState($entity));

        $this->uow->detach($entity);

        $this->assertNull($this->uow->getState($entity));

        $db = $this->createMock(Database::class);
        $db->expects($this->never())->method('withTransaction');
        $db->expects($this->never())->method('deleteDocument');

        $this->uow->flush($db);
    }

    public function testClearResetsAllSplObjectStorage(): void
    {
        $e1 = new TestEntity();
        $e1->id = 'clear-1';
        $e1->name = 'A';
        $e1->email = 'a@example.com';
        $e1->age = 20;
        $e1->active = true;

        $e2 = new TestEntity();
        $e2->id = 'clear-2';
        $e2->name = 'B';
        $e2->email = 'b@example.com';
        $e2->age = 25;
        $e2->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'clear-2', $e2);
        $this->uow->registerManaged($e2, $metadata);

        $this->uow->persist($e1);
        $this->uow->remove($e2);

        $this->uow->clear();

        $this->assertNull($this->uow->getState($e1));
        $this->assertNull($this->uow->getState($e2));
        $this->assertEmpty(\iterator_to_array($this->identityMap->all()));
    }

    public function testCascadePersistDeeplyNestedEntities(): void
    {
        $innerPost = new TestPost();
        $innerPost->id = 'deep-post';
        $innerPost->title = 'Deep Post';
        $innerPost->content = 'Content';

        $author = new TestEntity();
        $author->id = 'deep-author';
        $author->name = 'Deep Author';
        $author->email = 'deep@example.com';
        $author->age = 30;
        $author->active = true;
        $author->posts = [$innerPost];

        $innerPost->author = $author;

        $outerUser = new TestEntity();
        $outerUser->id = 'outer-user';
        $outerUser->name = 'Outer';
        $outerUser->email = 'outer@example.com';
        $outerUser->age = 40;
        $outerUser->active = true;
        $outerUser->posts = [$innerPost];

        $this->uow->persist($outerUser);

        $this->assertEquals(EntityState::New, $this->uow->getState($outerUser));
        $this->assertEquals(EntityState::New, $this->uow->getState($innerPost));
        $this->assertEquals(EntityState::New, $this->uow->getState($author));
    }

    public function testCascadePersistDoesNotRepersistTrackedEntities(): void
    {
        $post = new TestPost();
        $post->id = 'tracked-post';
        $post->title = 'Tracked';
        $post->content = 'Content';

        $user = new TestEntity();
        $user->id = 'tracked-user';
        $user->name = 'Tracked';
        $user->email = 'tracked@example.com';
        $user->age = 25;
        $user->active = true;
        $user->posts = [$post];

        $this->uow->persist($post);
        $this->assertEquals(EntityState::New, $this->uow->getState($post));

        $this->uow->persist($user);
        $this->assertEquals(EntityState::New, $this->uow->getState($user));
        $this->assertEquals(EntityState::New, $this->uow->getState($post));
    }

    public function testRemoveUntrackedEntityDoesNothing(): void
    {
        $entity = new TestEntity();
        $entity->id = 'untracked-1';
        $entity->name = 'Untracked';
        $entity->email = 'untracked@example.com';

        $this->uow->remove($entity);

        $this->assertNull($this->uow->getState($entity));
    }

    public function testFlushClearsScheduledInsertionsAfterExecution(): void
    {
        $entity = new TestEntity();
        $entity->id = 'flush-clear-1';
        $entity->name = 'FlushClear';
        $entity->email = 'flushclear@example.com';
        $entity->age = 20;
        $entity->active = true;

        $this->uow->persist($entity);

        $db = self::createStub(Database::class);
        $db->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $createdDoc = new Document([
            '$id' => 'flush-clear-1',
            '$version' => 1,
            '$createdAt' => '2024-01-01 00:00:00',
            '$updatedAt' => '2024-01-01 00:00:00',
        ]);

        $db->method('createDocument')->willReturn($createdDoc);

        $this->uow->flush($db);

        $db2 = $this->createMock(Database::class);
        $db2->expects($this->never())->method('withTransaction');

        $this->uow->flush($db2);
    }

    public function testFlushClearsScheduledDeletionsAfterExecution(): void
    {
        $entity = new TestEntity();
        $entity->id = 'flush-del-clear';
        $entity->name = 'FlushDelClear';
        $entity->email = 'flushdelclear@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'flush-del-clear', $entity);
        $this->uow->registerManaged($entity, $metadata);
        $this->uow->remove($entity);

        $db = self::createStub(Database::class);
        $db->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });
        $db->method('deleteDocument')->willReturn(true);

        $this->uow->flush($db);

        $db2 = $this->createMock(Database::class);
        $db2->expects($this->never())->method('withTransaction');

        $this->uow->flush($db2);
    }

    public function testFlushInsertTransitionsEntityToManaged(): void
    {
        $entity = new TestEntity();
        $entity->id = 'transition-1';
        $entity->name = 'Transition';
        $entity->email = 'transition@example.com';
        $entity->age = 20;
        $entity->active = true;

        $this->uow->persist($entity);
        $this->assertEquals(EntityState::New, $this->uow->getState($entity));

        $db = self::createStub(Database::class);
        $db->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $createdDoc = new Document([
            '$id' => 'transition-1',
            '$version' => 1,
            '$createdAt' => '2024-01-01 00:00:00',
            '$updatedAt' => '2024-01-01 00:00:00',
        ]);

        $db->method('createDocument')->willReturn($createdDoc);

        $this->uow->flush($db);

        $this->assertEquals(EntityState::Managed, $this->uow->getState($entity));
    }

    public function testFlushDeleteRemovesEntityFromTracking(): void
    {
        $entity = new TestEntity();
        $entity->id = 'del-track-1';
        $entity->name = 'DelTrack';
        $entity->email = 'deltrack@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->identityMap->put('users', 'del-track-1', $entity);
        $this->uow->registerManaged($entity, $metadata);
        $this->uow->remove($entity);

        $db = self::createStub(Database::class);
        $db->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });
        $db->method('deleteDocument')->willReturn(true);

        $this->uow->flush($db);

        $this->assertNull($this->uow->getState($entity));
        $this->assertFalse($this->identityMap->has('users', 'del-track-1'));
    }
}
