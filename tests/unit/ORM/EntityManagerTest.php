<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\ORM\EntityManager;
use Utopia\Database\ORM\EntityMapper;
use Utopia\Database\ORM\EntityState;
use Utopia\Database\ORM\IdentityMap;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\ORM\UnitOfWork;
use Utopia\Database\Query;

#[AllowMockObjectsWithoutExpectations]
class EntityManagerTest extends TestCase
{
    private EntityManager $em;

    private Database $db;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->db = $this->createMock(Database::class);
        $this->em = new EntityManager($this->db);
    }

    public function testPersistDelegatesToUnitOfWork(): void
    {
        $entity = new TestEntity();
        $entity->id = 'persist-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->em->persist($entity);

        $this->assertEquals(EntityState::New, $this->em->getUnitOfWork()->getState($entity));
    }

    public function testRemoveDelegatesToUnitOfWork(): void
    {
        $entity = new TestEntity();
        $entity->id = 'remove-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $this->em->getIdentityMap()->put('users', 'remove-1', $entity);
        $this->em->getUnitOfWork()->registerManaged($entity, $metadata);

        $this->em->remove($entity);

        $this->assertEquals(EntityState::Removed, $this->em->getUnitOfWork()->getState($entity));
    }

    public function testFindChecksIdentityMapFirst(): void
    {
        $entity = new TestEntity();
        $entity->id = 'cached-1';
        $entity->name = 'Cached';
        $entity->email = 'cached@example.com';

        $this->em->getIdentityMap()->put('users', 'cached-1', $entity);

        $this->db->expects($this->never())
            ->method('getDocument');

        $result = $this->em->find(TestEntity::class, 'cached-1');

        $this->assertSame($entity, $result);
    }

    public function testFindFallsBackToDatabase(): void
    {
        $doc = new Document([
            '$id' => 'db-1',
            '$version' => 1,
            'name' => 'FromDB',
            'email' => 'db@example.com',
            'age' => 30,
            'active' => true,
        ]);

        $this->db->expects($this->once())
            ->method('getDocument')
            ->with('users', 'db-1')
            ->willReturn($doc);

        /** @var TestEntity $result */
        $result = $this->em->find(TestEntity::class, 'db-1');

        $this->assertInstanceOf(TestEntity::class, $result);
        $this->assertEquals('db-1', $result->id);
        $this->assertEquals('FromDB', $result->name);
    }

    public function testFindReturnsNullForEmptyDocument(): void
    {
        $this->db->expects($this->once())
            ->method('getDocument')
            ->willReturn(new Document());

        $result = $this->em->find(TestEntity::class, 'nonexistent');

        $this->assertNull($result);
    }

    public function testFindRegistersEntityAsManaged(): void
    {
        $doc = new Document([
            '$id' => 'managed-find-1',
            'name' => 'Managed',
            'email' => 'managed@example.com',
            'age' => 25,
            'active' => true,
        ]);

        $this->db->method('getDocument')->willReturn($doc);

        $result = $this->em->find(TestEntity::class, 'managed-find-1');

        $this->assertNotNull($result);
        $this->assertEquals(EntityState::Managed, $this->em->getUnitOfWork()->getState($result));
    }

    public function testFindPutsEntityInIdentityMap(): void
    {
        $doc = new Document([
            '$id' => 'identity-1',
            'name' => 'Identity',
            'email' => 'identity@example.com',
            'age' => 20,
            'active' => true,
        ]);

        $this->db->method('getDocument')->willReturn($doc);

        $this->em->find(TestEntity::class, 'identity-1');

        $this->assertTrue($this->em->getIdentityMap()->has('users', 'identity-1'));
    }

    public function testFindReturnsSameInstanceOnSecondCall(): void
    {
        $doc = new Document([
            '$id' => 'repeat-1',
            'name' => 'Repeat',
            'email' => 'repeat@example.com',
            'age' => 20,
            'active' => true,
        ]);

        $this->db->expects($this->once())
            ->method('getDocument')
            ->willReturn($doc);

        $first = $this->em->find(TestEntity::class, 'repeat-1');
        $second = $this->em->find(TestEntity::class, 'repeat-1');

        $this->assertSame($first, $second);
    }

    public function testFindManyHydratesAllDocuments(): void
    {
        $docs = [
            new Document([
                '$id' => 'many-1',
                'name' => 'Alice',
                'email' => 'alice@example.com',
                'age' => 25,
                'active' => true,
            ]),
            new Document([
                '$id' => 'many-2',
                'name' => 'Bob',
                'email' => 'bob@example.com',
                'age' => 30,
                'active' => false,
            ]),
        ];

        $this->db->expects($this->once())
            ->method('find')
            ->with('users', [])
            ->willReturn($docs);

        $results = $this->em->findMany(TestEntity::class);

        $this->assertCount(2, $results);
        $this->assertInstanceOf(TestEntity::class, $results[0]);
        $this->assertInstanceOf(TestEntity::class, $results[1]);
        $this->assertEquals('Alice', $results[0]->name);
        $this->assertEquals('Bob', $results[1]->name);
    }

    public function testFindManyWithEmptyResults(): void
    {
        $this->db->method('find')->willReturn([]);

        $results = $this->em->findMany(TestEntity::class);

        $this->assertEmpty($results);
    }

    public function testFindManyRegistersAllAsManaged(): void
    {
        $docs = [
            new Document([
                '$id' => 'managed-many-1',
                'name' => 'A',
                'email' => 'a@example.com',
                'age' => 20,
                'active' => true,
            ]),
            new Document([
                '$id' => 'managed-many-2',
                'name' => 'B',
                'email' => 'b@example.com',
                'age' => 25,
                'active' => true,
            ]),
        ];

        $this->db->method('find')->willReturn($docs);

        $results = $this->em->findMany(TestEntity::class);

        foreach ($results as $entity) {
            $this->assertEquals(EntityState::Managed, $this->em->getUnitOfWork()->getState($entity));
        }
    }

    public function testFindManyWithQueries(): void
    {
        $queries = [Query::equal('active', [true])];

        $this->db->expects($this->once())
            ->method('find')
            ->with('users', $queries)
            ->willReturn([]);

        $this->em->findMany(TestEntity::class, $queries);
    }

    public function testFindOneAddsLimitAndReturnsFirst(): void
    {
        $doc = new Document([
            '$id' => 'one-1',
            'name' => 'Only',
            'email' => 'only@example.com',
            'age' => 30,
            'active' => true,
        ]);

        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'users',
                $this->callback(function (array $queries) {
                    $lastQuery = end($queries);

                    return $lastQuery instanceof Query
                        && $lastQuery->getMethod()->value === 'limit';
                })
            )
            ->willReturn([$doc]);

        /** @var TestEntity $result */
        $result = $this->em->findOne(TestEntity::class);

        $this->assertInstanceOf(TestEntity::class, $result);
        $this->assertEquals('Only', $result->name);
    }

    public function testFindOneReturnsNullWhenNoResults(): void
    {
        $this->db->method('find')->willReturn([]);

        $result = $this->em->findOne(TestEntity::class);

        $this->assertNull($result);
    }

    public function testFindOneWithCustomQueries(): void
    {
        $this->db->expects($this->once())
            ->method('find')
            ->with(
                'users',
                $this->callback(function (array $queries) {
                    return count($queries) === 2;
                })
            )
            ->willReturn([]);

        $this->em->findOne(TestEntity::class, [Query::equal('name', ['Test'])]);
    }

    public function testFindOneRegistersAsManaged(): void
    {
        $doc = new Document([
            '$id' => 'managed-one-1',
            'name' => 'Managed',
            'email' => 'managed@example.com',
            'age' => 25,
            'active' => true,
        ]);

        $this->db->method('find')->willReturn([$doc]);

        $result = $this->em->findOne(TestEntity::class);

        $this->assertNotNull($result);
        $this->assertEquals(EntityState::Managed, $this->em->getUnitOfWork()->getState($result));
    }

    public function testCreateCollectionFromEntityCallsCreateCollection(): void
    {
        $this->db->expects($this->once())
            ->method('createCollection')
            ->with(
                $this->equalTo('users'),
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->equalTo(true),
            )
            ->willReturn(new Document(['$id' => 'users']));

        $this->db->expects($this->once())
            ->method('createRelationship')
            ->with($this->isInstanceOf(\Utopia\Database\Relationship::class));

        $this->em->createCollectionFromEntity(TestEntity::class);
    }

    public function testCreateCollectionFromEntityReturnsDocument(): void
    {
        $returnDoc = new Document(['$id' => 'users']);

        $this->db->method('createCollection')->willReturn($returnDoc);
        $this->db->method('createRelationship')->willReturn(true);

        $result = $this->em->createCollectionFromEntity(TestEntity::class);

        $this->assertInstanceOf(Document::class, $result);
        $this->assertEquals('users', $result->getAttribute('$id'));
    }

    public function testCreateCollectionFromEntityWithNoRelationships(): void
    {
        $this->db->expects($this->once())
            ->method('createCollection')
            ->willReturn(new Document(['$id' => 'posts']));

        $this->db->expects($this->once())
            ->method('createRelationship');

        $this->em->createCollectionFromEntity(TestPost::class);
    }

    public function testDetachDelegatesToUnitOfWork(): void
    {
        $entity = new TestEntity();
        $entity->id = 'detach-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->em->persist($entity);
        $this->assertEquals(EntityState::New, $this->em->getUnitOfWork()->getState($entity));

        $this->em->detach($entity);

        $this->assertNull($this->em->getUnitOfWork()->getState($entity));
    }

    public function testClearResetsUnitOfWork(): void
    {
        $entity = new TestEntity();
        $entity->id = 'clear-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->em->persist($entity);
        $this->em->clear();

        $this->assertNull($this->em->getUnitOfWork()->getState($entity));
    }

    public function testClearResetsIdentityMap(): void
    {
        $entity = new TestEntity();
        $entity->id = 'clear-map-1';
        $entity->name = 'Test';
        $entity->email = 'test@example.com';

        $this->em->getIdentityMap()->put('users', 'clear-map-1', $entity);
        $this->em->clear();

        $this->assertEmpty(\iterator_to_array($this->em->getIdentityMap()->all()));
    }

    public function testGetUnitOfWorkReturnsUnitOfWork(): void
    {
        $this->assertInstanceOf(UnitOfWork::class, $this->em->getUnitOfWork());
    }

    public function testGetIdentityMapReturnsIdentityMap(): void
    {
        $this->assertInstanceOf(IdentityMap::class, $this->em->getIdentityMap());
    }

    public function testGetMetadataFactoryReturnsMetadataFactory(): void
    {
        $this->assertInstanceOf(MetadataFactory::class, $this->em->getMetadataFactory());
    }

    public function testGetEntityMapperReturnsEntityMapper(): void
    {
        $this->assertInstanceOf(EntityMapper::class, $this->em->getEntityMapper());
    }

    public function testFlushDelegatesToUnitOfWork(): void
    {
        $this->db->expects($this->never())
            ->method('withTransaction');

        $this->em->flush();
    }

    public function testFlushWithPendingInsert(): void
    {
        $entity = new TestEntity();
        $entity->id = 'flush-1';
        $entity->name = 'Flush';
        $entity->email = 'flush@example.com';
        $entity->age = 25;
        $entity->active = true;

        $this->em->persist($entity);

        $createdDoc = new Document([
            '$id' => 'flush-1',
            '$version' => 1,
            '$createdAt' => '2024-01-01 00:00:00',
            '$updatedAt' => '2024-01-01 00:00:00',
            'name' => 'Flush',
            'email' => 'flush@example.com',
            'age' => 25,
            'active' => true,
        ]);

        $this->db->expects($this->once())
            ->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $this->db->expects($this->once())
            ->method('createDocument')
            ->with('users', $this->isInstanceOf(Document::class))
            ->willReturn($createdDoc);

        $this->em->flush();

        $this->assertEquals(EntityState::Managed, $this->em->getUnitOfWork()->getState($entity));
    }

    public function testFlushWithPendingDelete(): void
    {
        $entity = new TestEntity();
        $entity->id = 'flush-del-1';
        $entity->name = 'Delete';
        $entity->email = 'delete@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $this->em->getIdentityMap()->put('users', 'flush-del-1', $entity);
        $this->em->getUnitOfWork()->registerManaged($entity, $metadata);
        $this->em->remove($entity);

        $this->db->expects($this->once())
            ->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $this->db->expects($this->once())
            ->method('deleteDocument')
            ->with('users', 'flush-del-1');

        $this->em->flush();
    }

    public function testFlushWithPendingUpdate(): void
    {
        $entity = new TestEntity();
        $entity->id = 'flush-upd-1';
        $entity->name = 'Before';
        $entity->email = 'update@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $this->em->getIdentityMap()->put('users', 'flush-upd-1', $entity);
        $this->em->getUnitOfWork()->registerManaged($entity, $metadata);

        $entity->name = 'After';

        $updatedDoc = new Document([
            '$id' => 'flush-upd-1',
            '$version' => 2,
            '$createdAt' => '2024-01-01 00:00:00',
            '$updatedAt' => '2024-01-02 00:00:00',
            'name' => 'After',
            'email' => 'update@example.com',
            'age' => 20,
            'active' => true,
        ]);

        $this->db->expects($this->once())
            ->method('withTransaction')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            });

        $this->db->expects($this->once())
            ->method('updateDocument')
            ->with('users', 'flush-upd-1', $this->isInstanceOf(Document::class))
            ->willReturn($updatedDoc);

        $this->em->flush();
    }

    public function testPersistMultipleEntities(): void
    {
        $e1 = new TestEntity();
        $e1->id = 'multi-1';
        $e1->name = 'A';
        $e1->email = 'a@example.com';

        $e2 = new TestEntity();
        $e2->id = 'multi-2';
        $e2->name = 'B';
        $e2->email = 'b@example.com';

        $this->em->persist($e1);
        $this->em->persist($e2);

        $this->assertEquals(EntityState::New, $this->em->getUnitOfWork()->getState($e1));
        $this->assertEquals(EntityState::New, $this->em->getUnitOfWork()->getState($e2));
    }

    public function testRemoveUntrackedEntityDoesNothing(): void
    {
        $entity = new TestEntity();
        $entity->id = 'untracked-1';
        $entity->name = 'Untracked';
        $entity->email = 'untracked@example.com';

        $this->em->remove($entity);

        $this->assertNull($this->em->getUnitOfWork()->getState($entity));
    }

    public function testPersistThenRemoveNewEntity(): void
    {
        $entity = new TestEntity();
        $entity->id = 'pr-1';
        $entity->name = 'PersistRemove';
        $entity->email = 'pr@example.com';

        $this->em->persist($entity);
        $this->em->remove($entity);

        $this->assertNull($this->em->getUnitOfWork()->getState($entity));
    }

    public function testPersistCascadesToRelationships(): void
    {
        $post = new TestPost();
        $post->id = 'cascade-post-1';
        $post->title = 'Cascade Post';
        $post->content = 'Content';

        $user = new TestEntity();
        $user->id = 'cascade-user-1';
        $user->name = 'User';
        $user->email = 'user@example.com';
        $user->posts = [$post];

        $this->em->persist($user);

        $this->assertEquals(EntityState::New, $this->em->getUnitOfWork()->getState($user));
        $this->assertEquals(EntityState::New, $this->em->getUnitOfWork()->getState($post));
    }

    public function testDetachRemovesFromIdentityMap(): void
    {
        $entity = new TestEntity();
        $entity->id = 'detach-map-1';
        $entity->name = 'DetachMap';
        $entity->email = 'detachmap@example.com';

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $this->em->getIdentityMap()->put('users', 'detach-map-1', $entity);
        $this->em->getUnitOfWork()->registerManaged($entity, $metadata);

        $this->em->detach($entity);

        $this->assertFalse($this->em->getIdentityMap()->has('users', 'detach-map-1'));
    }

    public function testFindManyPutsEntitiesInIdentityMap(): void
    {
        $docs = [
            new Document([
                '$id' => 'findmany-map-1',
                'name' => 'A',
                'email' => 'a@example.com',
                'age' => 20,
                'active' => true,
            ]),
        ];

        $this->db->method('find')->willReturn($docs);

        $this->em->findMany(TestEntity::class);

        $this->assertTrue($this->em->getIdentityMap()->has('users', 'findmany-map-1'));
    }

    public function testConstructorCreatesAllComponents(): void
    {
        $em = new EntityManager($this->db);

        $this->assertInstanceOf(UnitOfWork::class, $em->getUnitOfWork());
        $this->assertInstanceOf(IdentityMap::class, $em->getIdentityMap());
        $this->assertInstanceOf(MetadataFactory::class, $em->getMetadataFactory());
        $this->assertInstanceOf(EntityMapper::class, $em->getEntityMapper());
    }
}
