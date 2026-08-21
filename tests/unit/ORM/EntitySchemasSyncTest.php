<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\ORM\EntityManager;
use Utopia\Database\ORM\MetadataFactory;

class EntitySchemasSyncTest extends TestCase
{
    protected Database $db;

    protected Adapter $adapter;

    protected EntityManager $em;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->db = $this->createMock(Database::class);
        $this->adapter = self::createStub(Adapter::class);
        $this->adapter->method('getDatabase')->willReturn('test_db');
        $this->db->method('getAdapter')->willReturn($this->adapter);
        $this->em = new EntityManager($this->db);
    }

    public function testSyncCreatesCollectionWhenItDoesNotExist(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(false);

        $this->db->expects($this->once())
            ->method('createCollection')
            ->with($this->callback(function (mixed $collection): bool {
                return $collection instanceof Collection
                    && $collection->id === 'users';
            }))
            ->willReturn(new Document(['$id' => 'users']));

        $this->db->expects($this->once())
            ->method('createRelationship');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncDiffsAndAppliesChangesWhenCollectionExists(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: [
                Attribute::string(key: 'name', size: 255, required: true),
            ],
            indexes: [],
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->never())
            ->method('createCollection');

        $this->db->expects($this->atLeastOnce())
            ->method('createAttribute');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncIsNoOpWhenNoChangesNeeded(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $defs = $this->em->getEntityMapper()->toCollectionDefinitions($metadata);

        /** @var \Utopia\Database\Collection $desired */
        $desired = $defs['collection'];

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: $desired->attributes,
            indexes: $desired->indexes,
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->never())
            ->method('createCollection');

        $this->db->expects($this->never())
            ->method('createAttribute');

        $this->db->expects($this->never())
            ->method('deleteAttribute');

        $this->db->expects($this->never())
            ->method('createIndex');

        $this->db->expects($this->never())
            ->method('deleteIndex');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncDetectsNewAttributes(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: [],
            indexes: [],
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->atLeastOnce())
            ->method('createAttribute');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncDetectsDroppedAttributes(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $defs = $this->em->getEntityMapper()->toCollectionDefinitions($metadata);

        /** @var \Utopia\Database\Collection $desired */
        $desired = $defs['collection'];

        $extraAttr = Attribute::string(key: 'obsolete_field', size: 100);

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: [...$desired->attributes, $extraAttr],
            indexes: $desired->indexes,
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->once())
            ->method('deleteAttribute')
            ->with('users', 'obsolete_field');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncDetectsNewIndexes(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $defs = $this->em->getEntityMapper()->toCollectionDefinitions($metadata);

        /** @var \Utopia\Database\Collection $desired */
        $desired = $defs['collection'];

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: $desired->attributes,
            indexes: [],
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->atLeastOnce())
            ->method('createIndex');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncDetectsDroppedIndexes(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $defs = $this->em->getEntityMapper()->toCollectionDefinitions($metadata);

        /** @var \Utopia\Database\Collection $desired */
        $desired = $defs['collection'];

        $extraIndex = new \Utopia\Database\Index(key: 'idx_old', type: \Utopia\Query\Schema\IndexType::Index, attributes: ['name']);

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: $desired->attributes,
            indexes: [...$desired->indexes, $extraIndex],
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->once())
            ->method('deleteIndex')
            ->with('users', 'idx_old');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }

    public function testSyncDoesNotCallCreateCollectionWhenAlreadyExists(): void
    {
        $this->db->expects($this->once())
            ->method('exists')
            ->with('test_db', 'users')
            ->willReturn(true);

        $metadata = $this->em->getMetadataFactory()->getMetadata(TestEntity::class);
        $defs = $this->em->getEntityMapper()->toCollectionDefinitions($metadata);

        /** @var \Utopia\Database\Collection $desired */
        $desired = $defs['collection'];

        $collectionDoc = new Collection(
            id: 'users',
            name: 'users',
            attributes: $desired->attributes,
            indexes: $desired->indexes,
            permissions: [],
            documentSecurity: true,
        );

        $this->db->expects($this->once())
            ->method('getCollection')
            ->with('users')
            ->willReturn($collectionDoc);

        $this->db->expects($this->never())
            ->method('createCollection');

        $this->em->syncCollectionFromEntity(TestEntity::class);
    }
}
