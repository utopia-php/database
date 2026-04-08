<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\ORM\EntityMapper;
use Utopia\Database\ORM\IdentityMap;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Query\Schema\ColumnType;

class EntityMapperTest extends TestCase
{
    private EntityMapper $mapper;

    private MetadataFactory $metadataFactory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->metadataFactory = new MetadataFactory();
        $this->mapper = new EntityMapper($this->metadataFactory);
    }

    public function testToDocument(): void
    {
        $entity = new TestEntity();
        $entity->id = 'user-123';
        $entity->name = 'John';
        $entity->email = 'john@example.com';
        $entity->age = 30;
        $entity->active = true;
        $entity->version = 1;
        $entity->permissions = ['read("any")'];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $doc = $this->mapper->toDocument($entity, $metadata);

        $this->assertEquals('user-123', $doc->getAttribute('$id'));
        $this->assertEquals('John', $doc->getAttribute('name'));
        $this->assertEquals('john@example.com', $doc->getAttribute('email'));
        $this->assertEquals(30, $doc->getAttribute('age'));
        $this->assertTrue($doc->getAttribute('active'));
        $this->assertEquals(1, $doc->getAttribute('$version'));
        $this->assertEquals(['read("any")'], $doc->getAttribute('$permissions'));
    }

    public function testToEntity(): void
    {
        $doc = new Document([
            '$id' => 'user-456',
            '$version' => 2,
            '$createdAt' => '2024-01-01 00:00:00',
            '$updatedAt' => '2024-01-02 00:00:00',
            '$permissions' => ['read("any")'],
            'name' => 'Jane',
            'email' => 'jane@example.com',
            'age' => 25,
            'active' => false,
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $identityMap = new IdentityMap();

        /** @var TestEntity $entity */
        $entity = $this->mapper->toEntity($doc, $metadata, $identityMap);

        $this->assertInstanceOf(TestEntity::class, $entity);
        $this->assertEquals('user-456', $entity->id);
        $this->assertEquals(2, $entity->version);
        $this->assertEquals('2024-01-01 00:00:00', $entity->createdAt);
        $this->assertEquals('2024-01-02 00:00:00', $entity->updatedAt);
        $this->assertEquals(['read("any")'], $entity->permissions);
        $this->assertEquals('Jane', $entity->name);
        $this->assertEquals('jane@example.com', $entity->email);
        $this->assertEquals(25, $entity->age);
        $this->assertFalse($entity->active);
    }

    public function testToEntityUsesIdentityMap(): void
    {
        $doc = new Document([
            '$id' => 'user-789',
            'name' => 'Alice',
            'email' => 'alice@example.com',
            'age' => 28,
            'active' => true,
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $identityMap = new IdentityMap();

        $entity1 = $this->mapper->toEntity($doc, $metadata, $identityMap);
        $entity2 = $this->mapper->toEntity($doc, $metadata, $identityMap);

        $this->assertSame($entity1, $entity2);
    }

    public function testTakeSnapshot(): void
    {
        $entity = new TestEntity();
        $entity->id = 'snap-1';
        $entity->name = 'Bob';
        $entity->email = 'bob@example.com';
        $entity->age = 35;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $snapshot = $this->mapper->takeSnapshot($entity, $metadata);

        $this->assertEquals('snap-1', $snapshot['$id']);
        $this->assertEquals('Bob', $snapshot['name']);
        $this->assertEquals('bob@example.com', $snapshot['email']);
        $this->assertEquals(35, $snapshot['age']);
        $this->assertTrue($snapshot['active']);
    }

    public function testSnapshotChangesDetected(): void
    {
        $entity = new TestEntity();
        $entity->id = 'snap-2';
        $entity->name = 'Before';
        $entity->email = 'before@example.com';
        $entity->age = 20;
        $entity->active = true;

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $snapshot1 = $this->mapper->takeSnapshot($entity, $metadata);

        $entity->name = 'After';
        $snapshot2 = $this->mapper->takeSnapshot($entity, $metadata);

        $this->assertNotEquals($snapshot1, $snapshot2);
        $this->assertEquals('Before', $snapshot1['name']);
        $this->assertEquals('After', $snapshot2['name']);
    }

    public function testGetId(): void
    {
        $entity = new TestEntity();
        $entity->id = 'id-test';

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->assertEquals('id-test', $this->mapper->getId($entity, $metadata));
    }

    public function testToCollectionDefinitions(): void
    {
        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $defs = $this->mapper->toCollectionDefinitions($metadata);

        $collection = $defs['collection'];
        $relationships = $defs['relationships'];

        $this->assertEquals('users', $collection->id);
        $this->assertTrue($collection->documentSecurity);
        $this->assertCount(4, $collection->attributes);
        $this->assertCount(2, $collection->indexes);

        $attrKeys = array_map(fn ($a) => $a->key, $collection->attributes);
        $this->assertContains('name', $attrKeys);
        $this->assertContains('email', $attrKeys);
        $this->assertContains('age', $attrKeys);
        $this->assertContains('active', $attrKeys);

        $nameAttr = $collection->attributes[0];
        $this->assertEquals(ColumnType::String, $nameAttr->type);
        $this->assertEquals(255, $nameAttr->size);
        $this->assertTrue($nameAttr->required);

        $this->assertCount(1, $relationships);
        $this->assertEquals('users', $relationships[0]->collection);
        $this->assertEquals('posts', $relationships[0]->relatedCollection);
    }

    public function testApplyDocumentToEntity(): void
    {
        $entity = new TestEntity();
        $entity->id = '';
        $entity->version = null;
        $entity->createdAt = null;
        $entity->updatedAt = null;

        $doc = new Document([
            '$id' => 'generated-id',
            '$version' => 1,
            '$createdAt' => '2024-06-01 12:00:00',
            '$updatedAt' => '2024-06-01 12:00:00',
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $this->mapper->applyDocumentToEntity($doc, $entity, $metadata);

        $this->assertEquals('generated-id', $entity->id);
        $this->assertEquals(1, $entity->version);
        $this->assertEquals('2024-06-01 12:00:00', $entity->createdAt);
        $this->assertEquals('2024-06-01 12:00:00', $entity->updatedAt);
    }
}
