<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\ORM\EntityMapper;
use Utopia\Database\ORM\IdentityMap;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;

class EntityMapperAdvancedTest extends TestCase
{
    private EntityMapper $mapper;

    private MetadataFactory $metadataFactory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->metadataFactory = new MetadataFactory();
        $this->mapper = new EntityMapper($this->metadataFactory);
    }

    public function testToDocumentWithNullSingleRelationship(): void
    {
        $post = new TestPost();
        $post->id = 'post-null-rel';
        $post->title = 'No Author';
        $post->content = 'Content';
        $post->author = null;

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $doc = $this->mapper->toDocument($post, $metadata);

        $this->assertNull($doc->getAttribute('author'));
    }

    public function testToDocumentWithNullArrayRelationship(): void
    {
        $entity = new TestEntity();
        $entity->id = 'user-null-posts';
        $entity->name = 'No Posts';
        $entity->email = 'noposts@example.com';
        $entity->age = 20;
        $entity->active = true;
        $entity->posts = [];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $doc = $this->mapper->toDocument($entity, $metadata);

        $this->assertEquals([], $doc->getAttribute('posts'));
    }

    public function testToDocumentWithNestedEntityObjectsInRelationships(): void
    {
        $post = new TestPost();
        $post->id = 'nested-post-1';
        $post->title = 'Nested';
        $post->content = 'Content';

        $entity = new TestEntity();
        $entity->id = 'user-nested';
        $entity->name = 'With Posts';
        $entity->email = 'nested@example.com';
        $entity->age = 30;
        $entity->active = true;
        $entity->posts = [$post];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $doc = $this->mapper->toDocument($entity, $metadata);

        $posts = $doc->getAttribute('posts');
        $this->assertCount(1, $posts);
        $this->assertInstanceOf(Document::class, $posts[0]);
        $this->assertEquals('nested-post-1', $posts[0]->getAttribute('$id'));
        $this->assertEquals('Nested', $posts[0]->getAttribute('title'));
    }

    public function testToDocumentWithStringIdsInRelationships(): void
    {
        $entity = new TestEntity();
        $entity->id = 'user-string-rels';
        $entity->name = 'String Rels';
        $entity->email = 'stringrels@example.com';
        $entity->age = 25;
        $entity->active = true;
        $entity->posts = ['post-id-1', 'post-id-2'];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $doc = $this->mapper->toDocument($entity, $metadata);

        $posts = $doc->getAttribute('posts');
        $this->assertEquals(['post-id-1', 'post-id-2'], $posts);
    }

    public function testToDocumentWithSingleObjectRelationship(): void
    {
        $author = new TestEntity();
        $author->id = 'author-obj-1';
        $author->name = 'Author';
        $author->email = 'author@example.com';
        $author->age = 40;
        $author->active = true;

        $post = new TestPost();
        $post->id = 'post-obj-rel';
        $post->title = 'Post';
        $post->content = 'Content';
        $post->author = $author;

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $doc = $this->mapper->toDocument($post, $metadata);

        $authorDoc = $doc->getAttribute('author');
        $this->assertInstanceOf(Document::class, $authorDoc);
        $this->assertEquals('author-obj-1', $authorDoc->getAttribute('$id'));
    }

    public function testToEntityWithNestedDocumentRelationships(): void
    {
        $postDoc = new Document([
            '$id' => 'nested-doc-post',
            'title' => 'Nested Post',
            'content' => 'Content',
        ]);

        $userDoc = new Document([
            '$id' => 'nested-doc-user',
            'name' => 'User',
            'email' => 'user@example.com',
            'age' => 25,
            'active' => true,
            'posts' => [$postDoc],
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $identityMap = new IdentityMap();

        /** @var TestEntity $entity */
        $entity = $this->mapper->toEntity($userDoc, $metadata, $identityMap);

        $this->assertCount(1, $entity->posts);
        $this->assertInstanceOf(TestPost::class, $entity->posts[0]);
        $this->assertEquals('nested-doc-post', $entity->posts[0]->id);
        $this->assertEquals('Nested Post', $entity->posts[0]->title);
    }

    public function testToEntityWithEmptyRelationshipArrays(): void
    {
        $doc = new Document([
            '$id' => 'empty-rels',
            'name' => 'NoRels',
            'email' => 'norels@example.com',
            'age' => 20,
            'active' => true,
            'posts' => null,
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $identityMap = new IdentityMap();

        /** @var TestEntity $entity */
        $entity = $this->mapper->toEntity($doc, $metadata, $identityMap);

        $this->assertEquals([], $entity->posts);
    }

    public function testToEntityHandlesMixedArray(): void
    {
        $postDoc = new Document([
            '$id' => 'mixed-post-1',
            'title' => 'Mixed',
            'content' => 'Content',
        ]);

        $doc = new Document([
            '$id' => 'mixed-user',
            'name' => 'Mixed',
            'email' => 'mixed@example.com',
            'age' => 25,
            'active' => true,
            'posts' => [$postDoc, 'string-id-1'],
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $identityMap = new IdentityMap();

        /** @var TestEntity $entity */
        $entity = $this->mapper->toEntity($doc, $metadata, $identityMap);

        $this->assertCount(2, $entity->posts);
        $this->assertInstanceOf(TestPost::class, $entity->posts[0]);
        $this->assertEquals('string-id-1', $entity->posts[1]);
    }

    public function testToEntityWithUninitializedPropertiesDoesNotCrash(): void
    {
        $doc = new Document([
            '$id' => 'uninit-1',
            'name' => 'Uninit',
            'email' => 'uninit@example.com',
            'age' => 20,
            'active' => true,
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $identityMap = new IdentityMap();

        $entity = $this->mapper->toEntity($doc, $metadata, $identityMap);

        $this->assertInstanceOf(TestEntity::class, $entity);
    }

    public function testTakeSnapshotStoresRelationshipIdsNotFullObjects(): void
    {
        $post = new TestPost();
        $post->id = 'snap-post-1';
        $post->title = 'Snap Post';
        $post->content = 'Content';

        $entity = new TestEntity();
        $entity->id = 'snap-user-1';
        $entity->name = 'Snap User';
        $entity->email = 'snap@example.com';
        $entity->age = 30;
        $entity->active = true;
        $entity->posts = [$post];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $snapshot = $this->mapper->takeSnapshot($entity, $metadata);

        $this->assertEquals(['snap-post-1'], $snapshot['posts']);
    }

    public function testTakeSnapshotWithEmptyRelationships(): void
    {
        $entity = new TestEntity();
        $entity->id = 'snap-empty-1';
        $entity->name = 'Snap Empty';
        $entity->email = 'snapempty@example.com';
        $entity->age = 20;
        $entity->active = true;
        $entity->posts = [];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $snapshot = $this->mapper->takeSnapshot($entity, $metadata);

        $this->assertEquals([], $snapshot['posts']);
    }

    public function testTakeSnapshotWithSingleObjectRelationship(): void
    {
        $author = new TestEntity();
        $author->id = 'snap-author-1';
        $author->name = 'Author';
        $author->email = 'author@example.com';
        $author->age = 40;
        $author->active = true;

        $post = new TestPost();
        $post->id = 'snap-post-obj';
        $post->title = 'Title';
        $post->content = 'Content';
        $post->author = $author;

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $snapshot = $this->mapper->takeSnapshot($post, $metadata);

        $this->assertEquals('snap-author-1', $snapshot['author']);
    }

    public function testTakeSnapshotWithStringRelationship(): void
    {
        $post = new TestPost();
        $post->id = 'snap-str-1';
        $post->title = 'String Rel';
        $post->content = 'Content';
        $post->author = 'author-id-string';

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $snapshot = $this->mapper->takeSnapshot($post, $metadata);

        $this->assertEquals('author-id-string', $snapshot['author']);
    }

    public function testToCollectionDefinitionsGeneratesCorrectRelationshipTypes(): void
    {
        $metadata = $this->metadataFactory->getMetadata(TestAllRelationsEntity::class);
        $defs = $this->mapper->toCollectionDefinitions($metadata);

        $relationships = $defs['relationships'];

        $this->assertCount(4, $relationships);

        $types = array_map(fn ($r) => $r->type, $relationships);
        $this->assertContains(RelationType::OneToOne, $types);
        $this->assertContains(RelationType::ManyToOne, $types);
        $this->assertContains(RelationType::OneToMany, $types);
        $this->assertContains(RelationType::ManyToMany, $types);
    }

    public function testToCollectionDefinitionsGeneratesCorrectAttributes(): void
    {
        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $defs = $this->mapper->toCollectionDefinitions($metadata);

        $collection = $defs['collection'];
        $attrs = $collection->attributes;

        $this->assertCount(4, $attrs);

        $nameAttr = $attrs[0];
        $this->assertEquals('name', $nameAttr->key);
        $this->assertEquals(ColumnType::String, $nameAttr->type);
        $this->assertEquals(255, $nameAttr->size);
        $this->assertTrue($nameAttr->required);

        $emailAttr = $attrs[1];
        $this->assertEquals('email', $emailAttr->key);
        $this->assertEquals(ColumnType::String, $emailAttr->type);

        $ageAttr = $attrs[2];
        $this->assertEquals('age', $ageAttr->key);
        $this->assertEquals(ColumnType::Integer, $ageAttr->type);

        $activeAttr = $attrs[3];
        $this->assertEquals('active', $activeAttr->key);
        $this->assertEquals(ColumnType::Boolean, $activeAttr->type);
    }

    public function testToCollectionDefinitionsWithCustomKeyColumn(): void
    {
        $metadata = $this->metadataFactory->getMetadata(TestCustomKeyEntity::class);
        $defs = $this->mapper->toCollectionDefinitions($metadata);

        $attrs = $defs['collection']->attributes;
        $this->assertCount(1, $attrs);
        $this->assertEquals('display_name', $attrs[0]->key);
    }

    public function testToCollectionDefinitionsRelationshipKeys(): void
    {
        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $defs = $this->mapper->toCollectionDefinitions($metadata);

        $relationships = $defs['relationships'];
        $this->assertCount(1, $relationships);
        $this->assertEquals('users', $relationships[0]->collection);
        $this->assertEquals('posts', $relationships[0]->relatedCollection);
        $this->assertEquals('posts', $relationships[0]->key);
        $this->assertEquals('author', $relationships[0]->twoWayKey);
        $this->assertTrue($relationships[0]->twoWay);
    }

    public function testRoundTripEntityDocumentEntity(): void
    {
        $entity = new TestEntity();
        $entity->id = 'round-trip-1';
        $entity->name = 'RoundTrip';
        $entity->email = 'roundtrip@example.com';
        $entity->age = 42;
        $entity->active = false;
        $entity->version = 3;
        $entity->permissions = ['read("any")'];

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $doc = $this->mapper->toDocument($entity, $metadata);

        $identityMap = new IdentityMap();
        /** @var TestEntity $restored */
        $restored = $this->mapper->toEntity($doc, $metadata, $identityMap);

        $this->assertEquals($entity->id, $restored->id);
        $this->assertEquals($entity->name, $restored->name);
        $this->assertEquals($entity->email, $restored->email);
        $this->assertEquals($entity->age, $restored->age);
        $this->assertEquals($entity->active, $restored->active);
        $this->assertEquals($entity->version, $restored->version);
        $this->assertEquals($entity->permissions, $restored->permissions);
    }

    public function testToEntityWithSingleDocumentRelationship(): void
    {
        $authorDoc = new Document([
            '$id' => 'author-doc-1',
            'name' => 'Author',
            'email' => 'author@example.com',
            'age' => 35,
            'active' => true,
        ]);

        $postDoc = new Document([
            '$id' => 'post-with-author',
            'title' => 'Post',
            'content' => 'Content',
            'author' => $authorDoc,
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $identityMap = new IdentityMap();

        /** @var TestPost $post */
        $post = $this->mapper->toEntity($postDoc, $metadata, $identityMap);

        $this->assertInstanceOf(TestEntity::class, $post->author);
        $this->assertEquals('author-doc-1', $post->author->id);
    }

    public function testToEntityWithStringRelationshipValue(): void
    {
        $postDoc = new Document([
            '$id' => 'post-string-author',
            'title' => 'Post',
            'content' => 'Content',
            'author' => 'author-string-id',
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $identityMap = new IdentityMap();

        /** @var TestPost $post */
        $post = $this->mapper->toEntity($postDoc, $metadata, $identityMap);

        $this->assertEquals('author-string-id', $post->author);
    }

    public function testToEntityWithNullRelationshipSetsDefault(): void
    {
        $postDoc = new Document([
            '$id' => 'post-null-author',
            'title' => 'Post',
            'content' => 'Content',
            'author' => null,
        ]);

        $metadata = $this->metadataFactory->getMetadata(TestPost::class);
        $identityMap = new IdentityMap();

        /** @var TestPost $post */
        $post = $this->mapper->toEntity($postDoc, $metadata, $identityMap);

        $this->assertNull($post->author);
    }

    public function testToDocumentIncludesTenantProperty(): void
    {
        $entity = new TestTenantEntity();
        $entity->id = 'tenant-1';
        $entity->tenantId = 'org-123';
        $entity->name = 'Tenant Item';

        $metadata = $this->metadataFactory->getMetadata(TestTenantEntity::class);
        $doc = $this->mapper->toDocument($entity, $metadata);

        $this->assertEquals('org-123', $doc->getAttribute('$tenant'));
    }

    public function testGetIdReturnsNullWhenNoIdProperty(): void
    {
        $entity = new TestEntity();
        $entity->id = 'test-id';

        $metadata = $this->metadataFactory->getMetadata(TestEntity::class);
        $result = $this->mapper->getId($entity, $metadata);

        $this->assertEquals('test-id', $result);
    }
}
