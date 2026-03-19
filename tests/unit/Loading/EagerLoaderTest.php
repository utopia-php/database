<?php

namespace Tests\Unit\Loading;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Loading\EagerLoader;
use Utopia\Database\RelationType;

class EagerLoaderTest extends TestCase
{
    private EagerLoader $eagerLoader;

    private Database $db;

    protected function setUp(): void
    {
        $this->eagerLoader = new EagerLoader();
        $this->db = $this->createMock(Database::class);
    }

    public function testLoadWithEmptyDocumentsReturnsEmpty(): void
    {
        $collection = new Document(['$id' => 'users', 'attributes' => []]);
        $result = $this->eagerLoader->load([], ['author'], $collection, $this->db);
        $this->assertSame([], $result);
    }

    public function testLoadWithEmptyRelationsReturnsUnchangedDocuments(): void
    {
        $docs = [new Document(['$id' => 'doc1'])];
        $collection = new Document(['$id' => 'users', 'attributes' => []]);
        $result = $this->eagerLoader->load($docs, [], $collection, $this->db);
        $this->assertSame($docs, $result);
    }

    public function testLoadWithSingleRelationshipPopulatesRelatedDocuments(): void
    {
        $authorDoc = new Document(['$id' => 'a1', 'name' => 'Alice']);

        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'author',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'users',
                        'relationType' => RelationType::ManyToOne->value,
                        'twoWay' => false,
                        'twoWayKey' => '',
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$authorDoc]);

        $docs = [new Document(['$id' => 'p1', 'author' => 'a1'])];
        $result = $this->eagerLoader->load($docs, ['author'], $collectionMeta, $this->db);

        $this->assertInstanceOf(Document::class, $result[0]->getAttribute('author'));
        $this->assertEquals('Alice', $result[0]->getAttribute('author')->getAttribute('name'));
    }

    public function testLoadDistributesRelatedDocsBackToParents(): void
    {
        $authorDoc = new Document(['$id' => 'a1', 'name' => 'Alice']);

        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'author',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'users',
                        'relationType' => RelationType::ManyToOne->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$authorDoc]);

        $docs = [
            new Document(['$id' => 'p1', 'author' => 'a1']),
            new Document(['$id' => 'p2', 'author' => 'a1']),
        ];

        $result = $this->eagerLoader->load($docs, ['author'], $collectionMeta, $this->db);

        $this->assertEquals('Alice', $result[0]->getAttribute('author')->getAttribute('name'));
        $this->assertEquals('Alice', $result[1]->getAttribute('author')->getAttribute('name'));
    }

    public function testLoadHandlesOneToOneRelationship(): void
    {
        $profileDoc = new Document(['$id' => 'pr1', 'bio' => 'Hello']);

        $collectionMeta = new Document([
            '$id' => 'users',
            'attributes' => [
                new Document([
                    'key' => 'profile',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'profiles',
                        'relationType' => RelationType::OneToOne->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$profileDoc]);

        $docs = [new Document(['$id' => 'u1', 'profile' => 'pr1'])];
        $result = $this->eagerLoader->load($docs, ['profile'], $collectionMeta, $this->db);

        $profile = $result[0]->getAttribute('profile');
        $this->assertInstanceOf(Document::class, $profile);
        $this->assertEquals('Hello', $profile->getAttribute('bio'));
    }

    public function testLoadHandlesOneToManyRelationship(): void
    {
        $comment1 = new Document(['$id' => 'c1', 'text' => 'Great']);
        $comment2 = new Document(['$id' => 'c2', 'text' => 'Nice']);

        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'comments',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'comments',
                        'relationType' => RelationType::OneToMany->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$comment1, $comment2]);

        $docs = [new Document(['$id' => 'p1', 'comments' => ['c1', 'c2']])];
        $result = $this->eagerLoader->load($docs, ['comments'], $collectionMeta, $this->db);

        $comments = $result[0]->getAttribute('comments');
        $this->assertIsArray($comments);
        $this->assertCount(2, $comments);
    }

    public function testLoadHandlesNestedDotNotationPaths(): void
    {
        $authorDoc = new Document(['$id' => 'a1', 'name' => 'Alice', 'profile' => 'pr1']);
        $profileDoc = new Document(['$id' => 'pr1', 'bio' => 'Dev']);

        $postCollection = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'author',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'users',
                        'relationType' => RelationType::ManyToOne->value,
                    ]),
                ]),
            ],
        ]);

        $userCollection = new Document([
            '$id' => 'users',
            'attributes' => [
                new Document([
                    'key' => 'profile',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'profiles',
                        'relationType' => RelationType::OneToOne->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')
            ->willReturnOnConsecutiveCalls([$authorDoc], [$profileDoc]);
        $this->db->method('getCollection')
            ->willReturn($userCollection);

        $docs = [new Document(['$id' => 'p1', 'author' => 'a1'])];
        $result = $this->eagerLoader->load($docs, ['author.profile'], $postCollection, $this->db);

        $this->assertEquals('Alice', $result[0]->getAttribute('author')->getAttribute('name'));
    }

    public function testLoadWithNoForeignKeysFoundReturnsUnchanged(): void
    {
        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'author',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'users',
                        'relationType' => RelationType::ManyToOne->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->expects($this->never())->method('find');

        $docs = [new Document(['$id' => 'p1', 'author' => ''])];
        $result = $this->eagerLoader->load($docs, ['author'], $collectionMeta, $this->db);
        $this->assertEquals('', $result[0]->getAttribute('author'));
    }

    public function testLoadHandlesStringIDsInRelationships(): void
    {
        $tagDoc = new Document(['$id' => 'tag-uuid-123', 'label' => 'PHP']);

        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'tags',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'tags',
                        'relationType' => RelationType::ManyToMany->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$tagDoc]);

        $docs = [new Document(['$id' => 'p1', 'tags' => ['tag-uuid-123']])];
        $result = $this->eagerLoader->load($docs, ['tags'], $collectionMeta, $this->db);

        $tags = $result[0]->getAttribute('tags');
        $this->assertCount(1, $tags);
        $this->assertEquals('PHP', $tags[0]->getAttribute('label'));
    }

    public function testLoadHandlesDocumentObjectsInRelationships(): void
    {
        $tagDoc = new Document(['$id' => 't1', 'label' => 'PHP']);

        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'tags',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'tags',
                        'relationType' => RelationType::ManyToMany->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$tagDoc]);

        $existingTagRef = new Document(['$id' => 't1']);
        $docs = [new Document(['$id' => 'p1', 'tags' => [$existingTagRef]])];
        $result = $this->eagerLoader->load($docs, ['tags'], $collectionMeta, $this->db);

        $tags = $result[0]->getAttribute('tags');
        $this->assertCount(1, $tags);
        $this->assertEquals('PHP', $tags[0]->getAttribute('label'));
    }

    public function testLoadWithNonExistentRelationAttributeSkips(): void
    {
        $collectionMeta = new Document([
            '$id' => 'posts',
            'attributes' => [
                new Document([
                    'key' => 'title',
                    'type' => 'string',
                ]),
            ],
        ]);

        $this->db->expects($this->never())->method('find');

        $docs = [new Document(['$id' => 'p1', 'title' => 'Hello'])];
        $result = $this->eagerLoader->load($docs, ['author'], $collectionMeta, $this->db);

        $this->assertEquals('Hello', $result[0]->getAttribute('title'));
    }

    public function testLoadHandlesDocumentValueInOneToOne(): void
    {
        $profileDoc = new Document(['$id' => 'pr1', 'bio' => 'Dev']);

        $collectionMeta = new Document([
            '$id' => 'users',
            'attributes' => [
                new Document([
                    'key' => 'profile',
                    'type' => 'relationship',
                    'options' => new Document([
                        'relatedCollection' => 'profiles',
                        'relationType' => RelationType::OneToOne->value,
                    ]),
                ]),
            ],
        ]);

        $this->db->method('find')->willReturn([$profileDoc]);

        $existingRef = new Document(['$id' => 'pr1']);
        $docs = [new Document(['$id' => 'u1', 'profile' => $existingRef])];
        $result = $this->eagerLoader->load($docs, ['profile'], $collectionMeta, $this->db);

        $profile = $result[0]->getAttribute('profile');
        $this->assertEquals('Dev', $profile->getAttribute('bio'));
    }
}
