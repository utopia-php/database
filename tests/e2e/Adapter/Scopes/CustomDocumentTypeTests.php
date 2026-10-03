<?php

namespace Tests\E2E\Adapter\Scopes;

use Tests\E2E\Adapter\Support\Post;
use Tests\E2E\Adapter\Support\User;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait CustomDocumentTypeTests
{
    public function testSetDocumentType(): void
    {
        $database = $this->getDatabase();

        $database->setDocumentType('users', User::class);

        $this->assertSame(User::class, $database->getDocumentType('users'));

        $database->clearDocumentType('users');

        $database->setDocumentType('users', User::class);
        $database->setDocumentType('posts', Post::class);

        $this->assertSame(User::class, $database->getDocumentType('users'));
        $this->assertSame(Post::class, $database->getDocumentType('posts'));

        $database->clearAllDocumentTypes();

        $this->assertNull($database->getDocumentType('users'));
        $this->assertNull($database->getDocumentType('posts'));
    }

    public function testGetDocumentTypeReturnsNull(): void
    {
        $database = $this->getDatabase();

        $this->assertNull($database->getDocumentType('nonexistent_collection'));
    }

    public function testSetDocumentTypeWithInvalidClass(): void
    {
        $database = $this->getDatabase();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not exist');

        $database->setDocumentType('users', 'NonExistentClass');
    }

    public function testSetDocumentTypeWithNonDocumentClass(): void
    {
        $database = $this->getDatabase();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('must extend');

        $database->setDocumentType('users', \stdClass::class);
    }

    public function testClearDocumentType(): void
    {
        $database = $this->getDatabase();

        $database->setDocumentType('users', User::class);
        $this->assertSame(User::class, $database->getDocumentType('users'));

        $database->clearDocumentType('users');
        $this->assertNull($database->getDocumentType('users'));
    }

    public function testClearAllDocumentTypes(): void
    {
        $database = $this->getDatabase();

        $database->setDocumentType('users', User::class);
        $database->setDocumentType('posts', Post::class);

        $this->assertSame(User::class, $database->getDocumentType('users'));
        $this->assertSame(Post::class, $database->getDocumentType('posts'));

        $database->clearAllDocumentTypes();

        $this->assertNull($database->getDocumentType('users'));
        $this->assertNull($database->getDocumentType('posts'));
    }

    public function testMethodChaining(): void
    {
        $database = $this->getDatabase();

        $result = $database->setDocumentType('users', User::class);

        $this->assertSame($database, $result);

        $database
            ->setDocumentType('users', User::class)
            ->setDocumentType('posts', Post::class);

        $this->assertSame(User::class, $database->getDocumentType('users'));
        $this->assertSame(Post::class, $database->getDocumentType('posts'));

        $database->clearAllDocumentTypes();
    }

    public function testCustomDocumentTypeWithGetDocument(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'customUsers', attributes: [
            Attribute::string(key: 'email', size: 255, required: true),
            Attribute::string(key: 'name', size: 255, required: true),
            Attribute::string(key: 'status', size: 50, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->setDocumentType('customUsers', User::class);

        $created = $database->createDocument('customUsers', new Document([
            '$id' => ID::unique(),
            'email' => 'test@example.com',
            'name' => 'Test User',
            'status' => 'active',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->assertInstanceOf(User::class, $created);
        $this->assertSame('test@example.com', $created->getEmail());
        $this->assertSame('Test User', $created->getName());
        $this->assertTrue($created->isActive());

        $fetched = $database->getDocument('customUsers', $created->getId());
        $this->assertInstanceOf(User::class, $fetched);
        $this->assertSame('test@example.com', $fetched->getEmail());
        $this->assertTrue($fetched->isActive());

        $database->deleteCollection('customUsers');
        $database->clearDocumentType('customUsers');
    }

    public function testCustomDocumentTypeWithFind(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'customPosts', attributes: [
            Attribute::string(key: 'title', size: 255, required: true),
            Attribute::string(key: 'content', size: 5000, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $database->setDocumentType('customPosts', Post::class);

        $database->createDocument('customPosts', new Document([
            '$id' => ID::unique(),
            'title' => 'First Post',
            'content' => 'This is the first post',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument('customPosts', new Document([
            '$id' => ID::unique(),
            'title' => 'Second Post',
            'content' => 'This is the second post',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $posts = $database->find('customPosts', [Query::limit(10)]);

        $this->assertCount(2, $posts);
        $this->assertInstanceOf(Post::class, $posts[0]);
        $this->assertInstanceOf(Post::class, $posts[1]);
        $this->assertSame('First Post', $posts[0]->getTitle());
        $this->assertSame('Second Post', $posts[1]->getTitle());

        $database->deleteCollection('customPosts');
        $database->clearDocumentType('customPosts');
    }

    public function testCustomDocumentTypeWithUpdateDocument(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'customUsersUpdate', attributes: [
            Attribute::string(key: 'email', size: 255, required: true),
            Attribute::string(key: 'name', size: 255, required: true),
            Attribute::string(key: 'status', size: 50, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        $database->setDocumentType('customUsersUpdate', User::class);

        $created = $database->createDocument('customUsersUpdate', new Document([
            '$id' => ID::unique(),
            'email' => 'original@example.com',
            'name' => 'Original Name',
            'status' => 'active',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $updated = $database->updateDocument('customUsersUpdate', $created->getId(), new Document([
            '$id' => $created->getId(),
            'email' => 'updated@example.com',
            'name' => 'Updated Name',
            'status' => 'inactive',
        ]));

        $this->assertInstanceOf(User::class, $updated);
        $this->assertSame('updated@example.com', $updated->getEmail());
        $this->assertSame('Updated Name', $updated->getName());
        $this->assertFalse($updated->isActive());

        $database->deleteCollection('customUsersUpdate');
        $database->clearDocumentType('customUsersUpdate');
    }

    public function testDefaultDocumentForUnmappedCollection(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'unmappedCollection', attributes: [
            Attribute::string(key: 'data', size: 255, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $created = $database->createDocument('unmappedCollection', new Document([
            '$id' => ID::unique(),
            'data' => 'test data',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->assertSame(Document::class, $created::class);

        $database->deleteCollection('unmappedCollection');
    }
}
