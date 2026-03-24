<?php

namespace Tests\Unit;

use DateTime;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoneAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class TestUserDocument extends Document
{
    public function getEmail(): string
    {
        /** @var string $value */
        $value = $this->getAttribute('email', '');

        return $value;
    }

    public function getName(): string
    {
        /** @var string $value */
        $value = $this->getAttribute('name', '');

        return $value;
    }

    public function isActive(): bool
    {
        return $this->getAttribute('status') === 'active';
    }
}

class TestPostDocument extends Document
{
    public function getTitle(): string
    {
        /** @var string $value */
        $value = $this->getAttribute('title', '');

        return $value;
    }

    public function getContent(): string
    {
        /** @var string $value */
        $value = $this->getAttribute('content', '');

        return $value;
    }
}

class CustomDocumentTypeTest extends TestCase
{
    private Database $database;

    private Adapter $adapter;

    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);

        $this->adapter->method('getSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('getTenantPerDocument')->willReturn(false);
        $this->adapter->method('getIdAttributeType')->willReturn('string');
        $this->adapter->method('getMinDateTime')->willReturn(new DateTime('1970-01-01 00:00:00'));
        $this->adapter->method('getMaxDateTime')->willReturn(new DateTime('2999-12-31 23:59:59'));
        $this->adapter->method('getMaxUIDLength')->willReturn(36);
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return match ($cap) {
                Capability::DefinedAttributes => true,
                default => false,
            };
        });
        $this->adapter->method('castingBefore')->willReturnCallback(
            fn (Document $collection, Document $document) => $document
        );
        $this->adapter->method('castingAfter')->willReturnCallback(
            fn (Document $collection, Document $document) => $document
        );
        $this->adapter->method('withTransaction')->willReturnCallback(
            fn (callable $callback) => $callback()
        );
        $this->adapter->method('getSequences')->willReturnCallback(
            fn (string $collection, array $documents) => $documents
        );

        $cache = new Cache(new NoneAdapter());
        $this->database = new Database($this->adapter, $cache);
        $this->database->disableValidation();
        $this->database->disableFilters();
    }

    public function testSetDocumentTypeStoresMapping(): void
    {
        $this->database->setDocumentType('users', TestUserDocument::class);
        $this->assertEquals(TestUserDocument::class, $this->database->getDocumentType('users'));
    }

    public function testGetDocumentTypeReturnsClass(): void
    {
        $this->database->setDocumentType('posts', TestPostDocument::class);
        $this->assertEquals(TestPostDocument::class, $this->database->getDocumentType('posts'));
    }

    public function testGetDocumentTypeReturnsNullForUnmapped(): void
    {
        $this->assertNull($this->database->getDocumentType('nonexistent'));
    }

    public function testSetDocumentTypeValidatesClassExists(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not exist');

        /** @phpstan-ignore-next-line */
        $this->database->setDocumentType('users', 'NonExistentClass');
    }

    public function testSetDocumentTypeValidatesClassExtendsDocument(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('must extend');

        /** @phpstan-ignore-next-line */
        $this->database->setDocumentType('users', \stdClass::class);
    }

    public function testClearDocumentTypeRemovesMapping(): void
    {
        $this->database->setDocumentType('users', TestUserDocument::class);
        $this->assertEquals(TestUserDocument::class, $this->database->getDocumentType('users'));

        $this->database->clearDocumentType('users');
        $this->assertNull($this->database->getDocumentType('users'));
    }

    public function testClearAllDocumentTypesRemovesAll(): void
    {
        $this->database->setDocumentType('users', TestUserDocument::class);
        $this->database->setDocumentType('posts', TestPostDocument::class);

        $this->assertEquals(TestUserDocument::class, $this->database->getDocumentType('users'));
        $this->assertEquals(TestPostDocument::class, $this->database->getDocumentType('posts'));

        $this->database->clearAllDocumentTypes();

        $this->assertNull($this->database->getDocumentType('users'));
        $this->assertNull($this->database->getDocumentType('posts'));
    }

    public function testMethodChaining(): void
    {
        $result = $this->database->setDocumentType('users', TestUserDocument::class);
        $this->assertInstanceOf(Database::class, $result);

        $this->database
            ->setDocumentType('users', TestUserDocument::class)
            ->setDocumentType('posts', TestPostDocument::class);

        $this->assertEquals(TestUserDocument::class, $this->database->getDocumentType('users'));
        $this->assertEquals(TestPostDocument::class, $this->database->getDocumentType('posts'));
    }

    public function testClearDocumentTypeReturnsSelf(): void
    {
        $this->database->setDocumentType('users', TestUserDocument::class);
        $result = $this->database->clearDocumentType('users');
        $this->assertInstanceOf(Database::class, $result);
    }

    public function testClearAllDocumentTypesReturnsSelf(): void
    {
        $result = $this->database->clearAllDocumentTypes();
        $this->assertInstanceOf(Database::class, $result);
    }

    public function testCreateDocumentInstanceReturnsCorrectType(): void
    {
        $collection = new Document([
            '$id' => 'users',
            '$collection' => Database::METADATA,
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            'name' => 'users',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => false,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id) use ($collection) {
                if ($col->getId() === Database::METADATA && $id === 'users') {
                    return $collection;
                }

                return new Document();
            }
        );

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->database->setDocumentType('users', TestUserDocument::class);

        $this->database->getAuthorization()->cleanRoles();
        $this->database->getAuthorization()->addRole('any');

        $result = $this->database->createDocument('users', new Document([
            '$id' => 'user1',
            '$permissions' => [],
            'email' => 'test@example.com',
            'name' => 'Test User',
            'status' => 'active',
        ]));

        $this->assertInstanceOf(TestUserDocument::class, $result);
        $this->assertEquals('test@example.com', $result->getEmail());
        $this->assertEquals('Test User', $result->getName());
        $this->assertTrue($result->isActive());
    }

    public function testFindResultsUseMappedType(): void
    {
        $collection = new Document([
            '$id' => 'posts',
            '$collection' => Database::METADATA,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ],
            'name' => 'posts',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => false,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id) use ($collection) {
                if ($col->getId() === Database::METADATA && $id === 'posts') {
                    return $collection;
                }

                return new Document();
            }
        );

        $this->adapter->method('find')->willReturn([
            new Document([
                '$id' => 'post1',
                '$permissions' => [],
                'title' => 'First Post',
                'content' => 'Content of first post',
            ]),
            new Document([
                '$id' => 'post2',
                '$permissions' => [],
                'title' => 'Second Post',
                'content' => 'Content of second post',
            ]),
        ]);

        $this->database->setDocumentType('posts', TestPostDocument::class);

        $this->database->getAuthorization()->cleanRoles();
        $this->database->getAuthorization()->addRole('any');

        $results = $this->database->find('posts');

        $this->assertCount(2, $results);
        $this->assertInstanceOf(TestPostDocument::class, $results[0]);
        $this->assertInstanceOf(TestPostDocument::class, $results[1]);
        $this->assertEquals('First Post', $results[0]->getTitle());
        $this->assertEquals('Second Post', $results[1]->getTitle());
    }

    public function testUnmappedCollectionReturnsBaseDocument(): void
    {
        $collection = new Document([
            '$id' => 'generic',
            '$collection' => Database::METADATA,
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            'name' => 'generic',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => false,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $id) use ($collection) {
                if ($col->getId() === Database::METADATA && $id === 'generic') {
                    return $collection;
                }

                return new Document();
            }
        );

        $this->adapter->method('createDocument')->willReturnCallback(
            fn (Document $col, Document $doc) => $doc
        );

        $this->database->getAuthorization()->cleanRoles();
        $this->database->getAuthorization()->addRole('any');

        $result = $this->database->createDocument('generic', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'data' => 'test',
        ]));

        $this->assertInstanceOf(Document::class, $result);
        $this->assertNotInstanceOf(TestUserDocument::class, $result);
        $this->assertNotInstanceOf(TestPostDocument::class, $result);
    }
}
