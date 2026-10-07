<?php

namespace Tests\Unit\Documents;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class CreateDocumentLogicTest extends TestCase
{
    private Adapter&Stub $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);
        $this->adapter->method('getSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('getTenantPerDocument')->willReturn(false);
        $this->adapter->method('getNamespace')->willReturn('');
        $this->adapter->method('limits')->willReturn(new Limits(
            string: 16777215,
            varchar: 16383,
            integer: 2147483647,
            bigInteger: 0,
            attributes: 0,
            indexes: 64,
            defaultAttributes: 0,
            defaultIndexes: 0,
            indexLength: 768,
            uidLength: 36,
            documentSize: 0,
            minDateTime: new DateTime('0000-01-01'),
            maxDateTime: new DateTime('9999-12-31'),
            idType: ColumnType::String,
            keywords: [],
            internalIndexKeys: [],
        ));
        $this->adapter->method('getCountOfAttributes')->willReturn(0);
        $this->adapter->method('getCountOfIndexes')->willReturn(0);
        $this->adapter->method('getAttributeWidth')->willReturn(0);
        $this->adapter->method('filter')->willReturnArgument(0);
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
            ]);
        });
        $this->adapter->method('startTransaction')->willReturn(true);
        $this->adapter->method('commitTransaction')->willReturn(true);
        $this->adapter->method('rollbackTransaction')->willReturn(true);
        $this->adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $this->adapter->method('createDocument')->willReturnArgument(1);
        $this->adapter->method('createDocuments')->willReturnCallback(function (Document $col, array $docs) {
            return $docs;
        });
        $this->adapter->method('getSequences')->willReturnArgument(1);

        $cache = new Cache(new None());
        $this->database = new Database($this->adapter, $cache);
        $this->database->getAuthorization()->addRole(Role::any()->toString());
    }

    /**
     * @param  array<int, Document>  $attributes
     * @param  array<int, string>  $permissions
     */
    private function setupCollection(string $id, array $attributes = [], array $permissions = []): void
    {
        if (empty($permissions)) {
            $permissions = [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ];
        }

        $collection = new Document([
            '$id' => $id,
            '$collection' => Database::METADATA,
            '$permissions' => $permissions,
            'name' => $id,
            'attributes' => $attributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($id, $collection) {
                if ($col->getId() === Database::METADATA && $docId === $id) {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return Database::collectionDefinition();
                }

                return new Document();
            }
        );
    }

    public function testCreateDocumentSetsCreatedAtAndUpdatedAt(): void
    {
        $this->setupCollection('testCol');

        $doc = new Document([
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'testCol',
        ]);

        $result = $this->database->createDocument('testCol', $doc);
        $this->assertNotNull($result->getCreatedAt());
        $this->assertNotNull($result->getUpdatedAt());
    }

    public function testCreateDocumentGeneratesIdIfEmpty(): void
    {
        $this->setupCollection('testCol');

        $doc = new Document([
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'testCol',
        ]);

        $result = $this->database->createDocument('testCol', $doc);
        $this->assertNotEmpty($result->getId());
    }

    public function testCreateDocumentUsesProvidedId(): void
    {
        $this->setupCollection('testCol');

        $doc = new Document([
            '$id' => 'custom-id',
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'testCol',
        ]);

        $result = $this->database->createDocument('testCol', $doc);
        $this->assertSame('custom-id', $result->getId());
    }

    public function testCreateDocumentValidatesStructureWhenEnabled(): void
    {
        $attributes = [
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => true, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $attributes);
        $this->database->enableValidation();

        $doc = new Document([
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'testCol',
        ]);

        $this->expectException(StructureException::class);
        $this->database->createDocument('testCol', $doc);
    }

    public function testCreateDocumentSkipsValidationWhenDisabled(): void
    {
        $attributes = [
            new Document(['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => true, 'array' => false, 'signed' => true, 'filters' => []]),
        ];
        $this->setupCollection('testCol', $attributes);

        $doc = new Document([
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'testCol',
        ]);

        $result = $this->database->skipValidation(fn () => $this->database->createDocument('testCol', $doc));
        $this->assertNotEmpty($result->getId());
    }

    public function testCreateDocumentChecksCreatePermission(): void
    {
        $collection = new Document([
            '$id' => 'restricted',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::create(Role::user('admin'))],
            'name' => 'restricted',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'restricted') {
                    return $collection;
                }

                return new Document();
            }
        );

        $db = new Database($this->adapter, new Cache(new None()));

        $this->expectException(AuthorizationException::class);
        $db->createDocument('restricted', new Document([
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'restricted',
        ]));
    }

    public function testCreateDocumentSetsCollectionAttribute(): void
    {
        $this->setupCollection('testCol');

        $doc = new Document([
            '$permissions' => [Permission::read(Role::any())],
            '$collection' => 'testCol',
        ]);

        $result = $this->database->createDocument('testCol', $doc);
        $this->assertSame('testCol', $result->getAttribute('$collection'));
    }

    public function testCreateDocumentValidatesPermissionsFormat(): void
    {
        $this->setupCollection('testCol');
        $this->database->enableValidation();

        $doc = new Document([
            '$permissions' => ['invalid-permission-format'],
            '$collection' => 'testCol',
        ]);

        $this->expectException(\Utopia\Database\Exception::class);
        $this->database->createDocument('testCol', $doc);
    }

    public function testCreateDocumentsCreatesEveryDocument(): void
    {
        $this->setupCollection('testCol');

        $docs = [
            new Document(['$permissions' => [Permission::read(Role::any())], '$collection' => 'testCol']),
            new Document(['$permissions' => [Permission::read(Role::any())], '$collection' => 'testCol']),
        ];

        $count = 0;
        $this->database->createDocuments('testCol', $docs, 100, function (Document $doc) use (&$count) {
            $count++;
        });

        $this->assertSame(2, $count);
    }

    public function testCreateDocumentsCallsOnNextCallbackPerDoc(): void
    {
        $this->setupCollection('testCol');

        $docs = [
            new Document(['$permissions' => [Permission::read(Role::any())], '$collection' => 'testCol']),
        ];

        $called = false;
        $this->database->createDocuments('testCol', $docs, 100, function () use (&$called) {
            $called = true;
        });

        $this->assertTrue($called);
    }

    public function testCreateDocumentsCallsOnErrorCallbackOnFailure(): void
    {
        $this->setupCollection('testCol');

        $docs = [
            new Document(['$permissions' => [Permission::read(Role::any())], '$collection' => 'testCol']),
        ];

        $errorCaught = false;
        $this->database->createDocuments('testCol', $docs, 100, function () {
            throw new \RuntimeException('onNext error');
        }, function (\Throwable $e) use (&$errorCaught) {
            $errorCaught = true;
            $this->assertSame('onNext error', $e->getMessage());
        });

        $this->assertTrue($errorCaught);
    }

    public function testCreateDocumentsReturnsZeroForEmptyArray(): void
    {
        $this->setupCollection('testCol');
        $count = $this->database->createDocuments('testCol', []);
        $this->assertSame(0, $count);
    }

    public function testCreateDocumentSetsEmptyPermissionsWhenNoneProvided(): void
    {
        $this->setupCollection('testCol');

        $doc = new Document([
            '$collection' => 'testCol',
        ]);

        $result = $this->database->createDocument('testCol', $doc);
        $this->assertSame([], $result->getPermissions());
    }
}
