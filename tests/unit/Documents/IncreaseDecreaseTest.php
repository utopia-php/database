<?php

namespace Tests\Unit\Documents;

use DateTime;
use InvalidArgumentException;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class IncreaseDecreaseTest extends TestCase
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
        $this->adapter->method('getIdAttributeType')->willReturn('string');
        $this->adapter->method('getMaxUIDLength')->willReturn(36);
        $this->adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $this->adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $this->adapter->method('getLimitForString')->willReturn(16777215);
        $this->adapter->method('getLimitForInt')->willReturn(2147483647);
        $this->adapter->method('getLimitForAttributes')->willReturn(0);
        $this->adapter->method('getLimitForIndexes')->willReturn(64);
        $this->adapter->method('getMaxIndexLength')->willReturn(768);
        $this->adapter->method('getMaxVarcharLength')->willReturn(16383);
        $this->adapter->method('getDocumentSizeLimit')->willReturn(0);
        $this->adapter->method('getCountOfAttributes')->willReturn(0);
        $this->adapter->method('getCountOfIndexes')->willReturn(0);
        $this->adapter->method('getAttributeWidth')->willReturn(0);
        $this->adapter->method('getInternalIndexesKeys')->willReturn([]);
        $this->adapter->method('filter')->willReturnArgument(0);
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::Index,
                Capability::IndexArray,
                Capability::UniqueIndex,
                Capability::DefinedAttributes,
            ]);
        });
        $this->adapter->method('castingBefore')->willReturnArgument(1);
        $this->adapter->method('castingAfter')->willReturnArgument(1);
        $this->adapter->method('startTransaction')->willReturn(true);
        $this->adapter->method('commitTransaction')->willReturn(true);
        $this->adapter->method('rollbackTransaction')->willReturn(true);
        $this->adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $this->adapter->method('increaseDocumentAttribute')->willReturn(true);

        $cache = new Cache(new None());
        $this->database = new Database($this->adapter, $cache);
        $this->database->getAuthorization()->addRole(Role::any()->toString());
    }

    private function setupCollectionWithDocument(
        string $collectionId,
        Document $existingDoc,
        array $attributes = [],
    ): void {
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $collection = new Document([
            '$id' => $collectionId,
            '$collection' => Database::METADATA,
            '$permissions' => $permissions,
            'name' => $collectionId,
            'attributes' => $attributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collectionId, $collection, $existingDoc) {
                if ($col->getId() === Database::METADATA && $docId === $collectionId) {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }
                if ($col->getId() === $collectionId && $docId === $existingDoc->getId()) {
                    return $existingDoc;
                }

                return new Document();
            }
        );
    }

    private function intAttribute(string $key): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => ColumnType::Integer->value,
            'size' => 0,
            'required' => false,
            'array' => false,
            'signed' => true,
            'filters' => [],
        ]);
    }

    private function floatAttribute(string $key): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => ColumnType::Double->value,
            'size' => 0,
            'required' => false,
            'array' => false,
            'signed' => true,
            'filters' => [],
        ]);
    }

    public function testIncreaseDocumentAttribute(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter');
        $this->assertSame(6, $result->getAttribute('counter'));
    }

    public function testIncreaseDocumentAttributeByCustomValue(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'score' => 10.0,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->floatAttribute('score')]);

        $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'score', 2.5);
        $this->assertSame(12.5, $result->getAttribute('score'));
    }

    public function testIncreaseDocumentAttributeWithMax(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 8,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', 1, 10);
        $this->assertSame(9, $result->getAttribute('counter'));
    }

    public function testIncreaseDocumentAttributeExceedsMax(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 10,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->expectException(LimitException::class);
        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', 1, 10);
    }

    public function testIncreaseDocumentAttributeWithZeroValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be numeric and greater than 0');

        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', 0);
    }

    public function testIncreaseDocumentAttributeWithNegativeValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be numeric and greater than 0');

        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', -1);
    }

    public function testIncreaseDocumentAttributeNotFound(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->expectException(NotFoundException::class);
        $this->database->increaseDocumentAttribute('testCol', 'nonexistent', 'counter');
    }

    public function testDecreaseDocumentAttribute(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 10,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter');
        $this->assertSame(9, $result->getAttribute('counter'));
    }

    public function testDecreaseDocumentAttributeWithMin(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter', 1, 0);
        $this->assertSame(4, $result->getAttribute('counter'));
    }

    public function testDecreaseDocumentAttributeExceedsMin(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 3,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->expectException(LimitException::class);
        $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter', 5, 0);
    }

    public function testDecreaseDocumentAttributeWithZeroValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be numeric and greater than 0');

        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter', 0);
    }
}
