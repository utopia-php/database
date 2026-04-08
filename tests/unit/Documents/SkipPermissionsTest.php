<?php

namespace Tests\Unit\Documents;

use DateTime;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class SkipPermissionsTest extends TestCase
{
    private function makeAdapter(): Adapter&Stub
    {
        $adapter = self::createStub(Adapter::class);
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('getIdAttributeType')->willReturn('string');
        $adapter->method('getMaxUIDLength')->willReturn(36);
        $adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $adapter->method('getLimitForString')->willReturn(16777215);
        $adapter->method('getLimitForInt')->willReturn(2147483647);
        $adapter->method('getLimitForAttributes')->willReturn(0);
        $adapter->method('getLimitForIndexes')->willReturn(64);
        $adapter->method('getMaxIndexLength')->willReturn(768);
        $adapter->method('getMaxVarcharLength')->willReturn(16383);
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::Index,
                Capability::IndexArray,
                Capability::UniqueIndex,
                Capability::DefinedAttributes,
            ]);
        });
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('startTransaction')->willReturn(true);
        $adapter->method('commitTransaction')->willReturn(true);
        $adapter->method('rollbackTransaction')->willReturn(true);
        $adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $adapter->method('createDocument')->willReturnArgument(1);
        $adapter->method('getSequences')->willReturnArgument(1);

        return $adapter;
    }

    private function buildDatabase(Adapter&Stub $adapter): Database
    {
        $cache = new Cache(new None());

        return new Database($adapter, $cache);
    }

    public function testGetDocumentWithSkippedPermissions(): void
    {
        $adapter = $this->makeAdapter();

        $restrictedDoc = new Document([
            '$id' => 'doc1',
            '$collection' => 'secret',
            '$permissions' => [Permission::read(Role::user('admin'))],
            'title' => 'Confidential',
        ]);

        $collection = new Document([
            '$id' => 'secret',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::user('admin'))],
            'name' => 'secret',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection, $restrictedDoc) {
                if ($col->getId() === Database::METADATA && $docId === 'secret') {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }
                if ($col->getId() === 'secret' && $docId === 'doc1') {
                    return $restrictedDoc;
                }

                return new Document();
            }
        );

        $db = $this->buildDatabase($adapter);

        $noPermResult = $db->getDocument('secret', 'doc1');
        $this->assertTrue($noPermResult->isEmpty());

        $result = $db->getAuthorization()->skip(function () use ($db) {
            return $db->getDocument('secret', 'doc1');
        });

        $this->assertFalse($result->isEmpty());
        $this->assertSame('doc1', $result->getId());
        $this->assertSame('Confidential', $result->getAttribute('title'));
    }

    public function testCreateDocumentWithSkippedPermissions(): void
    {
        $adapter = $this->makeAdapter();

        $titleAttr = new Document([
            '$id' => 'title',
            'key' => 'title',
            'type' => 'string',
            'size' => 256,
            'required' => false,
            'array' => false,
            'signed' => true,
            'filters' => [],
        ]);

        $collection = new Document([
            '$id' => 'restricted',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::create(Role::user('admin'))],
            'name' => 'restricted',
            'attributes' => [$titleAttr],
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'restricted') {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }

                return new Document();
            }
        );

        $db = $this->buildDatabase($adapter);

        $result = $db->getAuthorization()->skip(function () use ($db) {
            return $db->createDocument('restricted', new Document([
                '$permissions' => [Permission::read(Role::any())],
                '$collection' => 'restricted',
                'title' => 'Created via skip',
            ]));
        });

        $this->assertNotEmpty($result->getId());
        $this->assertSame('Created via skip', $result->getAttribute('title'));
    }
}
