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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class SkipPermissionsTest extends TestCase
{
    private function makeAdapter(): Adapter&Stub
    {
        $adapter = self::createStub(Adapter::class);
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('limits')->willReturn(new Limits(
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
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::IndexKey,
                Capability::IndexArray,
                Capability::IndexUnique,
                Capability::DefinedAttributes,
            ]);
        });
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
                    return Database::collectionDefinition();
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
                    return Database::collectionDefinition();
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
