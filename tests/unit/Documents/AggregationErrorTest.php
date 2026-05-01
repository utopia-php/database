<?php

namespace Tests\Unit\Documents;

use DateTime;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

class AggregationErrorTest extends TestCase
{
    private function buildDatabase(array $capabilities): Database
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
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) use ($capabilities) {
            return in_array($cap, $capabilities);
        });

        $collection = new Document([
            '$id' => 'testCol',
            '$collection' => Database::METADATA,
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any())],
            'name' => 'testCol',
            'attributes' => [
                new Document(['$id' => 'amount', 'key' => 'amount', 'type' => 'double', 'size' => 0, 'required' => false, 'array' => false]),
            ],
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collection) {
                if ($col->getId() === Database::METADATA && $docId === 'testCol') {
                    return $collection;
                }

                return new Document();
            }
        );

        $adapter->method('find')->willReturn([]);
        $adapter->method('count')->willReturn(0);
        $adapter->method('sum')->willReturn(0);

        $cache = new Cache(new None());
        $db = new Database($adapter, $cache);
        $db->getAuthorization()->addRole(Role::any()->toString());

        return $db;
    }

    public function testFindWithAggregationOnUnsupportedAdapterThrows(): void
    {
        $db = $this->buildDatabase([
            Capability::Index,
            Capability::IndexArray,
            Capability::UniqueIndex,
            Capability::DefinedAttributes,
        ]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Aggregation queries are not supported');
        $db->skipValidation(fn () => $db->find('testCol', [Query::count('*', 'cnt')]));
    }

    public function testFindWithAggregationSkipsRelationshipPopulation(): void
    {
        $db = $this->buildDatabase([
            Capability::Index,
            Capability::IndexArray,
            Capability::UniqueIndex,
            Capability::DefinedAttributes,
            Capability::Aggregations,
        ]);

        $results = $db->skipValidation(fn () => $db->find('testCol', [Query::count('*', 'cnt')]));
        $this->assertIsArray($results);
    }

    public function testFindWithCursorAndAggregationThrows(): void
    {
        $db = $this->buildDatabase([
            Capability::Index,
            Capability::IndexArray,
            Capability::UniqueIndex,
            Capability::DefinedAttributes,
            Capability::Aggregations,
        ]);

        $cursorDoc = new Document([
            '$id' => 'c1',
            '$collection' => 'testCol',
            '$sequence' => '100',
        ]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Cursor pagination is not supported with aggregation');
        $db->skipValidation(fn () => $db->find('testCol', [
            Query::count('*', 'cnt'),
            Query::cursorAfter($cursorDoc),
        ]));
    }

    public function testFindWithJoinOnUnsupportedAdapterThrows(): void
    {
        $db = $this->buildDatabase([
            Capability::Index,
            Capability::IndexArray,
            Capability::UniqueIndex,
            Capability::DefinedAttributes,
        ]);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Join queries are not supported');
        $db->skipValidation(fn () => $db->find('testCol', [Query::join('other', 'fk', '$id')]));
    }

    public function testSumValidatesQueriesWhenEnabled(): void
    {
        $db = $this->buildDatabase([
            Capability::Index,
            Capability::IndexArray,
            Capability::UniqueIndex,
            Capability::DefinedAttributes,
        ]);
        $db->enableValidation();

        $this->expectException(QueryException::class);
        $db->sum('testCol', 'amount', [Query::equal('nonexistent', ['val'])]);
    }
}
