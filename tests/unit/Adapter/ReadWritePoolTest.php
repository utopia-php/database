<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

#[AllowMockObjectsWithoutExpectations]
class ReadWritePoolTest extends TestCase
{
    /** @var UtopiaPool<Adapter>&Stub */
    private UtopiaPool $writePool;

    /** @var UtopiaPool<Adapter>&Stub */
    private UtopiaPool $readPool;

    private ReadWritePool $pool;

    /** @var Adapter&MockObject */
    private Adapter $writeAdapter;

    /** @var Adapter&MockObject */
    private Adapter $readAdapter;

    protected function setUp(): void
    {
        $this->writeAdapter = $this->createMock(Adapter::class);
        $this->readAdapter = $this->createMock(Adapter::class);

        $this->writePool = self::createStub(UtopiaPool::class);
        $this->readPool = self::createStub(UtopiaPool::class);

        $this->writePool->method('use')->willReturnCallback(function (callable $callback) {
            return $callback($this->writeAdapter);
        });

        $this->readPool->method('use')->willReturnCallback(function (callable $callback) {
            return $callback($this->readAdapter);
        });

        $this->pool = new ReadWritePool($this->writePool, $this->readPool);
        $this->pool->setAuthorization(new Authorization());
    }

    public function testReadMethodsRouteToReadPool(): void
    {
        $readMethods = [
            'find',
            'getDocument',
            'count',
            'sum',
            'exists',
            'list',
            'getSizeOfCollection',
            'getSizeOfCollectionOnDisk',
            'ping',
            'getConnectionId',
            'getDocumentSizeLimit',
            'getAttributeWidth',
            'getCountOfAttributes',
            'getCountOfIndexes',
            'getLimitForString',
            'getLimitForInt',
            'getLimitForAttributes',
            'getLimitForIndexes',
            'getMaxIndexLength',
            'getMaxVarcharLength',
            'getMaxUIDLength',
            'getIdAttributeType',
            'supports',
        ];

        foreach ($readMethods as $method) {
            $this->readAdapter->expects($this->atLeastOnce())
                ->method($method)
                ->willReturn($this->getDefaultReturnForMethod($method));
        }

        foreach ($readMethods as $method) {
            $args = $this->getDefaultArgsForMethod($method);
            $this->pool->delegate($method, $args);
        }
    }

    public function testWriteMethodRoutesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);
    }

    public function testDeleteDocumentRoutesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('deleteDocument')
            ->willReturn(true);

        $this->pool->delegate('deleteDocument', ['collection', 'id']);
    }

    public function testUpdateDocumentRoutesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('updateDocument')
            ->willReturn(new Document());

        $this->pool->delegate('updateDocument', [new Document(), 'id', new Document(), false]);
    }

    public function testCreateCollectionRoutesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('createCollection')
            ->willReturn(true);

        $this->pool->delegate('createCollection', ['testCollection', [], []]);
    }

    public function testStickyModeRoutesReadsToWritePoolAfterWrite(): void
    {
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(5000);

        $this->writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);

        $this->writeAdapter->expects($this->once())
            ->method('find')
            ->willReturn([]);

        $result = $this->pool->delegate('find', [new Document(), [], 25, 0, [], [], [], \Utopia\Query\CursorDirection::After, \Utopia\Database\PermissionType::Read]);
        $this->assertSame([], $result);
    }

    public function testStickyDurationExpiry(): void
    {
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(1);

        $this->writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);

        usleep(2000);

        $this->readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testStickyDisabledRoutesReadNormally(): void
    {
        $this->pool->setSticky(false);

        $this->writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);

        $this->readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testSetStickyDurationIsChainable(): void
    {
        $result = $this->pool->setStickyDuration(3000);
        $this->assertSame($this->pool, $result);
    }

    public function testSetStickyIsChainable(): void
    {
        $result = $this->pool->setSticky(true);
        $this->assertSame($this->pool, $result);
    }

    public function testReadAfterMultipleWritesStaysSticky(): void
    {
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(5000);

        $this->writeAdapter->method('createDocument')
            ->willReturn(new Document());
        $this->writeAdapter->method('deleteDocument')
            ->willReturn(true);

        $this->pool->delegate('createDocument', [new Document(), new Document()]);
        $this->pool->delegate('deleteDocument', ['collection', 'id']);

        $this->writeAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testReadBeforeAnyWriteGoesToReadPool(): void
    {
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(5000);

        $this->readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testNonReadNonStandardMethodGoesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('createAttribute')
            ->willReturn(true);

        $attr = new \Utopia\Database\Attribute(key: 'test', type: \Utopia\Query\Schema\ColumnType::String, size: 128);
        $this->pool->delegate('createAttribute', ['collection', $attr]);
    }

    public function testCreateIndexRoutesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('createIndex')
            ->willReturn(true);

        $index = new \Utopia\Database\Index(key: 'idx', type: \Utopia\Query\Schema\IndexType::Key, attributes: ['col']);
        $this->pool->delegate('createIndex', ['collection', $index, [], []]);
    }

    public function testDeleteCollectionRoutesToWritePool(): void
    {
        $this->writeAdapter->expects($this->once())
            ->method('deleteCollection')
            ->willReturn(true);

        $this->pool->delegate('deleteCollection', ['collection']);
    }

    /**
     * @return mixed
     */
    private function getDefaultReturnForMethod(string $method): mixed
    {
        return match ($method) {
            'find', 'list' => [],
            'getDocument' => new Document(),
            'count', 'sum', 'getSizeOfCollection', 'getSizeOfCollectionOnDisk',
            'getDocumentSizeLimit', 'getAttributeWidth', 'getCountOfAttributes',
            'getCountOfIndexes', 'getLimitForString', 'getLimitForInt',
            'getLimitForAttributes', 'getLimitForIndexes', 'getMaxIndexLength',
            'getMaxVarcharLength', 'getMaxUIDLength' => 0,
            'exists', 'ping', 'supports' => true,
            'getConnectionId', 'getIdAttributeType' => 'string',
            'getSchemaAttributes' => [],
            default => null,
        };
    }

    /**
     * @return array<mixed>
     */
    private function getDefaultArgsForMethod(string $method): array
    {
        return match ($method) {
            'find' => [new Document(), [], 25, 0, [], [], [], \Utopia\Query\CursorDirection::After, \Utopia\Database\PermissionType::Read],
            'getDocument' => [new Document(), 'id', [], false],
            'count' => [new Document(), [], null],
            'sum' => [new Document(), 'attr', [], null],
            'exists' => ['db', null],
            'list' => [],
            'getSizeOfCollection', 'getSizeOfCollectionOnDisk' => ['collection'],
            'ping' => [],
            'getConnectionId' => [],
            'getDocumentSizeLimit' => [],
            'getAttributeWidth' => [new Document()],
            'getCountOfAttributes' => [new Document()],
            'getCountOfIndexes' => [new Document()],
            'getLimitForString', 'getLimitForInt',
            'getLimitForAttributes', 'getLimitForIndexes',
            'getMaxIndexLength', 'getMaxVarcharLength',
            'getMaxUIDLength' => [],
            'getIdAttributeType' => [],
            'supports' => [\Utopia\Database\Capability::Index],
            'getSchemaAttributes' => ['collection'],
            default => [],
        };
    }
}
