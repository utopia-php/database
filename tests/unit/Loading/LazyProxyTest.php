<?php

namespace Tests\Unit\Loading;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Loading\BatchLoader;
use Utopia\Database\Loading\LazyProxy;

class LazyProxyTest extends TestCase
{
    private Database $db;

    private BatchLoader $batchLoader;

    protected function setUp(): void
    {
        $this->db = self::createStub(Database::class);
        $this->batchLoader = new BatchLoader($this->db);
    }

    public function testConstructorRegistersWithBatchLoader(): void
    {
        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $this->assertFalse($proxy->isResolved());
        $this->assertEquals('u1', $proxy->getId());
    }

    public function testIsResolvedReturnsFalseInitially(): void
    {
        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $this->assertFalse($proxy->isResolved());
    }

    public function testResolveWithPopulatesDocumentData(): void
    {
        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $doc = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $proxy->resolveWith($doc);

        $this->assertTrue($proxy->isResolved());
        $this->assertEquals('Alice', $proxy->getAttribute('name'));
    }

    public function testIsResolvedReturnsTrueAfterResolveWith(): void
    {
        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $proxy->resolveWith(new Document(['$id' => 'u1']));
        $this->assertTrue($proxy->isResolved());
    }

    public function testGetAttributeTriggersLazyResolution(): void
    {
        $doc = new Document(['$id' => 'u1', 'name' => 'Bob']);
        $this->db->method('find')->willReturn([$doc]);

        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $name = $proxy->getAttribute('name');

        $this->assertEquals('Bob', $name);
        $this->assertTrue($proxy->isResolved());
    }

    public function testOffsetGetTriggersLazyResolution(): void
    {
        $doc = new Document(['$id' => 'u1', 'email' => 'bob@test.com']);
        $this->db->method('find')->willReturn([$doc]);

        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $email = $proxy['email'];

        $this->assertEquals('bob@test.com', $email);
        $this->assertTrue($proxy->isResolved());
    }

    public function testOffsetExistsTriggersLazyResolution(): void
    {
        $doc = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $this->db->method('find')->willReturn([$doc]);

        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $exists = isset($proxy['name']);

        $this->assertTrue($exists);
        $this->assertTrue($proxy->isResolved());
    }

    public function testGetArrayCopyTriggersLazyResolution(): void
    {
        $doc = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $this->db->method('find')->willReturn([$doc]);

        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $copy = $proxy->getArrayCopy();

        $this->assertArrayHasKey('name', $copy);
        $this->assertTrue($proxy->isResolved());
    }

    public function testIsEmptyTriggersLazyResolution(): void
    {
        $doc = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $this->db->method('find')->willReturn([$doc]);

        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $empty = $proxy->isEmpty();

        $this->assertFalse($empty);
        $this->assertTrue($proxy->isResolved());
    }

    public function testResolveWithNullDocument(): void
    {
        $proxy = new LazyProxy($this->batchLoader, 'users', 'u1');
        $proxy->resolveWith(null);

        $this->assertTrue($proxy->isResolved());
    }

    public function testMultipleProxiesBatchResolvedTogether(): void
    {
        $db = $this->createMock(Database::class);
        $batchLoader = new BatchLoader($db);

        $doc1 = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $doc2 = new Document(['$id' => 'u2', 'name' => 'Bob']);

        $db->expects($this->once())
            ->method('find')
            ->willReturn([$doc1, $doc2]);

        $proxy1 = new LazyProxy($batchLoader, 'users', 'u1');
        $proxy2 = new LazyProxy($batchLoader, 'users', 'u2');

        $proxy1->getAttribute('name');

        $this->assertTrue($proxy1->isResolved());
        $this->assertTrue($proxy2->isResolved());
        $this->assertEquals('Alice', $proxy1->getAttribute('name'));
        $this->assertEquals('Bob', $proxy2->getAttribute('name'));
    }

    public function testBatchLoaderResolveWithNoPendingReturnsNull(): void
    {
        $result = $this->batchLoader->resolve('nonexistent', 'id1');
        $this->assertNull($result);
    }

    public function testBatchLoaderResolveClearsPendingAfterResolution(): void
    {
        $db = $this->createMock(Database::class);
        $batchLoader = new BatchLoader($db);

        $doc = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $db->expects($this->once())
            ->method('find')
            ->willReturn([$doc]);

        $proxy = new LazyProxy($batchLoader, 'users', 'u1');
        $batchLoader->resolve('users', 'u1');

        $result = $batchLoader->resolve('users', 'u1');
        $this->assertNull($result);
    }

    public function testBatchLoaderResolveFetchesAllPendingAtOnce(): void
    {
        $db = $this->createMock(Database::class);
        $batchLoader = new BatchLoader($db);

        $doc1 = new Document(['$id' => 'u1', 'name' => 'Alice']);
        $doc2 = new Document(['$id' => 'u2', 'name' => 'Bob']);
        $doc3 = new Document(['$id' => 'u3', 'name' => 'Charlie']);

        $db->expects($this->once())
            ->method('find')
            ->willReturn([$doc1, $doc2, $doc3]);

        new LazyProxy($batchLoader, 'users', 'u1');
        new LazyProxy($batchLoader, 'users', 'u2');
        new LazyProxy($batchLoader, 'users', 'u3');

        $result = $batchLoader->resolve('users', 'u1');
        $this->assertInstanceOf(Document::class, $result);
        $this->assertEquals('u1', $result->getId());
    }
}
