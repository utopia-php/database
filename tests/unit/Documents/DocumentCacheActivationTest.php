<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\LeasableHashCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class DocumentCacheActivationTest extends TestCase
{
    private const string COLLECTION = 'webhooks';

    /**
     * @return array<string, array{\Closure(): CacheAdapter, bool}>
     */
    public static function interleavings(): array
    {
        return [
            'equal generations' => [static fn (): CacheAdapter => new MemoryCache(), false],
            'the first writer flushed away' => [static fn (): CacheAdapter => new LeasableHashCache(), true],
        ];
    }

    /**
     * @param  \Closure(): CacheAdapter  $cacheAdapter
     */
    #[DataProvider('interleavings')]
    public function testAWriterActivatingWhileAnotherIsInFlightLeavesItsBarrierInPlace(\Closure $cacheAdapter, bool $flush): void
    {
        $cache = new Cache($cacheAdapter());
        $namespace = 'activation_'.\uniqid();
        $firstAdapter = new CountingMemory();
        $first = $this->database($firstAdapter, $cache, $namespace);
        $cache->flush();
        $second = $this->database(new CountingMemory(), $cache, $namespace);
        $cache->flush();
        [$collectionKey] = $first->getCacheKeys(self::COLLECTION);

        $second->withTransaction(function () use ($first, $second, $firstAdapter, $cache, $collectionKey, $flush): void {
            $first->withTransaction(function () use ($first, $second, $cache, $flush): void {
                $first->updateDocuments(self::COLLECTION, new Document(['name' => 'first']));
                if ($flush) {
                    $cache->flush();
                }
                $second->updateDocuments(self::COLLECTION, new Document(['name' => 'second']));
            });

            $epoch = $cache->load($collectionKey.'#epoch', Database::TTL);
            $this->assertIsString($epoch);
            $this->assertStringStartsWith('blocked:', $epoch, 'the second writer is still in flight');

            $firstAdapter->reset();
            $first->getDocument(self::COLLECTION, 'hook');
            $first->getDocument(self::COLLECTION, 'hook');
            $this->assertSame(2, $firstAdapter->documentReads, 'no read is cached while the barrier stands');
        });

        $epoch = $cache->load($collectionKey.'#epoch', Database::TTL);
        $this->assertIsString($epoch);
        $this->assertStringStartsWith('active:', $epoch, 'the last writer to finish publishes the epoch');

        $firstAdapter->reset();
        $first->getDocument(self::COLLECTION, 'hook');
        $first->getDocument(self::COLLECTION, 'hook');
        $this->assertSame(1, $firstAdapter->documentReads, 'caching resumes once no writer is in flight');
    }

    private function database(CountingMemory $adapter, Cache $cache, string $namespace): Database
    {
        $database = new Database($adapter, $cache);
        $database->setDatabase('activation')->setNamespace($namespace);
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'hook', 'name' => 'original']));

        return $database;
    }
}
