<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\LeasableHashCache;
use Tests\Unit\Support\CountingMemory;
use Tests\Unit\Support\UncachedTwin;
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

        $second->withTransaction(function () use ($first, $second, $cache, $flush): void {
            $first->withTransaction(function () use ($first, $second, $cache, $flush): void {
                $first->updateDocuments(self::COLLECTION, new Document(['name' => 'first']));
                if ($flush) {
                    $cache->flush();
                }
                $second->updateDocuments(self::COLLECTION, new Document(['name' => 'second']));
            });

            $this->assertSame('first', $first->getDocument(self::COLLECTION, 'hook')->getAttribute('name'));
            UncachedTwin::of($first)->updateDocument(self::COLLECTION, 'hook', new Document(['name' => 'changed']));
            $this->assertSame('changed', $first->getDocument(self::COLLECTION, 'hook')->getAttribute('name'), 'no read is cached while the second writer is in flight');
        });

        $this->assertSame('changed', $first->getDocument(self::COLLECTION, 'hook')->getAttribute('name'));
        $firstAdapter->reset();
        $this->assertSame('changed', $first->getDocument(self::COLLECTION, 'hook')->getAttribute('name'));
        $this->assertSame(0, $firstAdapter->documentReads, 'caching resumes once no writer is in flight');
    }

    private function database(CountingMemory $adapter, Cache $cache, string $namespace): Database
    {
        $database = new Database($adapter, $cache);
        $database->setDatabase('activation')->setNamespace($namespace);
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'hook', 'name' => 'original']));

        return $database;
    }
}
