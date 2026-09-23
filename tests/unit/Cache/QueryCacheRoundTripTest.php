<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

final class QueryCacheRoundTripTest extends TestCase
{
    public function testACachedFindCostsThreeCacheRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->find('posts', [Query::orderAsc('$id')]);
        $adapter->observeFinds('posts');
        $cache->resetOperations();

        $this->assertSame(['first'], $this->ids($database->find('posts', [Query::orderAsc('$id')])));

        $this->assertSame(0, $adapter->getObservedFinds(), 'The second find must be served from the query cache');
        $this->assertSame(3, $cache->getOperations(), 'A hit reads the epoch, the started generation it was published under and the entry');
    }

    public function testAnUncachedFindCostsFiveCacheRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $adapter->observeFinds('posts');
        $cache->resetOperations();

        $this->assertSame(['first'], $this->ids($database->find('posts', [Query::orderAsc('$id')])));

        $this->assertSame(1, $adapter->getObservedFinds(), 'The first find after a write must read the database');
        $this->assertSame(5, $cache->getOperations(), 'A miss reads the epoch, its started generation, the entry and the entry\'s lease, then stores the result');
    }

    /**
     * @return array{Database, ObservedMemory, CountingCache}
     */
    private function createDatabase(): array
    {
        $adapter = new ObservedMemory();
        $cache = new CountingCache(new LeasableHashCache());
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('round_trips')
            ->setNamespace('round_trips_'.\uniqid());
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->setQueryCache(new QueryCache(new Cache($cache)));
        $database->createCollection(new Collection(id: 'posts', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ], documentSecurity: false));
        $database->createDocument('posts', new Document(['$id' => 'first']));

        return [$database, $adapter, $cache];
    }

    /**
     * @param  array<Document>  $documents
     * @return array<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(
            static fn (Document $document): string => $document->getId(),
            $documents,
        );
    }
}
