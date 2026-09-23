<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class QueryCachePurgeTest extends TestCase
{
    public function testPurgeCachedQueriesDropsTheResultsFindCached(): void
    {
        [$reader, $bypass] = $this->createDatabases('posts_'.\uniqid());

        $this->assertSame(['first'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
        $bypass->createDocument('posts', new Document(['$id' => 'second']));
        $this->assertSame(
            ['first'],
            $this->ids($reader->find('posts', [Query::orderAsc('$id')])),
            'A write the query cache never saw stays invisible until the cache is purged',
        );

        $this->assertTrue($reader->purgeCachedQueries('posts'));

        $this->assertSame(['first', 'second'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
    }

    public function testPurgeCachedQueriesReachesTheNamespaceItNames(): void
    {
        $namespace = 'posts_'.\uniqid();
        $queryCache = new QueryCache(new Cache(new LeasableHashCache()));
        [$reader, $bypass] = $this->createDatabases($namespace, $queryCache);
        $caller = new Database(new DatabaseMemory(), new Cache(new LeasableHashCache()));
        $caller
            ->setDatabase('purge')
            ->setNamespace('caller_'.\uniqid())
            ->setQueryCache($queryCache);
        $caller->create();

        $this->assertSame(['first'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
        $bypass->createDocument('posts', new Document(['$id' => 'second']));

        $this->assertTrue($caller->purgeCachedQueries('posts', $namespace));

        $this->assertSame(['first', 'second'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
    }

    public function testPurgeCachedQueriesReportsAQueryCacheItCouldNotPurge(): void
    {
        $queryCache = new FailingMemory();
        [$reader] = $this->createDatabases('posts_'.\uniqid(), new QueryCache(new Cache($queryCache)));
        $queryCache->failBlocks();

        $this->assertFalse($reader->purgeCachedQueries('posts'));
    }

    /**
     * A reader with the query cache and a writer on the same data without it,
     * like a migration or a worker that bypasses the reader's invalidation.
     *
     * @return array{Database, Database}
     */
    private function createDatabases(string $namespace, ?QueryCache $queryCache = null): array
    {
        $adapter = new DatabaseMemory();
        $cache = new Cache(new LeasableHashCache());
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $databases = [];
        foreach ([0, 1] as $ignored) {
            $database = new Database($adapter, $cache);
            $database
                ->setAuthorization($authorization)
                ->setDatabase('purge')
                ->setNamespace($namespace);
            $databases[] = $database;
        }
        [$reader, $bypass] = $databases;

        $reader->create();
        $reader->setQueryCache($queryCache ?? new QueryCache(new Cache(new LeasableHashCache())));
        $reader->createCollection(new Collection(id: 'posts', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ], documentSecurity: false));
        $reader->createDocument('posts', new Document(['$id' => 'first']));

        return [$reader, $bypass];
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
