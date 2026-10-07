<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolCastingTest extends TestCase
{
    private int $checkouts = 0;

    public function testAFindCastsItsPageInOneCheckout(): void
    {
        $adapter = new CastCountingSQLite();
        $database = $this->database($adapter);
        $database->createDocuments('posts', $this->posts(['a', 'b', 'c', 'd', 'e']));
        $database->find('posts', [Query::limit(5)]);

        $adapter->reset();
        $this->checkouts = 0;
        $one = $database->find('posts', [Query::limit(1)]);
        $checkoutsForOne = $this->checkouts;

        $this->checkouts = 0;
        $five = $database->find('posts', [Query::limit(5)]);

        $this->assertCount(1, $one);
        $this->assertCount(5, $five);
        $this->assertSame($checkoutsForOne, $this->checkouts, 'Casting a page must not check out a connection per document');
        $this->assertSame(0, $adapter->singles);
        $this->assertSame([['a'], ['a', 'b', 'c', 'd', 'e']], $adapter->batches);
    }

    public function testAnEmptyPageAsksNoCasting(): void
    {
        $adapter = new CastCountingSQLite();
        $database = $this->database($adapter);
        $database->find('posts', [Query::equal('title', ['none'])]);

        $adapter->reset();
        $this->assertSame([], $database->find('posts', [Query::equal('title', ['none'])]));
        $this->assertSame(0, $adapter->singles);
        $this->assertSame([], $adapter->batches);
    }

    public function testBulkWritesCastEachBatchInOneCall(): void
    {
        $adapter = new CastCountingSQLite();
        $database = $this->database($adapter);

        $this->assertSame(3, $database->createDocuments('posts', $this->posts(['a', 'b', 'c'])));
        $this->assertSame([['a', 'b', 'c']], $adapter->batches);

        $adapter->reset();
        $this->assertSame(3, $database->updateDocuments('posts', new Document(['title' => 'renamed'])));
        $this->assertSame([['a', 'b', 'c'], ['a', 'b', 'c']], $adapter->batches, 'The read of the batch, then the updated batch');

        $adapter->reset();
        $this->assertSame(2, $database->upsertDocuments('posts', $this->posts(['d', 'e'])));
        $this->assertSame([['d', 'e']], $adapter->batches);

        $this->assertSame(0, $adapter->singles);
        $this->assertSame(['renamed', 'renamed', 'renamed', 'd', 'e'], \array_map(
            fn (Document $document): mixed => $document->getAttribute('title'),
            $database->find('posts', [Query::orderAsc('$id')]),
        ));
    }

    /**
     * @param  list<string>  $ids
     * @return list<Document>
     */
    private function posts(array $ids): array
    {
        return \array_map(fn (string $id): Document => new Document([
            Document::ID => $id,
            'title' => $id,
            Document::PERMISSIONS => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]), $ids);
    }

    private function database(Adapter $adapter): Database
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(function (callable $callback) use ($adapter): mixed {
            $this->checkouts++;

            return $callback($adapter);
        });

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        $database = new Database($pool, new Cache(new MemoryCache()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('pool_casting')
            ->setNamespace('pool_casting_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'posts',
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));

        return $database;
    }
}
