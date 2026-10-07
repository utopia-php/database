<?php

namespace Tests\Unit\Documents;

use Closure;
use Override;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Role;

final class NegativeCacheEpochTest extends TestCase
{
    public function testAMissObservedBeforeAConcurrentCreateIsNotServedAfterIt(): void
    {
        $adapter = new InterceptingMemory();
        $database = $this->createDatabase($adapter);

        $adapter->interceptNextGetDocument('webhooks', 'hook', function () use ($database): void {
            $database->createDocument('webhooks', $this->hook());
        });

        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty(), 'The read observed the row before it was created');
        $this->assertSame(
            'created',
            $database->getDocument('webhooks', 'hook')->getAttribute('name'),
            'A miss observed before a concurrent create must not be served after it',
        );
    }

    public function testAMissObservedBeforeAConcurrentBatchCreateIsNotServedAfterIt(): void
    {
        $adapter = new InterceptingMemory();
        $database = $this->createDatabase($adapter);

        $adapter->interceptNextGetDocument('webhooks', 'hook', function () use ($database): void {
            $database->createDocuments('webhooks', [$this->hook()]);
        });

        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty(), 'The read observed the row before it was created');
        $this->assertSame(
            'created',
            $database->getDocument('webhooks', 'hook')->getAttribute('name'),
            'A miss observed before a concurrent batch create must not be served after it',
        );
    }

    public function testAMissWithNoConcurrentWriteIsServedFromTheCache(): void
    {
        $adapter = new InterceptingMemory();
        $database = $this->createDatabase($adapter);

        $this->assertTrue($database->getDocument('webhooks', 'absent')->isEmpty());
        $adapter->reset();

        $this->assertTrue($database->getDocument('webhooks', 'absent')->isEmpty());
        $this->assertSame(0, $adapter->documentReads, 'Without a concurrent write the miss must be served from the negative cache');
    }

    private function hook(): Document
    {
        return new Document([
            '$id' => 'hook',
            'name' => 'created',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]);
    }

    private function createDatabase(InterceptingMemory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new LeasedMemoryCache()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('negative_cache_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        return $database;
    }
}

/**
 * Runs a one-shot callback in the middle of a single getDocument(), after the
 * adapter has decided what the row looks like, so a test can land a concurrent
 * write between the observation and whatever the caller does with it.
 */
final class InterceptingMemory extends CountingMemory
{
    private ?Closure $callback = null;

    private string $collection = '';

    private string $document = '';

    public function interceptNextGetDocument(string $collection, string $id, Closure $callback): void
    {
        $this->collection = $collection;
        $this->document = $id;
        $this->callback = $callback;
    }

    #[Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $document = parent::getDocument($collection, $id, $queries, $forUpdate);

        if (
            $this->callback !== null
            && $collection->getId() === $this->collection
            && $id === $this->document
        ) {
            $callback = $this->callback;
            $this->callback = null;
            $callback();
        }

        return $document;
    }
}

/**
 * A memory cache with the generations of the Leasable contract, so a save that
 * raced a purge is refused as it is on Redis.
 */
final class LeasedMemoryCache extends MemoryCache implements Leasable
{
    /** @var array<string, int> */
    private array $generations = [];

    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    /**
     * @param  array<int|string, mixed>|string  $data
     * @return bool|string|array<int|string, mixed>
     */
    #[Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    #[Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;

        return parent::purge($key, $hash);
    }

    #[Override]
    public function flush(): bool
    {
        $this->generations = [];

        return parent::flush();
    }
}
