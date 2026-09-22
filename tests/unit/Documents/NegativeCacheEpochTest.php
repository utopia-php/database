<?php

namespace Tests\Unit\Documents;

use Closure;
use Override;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class NegativeCacheEpochTest extends TestCase
{
    private const EMPTY_MARKER = '$empty';

    public function testAMissObservedBeforeAConcurrentWriteIsNotNegativeCached(): void
    {
        $cache = new LeasedMemoryCache();
        $adapter = new InterceptingMemory();
        $database = $this->createDatabase($adapter, $cache);

        // The read observes the row's absence, then a concurrent writer commits
        // it and rotates the collection's cache epoch before the reader gets to
        // write its marker. saveWithLease leases the document key's generation,
        // which a rotation never touches, so nothing else stops the write.
        $adapter->interceptNextGetDocument('webhooks', 'hook', function () use ($database): void {
            $database->createDocument('webhooks', new Document([
                '$id' => 'hook',
                'name' => 'created',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
            ]));
        });

        $before = $this->markerKeys($cache);
        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty());

        $this->assertSame(
            [],
            $this->markersAddedSince($cache, $before),
            'A miss observed before the epoch rotated must not be negative cached',
        );

        $this->assertSame(
            'created',
            $database->getDocument('webhooks', 'hook')->getAttribute('name'),
            'The document written during the read must be served afterwards',
        );
    }

    public function testAMissWithNoConcurrentWriteIsStillNegativeCached(): void
    {
        $cache = new LeasedMemoryCache();
        $database = $this->createDatabase(new InterceptingMemory(), $cache);

        $before = $this->markerKeys($cache);
        $this->assertTrue($database->getDocument('webhooks', 'absent')->isEmpty());

        $this->assertCount(
            1,
            $this->markersAddedSince($cache, $before),
            'Without a rotation the negative cache must still be written',
        );

        $this->assertTrue($database->getDocument('webhooks', 'absent')->isEmpty());
    }

    private function createDatabase(DatabaseMemory $adapter, MemoryCache $cache): Database
    {
        $database = new Database($adapter, new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('negative_cache_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(new Collection(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        return $database;
    }

    /**
     * @param  list<string>  $before
     * @return list<string>
     */
    private function markersAddedSince(MemoryCache $cache, array $before): array
    {
        return \array_values(\array_diff($this->markerKeys($cache), $before));
    }

    /**
     * @return list<string>
     */
    private function markerKeys(MemoryCache $cache): array
    {
        $keys = [];
        foreach ($cache->store as $key => $entry) {
            $data = \is_array($entry) ? ($entry['data'] ?? null) : null;
            if (\is_array($data) && isset($data[self::EMPTY_MARKER])) {
                $keys[] = (string) $key;
            }
        }

        return $keys;
    }
}

/**
 * Runs a one-shot callback in the middle of a single getDocument(), after the
 * adapter has decided what the row looks like, so a test can land a concurrent
 * write between the observation and whatever the caller does with it.
 */
final class InterceptingMemory extends DatabaseMemory
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
