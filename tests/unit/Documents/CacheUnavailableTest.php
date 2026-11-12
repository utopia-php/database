<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\UnavailableCache;
use Tests\Unit\Support\UncachedTwin;
use Throwable;
use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

final class CacheUnavailableTest extends TestCase
{
    private const string OPEN = 'open';

    private const string FAILING_WRITES = 'failing writes';

    /** @return array<string, array{string, Closure(Database): mixed}> */
    public static function writes(): array
    {
        $writes = [
            'update' => static fn (Database $database): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated'])),
            'update in a transaction' => static fn (Database $database): mixed => $database->withTransaction(
                static fn (): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']))
            ),
            'delete' => static fn (Database $database): bool => $database->deleteDocument('webhooks', 'hook'),
            'batch update' => static fn (Database $database): int => $database->updateDocuments(
                'webhooks',
                new Document(['name' => 'updated']),
                [Query::equal('$id', ['hook'])],
            ),
        ];

        $cases = [];
        foreach ([self::OPEN, self::FAILING_WRITES] as $outage) {
            foreach ($writes as $name => $write) {
                $cases["{$name}, {$outage}"] = [$outage, $write];
            }
        }

        return $cases;
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testAWriteTheCacheCannotInvalidateIsRolledBack(string $outage, Closure $write): void
    {
        $cache = new UnavailableCache();
        $database = $this->createDatabase($cache);
        $outage === self::OPEN ? $cache->open() : $cache->failWrites();

        $failure = $this->attempt(fn (): mixed => $write($database));
        $cache->recover();

        $this->assertInstanceOf(Unavailable::class, $failure, 'A write whose cache invalidation could not be confirmed must fail as unavailable, so a caller can tell it was refused');

        $this->assertSame('original', $this->stored($database), 'The write must be rolled back');
        $this->assertSame('original', $this->cached($database));
    }

    /**
     * One Redis timeout used to refuse the write outright, surfacing to the client as a 500 for a
     * cache that answered the very next call. A blip shorter than the retries must not cost the write.
     *
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('blips')]
    public function testAWriteRidesOutAPurgeBlip(Closure $write): void
    {
        $cache = new UnavailableCache();
        $database = $this->createDatabase($cache);
        $cache->failNextPurges(2);

        $write($database);

        $this->assertSame('updated', $this->stored($database));
        $this->assertSame('updated', $this->cached($database), 'The document must not be served from the cache as it was before the write');
    }

    /** @return array<string, array{Closure(Database): mixed}> */
    public static function blips(): array
    {
        return [
            'update' => [static fn (Database $database): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']))],
            'update in a transaction' => [static fn (Database $database): mixed => $database->withTransaction(
                static fn (): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']))
            )],
            'batch update' => [static fn (Database $database): int => $database->updateDocuments(
                'webhooks',
                new Document(['name' => 'updated']),
                [Query::equal('$id', ['hook'])],
            )],
        ];
    }

    /**
     * A purge that outlasts its retries used to refuse the write. Blocking the collection's epoch before the commit
     * keeps the document cached before the write from being served, so the write commits instead.
     *
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('singleWrites')]
    public function testAWritePastThePurgeRetriesCommitsBehindATombstone(Closure $write): void
    {
        $cache = new UnavailableCache();
        $database = $this->createDatabase($cache);
        $cache->failNextPurges(3);

        $failure = $this->attempt(fn (): mixed => $write($database));

        $this->assertNull($failure, 'A write the cache can still block must not be refused');
        $this->assertSame('updated', $this->stored($database));
        $this->assertSame('updated', $this->cached($database), 'The document must not be served from the cache as it was before the write');
    }

    /** @return array<string, array{Closure(Database): mixed}> */
    public static function singleWrites(): array
    {
        return \array_diff_key(self::blips(), ['batch update' => true]);
    }

    public function testAPurgeFailingAfterTheCommitRetiresTheCollectionInstead(): void
    {
        $cache = new UnavailableCache();
        $adapter = new CommitHookMemory();
        $database = $this->createDatabase($cache, $adapter);
        $adapter->afterCommit = $cache->failWrites(...);

        $failure = $this->attempt(fn (): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated'])));
        $cache->recover();

        $this->assertNull($failure, 'A stored write must not be reported as failed because its cache invalidation failed');
        $this->assertSame('updated', $this->stored($database));
        $this->assertSame('updated', $this->cached($database), 'A committed write whose purge failed must not leave the old document cached');
    }

    /**
     * Publishing the epoch after a batch write commits can miss a Redis timeout. The write is stored and its
     * tombstone stays in place, so it must report success while readers keep bypassing the cache.
     */
    public function testACommittedWriteWhoseEpochCannotBePublishedStaysFailClosed(): void
    {
        $cache = new UnavailableCache();
        $adapter = new CommitHookMemory();
        $database = $this->createDatabase($cache, $adapter);
        $adapter->afterCommit = fn () => $cache->failNextSaves(3);

        $failure = $this->attempt(fn (): int => $database->updateDocuments(
            'webhooks',
            new Document(['name' => 'updated']),
            [Query::equal('$id', ['hook'])],
        ));

        $this->assertNull($failure, 'A stored write must not be reported as failed because its epoch could not be published');
        $this->assertSame('updated', $this->stored($database));
        $this->assertSame('updated', $this->cached($database));
        $this->assertSame('updated', $this->cached($database), 'A refill while the tombstone stands must not cache the old document');
    }

    public function testAnEpochPublishBlipAfterTheCommitIsRetried(): void
    {
        $cache = new UnavailableCache();
        $adapter = new CommitHookMemory();
        $database = $this->createDatabase($cache, $adapter);
        $adapter->afterCommit = fn () => $cache->failNextSaves(2);

        $database->updateDocuments('webhooks', new Document(['name' => 'updated']), [Query::equal('$id', ['hook'])]);

        $collectionKey = $database->getCacheBaseKeys('webhooks')[0];
        $this->assertNotNull((new \Utopia\Database\Cache\Mutation($database->getCache()))->read($collectionKey, 3600)->value);
        $this->assertSame('updated', $this->cached($database));
    }

    public function testPurgeCachedCollectionReportsACacheThatCannotConfirmIt(): void
    {
        $cache = new UnavailableCache();
        $database = $this->createDatabase($cache);
        $cache->open();

        $this->expectException(\Utopia\Cache\Exception\Unavailable::class);

        $database->purgeCachedCollection('webhooks');
    }

    public function testASchemaChangeRetiresCachedDocumentsBeforeItCommits(): void
    {
        $cache = new UnavailableCache();
        $adapter = new CommitHookMemory();
        $database = $this->createDatabase($cache, $adapter);
        $adapter->afterCommit = $cache->open(...);

        $this->attempt(fn () => $database->renameAttribute('webhooks', 'name', 'title'));
        $cache->recover();

        $this->assertSame(['title'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $database->getCollection('webhooks')->attributes()));
        $document = $database->getDocument('webhooks', 'hook');
        $this->assertSame('original', $document->getAttribute('title'), 'A document cached before a schema change must not be served once the cache recovers');
        $this->assertNull($document->getAttribute('name'));
    }

    public function testASchemaChangeTheCacheCannotInvalidateIsRolledBack(): void
    {
        $cache = new UnavailableCache();
        $database = $this->createDatabase($cache);
        $cache->open();

        $failure = $this->attempt(fn () => $database->renameAttribute('webhooks', 'name', 'title'));
        $cache->recover();

        $this->assertInstanceOf(Throwable::class, $failure, 'A schema change whose cache invalidation could not be confirmed must fail');

        $this->assertSame(['name'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $database->getCollection('webhooks')->attributes()));
        $this->assertSame('original', $this->cached($database));
    }

    /**
     * @param  Closure(): mixed  $write
     */
    private function attempt(Closure $write): ?Throwable
    {
        try {
            $write();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }

    private function stored(Database $database): mixed
    {
        return UncachedTwin::of($database)->getDocument('webhooks', 'hook')->getAttribute('name');
    }

    private function cached(Database $database): mixed
    {
        return $database->getDocument('webhooks', 'hook')->getAttribute('name');
    }

    private function createDatabase(UnavailableCache $cache, DatabaseMemory $adapter = new DatabaseMemory()): Database
    {
        $database = new Database($adapter, new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('unavailable_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createDocument('webhooks', new Document(['$id' => 'hook', 'name' => 'original']));
        $this->assertSame('original', $this->cached($database));
        $this->assertSame('original', $this->cached($database));

        return $database;
    }
}

final class CommitHookMemory extends DatabaseMemory
{
    public ?Closure $afterCommit = null;

    #[\Override]
    public function commitTransaction(): bool
    {
        $committed = parent::commitTransaction();
        if ($committed && ! $this->inTransaction() && $this->afterCommit !== null) {
            $afterCommit = $this->afterCommit;
            $this->afterCommit = null;
            $afterCommit();
        }

        return $committed;
    }
}
