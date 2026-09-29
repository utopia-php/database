<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

final class DocumentCacheEpochTest extends TestCase
{
    public function testOuterTransactionCannotPublishAStalePointCacheEntryBeforeCommit(): void
    {
        [$writer, $reader, $adapter, $path] = $this->createSharedSQLiteDatabases();

        try {
            $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));

            $duringCommit = null;
            $adapter->pauseNextCommit(function () use ($reader, &$duringCommit): void {
                $duringCommit = $reader->getDocument('users', 'user')->getAttribute('name');
            });

            $writer->withTransaction(function () use ($writer): void {
                $writer->updateDocument('users', 'user', new Document(['name' => 'updated']));
            });

            $this->assertSame('original', $duringCommit);
            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'));
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testPurgeCachedCollectionDoesNotThrowWhenEpochPurgeReturnsFalse(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->purgeCachedCollection('webhooks'));

        [$collectionKey] = $database->getCacheKeys('webhooks');
        $epochKey = $collectionKey.'#epoch';
        $before = $database->getCache()->load($epochKey, Database::TTL);
        $this->assertIsString($before);
        $this->assertNotSame('', $before);

        $adapter->failPurges();

        $this->assertTrue($database->purgeCachedCollection('webhooks'));
        $this->assertTrue($database->purgeCachedDocument('webhooks', 'hook'));

        $after = $database->getCache()->load($epochKey, Database::TTL);
        $this->assertIsString($after);
        $this->assertNotSame($before, $after);
    }

    public function testCreateDocumentsDoesNotFailWhenEpochPurgeReturnsFalse(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->purgeCachedCollection('webhooks'));
        $adapter->failPurges();

        $created = $database->createDocuments('webhooks', [new Document([
            '$id' => 'hook',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ])]);

        $this->assertSame(1, $created);
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getId());
    }

    public function testBlockFailureRollsBackTheMutation(): void
    {
        $cache = new FailDocumentEpochMemory();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $cache->failBlocks();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $this->fail('Document cache block failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('block document cache epoch', $error->getMessage());
        }

        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testActivationFailureAfterCommitLeavesTheEpochBlocked(): void
    {
        $cache = new FailDocumentEpochMemory();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $cache->failActivations();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $this->fail('Document cache activation failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('activate document cache epoch', $error->getMessage());
        }

        [$collectionKey] = $database->getCacheKeys('webhooks', 'hook');
        $epoch = $database->getCache()->load($collectionKey.'#epoch', Database::TTL);
        $this->assertIsString($epoch);
        $this->assertStringStartsWith('blocked:', $epoch);
        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testActivationFailureDoesNotStrandOtherCollectionEpochs(): void
    {
        $cache = new FailDocumentEpochMemory();
        $database = $this->createDatabaseWithCache($cache);
        $database->createCollection(new Collection(id: 'logs', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $database->createDocument('logs', new Document([
            '$id' => 'log',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $this->assertSame('original', $database->getDocument('logs', 'log')->getAttribute('name'));
        $cache->failActivations('collection:webhooks#epoch');

        try {
            $database->withTransaction(function () use ($database): void {
                $this->renameDocument($database, 'webhooks', 'hook', 'updated');
                $this->renameDocument($database, 'logs', 'log', 'updated');
            });
            $this->fail('Document cache activation failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('activate document cache epoch', $error->getMessage());
        }

        [$webhooksKey] = $database->getCacheKeys('webhooks', 'hook');
        [$logsKey] = $database->getCacheKeys('logs', 'log');
        $webhooksEpoch = $database->getCache()->load($webhooksKey.'#epoch', Database::TTL);
        $logsEpoch = $database->getCache()->load($logsKey.'#epoch', Database::TTL);
        $this->assertIsString($webhooksEpoch);
        $this->assertStringStartsWith('blocked:', $webhooksEpoch);
        $this->assertIsString($logsEpoch);
        $this->assertStringNotContainsString('blocked:', $logsEpoch);
        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $this->assertSame('updated', $database->getDocument('logs', 'log')->getAttribute('name'));
    }

    public function testCacheFlushDuringTransactionCannotPreserveAStalePointCacheEntry(): void
    {
        [$writer, $reader, $adapter, $path] = $this->createSharedSQLiteDatabases();

        try {
            $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));

            $duringCommit = null;
            $adapter->pauseNextCommit(function () use ($reader, &$duringCommit): void {
                $this->assertTrue($reader->getCache()->flush());
                $duringCommit = $reader->getDocument('users', 'user')->getAttribute('name');
            });

            $writer->withTransaction(function () use ($writer): void {
                $writer->updateDocument('users', 'user', new Document(['name' => 'updated']));
            });

            $this->assertSame('original', $duringCommit);
            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'));
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testCacheFlushDuringActivationDoesNotFailTheCommittedMutation(): void
    {
        $cache = new FlushDuringActivationMemory();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $this->assertTrue($cache->flush());
        $cache->flushDuringActivation();
        $this->assertSame(1, $this->renameDocument($database, 'webhooks', 'hook', 'updated'));

        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testCacheFlushAfterActivationReadsGenerationsDoesNotFailTheCommittedMutation(): void
    {
        $cache = new FlushDuringActivationMemory();
        $database = $this->createDatabaseWithCache($cache);
        $this->assertTrue($cache->flush());
        $this->assertTrue($database->purgeCachedCollection('webhooks'));
        $cache->flushAfterReading('collection:webhooks#finished');

        $this->assertTrue($database->deleteCollection('webhooks'));
        $this->assertTrue($database->getCollection('webhooks')->isEmpty());
    }

    public function testActivationPurgeFailureStillPropagates(): void
    {
        $cache = new FlushDuringActivationMemory();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertTrue($cache->flush());
        $cache->failDuringActivation();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $this->fail('Document cache activation purge failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('finish document cache invalidation', $error->getMessage());
        }

        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testWritesDoNotAddKeysToACacheThatKeepsPurgedKeys(): void
    {
        $cache = new RedisLeasableCache();
        $database = $this->createDatabaseWithCache($cache);
        $database->setQueryCache(new QueryCache(new Cache($cache)));
        for ($index = 0; $index <= 20; $index++) {
            $database->createDocument('webhooks', new Document([
                '$id' => 'hook'.$index,
                'name' => 'hook '.$index,
            ]));
        }
        $readEveryHook = function () use ($database): void {
            for ($index = 0; $index <= 20; $index++) {
                $this->assertFalse($database->getDocument('webhooks', 'hook'.$index)->isEmpty());
            }
        };
        $readEveryHook();

        $keys = 0;
        for ($round = 1; $round <= 3; $round++) {
            $database->withTransaction(function () use ($database, $round): void {
                $database->updateDocument('webhooks', 'hook1', new Document(['name' => 'updated '.$round]));
                $database->updateDocument('webhooks', 'hook2', new Document(['name' => 'updated '.$round]));
            });
            $this->renameDocument($database, 'webhooks', 'hook3', 'updated '.$round);
            $readEveryHook();

            if ($round === 1) {
                $keys = \count($cache->keys());
            }
        }

        $this->assertSame($keys, \count($cache->keys()), 'A purged key stays behind in Redis, so writes and the reads between them must not leave keys of their own');
        $this->assertSame('updated 3', $database->getDocument('webhooks', 'hook1')->getAttribute('name'));
    }

    public function testOverlappingWritesSucceedOnACacheWithoutFields(): void
    {
        $cache = new FailDocumentEpochMemory();
        $writer = $this->createDatabaseWithCache($cache);
        $other = $this->createDatabaseWithCache($cache, $writer->getNamespace());
        foreach ([$writer, $other] as $database) {
            $database->createDocument('webhooks', new Document([
                '$id' => 'hook',
                'name' => 'original',
            ]));
        }

        $writer->withTransaction(function () use ($writer, $other, $cache): void {
            $this->renameDocument($writer, 'webhooks', 'hook', 'updated');
            $cache->failBlocks();
            try {
                $this->renameDocument($other, 'webhooks', 'hook', 'updated');
                $this->fail('The other writer\'s block did not fail');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('block document cache epoch', $error->getMessage());
            } finally {
                $cache->failBlocks(false);
            }
        });

        $this->assertSame('updated', $writer->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testActivationRejectsACorruptedOwnerRegistration(): void
    {
        $cache = new RedisLeasableCache();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $cache->corruptFieldWrites();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $this->fail('A corrupted document cache owner registration was accepted');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Invalid document cache owner', $error->getMessage());
        }

        [$collectionKey] = $database->getCacheKeys('webhooks', 'hook');
        $this->assertDocumentCacheEpochBlocked($database, $collectionKey);
        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    public function testActivationPropagatesAnOwnerReleaseFailure(): void
    {
        $cache = new RedisLeasableCache();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $cache->failFieldPurges();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $this->fail('A document cache owner release failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Failed to release document cache owner', $error->getMessage());
        }

        [$collectionKey] = $database->getCacheKeys('webhooks', 'hook');
        $this->assertDocumentCacheEpochBlocked($database, $collectionKey);
        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
    }

    private function renameDocument(Database $database, string $collection, string $id, string $name): int
    {
        return $database->updateDocuments($collection, new Document(['name' => $name]), [Query::equal('$id', [$id])]);
    }

    private function assertDocumentCacheEpochBlocked(Database $database, string $collectionKey): void
    {
        $epoch = $database->getCache()->load($collectionKey.'#epoch', Database::TTL);
        $this->assertIsString($epoch);
        $this->assertStringStartsWith('blocked:', $epoch);
    }

    /**
     * @return array{Database, FailPurgeMemory}
     */
    private function createDatabase(): array
    {
        $adapter = new FailPurgeMemory();
        $database = $this->createDatabaseWithCache($adapter);

        return [$database, $adapter];
    }

    private function createDatabaseWithCache(CacheAdapter $cache, ?string $namespace = null): Database
    {
        $database = new Database(new DatabaseMemory(), new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace($namespace ?? 'epoch_'.\uniqid());
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
     * @return array{Database, Database, PausedDocumentSQLite, string}
     */
    private function createSharedSQLiteDatabases(): array
    {
        $path = \tempnam(\sys_get_temp_dir(), 'document-cache-epoch-');
        if ($path === false) {
            throw new \RuntimeException('Failed to create SQLite test database');
        }

        $attributes = SQLite::getPDOAttributes();
        $attributes[\PDO::ATTR_PERSISTENT] = false;
        $writerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
        $readerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
        $writerConnection->exec('PRAGMA journal_mode = WAL');
        $writerConnection->exec('PRAGMA busy_timeout = 1000');
        $readerConnection->exec('PRAGMA busy_timeout = 1000');

        $adapter = new PausedDocumentSQLite($writerConnection);
        $cache = new MemoryCache();
        $writer = new Database($adapter, new Cache($cache));
        $reader = new Database(new SQLite($readerConnection), new Cache($cache));
        $namespace = 'shared_epoch_'.\uniqid();
        foreach ([$writer, $reader] as $database) {
            $database
                ->setDatabase('utopiaTests')
                ->setNamespace($namespace);
            $database->getAuthorization()->addRole(Role::any()->toString());
        }

        $writer->create();
        $writer->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $writer->createDocument('users', new Document([
            '$id' => 'user',
            'name' => 'original',
        ]));

        return [$writer, $reader, $adapter, $path];
    }

    private function removeSQLiteFiles(string $path): void
    {
        foreach ([$path, $path.'-wal', $path.'-shm'] as $file) {
            if (\is_file($file)) {
                \unlink($file);
            }
        }
    }

    public function testALostActivationDoesNotKeepTheCollectionUncached(): void
    {
        $cache = new FailDocumentEpochMemory();
        $adapter = new CountingMemory();
        $database = $this->createCountedDatabase($adapter, $cache);
        $cache->failActivations();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $this->fail('The activation did not fail');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('activate document cache epoch', $error->getMessage());
        }

        $adapter->reset();
        for ($read = 0; $read < 3; $read++) {
            $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        }
        $this->assertSame(3, $adapter->documentReads, 'A collection whose write has not activated stays uncached while the write is younger than the writer timeout');

        $database->setCacheWriterTimeout(0);
        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $adapter->reset();
        for ($read = 0; $read < 3; $read++) {
            $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        }
        $this->assertSame(0, $adapter->documentReads, 'Past the writer timeout the lost activation lapses and reads are served from the cache again, without a flush');
    }

    public function testAWriteAfterAnAbandonedWriteReenablesTheCollection(): void
    {
        $cache = new AbandoningCache();
        $adapter = new CountingMemory();
        $database = $this->createCountedDatabase($adapter, $cache);
        $cache->abandonNextWrite();

        try {
            $this->renameDocument($database, 'webhooks', 'hook', 'abandoned');
            $this->fail('The abandoned write released its registration');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('release document cache owner', $error->getMessage());
        }

        $this->renameDocument($database, 'webhooks', 'hook', 'second');
        $adapter->reset();
        $this->assertSame('second', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $this->assertSame('second', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $this->assertSame(2, $adapter->documentReads, 'A write younger than the writer timeout counts as in flight, so the next write leaves the collection blocked');

        $database->setCacheWriterTimeout(0);
        $this->renameDocument($database, 'webhooks', 'hook', 'third');
        $database->setCacheWriterTimeout(3600);

        $this->assertSame('third', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $adapter->reset();
        $this->assertSame('third', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $this->assertSame(0, $adapter->documentReads, 'The first write after the abandoned one passed the writer timeout releases it and re-enables the collection');
    }

    public function testAWriterYoungerThanTheTimeoutKeepsItsBlockUntilItsActivation(): void
    {
        $cache = new FailDocumentEpochMemory();
        [$writer, $reader, $path] = $this->createSQLiteDatabasesSharing($cache);

        try {
            $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));

            try {
                $writer->withTransaction(function () use ($writer, $reader, $cache): void {
                    $this->renameDocument($writer, 'users', 'user', 'updated');
                    $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
                    $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
                    $cache->failActivations();
                });
                $this->fail('The activation did not fail');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('activate document cache epoch', $error->getMessage());
            }

            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'), 'A reader must not cache what it read while a write younger than the writer timeout was in flight');
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testAWriterPastTheTimeoutRetiresWhatReadersFilledWhileItRan(): void
    {
        [$writer, $reader, $path] = $this->createSQLiteDatabasesSharing(new MemoryCache());
        $writer->setCacheWriterTimeout(0);
        $reader->setCacheWriterTimeout(0);

        try {
            $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));

            $writer->withTransaction(function () use ($writer, $reader): void {
                $this->renameDocument($writer, 'users', 'user', 'updated');
                $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
                $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
            });

            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'), 'What readers filled after the block lapsed must be retired when the write activates');
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testAWriteFinishingWhileAnotherIsInFlightLeavesTheCollectionBlocked(): void
    {
        $cache = new AbandoningCache();
        [$writer, $reader, $path] = $this->createSQLiteDatabasesSharing($cache);

        try {
            $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));

            try {
                $writer->withTransaction(function () use ($writer, $reader, $cache): void {
                    $this->renameDocument($writer, 'users', 'user', 'updated');
                    $this->assertTrue($reader->purgeCachedCollection('users'));
                    $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
                    $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
                    $cache->abandonNextWrite();
                });
                $this->fail('The abandoned write released its registration');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('release document cache owner', $error->getMessage());
            }

            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'), 'A write that finishes while another is in flight must not re-enable the collection');
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testAWriterReleasedAsAbandonedStillRetiresWhatReadersFilledWhileItRan(): void
    {
        [$writer, $reader, $path] = $this->createSQLiteDatabasesSharing(new RedisLeasableCache());
        $reader->setCacheWriterTimeout(0);

        try {
            $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));

            $writer->withTransaction(function () use ($writer, $reader): void {
                $this->renameDocument($writer, 'users', 'user', 'updated');
                $this->assertTrue($reader->purgeCachedCollection('users'));
                $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
                $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
            });

            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'), 'A write another writer released as abandoned must still retire, when it activates, what readers filled while it ran');
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testATransactionReadsItsBatchWriteAfterAnotherReaderRefilledTheDefinition(): void
    {
        $cache = new MemoryCache();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $definitionKey = \strtolower($database->getCacheBaseKeys(Database::METADATA, 'webhooks')[1]);
        $definition = $cache->load($definitionKey, Database::TTL);
        $this->assertIsArray($definition);

        $read = $database->withTransaction(function () use ($database, $cache, $definitionKey, $definition): mixed {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
            $cache->save($definitionKey, $definition);

            return $database->getDocument('webhooks', 'hook')->getAttribute('name');
        });

        $this->assertSame('updated', $read, 'A transaction must read what its batch write changed even when another reader saved the definition as it was before the write');
    }

    public function testADefinitionFilledAcrossAWriteOnACacheWithoutGenerationsIsDropped(): void
    {
        $cache = new InterleavingMemory();
        $database = $this->createDatabaseWithCache($cache);
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $database->purgeCachedDocument(Database::METADATA, 'webhooks');
        $cache->beforeNextSave(':_metadata:webhooks', function () use ($database): void {
            $this->renameDocument($database, 'webhooks', 'hook', 'updated');
        });
        $database->getCollection('webhooks');

        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'), 'A definition saved after a write retired its epoch must not keep serving that epoch');
    }

    private function createCountedDatabase(CountingMemory $adapter, CacheAdapter $cache): Database
    {
        $database = new Database($adapter, new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('epoch_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            'name' => 'original',
        ]));
        $this->assertSame('original', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        return $database;
    }

    /**
     * @return array{Database, Database, string}
     */
    private function createSQLiteDatabasesSharing(CacheAdapter $cache): array
    {
        $path = \tempnam(\sys_get_temp_dir(), 'document-cache-lapse-');
        if ($path === false) {
            throw new \RuntimeException('Failed to create SQLite test database');
        }

        $attributes = SQLite::getPDOAttributes();
        $attributes[\PDO::ATTR_PERSISTENT] = false;
        $writerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
        $readerConnection = new \PDO('sqlite:'.$path, null, null, $attributes);
        $writerConnection->exec('PRAGMA journal_mode = WAL');
        $writerConnection->exec('PRAGMA busy_timeout = 1000');
        $readerConnection->exec('PRAGMA busy_timeout = 1000');

        $writer = new Database(new SQLite($writerConnection), new Cache($cache));
        $reader = new Database(new SQLite($readerConnection), new Cache($cache));
        $namespace = 'lapse_'.\uniqid();
        foreach ([$writer, $reader] as $database) {
            $database
                ->setDatabase('utopiaTests')
                ->setNamespace($namespace);
            $database->getAuthorization()->addRole(Role::any()->toString());
        }

        $writer->create();
        $writer->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        $writer->createDocument('users', new Document([
            '$id' => 'user',
            'name' => 'original',
        ]));

        return [$writer, $reader, $path];
    }
}

final class PausedDocumentSQLite extends SQLite
{
    private ?Closure $commitCallback = null;

    public function pauseNextCommit(Closure $callback): void
    {
        $this->commitCallback = $callback;
    }

    #[\Override]
    public function commitTransaction(): bool
    {
        if ($this->inTransaction === 1) {
            $callback = $this->commitCallback;
            $this->commitCallback = null;
            $callback?->__invoke();
        }

        return parent::commitTransaction();
    }
}

final class FailPurgeMemory extends MemoryCache
{
    private bool $failing = false;

    public function failPurges(): void
    {
        $this->failing = true;
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->failing && \str_ends_with($key, '#epoch')) {
            return false;
        }

        return parent::purge($key, $hash);
    }
}

final class FailDocumentEpochMemory extends MemoryCache
{
    private bool $failingBlocks = false;

    private ?string $activationFailure = null;

    public function failBlocks(bool $failing = true): void
    {
        $this->failingBlocks = $failing;
    }

    public function failActivations(?string $key = null): void
    {
        $this->activationFailure = $key ?? '#epoch';
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if (\str_ends_with($key, '#epoch') && \is_string($data)) {
            if ($this->failingBlocks && \str_starts_with($data, 'blocked:')) {
                return false;
            }
            if (
                $this->activationFailure !== null
                && \str_contains($key, $this->activationFailure)
                && ! \str_starts_with($data, 'blocked:')
            ) {
                return false;
            }
        }

        return parent::save($key, $data, $hash);
    }
}

final class FlushDuringActivationMemory extends MemoryCache implements Leasable
{
    /** @var array<string, int> */
    private array $generations = [];

    private bool $flushDuringActivation = false;

    private bool $failDuringActivation = false;

    private ?string $flushAfterReading = null;

    public function flushDuringActivation(): void
    {
        $this->flushDuringActivation = true;
    }

    public function failDuringActivation(): void
    {
        $this->failDuringActivation = true;
    }

    public function flushAfterReading(string $key): void
    {
        $this->flushAfterReading = $key;
    }

    public function getGeneration(string $key): string
    {
        $generation = (string) ($this->generations[$key] ?? 0);
        if ($this->flushAfterReading !== null && \str_ends_with($key, $this->flushAfterReading)) {
            $this->flushAfterReading = null;
            $this->flush();
        }

        return $generation;
    }

    #[\Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->flushDuringActivation && \str_ends_with($key, '#finished')) {
            $this->flushDuringActivation = false;

            return $this->flush();
        }
        if ($this->failDuringActivation && \str_ends_with($key, '#finished')) {
            return false;
        }

        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;
        parent::purge($key, $hash);

        return true;
    }

    #[\Override]
    public function flush(): bool
    {
        $this->generations = [];

        return parent::flush();
    }
}

/**
 * A Redis-like cache that can abandon a write the way a killed worker does: the write stays registered and its
 * collection stays blocked, because the release of its registration is refused and its activation stops there.
 */
final class AbandoningCache implements CacheAdapter, Leasable
{
    private RedisLeasableCache $cache;

    private bool $abandoning = false;

    public function __construct()
    {
        $this->cache = new RedisLeasableCache();
    }

    public function abandonNextWrite(): void
    {
        $this->abandoning = true;
    }

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        return $this->cache->load($key, $ttl, $hash);
    }

    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        return $this->cache->save($key, $data, $hash);
    }

    public function touch(string $key, string $hash = ''): bool
    {
        return $this->cache->touch($key, $hash);
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        return $this->cache->list($key);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->abandoning && $hash !== '' && \str_ends_with($key, '#owners')) {
            $this->abandoning = false;

            return false;
        }

        return $this->cache->purge($key, $hash);
    }

    public function flush(): bool
    {
        return $this->cache->flush();
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return $this->cache->getSize();
    }

    public function getName(?string $key = null): string
    {
        return 'abandoning';
    }

    public function getGeneration(string $key): string
    {
        return $this->cache->getGeneration($key);
    }

    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        return $this->cache->saveWithLease($key, $data, $hash, $generation);
    }
}

/**
 * A cache without generations that runs a callback just before one save lands, as when a write commits between a
 * reader's database read and its fill.
 */
final class InterleavingMemory extends MemoryCache
{
    private ?string $fragment = null;

    private ?Closure $callback = null;

    public function beforeNextSave(string $fragment, Closure $callback): void
    {
        $this->fragment = $fragment;
        $this->callback = $callback;
    }

    #[\Override]
    public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        $callback = $this->callback;
        if ($callback !== null && $this->fragment !== null && \str_contains($key, $this->fragment)) {
            $this->callback = null;
            $callback();
        }

        return parent::save($key, $data, $hash);
    }
}
