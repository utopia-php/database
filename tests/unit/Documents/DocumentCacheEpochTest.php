<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

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

    public function testCreateDocumentDoesNotFailWhenEpochPurgeReturnsFalse(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->purgeCachedCollection('webhooks'));
        $adapter->failPurges();

        $document = $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $this->assertSame('hook', $document->getId());
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
            $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']));
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
            $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']));
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
                $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']));
                $database->updateDocument('logs', 'log', new Document(['name' => 'updated']));
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
        $updated = $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']));

        $this->assertSame('updated', $updated->getAttribute('name'));
        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
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
            $database->updateDocument('webhooks', 'hook', new Document(['name' => 'updated']));
            $this->fail('Document cache activation purge failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('finish document cache invalidation', $error->getMessage());
        }

        $this->assertSame('updated', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
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

    private function createDatabaseWithCache(MemoryCache $cache): Database
    {
        $database = new Database(new DatabaseMemory(), new Cache($cache));
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

    public function failBlocks(): void
    {
        $this->failingBlocks = true;
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

    public function flushDuringActivation(): void
    {
        $this->flushDuringActivation = true;
    }

    public function failDuringActivation(): void
    {
        $this->failDuringActivation = true;
    }

    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
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
