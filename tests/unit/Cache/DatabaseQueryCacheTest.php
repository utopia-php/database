<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class DatabaseQueryCacheTest extends TestCase
{
    public function testQueryCacheSeparatesCompleteExecutionShapes(): void
    {
        [$database] = $this->createDatabase();
        $this->createUsers($database);

        $ascending = $database->find('users', [
            Query::orderAsc('$id'),
            Query::limit(1),
        ]);
        $two = $database->find('users', [
            Query::orderAsc('$id'),
            Query::limit(2),
        ]);
        $offset = $database->find('users', [
            Query::orderAsc('$id'),
            Query::limit(1),
            Query::offset(1),
        ]);
        $descending = $database->find('users', [
            Query::orderDesc('$id'),
            Query::limit(1),
        ]);

        $cursorA = $database->getDocument('users', 'a');
        $cursorB = $database->getDocument('users', 'b');
        $afterA = $database->find('users', [
            Query::orderAsc('$id'),
            Query::cursorAfter($cursorA),
            Query::limit(1),
        ]);
        $afterB = $database->find('users', [
            Query::orderAsc('$id'),
            Query::cursorAfter($cursorB),
            Query::limit(1),
        ]);

        $this->assertSame(['a'], $this->ids($ascending));
        $this->assertSame(['a', 'b'], $this->ids($two));
        $this->assertSame(['b'], $this->ids($offset));
        $this->assertSame(['c'], $this->ids($descending));
        $this->assertSame(['b'], $this->ids($afterA));
        $this->assertSame(['c'], $this->ids($afterB));
    }

    public function testRandomOrderAlwaysBypassesQueryCache(): void
    {
        $adapter = new ObservedMemory();
        [$database] = $this->createDatabase($adapter);
        $this->createUsers($database);

        $adapter->observeFinds('users');
        $database->find('users', [Query::orderRandom(), Query::limit(1)]);
        $database->find('users', [Query::orderRandom(), Query::limit(1)]);

        $this->assertSame(2, $adapter->getObservedFinds());
    }

    public function testSetQueryCacheInstallsOneInvalidatorAndRemovesIt(): void
    {
        [$database, $queryAdapter] = $this->createDatabase(queryCache: false);
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));

        $queryCache = new QueryCache(new Cache($queryAdapter));
        $database->setQueryCache($queryCache);
        $database->setQueryCache($queryCache);

        $queryAdapter->resetPurges();
        $database->createDocument('users', new Document(['$id' => 'a']));
        $this->assertSame(2, $queryAdapter->getWrites('default:qcache:users#epoch'));

        $database->setQueryCache(null);
        $queryAdapter->resetPurges();
        $database->createDocument('users', new Document(['$id' => 'b']));
        $this->assertSame(0, $queryAdapter->getWrites('default:qcache:users#epoch'));
    }

    public function testSchemaAndCollectionMutationsInvalidateQueries(): void
    {
        [$database, $queryAdapter] = $this->createDatabase();
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));

        $queryAdapter->resetPurges();
        $database->updateCollection('users', $this->permissions(), false);
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->createAttribute('users', Attribute::string(key: 'name'));
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->updateAttribute('users', 'name', size: 128);
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->createIndex('users', Index::key(key: 'name', attributes: ['name']));
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->renameIndex('users', 'name', 'renamed');
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->deleteIndex('users', 'renamed');
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->deleteAttribute('users', 'name');
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));
    }

    public function testDeleteAndRecreateCannotReuseOldCollectionResults(): void
    {
        [$database] = $this->createDatabase();
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('users', new Document(['$id' => 'old']));
        $this->assertSame(['old'], $this->ids($database->find('users')));

        $database->deleteCollection('users');
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('users', new Document(['$id' => 'new']));

        $this->assertSame(['new'], $this->ids($database->find('users')));
    }

    public function testRolledBackTransactionCannotPoisonQueryCache(): void
    {
        [$database] = $this->createDatabase();
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('users', new Document(['$id' => 'committed']));
        $this->assertSame(['committed'], $this->ids($database->find('users')));

        try {
            $database->withTransaction(function () use ($database): void {
                $database->createDocument('users', new Document(['$id' => 'rolled-back']));
                $this->assertSame(
                    ['committed', 'rolled-back'],
                    $this->ids($database->find('users', [Query::orderAsc('$id')])),
                );

                throw new Conflict('rollback');
            });
        } catch (Conflict) {
        }

        $this->assertSame(
            ['committed'],
            $this->ids($database->find('users', [Query::orderAsc('$id')])),
        );
    }

    public function testMetadataReadStartedBeforePurgeCannotPublish(): void
    {
        $adapter = new ObservedMemory();
        [$database] = $this->createDatabase($adapter, queryCache: false, dataAdapter: new None());
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));
        $database->purgeCachedCollection('users');

        $adapter->observeMetadata('users', fn () => $database->purgeCachedCollection('users'));
        $database->getCollection('users');
        $database->getCollection('users');

        $this->assertSame(2, $adapter->getObservedMetadataReads());
    }

    public function testValidatorBuildStartedBeforePurgeCannotPublish(): void
    {
        $adapter = new ObservedMemory();
        [$database] = $this->createDatabase($adapter, queryCache: false);
        $database->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: $this->permissions(), documentSecurity: false));
        $database->getCollection('users');

        $adapter->observeValidators(
            fn () => $database->createAttribute('users', Attribute::integer(key: 'age')),
        );
        $database->find('users', [Query::equal('name', ['first'])]);
        $database->find('users', [Query::equal('name', ['second'])]);

        $this->assertSame(2, $adapter->getObservedValidators());
    }

    public function testMemoryCacheSeparatesRolesAndExecutionShapes(): void
    {
        [$database] = $this->createDatabase(queryCache: false);
        $database->setQueryCache(new QueryCache(new Cache(new MemoryCache())));
        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createCollection(new Collection(id: 'private', permissions: [
                Permission::create(Role::any()),
            ]));

            foreach ([
                ['a', 'user-1'],
                ['b', 'user-2'],
                ['c', 'user-1'],
            ] as [$id, $user]) {
                $database->createDocument('private', new Document([
                    '$id' => $id,
                    '$permissions' => [Permission::read(Role::user($user))],
                ]));
            }
        });

        $authorization = $database->getAuthorization();
        $authorization->addRole(Role::user('user-1')->toString());
        $this->assertSame(['a'], $this->ids($database->find('private', [
            Query::orderAsc('$id'),
            Query::limit(1),
        ])));
        $this->assertSame(['c'], $this->ids($database->find('private', [
            Query::orderDesc('$id'),
            Query::limit(1),
        ])));
        $this->assertSame(['a', 'c'], $this->ids($database->find('private', [
            Query::orderAsc('$id'),
            Query::limit(2),
        ])));

        $authorization->removeRole(Role::user('user-1')->toString());
        $authorization->addRole(Role::user('user-2')->toString());
        $this->assertSame(['b'], $this->ids($database->find('private', [
            Query::orderAsc('$id'),
            Query::limit(2),
        ])));
    }

    public function testMemoryCacheSeparatesPointSelectionVariants(): void
    {
        [$database] = $this->createDatabase(queryCache: false, dataAdapter: new MemoryCache());
        $database->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name'),
            Attribute::string(key: 'email'),
        ], permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('users', new Document([
            '$id' => 'user',
            'name' => 'Alice',
            'email' => 'alice@example.com',
        ]));

        $name = $database->getDocument('users', 'user', [Query::select(['name'])]);
        $email = $database->getDocument('users', 'user', [Query::select(['email'])]);

        $this->assertSame('Alice', $name->getAttribute('name'));
        $this->assertNull($name->getAttribute('email'));
        $this->assertSame('alice@example.com', $email->getAttribute('email'));
        $this->assertNull($email->getAttribute('name'));
    }

    public function testPermissionRevocationIsFreshAcrossDatabaseInstances(): void
    {
        $adapter = new DatabaseMemory();
        $authorization = new Authorization();
        $authorization->cleanRoles();
        $authorization->addRole(Role::user('user-1')->toString());
        $writer = new Database($adapter, new Cache(new None()));
        $reader = new Database($adapter, new Cache(new None()));

        foreach ([$writer, $reader] as $database) {
            $database
                ->setAuthorization($authorization)
                ->setDatabase('cache-tests')
                ->setNamespace('shared_metadata');
        }

        $writer->create();
        $writer->createCollection(new Collection(id: 'users', permissions: [
            Permission::read(Role::user('user-1')),
            Permission::create(Role::user('user-1')),
            Permission::update(Role::user('user-1')),
        ]));
        $reader->getCollection('users');

        $writer->updateCollection('users', [
            Permission::create(Role::user('user-1')),
            Permission::update(Role::user('user-1')),
        ], false);

        $this->expectException(AuthorizationException::class);
        $reader->find('users');
    }

    public function testSilentPermissionRevocationStillInvalidatesQueryCache(): void
    {
        [$database] = $this->createDatabase(queryCache: false);
        $database->setQueryCache(new QueryCache(new Cache(new MemoryCache())));
        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createCollection(new Collection(id: 'private', permissions: [
                Permission::create(Role::any()),
            ]));
            $database->createDocument('private', new Document([
                '$id' => 'secret',
                '$permissions' => [
                    Permission::read(Role::user('user-1')),
                    Permission::update(Role::user('user-1')),
                ],
            ]));
        });

        $database->getAuthorization()->addRole(Role::user('user-1')->toString());
        $this->assertSame(['secret'], $this->ids($database->find('private')));

        $database->getAuthorization()->skip(fn () => $database->silent(
            fn () => $database->updateDocument('private', 'secret', new Document([
                '$permissions' => [Permission::read(Role::user('user-2'))],
            ])),
        ));

        $this->assertSame([], $database->find('private'));
    }

    public function testJoinQueriesAlwaysBypassCacheAfterRelatedMutation(): void
    {
        $adapter = new JoinMemory();
        [$database] = $this->createDatabase($adapter);
        $database->createCollection(new Collection(id: 'parents', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(new Collection(id: 'children', permissions: $this->permissions(), documentSecurity: false));

        $queries = [Query::join('children', '$id', '$id')];
        $database->find('parents', $queries);
        $database->createDocument('children', new Document(['$id' => 'child']));
        $database->find('parents', $queries);

        $this->assertSame(2, $adapter->getJoinFinds());
    }

    public function testMandatoryInvalidationFailureEscapesSilentScope(): void
    {
        $cache = new FailingMemory();
        [$database] = $this->createDatabase(queryCache: false);
        $database->setQueryCache(new QueryCache(new Cache($cache)));
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));
        $database->find('users');
        $cache->seedEpoch('users');
        $cache->failPurges();

        $this->expectException(\RuntimeException::class);
        $database->silent(fn () => $database->createDocument('users', new Document(['$id' => 'user'])));
    }

    public function testCreateRollsBackWhenMandatoryInvalidationFails(): void
    {
        $this->assertMutationRollsBackOnInvalidationFailure(
            static fn (Database $database) => $database->createDocument('users', new Document([
                '$id' => 'created',
                'name' => 'created',
            ])),
        );
    }

    public function testBatchCreateRollsBackWhenMandatoryInvalidationFails(): void
    {
        $this->assertMutationRollsBackOnInvalidationFailure(
            static fn (Database $database) => $database->createDocuments('users', [
                new Document(['$id' => 'first', 'name' => 'first']),
                new Document(['$id' => 'second', 'name' => 'second']),
            ]),
        );
    }

    public function testUpsertRollsBackWhenMandatoryInvalidationFails(): void
    {
        $this->assertMutationRollsBackOnInvalidationFailure(
            static fn (Database $database) => $database->upsertDocument('users', new Document([
                '$id' => 'existing',
                'name' => 'updated',
            ])),
            sqlite: true,
        );
    }

    public function testUpdateRollsBackWhenMandatoryInvalidationFails(): void
    {
        $this->assertMutationRollsBackOnInvalidationFailure(
            static fn (Database $database) => $database->updateDocument(
                'users',
                'existing',
                new Document(['name' => 'updated']),
            ),
        );
    }

    public function testDeleteRollsBackWhenMandatoryInvalidationFails(): void
    {
        $this->assertMutationRollsBackOnInvalidationFailure(
            static fn (Database $database) => $database->deleteDocument('users', 'existing'),
        );
    }

    public function testSharedBlockedEpochPreventsPreCommitStaleFill(): void
    {
        [$writer, $reader, $writerAdapter, $readerAdapter, $cache, $path] = $this->createSharedSQLiteDatabases();

        try {
            $this->assertSame(
                ['existing' => 'original'],
                $this->names($reader->find('users', [Query::orderAsc('$id')])),
            );
            $readerAdapter->observeFinds('users');

            $duringCommit = [];
            $writerAdapter->pauseNextCommit(function () use ($reader, $cache, &$duringCommit): void {
                $epoch = $cache->load('default:qcache:users#epoch', 3600);
                $this->assertIsString($epoch);
                $this->assertStringStartsWith('blocked:', $epoch);
                $duringCommit = $this->names($reader->find('users', [Query::orderAsc('$id')]));
            });

            $writer->updateDocument('users', 'existing', new Document(['name' => 'updated']));

            $this->assertSame(['existing' => 'original'], $duringCommit);
            $this->assertSame(
                ['existing' => 'updated'],
                $this->names($reader->find('users', [Query::orderAsc('$id')])),
            );
            $this->assertSame(2, $readerAdapter->getObservedFinds());
            $this->assertSame(
                ['existing' => 'updated'],
                $this->names($reader->find('users', [Query::orderAsc('$id')])),
            );
            $this->assertSame(2, $readerAdapter->getObservedFinds());
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testActivationFailureAfterCommitLeavesSharedEpochBlocked(): void
    {
        [$writer, $reader, , $readerAdapter, $cache, $path] = $this->createSharedSQLiteDatabases();

        try {
            $this->assertSame(
                ['existing' => 'original'],
                $this->names($reader->find('users', [Query::orderAsc('$id')])),
            );
            $cache->failActivations();

            try {
                $writer->updateDocument('users', 'existing', new Document(['name' => 'updated']));
                $this->fail('Post-commit query-cache activation failure was not propagated');
            } catch (\RuntimeException $exception) {
                $this->assertStringContainsString('activate query cache', $exception->getMessage());
            }

            $epoch = $cache->load('default:qcache:users#epoch', 3600);
            $this->assertIsString($epoch);
            $this->assertStringStartsWith('blocked:', $epoch);

            $readerAdapter->observeFinds('users');
            $this->assertSame(
                ['existing' => 'updated'],
                $this->names($reader->find('users', [Query::orderAsc('$id')])),
            );
            $this->assertSame(
                ['existing' => 'updated'],
                $this->names($reader->find('users', [Query::orderAsc('$id')])),
            );
            $this->assertSame(2, $readerAdapter->getObservedFinds());
        } finally {
            $this->removeSQLiteFiles($path);
        }
    }

    public function testPooledRollbackCannotPoisonPointOrQueryCaches(): void
    {
        $child = new DatabaseMemory();
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($child),
        );
        $database = new Database(new Pool($connections), new Cache(new LeasableHashCache()));
        $database
            ->setDatabase('cache-tests')
            ->setNamespace('pooled_'.\uniqid())
            ->setQueryCache(new QueryCache(new Cache(new MemoryCache())));
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name'),
        ], permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('users', new Document([
            '$id' => 'user',
            'name' => 'committed',
        ]));
        $this->assertSame('committed', $database->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('committed', $database->find('users')[0]->getAttribute('name'));

        try {
            $database->withTransaction(function () use ($database): void {
                $database->updateDocument('users', 'user', new Document(['name' => 'rolled-back']));
                $this->assertTrue($database->getAdapter()->inTransaction());
                $this->assertSame('rolled-back', $database->getDocument('users', 'user')->getAttribute('name'));
                $this->assertSame('rolled-back', $database->find('users')[0]->getAttribute('name'));

                throw new Conflict('rollback');
            });
        } catch (Conflict) {
        }

        $this->assertSame('committed', $database->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('committed', $database->find('users')[0]->getAttribute('name'));
    }

    /**
     * @return array{Database, LeasableHashCache}
     */
    private function createDatabase(
        ?DatabaseMemory $adapter = null,
        bool $queryCache = true,
        ?CacheAdapter $dataAdapter = null,
    ): array {
        $dataAdapter ??= new LeasableHashCache();
        $queryAdapter = new LeasableHashCache();
        $database = new Database($adapter ?? new DatabaseMemory(), new Cache($dataAdapter));
        $database
            ->setDatabase('cache-tests')
            ->setNamespace('cache_'.\uniqid());
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());

        if ($queryCache) {
            $database->setQueryCache(new QueryCache(new Cache($queryAdapter)));
        }

        return [$database, $queryAdapter];
    }

    private function createUsers(Database $database): void
    {
        $database->createCollection(new Collection(id: 'users', permissions: $this->permissions(), documentSecurity: false));
        foreach (['a', 'b', 'c'] as $id) {
            $database->createDocument('users', new Document(['$id' => $id]));
        }
    }

    /**
     * @param  callable(Database): mixed  $mutation
     */
    private function assertMutationRollsBackOnInvalidationFailure(callable $mutation, bool $sqlite = false): void
    {
        $adapter = $sqlite
            ? new ObservedSQLite(new \PDO('sqlite::memory:'))
            : new ObservedMemory();
        $database = new Database($adapter, new Cache(new LeasableHashCache()));
        $database
            ->setDatabase('cache-tests')
            ->setNamespace('cache_'.\uniqid());
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('users', new Document([
            '$id' => 'existing',
            'name' => 'original',
        ]));

        $cache = new FailingMemory();
        $database->setQueryCache(new QueryCache(new Cache($cache)));
        $this->assertSame(
            ['existing' => 'original'],
            $this->names($database->find('users', [Query::orderAsc('$id')])),
        );

        $cache->seedEpoch('users');
        $cache->failPurges();
        try {
            $mutation($database);
            $this->fail('Mandatory invalidation failure was not propagated');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('query cache epoch', $exception->getMessage());
        }

        $adapter->observeFinds('users');
        $this->assertSame(
            ['existing' => 'original'],
            $this->names($database->find('users', [Query::orderAsc('$id')])),
        );
        $this->assertSame(1, $adapter->getObservedFinds(), 'The stale cached query result must stay blocked');
    }

    /**
     * @return array{Database, Database, PausedSQLite, ObservedSQLite, LeasableHashCache, string}
     */
    private function createSharedSQLiteDatabases(): array
    {
        $path = \tempnam(\sys_get_temp_dir(), 'database-query-cache-');
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

        $writerAdapter = new PausedSQLite($writerConnection);
        $readerAdapter = new ObservedSQLite($readerConnection);
        $writer = new Database($writerAdapter, new Cache(new None()));
        $reader = new Database($readerAdapter, new Cache(new None()));
        $namespace = 'shared_cache_'.\uniqid();
        foreach ([$writer, $reader] as $database) {
            $database
                ->setDatabase('cache-tests')
                ->setNamespace($namespace);
        }

        $writer->create();
        $writer->getAuthorization()->addRole(Role::any()->toString());
        $reader->getAuthorization()->addRole(Role::any()->toString());
        $writer->createCollection(new Collection(id: 'users', attributes: [
            Attribute::string(key: 'name', required: true),
        ], permissions: $this->permissions(), documentSecurity: false));
        $writer->createDocument('users', new Document([
            '$id' => 'existing',
            'name' => 'original',
        ]));

        $cache = new LeasableHashCache();
        $writer->setQueryCache(new QueryCache(new Cache($cache)));
        $reader->setQueryCache(new QueryCache(new Cache($cache)));

        return [$writer, $reader, $writerAdapter, $readerAdapter, $cache, $path];
    }

    private function removeSQLiteFiles(string $path): void
    {
        foreach ([$path, $path.'-wal', $path.'-shm'] as $file) {
            if (\is_file($file)) {
                \unlink($file);
            }
        }
    }

    /** @return array<string> */
    private function permissions(): array
    {
        return [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    /**
     * @param array<Document> $documents
     * @return array<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(
            static fn (Document $document): string => $document->getId(),
            $documents,
        );
    }

    /**
     * @param  array<Document>  $documents
     * @return array<string, string>
     */
    private function names(array $documents): array
    {
        $names = [];
        foreach ($documents as $document) {
            $name = $document->getAttribute('name');
            if (! \is_string($name)) {
                throw new \UnexpectedValueException('Expected document name to be a string');
            }
            $names[$document->getId()] = $name;
        }

        return $names;
    }
}
