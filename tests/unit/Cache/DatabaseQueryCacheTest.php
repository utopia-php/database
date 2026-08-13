<?php

namespace Tests\Unit\Cache;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\CursorDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

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
        $database->createCollection('users', permissions: $this->permissions());

        $queryCache = new QueryCache(new Cache($queryAdapter));
        $database->setQueryCache($queryCache);
        $database->setQueryCache($queryCache);

        $queryAdapter->resetPurges();
        $database->createDocument('users', new Document(['$id' => 'a']));
        $this->assertSame(1, $queryAdapter->getWrites('default:qcache:users#epoch'));

        $database->setQueryCache(null);
        $queryAdapter->resetPurges();
        $database->createDocument('users', new Document(['$id' => 'b']));
        $this->assertSame(0, $queryAdapter->getWrites('default:qcache:users#epoch'));
    }

    public function testSchemaAndCollectionMutationsInvalidateQueries(): void
    {
        [$database, $queryAdapter] = $this->createDatabase();
        $database->createCollection('users', permissions: $this->permissions());

        $queryAdapter->resetPurges();
        $database->updateCollection('users', $this->permissions(), false);
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->createAttribute('users', new Attribute('name', ColumnType::String, 255));
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->updateAttribute('users', 'name', size: 128);
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users#epoch'));

        $queryAdapter->resetPurges();
        $database->createIndex('users', new Index('name', IndexType::Key, ['name']));
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
        $database->createCollection('users', permissions: $this->permissions());
        $database->createDocument('users', new Document(['$id' => 'old']));
        $this->assertSame(['old'], $this->ids($database->find('users')));

        $database->deleteCollection('users');
        $database->createCollection('users', permissions: $this->permissions());
        $database->createDocument('users', new Document(['$id' => 'new']));

        $this->assertSame(['new'], $this->ids($database->find('users')));
    }

    public function testRolledBackTransactionCannotPoisonQueryCache(): void
    {
        [$database] = $this->createDatabase();
        $database->createCollection('users', permissions: $this->permissions());
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
        $database->createCollection('users', permissions: $this->permissions());
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
        $database->createCollection('users', [
            new Attribute('name', ColumnType::String, 255),
        ], permissions: $this->permissions());
        $database->getCollection('users');

        $adapter->observeValidators(
            fn () => $database->createAttribute('users', new Attribute('age', ColumnType::Integer)),
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
            $database->createCollection('private', permissions: [
                Permission::create(Role::any()),
            ], documentSecurity: true);

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
        $database->createCollection('users', [
            new Attribute('name', ColumnType::String, 255),
            new Attribute('email', ColumnType::String, 255),
        ], permissions: $this->permissions());
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
        $writer->createCollection('users', permissions: [
            Permission::read(Role::user('user-1')),
            Permission::create(Role::user('user-1')),
            Permission::update(Role::user('user-1')),
        ]);
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
            $database->createCollection('private', permissions: [
                Permission::create(Role::any()),
            ], documentSecurity: true);
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
        $database->createCollection('parents', permissions: $this->permissions());
        $database->createCollection('children', permissions: $this->permissions());

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
        $database->createCollection('users', permissions: $this->permissions());
        $database->find('users');
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
        $database->createCollection('users', [
            new Attribute('name', ColumnType::String, 255),
        ], permissions: $this->permissions());
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

        if ($queryCache) {
            $database->setQueryCache(new QueryCache(new Cache($queryAdapter)));
        }

        return [$database, $queryAdapter];
    }

    private function createUsers(Database $database): void
    {
        $database->createCollection('users', permissions: $this->permissions());
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
        $database->createCollection('users', [
            new Attribute('name', ColumnType::String, 255, required: true),
        ], permissions: $this->permissions());
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

final class LeasableHashCache implements CacheAdapter, Leasable
{
    /** @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>> */
    private array $store = [];

    /** @var array<string, int> */
    private array $generations = [];

    /** @var array<string, int> */
    private array $purges = [];

    /** @var array<string, int> */
    private array $writes = [];

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $hash = $hash === '' ? $key : $hash;
        $saved = $this->store[$key][$hash] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '') {
            return false;
        }

        $hash = $hash === '' ? $key : $hash;
        $this->writes[$key] = ($this->writes[$key] ?? 0) + 1;
        $this->store[$key][$hash] = ['time' => \time(), 'data' => $data];

        return $data;
    }

    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    public function touch(string $key, string $hash = ''): bool
    {
        $hash = $hash === '' ? $key : $hash;
        if (! isset($this->store[$key][$hash])) {
            return false;
        }

        $this->store[$key][$hash]['time'] = \time();

        return true;
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        return \array_keys($this->store[$key] ?? []);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        $this->purges[$key] = ($this->purges[$key] ?? 0) + 1;
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;

        if ($hash === '') {
            unset($this->store[$key]);
        } else {
            unset($this->store[$key][$hash]);
        }

        return true;
    }

    public function flush(): bool
    {
        $this->store = [];
        $this->generations = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->store);
    }

    public function getName(?string $key = null): string
    {
        return 'leasable-hash';
    }

    public function resetPurges(): void
    {
        $this->purges = [];
        $this->writes = [];
    }

    public function getPurges(string $key): int
    {
        return $this->purges[$key] ?? 0;
    }

    public function getWrites(string $key): int
    {
        return $this->writes[$key] ?? 0;
    }
}

final class ObservedMemory extends DatabaseMemory
{
    private ?Closure $metadataCallback = null;

    private ?Closure $validatorCallback = null;

    private ?string $metadataCollection = null;

    private ?string $findCollection = null;

    private int $metadataReads = 0;

    private int $validators = 0;

    private int $finds = 0;

    public function observeMetadata(string $collection, Closure $callback): void
    {
        $this->metadataCollection = $collection;
        $this->metadataCallback = $callback;
        $this->metadataReads = 0;
    }

    public function observeValidators(Closure $callback): void
    {
        $this->validatorCallback = $callback;
        $this->validators = 0;
    }

    public function observeFinds(string $collection): void
    {
        $this->findCollection = $collection;
        $this->finds = 0;
    }

    public function getObservedMetadataReads(): int
    {
        return $this->metadataReads;
    }

    public function getObservedValidators(): int
    {
        return $this->validators;
    }

    public function getObservedFinds(): int
    {
        return $this->finds;
    }

    #[\Override]
    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $document = parent::getDocument($collection, $id, $queries, $forUpdate);

        if ($collection->getId() === Database::METADATA && $id === $this->metadataCollection) {
            $this->metadataReads++;
            $callback = $this->metadataCallback;
            $this->metadataCallback = null;
            $callback?->__invoke();
        }

        return $document;
    }

    #[\Override]
    public function getIdAttributeType(): string
    {
        if ($this->validatorCallback !== null) {
            $this->validators++;
            $callback = $this->validatorCallback;
            $this->validatorCallback = null;
            $callback();
        } elseif ($this->validators > 0) {
            $this->validators++;
        }

        return parent::getIdAttributeType();
    }

    #[\Override]
    public function find(
        Document $collection,
        array $queries = [],
        ?int $limit = 25,
        ?int $offset = null,
        array $orderAttributes = [],
        array $orderTypes = [],
        array $cursor = [],
        CursorDirection $cursorDirection = CursorDirection::After,
        PermissionType $forPermission = PermissionType::Read,
    ): array {
        if ($collection->getId() === $this->findCollection) {
            $this->finds++;
        }

        return parent::find(
            $collection,
            $queries,
            $limit,
            $offset,
            $orderAttributes,
            $orderTypes,
            $cursor,
            $cursorDirection,
            $forPermission,
        );
    }
}

final class ObservedSQLite extends SQLite
{
    private ?string $findCollection = null;

    private int $finds = 0;

    public function observeFinds(string $collection): void
    {
        $this->findCollection = $collection;
        $this->finds = 0;
    }

    public function getObservedFinds(): int
    {
        return $this->finds;
    }

    #[\Override]
    public function find(
        Document $collection,
        array $queries = [],
        ?int $limit = 25,
        ?int $offset = null,
        array $orderAttributes = [],
        array $orderTypes = [],
        array $cursor = [],
        CursorDirection $cursorDirection = CursorDirection::After,
        PermissionType $forPermission = PermissionType::Read,
    ): array {
        if ($collection->getId() === $this->findCollection) {
            $this->finds++;
        }

        return parent::find(
            $collection,
            $queries,
            $limit,
            $offset,
            $orderAttributes,
            $orderTypes,
            $cursor,
            $cursorDirection,
            $forPermission,
        );
    }
}

final class JoinMemory extends DatabaseMemory
{
    private int $finds = 0;

    #[\Override]
    public function capabilities(): array
    {
        return [...parent::capabilities(), Capability::Joins];
    }

    #[\Override]
    public function find(
        Document $collection,
        array $queries = [],
        ?int $limit = 25,
        ?int $offset = null,
        array $orderAttributes = [],
        array $orderTypes = [],
        array $cursor = [],
        CursorDirection $cursorDirection = CursorDirection::After,
        PermissionType $forPermission = PermissionType::Read,
    ): array {
        if ($collection->getId() === 'parents') {
            $this->finds++;
        }

        $queries = \array_values(\array_filter(
            $queries,
            static fn (Query $query): bool => ! \in_array($query->getMethod(), [
                \Utopia\Query\Method::Join,
                \Utopia\Query\Method::LeftJoin,
                \Utopia\Query\Method::RightJoin,
                \Utopia\Query\Method::CrossJoin,
            ], true),
        ));

        return parent::find(
            $collection,
            $queries,
            $limit,
            $offset,
            $orderAttributes,
            $orderTypes,
            $cursor,
            $cursorDirection,
            $forPermission,
        );
    }

    public function getJoinFinds(): int
    {
        return $this->finds;
    }
}

final class FailingMemory extends MemoryCache
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
