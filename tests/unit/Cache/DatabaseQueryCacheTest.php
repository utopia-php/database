<?php

namespace Tests\Unit\Cache;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
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
        $this->assertSame(1, $queryAdapter->getPurges('default:qcache:users'));

        $database->setQueryCache(null);
        $queryAdapter->resetPurges();
        $database->createDocument('users', new Document(['$id' => 'b']));
        $this->assertSame(0, $queryAdapter->getPurges('default:qcache:users'));
    }

    public function testSchemaAndCollectionMutationsInvalidateQueries(): void
    {
        [$database, $queryAdapter] = $this->createDatabase();
        $database->createCollection('users', permissions: $this->permissions());

        $queryAdapter->resetPurges();
        $database->updateCollection('users', $this->permissions(), false);
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));

        $queryAdapter->resetPurges();
        $database->createAttribute('users', new Attribute('name', ColumnType::String, 255));
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));

        $queryAdapter->resetPurges();
        $database->updateAttribute('users', 'name', size: 128);
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));

        $queryAdapter->resetPurges();
        $database->createIndex('users', new Index('name', IndexType::Key, ['name']));
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));

        $queryAdapter->resetPurges();
        $database->renameIndex('users', 'name', 'renamed');
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));

        $queryAdapter->resetPurges();
        $database->deleteIndex('users', 'renamed');
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));

        $queryAdapter->resetPurges();
        $database->deleteAttribute('users', 'name');
        $this->assertGreaterThan(0, $queryAdapter->getPurges('default:qcache:users'));
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

        $adapter->observeValidators(fn () => $database->purgeCachedCollection('users'));
        $database->find('users', [Query::equal('name', ['first'])]);
        $database->find('users', [Query::equal('name', ['second'])]);

        $this->assertSame(2, $adapter->getObservedValidators());
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
}

final class LeasableHashCache implements CacheAdapter, Leasable
{
    /** @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>> */
    private array $store = [];

    /** @var array<string, int> */
    private array $generations = [];

    /** @var array<string, int> */
    private array $purges = [];

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
    }

    public function getPurges(string $key): int
    {
        return $this->purges[$key] ?? 0;
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
