<?php

namespace Tests\Unit\Cache;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

final class QueryCacheScopeTest extends TestCase
{
    /**
     * @return array<string, array{Closure(Database): Database, Closure(Database): Database}>
     */
    public static function scopes(): array
    {
        return [
            'tenant' => [
                static fn (Database $database) => $database->setSharedTables(true)->setTenant(1),
                static fn (Database $database) => $database->setSharedTables(true)->setTenant(2),
            ],
            'namespace' => [
                static fn (Database $database) => $database->setNamespace('writer'),
                static fn (Database $database) => $database->setNamespace('reader'),
            ],
            'database' => [
                static fn (Database $database) => $database->setDatabase('writer'),
                static fn (Database $database) => $database->setDatabase('reader'),
            ],
        ];
    }

    /**
     * @param  Closure(Database): Database  $writerScope
     * @param  Closure(Database): Database  $readerScope
     */
    #[DataProvider('scopes')]
    public function testAWriteLeavesTheCachedResultsOfAnotherScopeInPlace(Closure $writerScope, Closure $readerScope): void
    {
        $cache = new LeasableHashCache();
        [$writer] = $this->createDatabase($cache, $writerScope, 'writer-seed');
        [$reader, $readerAdapter] = $this->createDatabase($cache, $readerScope, 'reader-seed');

        $this->assertSame(['reader-seed'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
        $readerAdapter->observeFinds('posts');

        $writer->createDocument('posts', new Document(['$id' => 'written']));

        $this->assertSame(['reader-seed'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
        $this->assertSame(0, $readerAdapter->getObservedFinds(), 'A write in another scope must not rotate the reader\'s query cache');
    }

    /**
     * @param  Closure(Database): Database  $writerScope
     * @param  Closure(Database): Database  $readerScope
     */
    #[DataProvider('scopes')]
    public function testAPendingWriteLeavesTheQueryCacheOfAnotherScopeOn(Closure $writerScope, Closure $readerScope): void
    {
        $cache = new LeasableHashCache();
        [$writer, $writerAdapter] = $this->createDatabase($cache, $writerScope, 'writer-seed');
        [$reader, $readerAdapter] = $this->createDatabase($cache, $readerScope, 'reader-seed');

        $this->assertSame(['reader-seed'], $this->ids($reader->find('posts', [Query::orderAsc('$id')])));
        $readerAdapter->observeFinds('posts');

        $duringWrite = [];
        $writerAdapter->pauseNextCommit(function () use ($reader, &$duringWrite): void {
            $duringWrite = $this->ids($reader->find('posts', [Query::orderAsc('$id')]));
        });
        $writer->createDocument('posts', new Document(['$id' => 'written']));

        $this->assertSame(['reader-seed'], $duringWrite);
        $this->assertSame(0, $readerAdapter->getObservedFinds(), 'A write in progress in another scope must not switch the reader\'s query cache off');
    }

    public function testAWriteUnderAnotherTenantOfTheSameDatabaseLeavesTheCachedResultsInPlace(): void
    {
        [$database, $adapter] = $this->createTenants();

        $this->assertSame(['seed-2'], $this->ids($database->withTenant(2, fn (): array => $database->find('posts'))));
        $adapter->observeFinds('posts');

        $database->withTenant(1, fn (): Document => $database->createDocument('posts', new Document(['$id' => 'written'])));

        $this->assertSame(['seed-2'], $this->ids($database->withTenant(2, fn (): array => $database->find('posts'))));
        $this->assertSame(0, $adapter->getObservedFinds(), 'Tenant 1\'s write must leave tenant 2\'s cached result in place');
    }

    public function testAWriteRefreshesTheCachedResultsOfItsOwnTenant(): void
    {
        [$database, $adapter] = $this->createTenants();

        $this->assertSame(['seed-1'], $this->ids($database->withTenant(1, fn (): array => $database->find('posts', [Query::orderAsc('$id')]))));

        $database->withTenant(1, fn (): Document => $database->createDocument('posts', new Document(['$id' => 'written'])));

        $adapter->observeFinds('posts');
        $this->assertSame(['seed-1', 'written'], $this->ids($database->withTenant(1, fn (): array => $database->find('posts', [Query::orderAsc('$id')]))));
        $this->assertSame(['seed-1', 'written'], $this->ids($database->withTenant(1, fn (): array => $database->find('posts', [Query::orderAsc('$id')]))));
        $this->assertSame(1, $adapter->getObservedFinds(), 'The refreshed result must be cached again under the tenant\'s new epoch');
    }

    public function testATransactionRefreshesEveryTenantItWroteUnder(): void
    {
        [$database, $adapter] = $this->createTenants();

        foreach ([1, 2] as $tenant) {
            $database->withTenant($tenant, fn (): array => $database->find('posts', [Query::orderAsc('$id')]));
        }

        $database->withTransaction(function () use ($database): void {
            foreach ([1, 2] as $tenant) {
                $database->withTenant($tenant, fn (): Document => $database->createDocument('posts', new Document(['$id' => 'written-'.$tenant])));
            }
        });

        $adapter->observeFinds('posts');
        foreach ([1, 2] as $tenant) {
            foreach ([0, 1] as $ignored) {
                $this->assertSame(
                    ['seed-'.$tenant, 'written-'.$tenant],
                    $this->ids($database->withTenant($tenant, fn (): array => $database->find('posts', [Query::orderAsc('$id')]))),
                );
            }
        }
        $this->assertSame(2, $adapter->getObservedFinds(), 'Each tenant must miss once and then be served from its re-enabled query cache');
    }

    public function testTenantsNeverReadEachOthersCachedResults(): void
    {
        [$database] = $this->createTenants();

        foreach ([1, 2, 1, 2] as $tenant) {
            $this->assertSame(
                ['seed-'.$tenant],
                $this->ids($database->withTenant($tenant, fn (): array => $database->find('posts', [Query::orderAsc('$id')]))),
            );
        }
    }

    /**
     * @param  Closure(Database): Database  $scope
     * @return array{Database, ObservedMemory}
     */
    private function createDatabase(LeasableHashCache $cache, Closure $scope, string $seed): array
    {
        $adapter = new ObservedMemory();
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('scope')
            ->setNamespace('scope');
        $scope($database);
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->setQueryCache(new QueryCache(new Cache($cache)));
        $database->createCollection(new Collection(id: 'posts', permissions: $this->permissions(), documentSecurity: false));
        $database->createDocument('posts', new Document(['$id' => $seed]));

        return [$database, $adapter];
    }

    /**
     * @return array{Database, ObservedMemory}
     */
    private function createTenants(): array
    {
        $adapter = new ObservedMemory();
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('scope')
            ->setNamespace('tenants_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(1);
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->setQueryCache(new QueryCache(new Cache(new LeasableHashCache())));

        foreach ([1, 2] as $tenant) {
            $database->withTenant($tenant, function () use ($database, $tenant): void {
                $database->createCollection(new Collection(id: 'posts', permissions: $this->permissions(), documentSecurity: false));
                $database->createDocument('posts', new Document(['$id' => 'seed-'.$tenant]));
            });
        }

        return [$database, $adapter];
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
