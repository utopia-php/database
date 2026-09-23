<?php

namespace Tests\Unit\Cache;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Under tenant-per-document one write can hold the documents of any tenant, whichever tenant the
 * writer has selected, while find() caches its results per tenant. A write has to refresh the
 * cached results of each written document's tenant and leave every other tenant's in place.
 */
final class QueryCacheTenantPerDocumentTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string DOCUMENT = 'note';

    private const string ADDED = 'added';

    private const int TENANT = 5;

    private const int OTHER_TENANT = 6;

    private const int UNTOUCHED_TENANT = 7;

    private const string ALICE = 'alice';

    private const string BOB = 'bob';

    private const string DRAFT = 'draft';

    private const string FINAL = 'final';

    private ObservedSQLite $adapter;

    private Authorization $authorization;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new ObservedSQLite(new PDO('sqlite::memory:'));
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());

        $this->database = (new Database($this->adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('query_cache_tenant_per_document')
            ->setNamespace('query_cache_tenant_per_document')
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true)
            ->addHook(new Permissions());
        $this->database->create();
        $this->database->createCollection($this->notes());
        $this->database->setQueryCache(new QueryCache(new Cache(new LeasableHashCache())));

        foreach ([self::TENANT, self::OTHER_TENANT, self::UNTOUCHED_TENANT] as $tenant) {
            $this->database->createDocument(self::COLLECTION, $this->note($tenant, [self::ALICE, self::BOB]));
        }
    }

    public function testARevokingUpsertWithNoTenantSelectedRefreshesTheDocumentsTenant(): void
    {
        $this->cacheReads([self::TENANT]);

        $this->database->upsertDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE], self::FINAL)]);

        $this->assertRevokedUpsertRefreshed(self::TENANT);
    }

    public function testARevokingUpsertUnderAnotherTenantRefreshesTheDocumentsTenantOnly(): void
    {
        $this->cacheReads([self::TENANT, self::OTHER_TENANT]);

        $this->database->withTenant(
            self::OTHER_TENANT,
            fn (): int => $this->database->upsertDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE], self::FINAL)]),
        );

        $this->assertRevokedUpsertRefreshed(self::TENANT);
        $this->assertServedFromCache([self::OTHER_TENANT], 'The selected tenant holds none of the written documents, so its cached results must stay in place');
    }

    public function testACreateWithNoTenantSelectedRefreshesTheDocumentsTenant(): void
    {
        $this->cacheReads([self::TENANT]);

        $this->database->createDocument(self::COLLECTION, $this->note(self::TENANT, [self::ALICE, self::BOB], id: self::ADDED));

        $this->assertAddedTo([self::TENANT]);
    }

    public function testABatchCreateWithNoTenantSelectedRefreshesEveryDocumentsTenant(): void
    {
        $this->cacheReads([self::TENANT, self::OTHER_TENANT, self::UNTOUCHED_TENANT]);

        $this->database->createDocuments(self::COLLECTION, [
            $this->note(self::TENANT, [self::ALICE, self::BOB], id: self::ADDED),
            $this->note(self::OTHER_TENANT, [self::ALICE, self::BOB], id: self::ADDED),
        ]);

        $this->assertAddedTo([self::TENANT, self::OTHER_TENANT]);
        $this->assertServedFromCache([self::UNTOUCHED_TENANT], 'The batch wrote nothing under tenant 7');
    }

    public function testAnUpsertBatchAcrossTenantsRefreshesEachTenant(): void
    {
        $this->cacheReads([self::TENANT, self::OTHER_TENANT]);

        $this->database->upsertDocuments(self::COLLECTION, [
            $this->note(self::TENANT, [self::ALICE], self::FINAL),
            $this->note(self::OTHER_TENANT, [self::ALICE, self::BOB], self::FINAL),
        ]);

        $this->assertRevokedUpsertRefreshed(self::TENANT);
        $this->adapter->observeFinds(self::COLLECTION);
        foreach ([self::ALICE, self::BOB] as $reader) {
            $this->assertSame([self::DOCUMENT => self::FINAL], $this->titles($reader, self::OTHER_TENANT));
        }
        $this->assertSame(2, $this->adapter->getObservedFinds(), 'Tenant 6\'s readers must reach the database after the batch retitled its note');
    }

    public function testAWriteLeavesTheCachedResultsOfATenantItDoesNotTouchInPlace(): void
    {
        $this->cacheReads([self::UNTOUCHED_TENANT]);

        $this->database->upsertDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE], self::FINAL)]);

        $this->assertServedFromCache([self::UNTOUCHED_TENANT], 'An upsert of tenant 5\'s document must leave tenant 7\'s cached results in place');
    }

    public function testAWriteUnderTheDocumentsTenantRefreshesThatTenant(): void
    {
        $this->cacheReads([self::TENANT]);

        $this->database->withTenant(
            self::TENANT,
            fn (): int => $this->database->upsertDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE], self::FINAL)]),
        );

        $this->assertRevokedUpsertRefreshed(self::TENANT);
    }

    public function testWithoutTenantPerDocumentAWriteRefreshesTheSelectedTenant(): void
    {
        $adapter = new ObservedSQLite(new PDO('sqlite::memory:'));
        $database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('query_cache_selected_tenant')
            ->setNamespace('query_cache_selected_tenant')
            ->addHook(new Permissions());
        $database->create();
        $database->createCollection($this->notes());
        $database->setQueryCache(new QueryCache(new Cache(new LeasableHashCache())));
        $database->createDocument(self::COLLECTION, $this->note(self::TENANT, [self::ALICE, self::BOB]));
        $read = fn (): array => \array_map(
            static fn (Document $document): string => $document->getId(),
            $this->authorization->skip(fn (): array => $database->find(self::COLLECTION, [Query::orderAsc('$id')])),
        );
        $this->assertSame([self::DOCUMENT], $read());

        $database->createDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE, self::BOB], id: self::ADDED)]);

        $adapter->observeFinds(self::COLLECTION);
        $this->assertSame([self::ADDED, self::DOCUMENT], $read(), 'A document\'s own tenant must not pick the scope when tables are not shared per document');
        $this->assertSame(1, $adapter->getObservedFinds());
    }

    public function testTokensKeyEachDocumentUnderTheTenantItIsStoredUnder(): void
    {
        $queryCache = new QueryCache(new InvalidationCache());
        $scope = new Scope('host', 'database', 'namespace', self::UNTOUCHED_TENANT);
        $documents = [
            new Document(['$collection' => self::COLLECTION, '$tenant' => self::TENANT]),
            new Document(['$collection' => self::COLLECTION, '$tenant' => (string) self::OTHER_TENANT]),
            new Document(['$collection' => self::COLLECTION]),
        ];

        $keys = \array_keys((new Invalidator($queryCache))->tokens(Event::DocumentsCreate, $documents, $scope, tenantPerDocument: true));
        $expected = [
            $queryCache->getCollectionKey(new Scope('host', 'database', 'namespace', self::TENANT), self::COLLECTION),
            $queryCache->getCollectionKey(new Scope('host', 'database', 'namespace', self::OTHER_TENANT), self::COLLECTION),
            $queryCache->getCollectionKey($scope, self::COLLECTION),
        ];
        \sort($keys);
        \sort($expected);

        $this->assertSame($expected, $keys, 'Each document is keyed under its own tenant, and a document without one under the scope\'s');
        $this->assertSame(
            [$queryCache->getCollectionKey($scope, self::COLLECTION)],
            \array_keys((new Invalidator($queryCache))->tokens(Event::DocumentsCreate, $documents, $scope)),
            'Without tenant-per-document every document is keyed under the one scope',
        );
    }

    public function testAWrittenDocumentsOwnOptionsNameNoCollectionToInvalidate(): void
    {
        $queryCache = new QueryCache(new InvalidationCache());
        $scope = new Scope(namespace: 'namespace', tenant: self::TENANT);
        $document = new Document([
            '$collection' => self::COLLECTION,
            'options' => ['relatedCollection' => 'unrelated'],
        ]);

        $tokens = (new Invalidator($queryCache))->tokens(Event::DocumentCreate, $document, $scope);

        $this->assertSame([$queryCache->getCollectionKey($scope, self::COLLECTION)], \array_keys($tokens));
    }

    private function notes(): Collection
    {
        return new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: true,
        );
    }

    /**
     * @param  list<string>  $readers
     */
    private function note(int $tenant, array $readers, string $title = self::DRAFT, string $id = self::DOCUMENT): Document
    {
        return new Document([
            '$id' => $id,
            '$tenant' => $tenant,
            'title' => $title,
            '$permissions' => \array_map(
                static fn (string $reader): string => Permission::read(Role::user($reader)),
                $readers,
            ),
        ]);
    }

    /**
     * Alice and bob read each tenant's notes once from the database and once more from the cache.
     *
     * @param  list<int>  $tenants
     */
    private function cacheReads(array $tenants): void
    {
        foreach ($tenants as $tenant) {
            foreach ([self::ALICE, self::BOB] as $reader) {
                $this->assertSame([self::DOCUMENT => self::DRAFT], $this->titles($reader, $tenant));
            }
        }

        $this->assertServedFromCache($tenants, 'Every result must be cached before the write');
    }

    /**
     * @param  list<int>  $tenants
     */
    private function assertServedFromCache(array $tenants, string $message): void
    {
        $this->adapter->observeFinds(self::COLLECTION);
        foreach ($tenants as $tenant) {
            foreach ([self::ALICE, self::BOB] as $reader) {
                $this->assertSame([self::DOCUMENT => self::DRAFT], $this->titles($reader, $tenant), $message);
            }
        }
        $this->assertSame(0, $this->adapter->getObservedFinds(), $message);
    }

    private function assertRevokedUpsertRefreshed(int $tenant): void
    {
        $this->adapter->observeFinds(self::COLLECTION);
        $this->assertSame([self::DOCUMENT => self::FINAL], $this->titles(self::ALICE, $tenant));
        $this->assertSame(1, $this->adapter->getObservedFinds(), 'Alice\'s read must reach the database after the upsert');

        $this->assertNotContains(self::DRAFT, $this->titles(self::BOB, $tenant), 'Bob must not be served the cached title after the upsert');
        $this->assertSame(2, $this->adapter->getObservedFinds(), 'Bob\'s read must reach the database instead of the cache');
    }

    /**
     * @param  list<int>  $tenants
     */
    private function assertAddedTo(array $tenants): void
    {
        $this->adapter->observeFinds(self::COLLECTION);
        foreach ($tenants as $tenant) {
            $this->assertSame(
                [self::ADDED => self::DRAFT, self::DOCUMENT => self::DRAFT],
                $this->titles(self::ALICE, $tenant),
                "Tenant {$tenant}'s cached list must include the document written under it",
            );
        }
        $this->assertSame(\count($tenants), $this->adapter->getObservedFinds());
    }

    /**
     * @return array<string, mixed> Titles by document id
     */
    private function titles(string $reader, int $tenant): array
    {
        $roles = $this->authorization->getRoles();
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::user($reader)->toString());

        try {
            $titles = [];
            foreach ($this->database->withTenant($tenant, fn (): array => $this->database->find(self::COLLECTION, [Query::orderAsc('$id')])) as $document) {
                $titles[$document->getId()] = $document->getAttribute('title');
            }

            return $titles;
        } finally {
            $this->authorization->cleanRoles();
            foreach ($roles as $role) {
                $this->authorization->addRole($role);
            }
        }
    }
}
