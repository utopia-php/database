<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PermissionType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * Under shared tables a permission-filtered read only matches `_perms` rows
 * stored under the reader's tenant, so every write path has to store them there.
 */
final class UpsertTenancyTest extends TestCase
{
    private const int TENANT = 7;

    private const string COLLECTION = 'notes';

    private const string READER = 'reader';

    private PDO $pdo;

    private Authorization $authorization;

    private string $namespace;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
        $this->namespace = 'upsert_tenancy_'.\uniqid();

        $database = $this->database($this->adapter());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));
    }

    public function testAFirstWriteUpsertStoresItsPermissionRowsUnderTheTenant(): void
    {
        $this->database($this->adapter())->upsertDocuments(self::COLLECTION, [$this->note('upserted')]);

        $this->assertSame(
            [
                ['upserted', PermissionType::Read->value, self::TENANT],
                ['upserted', PermissionType::Update->value, self::TENANT],
            ],
            $this->permissionRows(),
            'An adapter whose first write is an upsert must store the permission rows under the tenant, like every other write path',
        );
    }

    public function testAFirstWriteUpsertIsVisibleToAPermissionFilteredRead(): void
    {
        $database = $this->database($this->adapter());
        $database->upsertDocuments(self::COLLECTION, [$this->note('upserted')]);

        $this->readAs(self::READER);

        $this->assertSame(['upserted'], $this->ids($database->find(self::COLLECTION)));
        $this->assertSame(1, $database->count(self::COLLECTION));
    }

    public function testEveryUpsertThroughAPoolIsVisibleToAPermissionFilteredRead(): void
    {
        $database = $this->database($this->pool($this->adapter()));
        $database->createDocument(self::COLLECTION, $this->note('created'));
        $database->upsertDocuments(self::COLLECTION, [$this->note('upserted')]);

        $this->readAs(self::READER);

        $this->assertSame(
            ['created', 'upserted'],
            $this->ids($database->find(self::COLLECTION)),
            'The pool hands every call its own write hooks, so an upsert cannot rely on an earlier write having registered the tenant',
        );
    }

    public function testAnAdapterUpsertStoresADocumentWithoutATenantUnderTheSelectedTenant(): void
    {
        $adapter = $this->adapter();
        $database = $this->database($adapter);

        $adapter->upsertDocuments($database->getCollection(self::COLLECTION), '', [
            new Change(new Document(), $this->note('upserted')),
        ]);

        $this->assertSame(
            [self::TENANT],
            $this->tenantsOf(self::COLLECTION),
            'A document without a tenant is stored under the selected tenant, as createDocuments() stores it',
        );
        $this->assertSame(
            [
                ['upserted', PermissionType::Read->value, self::TENANT],
                ['upserted', PermissionType::Update->value, self::TENANT],
            ],
            $this->permissionRows(),
        );
        $this->assertSame(
            'upserted',
            $this->authorization->skip(fn (): Document => $database->getDocument(self::COLLECTION, 'upserted'))->getId(),
        );
    }

    private function adapter(): SQLite
    {
        return new SQLite($this->pdo);
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($this->authorization)
            ->setDatabase('upsert_tenancy')
            ->setNamespace($this->namespace)
            ->setSharedTables(true)
            ->setTenant(self::TENANT)
            ->addHook(new Permissions());

        return $database;
    }

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return new Pool($connections);
    }

    private function note(string $id): Document
    {
        return new Document([
            '$id' => $id,
            'title' => $id,
            '$permissions' => [
                Permission::read(Role::user(self::READER)),
                Permission::update(Role::user(self::READER)),
            ],
        ]);
    }

    private function readAs(string $user): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::user($user)->toString());
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(static fn (Document $document): string => $document->getId(), \array_values($documents));
    }

    /**
     * @return list<array{0: string, 1: string, 2: int|null}>
     */
    private function permissionRows(): array
    {
        $table = $this->table(Storage::permissionsTable(self::COLLECTION));
        $statement = $this->pdo->query(
            'SELECT '.Storage::PERM_DOCUMENT.', '.Storage::PERM_TYPE.', '.Storage::TENANT
            ." FROM {$table} ORDER BY ".Storage::PERM_DOCUMENT.', '.Storage::PERM_TYPE,
        );
        $this->assertNotFalse($statement);

        /** @var list<array{0: string, 1: string, 2: int|null}> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_NUM);

        return $rows;
    }

    /**
     * @return list<int|null>
     */
    private function tenantsOf(string $collection): array
    {
        $statement = $this->pdo->query('SELECT '.Storage::TENANT." FROM {$this->table($collection)}");
        $this->assertNotFalse($statement);

        /** @var list<int|null> $tenants */
        $tenants = $statement->fetchAll(PDO::FETCH_COLUMN);

        return $tenants;
    }

    private function table(string $name): string
    {
        return "`{$this->namespace}_{$name}`";
    }
}
