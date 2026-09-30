<?php

namespace Tests\Unit\Tenancy;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Mismatch as MismatchException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

/**
 * Memory keeps one index record per collection and key, as MariaDB, MySQL, PostgreSQL and
 * MongoDB keep one physical index, so every tenant that declares the key shares it: it may go
 * only with the last tenant that lists it.
 */
final class SharedIndexOwnershipTest extends TestCase
{
    private const string COLLECTION = 'users';

    private Memory $adapter;

    public function testAnIndexStaysWhileAnotherTenantListsIt(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(1);
        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'byEmail'));
        $this->assertSame([], $this->indexKeys($database));

        $database->setTenant(2);
        $this->assertSame(['byEmail'], $this->indexKeys($database));
        $this->assertDuplicateEmailRefused($database, 'Another tenant deleting its index must leave the index this tenant lists');

        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'byEmail'));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'twin', 'email' => 'same@example.com']));
        $this->assertSame(2, $database->count(self::COLLECTION), 'The last tenant that lists the index drops it');
    }

    public function testAKeyAnotherTenantListsWithAnotherDefinitionIsRefused(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(1);
        $database->createIndex(self::COLLECTION, Index::key(key: 'byName', attributes: ['name']));

        $database->setTenant(2);
        try {
            $database->createIndex(self::COLLECTION, Index::unique(key: 'byName', attributes: ['name']));
            $this->fail('A key another tenant indexes differently must be refused');
        } catch (MismatchException $error) {
            $this->assertSame('Index exists in the shared table with another definition', $error->getMessage());
        }
        $this->assertSame(['byEmail'], $this->indexKeys($database));

        $this->assertTrue($database->createIndex(self::COLLECTION, Index::key(key: 'byName', attributes: ['name'])));
        $this->assertSame(['byEmail', 'byName'], $this->indexKeys($database));
    }

    public function testRenamingAnIndexKeepsTheNameOtherTenantsStillList(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(1);
        $this->assertTrue($database->renameIndex(self::COLLECTION, 'byEmail', 'emailIndex'));
        $this->assertSame(['emailIndex'], $this->indexKeys($database));
        $this->assertSame(IndexType::Unique, $this->adapter->findSharedIndex(self::COLLECTION, 'byEmail')?->type, 'Tenant 2 still lists the old name');

        $database->setTenant(2);
        $this->assertDuplicateEmailRefused($database, 'A tenant that has not renamed the index must keep it');
        $this->assertTrue($database->renameIndex(self::COLLECTION, 'byEmail', 'emailIndex'));
        $this->assertSame(['emailIndex'], $this->indexKeys($database));
        $this->assertDuplicateEmailRefused($database, 'The renamed index must hold for the tenant that renamed it');

        $database->setTenant(1);
        $this->assertNull($this->adapter->findSharedIndex(self::COLLECTION, 'byEmail'), 'No tenant lists the old name any more');
        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'emailIndex'));

        $database->setTenant(2);
        $this->assertDuplicateEmailRefused($database, 'Tenant 2 still lists the renamed index');

        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'emailIndex'));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'twin', 'email' => 'same@example.com']));
        $this->assertSame(2, $database->count(self::COLLECTION), 'Neither the renamed index nor the old name may outlive the last tenant that lists them');
    }

    public function testFindSharedIndexLooksOnlyAtOtherTenants(): void
    {
        $this->createSharedDatabase();

        $this->adapter->setTenant(1);
        $shared = $this->adapter->findSharedIndex(self::COLLECTION, 'BYEMAIL');
        $this->assertNotNull($shared);
        $this->assertSame('byEmail', $shared->key);
        $this->assertTrue($shared->isEquivalentTo(Index::unique(key: 'other', attributes: ['EMAIL'])));
        $this->assertFalse($shared->isEquivalentTo(Index::key(key: 'byEmail', attributes: ['email'])));

        $this->adapter->setTenant(3);
        $this->assertNotNull($this->adapter->findSharedIndex(self::COLLECTION, 'byEmail'));
        $this->assertNull($this->adapter->findSharedIndex(self::COLLECTION, 'byName'));
        $this->assertNull($this->adapter->findSharedIndex('others', 'byEmail'));

        $this->adapter->setSharedTables(false);
        $this->assertNull($this->adapter->findSharedIndex(self::COLLECTION, 'byEmail'));
    }

    public function testSQLiteKeepsAnIndexPerTenant(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $this->createSharedDatabase($adapter);

        $this->assertNull($adapter->findSharedIndex(self::COLLECTION, 'byEmail'));
    }

    private function assertDuplicateEmailRefused(Database $database, string $message): void
    {
        try {
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'twin', 'email' => 'same@example.com']));
            $this->fail($message);
        } catch (DuplicateException) {
            $this->addToAssertionCount(1);
        }
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database): array
    {
        return \array_map(static fn (Index $index): string => $index->key, \array_values($database->getCollection(self::COLLECTION)->indexes));
    }

    private function createSharedDatabase(?Adapter $adapter = null): Database
    {
        $this->adapter = new Memory();
        $authorization = new Authorization();
        $authorization->disable();

        $database = new Database($adapter ?? $this->adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('shared')
            ->setSharedTables(true)
            ->setTenant(1);
        $database->create();

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $database->createCollection(new Collection(
                id: self::COLLECTION,
                attributes: [Attribute::string(key: 'email', size: 64), Attribute::string(key: 'name', size: 64)],
                indexes: [Index::unique(key: 'byEmail', attributes: ['email'])],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
                documentSecurity: false,
            ));
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'user', 'email' => 'same@example.com']));
        }

        return $database;
    }
}
