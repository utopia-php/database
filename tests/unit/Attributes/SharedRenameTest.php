<?php

namespace Tests\Unit\Attributes;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class SharedRenameTest extends TestCase
{
    private const string COLLECTION = 'users';

    private const array TENANTS = [1, 2];

    public function testTenantsRenameAnAttributeInTurn(): void
    {
        $database = $this->createSharedDatabase();

        foreach (self::TENANTS as $tenant) {
            $database->setTenant($tenant);
            $database->renameAttribute(self::COLLECTION, 'age', 'years');
        }

        $this->assertEachTenantReadsItsValuesUnder($database, 'years', 'age');
    }

    public function testTenantsUpdateAnAttributeKeyInTurn(): void
    {
        $database = $this->createSharedDatabase();

        foreach (self::TENANTS as $tenant) {
            $database->setTenant($tenant);
            $this->assertSame('years', $database->updateAttribute(self::COLLECTION, 'age', new AttributeUpdate(required: true, key: 'years'))->key);
        }

        $this->assertEachTenantReadsItsValuesUnder($database, 'years', 'age');
    }

    public function testARenameOntoAColumnBesideTheOldOneIsRefused(): void
    {
        $database = $this->createSharedDatabase();
        $database->setTenant(2);
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'title', size: 32));
        $database->setTenant(1);

        try {
            $database->renameAttribute(self::COLLECTION, 'nick', 'title');
            $this->fail('A rename onto another attribute\'s column must be refused while the old column holds values');
        } catch (DuplicateException $e) {
            $this->assertSame('Attribute already exists', $e->getMessage());
        }

        try {
            $database->updateAttribute(self::COLLECTION, 'nick', new AttributeUpdate(key: 'title'));
            $this->fail('A key update onto another attribute\'s column must be refused while the old column holds values');
        } catch (DuplicateException $e) {
            $this->assertSame('Attribute already exists', $e->getMessage());
        }

        $this->assertSame(['age', 'nick'], $this->keys($database));
        $this->assertSame('nick1', $database->getDocument(self::COLLECTION, 'user')->getAttribute('nick'));
    }

    public function testARenameCompletesAnOrphanedRenameOutsideSharedTables(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->createDatabase($adapter);
        $database->createCollection($this->definition());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'user', 'age' => 30, 'nick' => 'nick']));
        $adapter->renameAttribute(self::COLLECTION, 'age', 'years');

        $database->renameAttribute(self::COLLECTION, 'age', 'years');

        $this->assertSame(['years', 'nick'], $this->keys($database));
        $this->assertSame(30, $database->getDocument(self::COLLECTION, 'user')->getAttribute('years'));
    }

    public function testARenameOfAMissingAttributeIsNotFound(): void
    {
        $database = $this->createSharedDatabase();

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Attribute not found');

        $database->renameAttribute(self::COLLECTION, 'missing', 'found');
    }

    private function assertEachTenantReadsItsValuesUnder(Database $database, string $key, string $previous): void
    {
        foreach (self::TENANTS as $tenant) {
            $database->setTenant($tenant);
            $document = $database->getDocument(self::COLLECTION, 'user');

            $this->assertSame([$key, 'nick'], $this->keys($database), "Tenant {$tenant} keys");
            $this->assertSame($tenant * 10, $document->getAttribute($key), "Tenant {$tenant} value");
            $this->assertFalse($document->offsetExists($previous), "Tenant {$tenant} old key");
        }
    }

    private function createSharedDatabase(): Database
    {
        $database = $this->createDatabase(new SQLite(new PDO('sqlite::memory:')), sharedTables: true);

        foreach (self::TENANTS as $tenant) {
            $database->setTenant($tenant);
            $database->createCollection($this->definition());
            $database->createDocument(self::COLLECTION, new Document([Document::ID => 'user', 'age' => $tenant * 10, 'nick' => "nick{$tenant}"]));
        }

        return $database;
    }

    private function createDatabase(SQLite $adapter, bool $sharedTables = false): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('shared_rename')
            ->setSharedTables($sharedTables)
            ->setTenant($sharedTables ? self::TENANTS[0] : null)
            ->setNamespace('shared_rename_'.\uniqid());
        $database->create();

        return $database;
    }

    private function definition(): Collection
    {
        return Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'age'),
                Attribute::string(key: 'nick', size: 32),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        );
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes(),
        );
    }
}
