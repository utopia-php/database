<?php

namespace Tests\Unit\Tenancy;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * When a rename's metadata write fails, Database undoes the rename. Under shared tables a
 * later tenant's rename only adopts the column the first tenant already renamed, so undoing it
 * would move the column back under every tenant that has migrated.
 */
final class SharedRenameRollbackTest extends TestCase
{
    private const string COLLECTION = 'users';

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
            'memory' => [static fn (): Adapter => new Memory()],
        ];
    }

    /**
     * @return array<string, array{Closure(): Adapter, Closure(FailingMetadataDatabase): mixed}>
     */
    public static function renames(): array
    {
        $rename = static fn (FailingMetadataDatabase $database): mixed => $database->renameAttribute(self::COLLECTION, 'age', 'years');
        $updateKey = static fn (FailingMetadataDatabase $database): mixed => $database->updateAttribute(self::COLLECTION, 'age', newKey: 'years');

        $cases = [];
        foreach (self::adapters() as $engine => [$adapter]) {
            $cases["{$engine} renameAttribute"] = [$adapter, $rename];
            $cases["{$engine} updateAttribute with a new key"] = [$adapter, $updateKey];
        }

        return $cases;
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  Closure(FailingMetadataDatabase): mixed  $rename
     */
    #[DataProvider('renames')]
    public function testAFailedAdoptionLeavesTheColumnRenamed(Closure $adapter, Closure $rename): void
    {
        $database = $this->createSharedDatabase($adapter());

        $database->setTenant(1);
        $rename($database);

        $database->setTenant(2);
        $database->failMetadataWrites(true);
        try {
            $rename($database);
            $this->fail('The metadata write failure must be reported');
        } catch (DatabaseException $error) {
            $this->assertStringContainsString('Failed to persist metadata', $error->getMessage());
        }
        $database->failMetadataWrites(false);

        $database->setTenant(1);
        $this->assertSame(10, $database->getDocument(self::COLLECTION, 'user')->getAttribute('years'), 'The tenant that already migrated must keep reading the renamed attribute');

        $database->setTenant(2);
        $rename($database);
        $this->assertSame(20, $database->getDocument(self::COLLECTION, 'user')->getAttribute('years'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  Closure(FailingMetadataDatabase): mixed  $rename
     */
    #[DataProvider('renames')]
    public function testAFailedRenameThisCallRanIsUndone(Closure $adapter, Closure $rename): void
    {
        $database = $this->createSharedDatabase($adapter());

        $database->setTenant(1);
        $database->failMetadataWrites(true);
        try {
            $rename($database);
            $this->fail('The metadata write failure must be reported');
        } catch (DatabaseException) {
            $this->addToAssertionCount(1);
        }
        $database->failMetadataWrites(false);

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $this->assertSame($tenant * 10, $database->getDocument(self::COLLECTION, 'user')->getAttribute('age'), 'The rename this call ran must be undone');
        }
    }

    public function testAPoolAsksItsAdapterWhetherARenameRan(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->createSharedDatabase($adapter);
        $database->setTenant(1);
        $database->renameAttribute(self::COLLECTION, 'age', 'years');

        $connections = $this->createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($adapter));
        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());
        $pool->setDatabase('utopiaTests');
        $pool->setNamespace('shared');
        $pool->setSharedTables(true);
        $pool->setTenant(1);

        $this->assertTrue($pool->isRenamed(self::COLLECTION, 'age', 'years'));
        $this->assertFalse($pool->isRenamed(self::COLLECTION, 'years', 'age'));
    }

    private function createSharedDatabase(Adapter $adapter): FailingMetadataDatabase
    {
        $authorization = new Authorization();
        $authorization->disable();

        $database = new FailingMetadataDatabase($adapter, new Cache(new None()));
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
                attributes: [Attribute::integer(key: 'age')],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
                documentSecurity: false,
            ));
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'user', 'age' => $tenant * 10]));
        }

        return $database;
    }
}
