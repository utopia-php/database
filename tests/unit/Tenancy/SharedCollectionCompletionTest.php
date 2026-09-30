<?php

namespace Tests\Unit\Tenancy;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
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

/**
 * Under shared tables every tenant declares its own definition of a collection id. The first
 * tenant's createCollection() creates the table; a later tenant's must add what its own
 * definition declares on top.
 */
final class SharedCollectionCompletionTest extends TestCase
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
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testALaterTenantGetsTheAttributesAndIndexesItDeclares(Closure $adapter): void
    {
        $database = $this->createSharedDatabase($adapter());

        $database->setTenant(1);
        $this->createUsers($database, []);
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'user', 'age' => 10]));

        $database->setTenant(2);
        $this->createUsers($database, [Attribute::string(key: 'title', size: 64)], [Index::unique(key: 'byTitle', attributes: ['title'])]);
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'user', 'age' => 20, 'title' => 'title2']));

        $this->assertSame('title2', $database->getDocument(self::COLLECTION, 'user')->getAttribute('title'));
        try {
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'other', 'age' => 21, 'title' => 'title2']));
            $this->fail('The unique index the later tenant declared must hold');
        } catch (DuplicateException) {
            $this->addToAssertionCount(1);
        }

        $database->setTenant(1);
        $this->assertSame(['age'], $this->keys($database));
        $this->assertSame(10, $database->getDocument(self::COLLECTION, 'user')->getAttribute('age'));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'other', 'age' => 11]));
        $this->assertSame(2, $database->count(self::COLLECTION));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testALaterTenantReusesAColumnOfTheSameType(Closure $adapter): void
    {
        $database = $this->createSharedDatabase($adapter());

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $this->createUsers($database, [Attribute::string(key: 'title', size: 64)]);
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'user', 'age' => $tenant, 'title' => 'title'.$tenant]));
        }

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $this->assertSame(['age', 'title'], $this->keys($database));
            $this->assertSame('title'.$tenant, $database->getDocument(self::COLLECTION, 'user')->getAttribute('title'));
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testALaterTenantCannotDeclareAnotherTenantsColumnWithAnotherType(Closure $adapter): void
    {
        $database = $this->createSharedDatabase($adapter());

        $database->setTenant(1);
        $this->createUsers($database, [Attribute::integer(key: 'score')]);

        $database->setTenant(2);
        try {
            $this->createUsers($database, [Attribute::string(key: 'label', size: 32), Attribute::string(key: 'score', size: 32)]);
            $this->fail('A column another tenant stores with another type must be refused');
        } catch (MismatchException $error) {
            $this->assertSame('Attribute exists in the shared table with another type', $error->getMessage());
        }

        $this->assertTrue($database->getCollection(self::COLLECTION)->isEmpty(), 'A refused definition must not be recorded');

        $adapter = $database->getAdapter();
        if ($adapter instanceof Feature\SchemaAttributes) {
            $this->assertNotContains(
                'label',
                \array_map(static fn (Document $column): string => $column->getId(), $adapter->getSchemaAttributes(self::COLLECTION)),
                'The refused definition must add none of its columns',
            );
        }

        $database->setTenant(1);
        $this->assertSame(['age', 'score'], $this->keys($database));
    }

    /**
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     */
    private function createUsers(Database $database, array $attributes, array $indexes = []): void
    {
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: 'age'), ...$attributes],
            indexes: $indexes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            \array_values($database->getCollection(self::COLLECTION)->attributes),
        );
    }

    private function createSharedDatabase(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->disable();

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('shared')
            ->setSharedTables(true)
            ->setTenant(1);
        $database->create();

        return $database;
    }
}
