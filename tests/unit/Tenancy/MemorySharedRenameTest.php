<?php

namespace Tests\Unit\Tenancy;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Memory keeps one attribute record per collection and every tenant's rows beside it, as a SQL
 * table does under shared tables, so a rename is the column's: the first tenant renames it, and
 * a later tenant finds it renamed.
 */
final class MemorySharedRenameTest extends TestCase
{
    private const string COLLECTION = 'users';

    public function testARenameOntoAnotherTenantsAttributeIsRefused(): void
    {
        $database = $this->createSharedDatabase();
        $database->setTenant(2);
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'label', size: 32));
        $database->updateDocument(self::COLLECTION, 'user', new Document(['label' => 'label2']));

        $database->setTenant(1);
        try {
            $database->renameAttribute(self::COLLECTION, 'nick', 'label');
            $this->fail('A rename onto another tenant\'s attribute must be refused while the old one holds values');
        } catch (DuplicateException $error) {
            $this->assertSame('Attribute already exists', $error->getMessage());
        }
        try {
            $database->updateAttribute(self::COLLECTION, 'nick', newKey: 'label');
            $this->fail('A key update onto another tenant\'s attribute must be refused while the old one holds values');
        } catch (DuplicateException $error) {
            $this->assertSame('Attribute already exists', $error->getMessage());
        }
        $this->assertSame('k1', $database->getDocument(self::COLLECTION, 'user')->getAttribute('nick'));

        $database->setTenant(2);
        $document = $database->getDocument(self::COLLECTION, 'user');
        $this->assertSame('k2', $document->getAttribute('nick'));
        $this->assertSame('label2', $document->getAttribute('label'));
    }

    public function testTenantsRenameAnAttributeInTurn(): void
    {
        $database = $this->createSharedDatabase();

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $this->assertTrue($database->renameAttribute(self::COLLECTION, 'age', 'years'));
        }

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $document = $database->getDocument(self::COLLECTION, 'user');
            $this->assertSame($tenant * 10, $document->getAttribute('years'));
            $this->assertFalse($document->offsetExists('age'));
        }
    }

    private function createSharedDatabase(): Database
    {
        $authorization = new Authorization();
        $authorization->disable();

        $database = new Database(new Memory(), new Cache(new None()));
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
                attributes: [Attribute::integer(key: 'age'), Attribute::string(key: 'nick', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
                documentSecurity: false,
            ));
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'user', 'age' => $tenant * 10, 'nick' => 'k'.$tenant]));
        }

        return $database;
    }
}
