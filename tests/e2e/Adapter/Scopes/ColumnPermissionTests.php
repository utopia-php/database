<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Permission;
use Utopia\Database\Role;

/**
 * Column permissions are set on a collection, never on a document.
 *
 * A grant may name one of the collection's columns, and then covers that column alone.
 * A grant naming no column covers every column, which is what every grant has always
 * meant and what every existing collection keeps meaning.
 */
trait ColumnPermissionTests
{
    public function testACollectionPermissionMayNameOneOfItsColumns(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrants',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
                Permission::create(Role::any()),
            ],
        ));

        $stored = $database->getCollection('columnGrants');

        $this->assertContains('read("any", "name")', $stored->getPermissions());
        $this->assertContains('read("team:hr", "salary")', $stored->getPermissions());
        $this->assertContains('create("any")', $stored->getPermissions());
    }

    public function testACollectionPermissionMayNotNameAColumnThatDoesNotExist(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->createCollection(Collection::create(
                id: 'columnGrantsUnknown',
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::read(Role::any(), 'nope')],
            ));
            $this->fail('Expected createCollection() to reject a permission naming an unknown column');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Column "nope" does not exist.', $e->getMessage());
        }
    }

    public function testUpdatingACollectionJudgesAColumnGrantAgainstItsOwnColumns(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsUpdate',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [Permission::create(Role::any())],
        ));

        $database->updateCollection('columnGrantsUpdate', new CollectionUpdate(permissions: [
            Permission::read(Role::any(), 'name'),
            Permission::create(Role::any()),
        ]));

        $this->assertContains(
            'read("any", "name")',
            $database->getCollection('columnGrantsUpdate')->getPermissions(),
        );

        try {
            $database->updateCollection('columnGrantsUpdate', new CollectionUpdate(permissions: [
                Permission::read(Role::any(), 'nope'),
            ]));
            $this->fail('Expected updateCollection() to reject a permission naming an unknown column');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Column "nope" does not exist.', $e->getMessage());
        }

        // The rejected update left the collection as it was.
        $this->assertContains(
            'read("any", "name")',
            $database->getCollection('columnGrantsUpdate')->getPermissions(),
        );
    }

    public function testADocumentPermissionMayNotNameAColumn(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsDocument',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        // Column permissions belong to the collection. A document saying otherwise is
        // refused rather than stored and ignored, which would read as a restriction that
        // silently does nothing.
        try {
            $database->createDocument('columnGrantsDocument', new Document([
                '$id' => 'row',
                'name' => 'Ada',
                'salary' => 120000,
                '$permissions' => [Permission::read(Role::any(), 'salary')],
            ]));
            $this->fail('Expected createDocument() to reject a column-scoped permission');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('is scoped to a column', $e->getMessage());
        }

        // The rejected write left nothing behind.
        $this->assertTrue($database->getDocument('columnGrantsDocument', 'row')->isEmpty());
    }

    public function testADocumentMayRecordAnotherCollectionsColumnPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        // Stands in for a row a caller keeps another collection's settings in -- Appwrite's
        // table list does this. Its own columns are nothing like the ones its permissions name.
        $database->createCollection(Collection::create(
            id: 'columnGrantsRecords',
            attributes: [
                Attribute::string(key: 'tableId', size: 64),
                Attribute::string(key: 'label', size: 64),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        $recorded = [
            Permission::read(Role::any(), 'name'),
            Permission::read(Role::team('hr'), 'salary'),
        ];

        try {
            $database->createDocument('columnGrantsRecords', new Document([
                '$id' => 'row',
                'tableId' => 'employees',
                'label' => 'Employees',
                '$permissions' => $recorded,
            ]));
            $this->fail('Expected createDocument() to reject columns belonging to another collection');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('is scoped to a column', $e->getMessage());
        }

        $database->withForeignColumnPermissions(fn () => $database->createDocument('columnGrantsRecords', new Document([
            '$id' => 'row',
            'tableId' => 'employees',
            'label' => 'Employees',
            '$permissions' => $recorded,
        ])));

        $stored = $database->getDocument('columnGrantsRecords', 'row');

        $this->assertSame('employees', $stored->getAttribute('tableId'));
        $this->assertContains('read("any", "name")', $stored->getPermissions());
        $this->assertContains('read("team:hr", "salary")', $stored->getPermissions());

        // Scoped to the callback: the next write is judged as strictly as the first.
        $this->assertFalse($database->hasForeignColumnPermissions());
    }

    public function testARecordedColumnGrantStillDecidesRowVisibilityByRoleAlone(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        // Appwrite's table list: each row records one table's permissions, column grants and
        // all. Whether a caller may see the row is a question about the role alone, so a column
        // narrows what the permission reaches without narrowing who holds it.
        $database->createCollection(Collection::create(
            id: 'columnGrantsListing',
            attributes: [Attribute::string(key: 'tableId', size: 64)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        $database->withForeignColumnPermissions(function () use ($database) {
            $database->createDocument('columnGrantsListing', new Document([
                '$id' => 'employees',
                'tableId' => 'employees',
                '$permissions' => [
                    Permission::read(Role::any(), 'name'),
                    Permission::read(Role::team('hr'), 'salary'),
                ],
            ]));
            $database->createDocument('columnGrantsListing', new Document([
                '$id' => 'invoices',
                'tableId' => 'invoices',
                '$permissions' => [Permission::read(Role::team('finance'))],
            ]));
        });

        $listed = function (array $roles) use ($database, $authorization): array {
            $previous = $authorization->getRoles();
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }

            try {
                $ids = \array_map(
                    static fn (Document $document): string => $document->getId(),
                    $database->find('columnGrantsListing'),
                );
            } finally {
                $authorization->cleanRoles();
                foreach ($previous as $role) {
                    $authorization->addRole($role);
                }
            }

            \sort($ids);

            return $ids;
        };

        $this->assertSame(
            ['employees'],
            $listed([Role::any()->toString()]),
            'A grant scoped to a column still admits its role to the row',
        );
        $this->assertSame(
            ['invoices'],
            $listed([Role::team('finance')->toString()]),
            'A role named by no grant on a row does not see it',
        );
    }

    public function testADocumentPermissionNamingNoColumnIsUnaffected(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsPlain',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        $database->createDocument('columnGrantsPlain', new Document([
            '$id' => 'row',
            'name' => 'Ada',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::user('alice')),
            ],
        ]));

        $stored = $database->getDocument('columnGrantsPlain', 'row');

        $this->assertSame('Ada', $stored->getAttribute('name'));
        $this->assertContains('read("any")', $stored->getPermissions());
        $this->assertContains('update("user:alice")', $stored->getPermissions());
    }
}
