<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
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

        $permissions = $stored->declaredPermissions();

        $this->assertNotNull($permissions);
        $this->assertContains('read("any", "name")', $permissions);
        $this->assertContains('read("team:hr", "salary")', $permissions);
        $this->assertContains('create("any")', $permissions);
    }

    /**
     * Regression: a column-scoped grant must not hide the collection from the role it names.
     *
     * Listing applies the permission gate, and two adapters answer it from the row's own
     * permission list rather than a side table of roles -- Postgres by JSONB containment,
     * Mongo by an exact `$in`. Both look for `read("any")`, so a stored `read("any", "name")`
     * matched nothing and the collection vanished for the very role it was granted to, while
     * getCollection() -- which carries no gate -- still returned it.
     */
    public function testACollectionWithAColumnScopedGrantIsStillListed(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->createCollection(Collection::create(
            id: 'columnGrantsListed',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name')],
        ));

        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            $ids = \array_map(
                static fn (Collection $collection): string => $collection->getId(),
                $database->listCollections(100),
            );
        } finally {
            $authorization->cleanRoles();
            foreach ($previous as $role) {
                $authorization->addRole($role);
            }
        }

        $this->assertContains('columnGrantsListed', $ids);
    }

    /**
     * The grants are stored as they were written; only the gate is narrowed to roles.
     */
    public function testAColumnScopedGrantIsReadBackAsItWasWritten(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsVerbatim',
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

        $stored = $database->getCollection('columnGrantsVerbatim');

        $this->assertSame([
            'read("any", "name")',
            'read("team:hr", "salary")',
            'create("any")',
        ], $stored->declaredPermissions());

        // The gate keeps the roles and drops the columns, which is what every adapter matches.
        $this->assertSame(['any', 'team:hr'], $stored->getPermissionsByType(PermissionType::Read));

        // And the column the gates read holds the projection, not the grants. Postgres matches
        // it by JSONB containment and Mongo by an exact `$in`, so a column stored here would
        // hide the collection from the role it names while getCollection() still returned it.
        $this->assertSame([
            'read("any")',
            'read("team:hr")',
            'create("any")',
        ], $stored->getPermissions());
    }

    /**
     * A collection naming no column stores what it has always stored: nothing is duplicated
     * for the collections that do not use the feature.
     */
    public function testACollectionWithoutColumnGrantsStoresNothingExtra(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsAbsent',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any()), Permission::create(Role::any())],
        ));

        $stored = $database->getCollection('columnGrantsAbsent');

        $this->assertSame(['read("any")', 'create("any")'], $stored->declaredPermissions());
        $this->assertSame([], $stored->getAttribute(Collection::COLUMN_PERMISSIONS));
    }

    /**
     * Dropping the last column-scoped grant also drops the list that used to describe it,
     * so a later read does not answer with permissions the collection no longer has.
     */
    public function testRemovingTheLastColumnGrantClearsTheStoredGrants(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = $database->createCollection(Collection::create(
            id: 'columnGrantsCleared',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name'), Permission::create(Role::any())],
        ));

        $this->assertSame(['read("any", "name")', 'create("any")'], $collection->declaredPermissions());
        $this->assertSame(['read("any")', 'create("any")'], $collection->getPermissions());

        $collection = $database->updateCollection('columnGrantsCleared', new CollectionUpdate(permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $this->assertSame(['read("any")', 'create("any")'], $collection->declaredPermissions());
        $this->assertSame(['read("any")', 'create("any")'], $collection->getPermissions());

        $stored = $database->getCollection('columnGrantsCleared');

        $this->assertSame([], $stored->getAttribute(Collection::COLUMN_PERMISSIONS));
        $this->assertSame(['read("any")', 'create("any")'], $stored->declaredPermissions());
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

        $updated = $database->getCollection('columnGrantsUpdate')->declaredPermissions();

        $this->assertNotNull($updated);
        $this->assertContains('read("any", "name")', $updated);

        try {
            $database->updateCollection('columnGrantsUpdate', new CollectionUpdate(permissions: [
                Permission::read(Role::any(), 'nope'),
            ]));
            $this->fail('Expected updateCollection() to reject a permission naming an unknown column');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Column "nope" does not exist.', $e->getMessage());
        }

        // The rejected update left the collection as it was.
        $updated = $database->getCollection('columnGrantsUpdate')->declaredPermissions();

        $this->assertNotNull($updated);
        $this->assertContains('read("any", "name")', $updated);
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
    /**
     * A row that records another collection's grants is refused like any other.
     *
     * Appwrite keeps one of these per table, and the grants it records name that table's
     * columns rather than the row's. There is no waiver for it: a column stored in a row's
     * permissions is enforced nowhere, and the read gate -- which matches roles, not grants --
     * would hide the row from the role it names. The caller splits them instead, keeping the
     * verbatim grants in a column of their own and the roles in the permissions.
     */
    public function testADocumentMayNotRecordAnotherCollectionsColumnPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsRecords',
            attributes: [
                Attribute::string(key: 'tableId', size: 64),
                Attribute::string(key: 'label', size: 64),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        try {
            $database->createDocument('columnGrantsRecords', new Document([
                '$id' => 'row',
                'tableId' => 'employees',
                'label' => 'Employees',
                '$permissions' => [
                    Permission::read(Role::any(), 'name'),
                    Permission::read(Role::team('hr'), 'salary'),
                ],
            ]));
            $this->fail('Expected createDocument() to reject columns belonging to another collection');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('is scoped to a column', $e->getMessage());
        }

        $this->assertTrue($database->getDocument('columnGrantsRecords', 'row')->isEmpty());
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
