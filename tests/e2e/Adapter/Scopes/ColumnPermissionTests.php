<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Filter;
use Utopia\Database\Mirror;
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

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
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
        )));

        $stored = $database->getCollection('columnGrants');

        $permissions = $stored->grants();

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

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsListed',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name')],
        )));

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

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
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
        )));

        $stored = $database->getCollection('columnGrantsVerbatim');

        $this->assertSame([
            'read("any", "name")',
            'read("team:hr", "salary")',
            'create("any")',
        ], $stored->grants());

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

        $this->assertSame(['read("any")', 'create("any")'], $stored->grants());
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

        $collection = $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsCleared',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name'), Permission::create(Role::any())],
        )));

        $this->assertSame(['read("any", "name")', 'create("any")'], $collection->grants());
        $this->assertSame(['read("any")', 'create("any")'], $collection->getPermissions());

        $collection = $database->updateCollection('columnGrantsCleared', new CollectionUpdate(permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]));

        $this->assertSame(['read("any")', 'create("any")'], $collection->grants());
        $this->assertSame(['read("any")', 'create("any")'], $collection->getPermissions());

        $stored = $database->getCollection('columnGrantsCleared');

        $this->assertSame([], $stored->getAttribute(Collection::COLUMN_PERMISSIONS));
        $this->assertSame(['read("any")', 'create("any")'], $stored->grants());
    }

    /**
     * The feature is off until the column is enforced.
     *
     * A scoped grant admits its role to the row -- that is what every gate matches -- and
     * nothing yet holds that role to the column, so the grant widens access rather than
     * narrowing it. Refused by default so no one writes one by accident; withColumnPermissions()
     * is the deliberate way in, and goes away once masking and query refusal land.
     */
    public function testAColumnScopedGrantIsRefusedUnlessAskedFor(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->createCollection(Collection::create(
                id: 'columnGrantsGated',
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::read(Role::any(), 'name')],
            ));
            $this->fail('Expected createCollection() to refuse a column-scoped grant');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Column-scoped permissions are not enabled', $e->getMessage());
        }

        $this->assertNull($database->findCollection('columnGrantsGated'));

        // An update may not introduce one either.
        $database->createCollection(Collection::create(
            id: 'columnGrantsGatedUpdate',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any())],
        ));

        try {
            $database->updateCollection('columnGrantsGatedUpdate', new CollectionUpdate(permissions: [
                Permission::read(Role::any(), 'name'),
            ]));
            $this->fail('Expected updateCollection() to refuse a column-scoped grant');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Column-scoped permissions are not enabled', $e->getMessage());
        }

        $this->assertSame(
            ['read("any")'],
            $database->getCollection('columnGrantsGatedUpdate')->grants(),
        );
    }

    /**
     * The toggle travels in a snapshot, like every other scoped flag.
     *
     * A snapshot captured inside the opt-in has to restore it, or a caller replaying that state
     * elsewhere -- another coroutine, a queued job -- is refused despite having captured the
     * permission to do exactly this.
     */
    public function testTheColumnPermissionsToggleSurvivesASnapshot(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if ($database instanceof Mirror) {
            // True of every toggle, not this one: replicate() propagates a snapshot explicitly
            // where it needs to, so there is nothing here for this test to pin.
            $this->markTestSkipped('Mirror does not override withSnapshot(), so a restored snapshot reaches the wrapper and not its source or destination.');
        }

        $snapshot = $database->withColumnPermissions(fn () => $database->snapshot());

        // The scope has closed, so the toggle is off again...
        $this->assertFalse($database->hasColumnPermissions());

        // ...but the snapshot still carries it.
        $database->withSnapshot($snapshot, fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsSnapshot',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name')],
        )));

        $this->assertContains(
            'read("any", "name")',
            $database->getCollection('columnGrantsSnapshot')->grants() ?? [],
        );
        $this->assertFalse($database->hasColumnPermissions());
    }

    public function testACollectionPermissionMayNotNameAColumnThatDoesNotExist(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
                id: 'columnGrantsUnknown',
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::read(Role::any(), 'nope')],
            )));
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

        $database->withColumnPermissions(fn () => $database->updateCollection('columnGrantsUpdate', new CollectionUpdate(permissions: [
            Permission::read(Role::any(), 'name'),
            Permission::create(Role::any()),
        ])));

        $updated = $database->getCollection('columnGrantsUpdate')->grants();

        $this->assertNotNull($updated);
        $this->assertContains('read("any", "name")', $updated);

        try {
            $database->withColumnPermissions(fn () => $database->updateCollection('columnGrantsUpdate', new CollectionUpdate(permissions: [
                Permission::read(Role::any(), 'nope'),
            ])));
            $this->fail('Expected updateCollection() to reject a permission naming an unknown column');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Column "nope" does not exist.', $e->getMessage());
        }

        // The rejected update left the collection as it was.
        $updated = $database->getCollection('columnGrantsUpdate')->grants();

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

    /**
     * The shape a consumer keeping a list of its collections has to use.
     *
     * This mirrors Appwrite: a `database_N` collection whose rows each describe one table,
     * carrying that table's grants so the console can show and edit them.
     * Those grants name the table's columns, not the row's, so they cannot live in the row's
     * permissions: the library refuses them there, and every read gate matches the role form
     * anyway, so a column stored in a row's permissions would hide the row from the role it
     * names. The consumer splits them -- roles gate the row, the grants ride in a column of
     * their own -- which is what the library does for a collection definition, by hand.
     */
    public function testAppwriteCollectionsSimulation(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        // The table itself, under the id Appwrite gives it. Its grants name its own columns.
        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'database_1_collection_1',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
            ],
        )));

        $grants = $database->getCollection('database_1_collection_1')->grants();
        $this->assertNotNull($grants);

        // Appwrite's list of the database's tables, one row per table, shaped like the real
        // thing: databaseId beside a columnPermissions column. Neither resembles the columns
        // the grants it records name.
        $database->createCollection(Collection::create(
            id: 'database_1',
            attributes: [
                Attribute::string(key: 'databaseId', size: Database::LENGTH_KEY),
                Attribute::string(key: 'columnPermissions', size: 1_000_000, filters: [Filter::Json]),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        // Putting them in the row's permissions is refused.
        try {
            $database->createDocument('database_1', new Document([
                '$id' => 'wrong',
                'databaseId' => 'database_1',
                'columnPermissions' => $grants,
                '$permissions' => $grants,
            ]));
            $this->fail('Expected createDocument() to reject grants naming another collection\'s columns');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('is scoped to a column', $e->getMessage());
        }

        // Splitting them is not.
        // The row's id is the table's id, as Appwrite writes it.
        $database->createDocument('database_1', new Document([
            '$id' => 'database_1_collection_1',
            'databaseId' => 'database_1',
            'columnPermissions' => $grants,
            '$permissions' => Permission::withoutColumns($grants),
        ]));

        $stored = $database->getDocument('database_1', 'database_1_collection_1');

        // The grants come back as the table has them, ready to report to a caller...
        $this->assertSame($grants, $stored->getAttribute('columnPermissions'));

        // ...while the row answers the gate with roles alone.
        $this->assertSame(['any', 'team:hr'], $stored->getPermissionsByType(PermissionType::Read));

        // So a role holding nothing but a column-scoped grant still finds the row it describes.
        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::team('hr')->toString());

        try {
            $ids = \array_map(
                static fn (Document $document): string => $document->getId(),
                $database->find('database_1'),
            );
        } finally {
            $authorization->cleanRoles();
            foreach ($previous as $role) {
                $authorization->addRole($role);
            }
        }

        $this->assertSame(['database_1_collection_1'], $ids);
    }

    /**
     * Every write path refuses it, not just createDocument().
     *
     * The rule lived inline in prepareDocument() at first, which only createDocument() reaches,
     * so a batch create, an update or an upsert stored the grant happily -- and a row holding a
     * column in its permissions is hidden from the role it names by the gates that match that
     * field directly. One case per entry point, so the rule cannot quietly shrink back to one.
     */
    public function testEveryWritePathRefusesAColumnScopedPermission(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(
            id: 'columnGrantsWrites',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));

        $database->createDocument('columnGrantsWrites', new Document([
            '$id' => 'row',
            'name' => 'Ada',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $scoped = [Permission::read(Role::any(), 'name')];

        $writes = [
            'createDocument' => fn () => $database->createDocument('columnGrantsWrites', new Document([
                '$id' => 'created', 'name' => 'x', '$permissions' => $scoped,
            ])),
            'createDocuments' => fn () => $database->createDocuments('columnGrantsWrites', [new Document([
                '$id' => 'batched', 'name' => 'x', '$permissions' => $scoped,
            ])]),
            'updateDocument' => fn () => $database->updateDocument('columnGrantsWrites', 'row', new Document([
                '$permissions' => $scoped,
            ])),
            'updateDocuments' => fn () => $database->updateDocuments('columnGrantsWrites', new Document([
                '$permissions' => $scoped,
            ])),
        ];

        // The upsert paths share one guard, and an adapter without upserts refuses the call
        // before reaching it.
        if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $writes['upsertDocument'] = fn () => $database->upsertDocument('columnGrantsWrites', new Document([
                '$id' => 'upserted', 'name' => 'x', '$permissions' => $scoped,
            ]));
            $writes['upsertDocuments'] = fn () => $database->upsertDocuments('columnGrantsWrites', [new Document([
                '$id' => 'upserted', 'name' => 'x', '$permissions' => $scoped,
            ])]);
        }

        foreach ($writes as $path => $write) {
            try {
                $write();
                $this->fail($path.'() stored a column-scoped permission on a row');
            } catch (DatabaseException $e) {
                $this->assertStringContainsString('is scoped to a column', $e->getMessage(), $path);
            }
        }

        // None of them left anything behind, and the row it tried to rewrite is untouched.
        $this->assertSame(
            ['read("any")', 'update("any")'],
            $database->getDocument('columnGrantsWrites', 'row')->getPermissions(),
        );
        $this->assertTrue($database->getDocument('columnGrantsWrites', 'created')->isEmpty());
        $this->assertTrue($database->getDocument('columnGrantsWrites', 'batched')->isEmpty());

        if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $this->assertTrue($database->getDocument('columnGrantsWrites', 'upserted')->isEmpty());
        }
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
