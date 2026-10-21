<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Attribute\Update as AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Collection\Update as CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Filter;
use Utopia\Database\Mirror;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Relationship\Update as RelationshipUpdate;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

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

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsListed',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name')],
        )));

        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            // Paged: the listing has no order, so it answers with the oldest collections
            // first and this one is created last. A single page would stop finding it once
            // the suite grows past it, which says nothing about the grant under test.
            $ids = [];

            for ($offset = 0; ; $offset += 100) {
                $page = $database->listCollections(100, $offset);

                foreach ($page as $collection) {
                    $ids[] = $collection->getId();
                }

                if (\count($page) < 100) {
                    break;
                }
            }
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

        $collection = $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsCleared',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::any(), 'name'), Permission::create(Role::any())],
        )));

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
            $database->getCollection('columnGrantsGatedUpdate')->declaredPermissions(),
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
            $database->getCollection('columnGrantsSnapshot')->declaredPermissions() ?? [],
        );
        $this->assertFalse($database->hasColumnPermissions());
    }

    /**
     * Only read and update may name a column, and createCollection() enforces it.
     *
     * A create brings a whole row into being and a delete removes one, so neither is narrowed
     * by naming a column, and write aggregates to all three. The rule is the validator's; this
     * pins that the collection write actually runs it.
     */
    public function testACollectionPermissionMayNotScopeCreateDeleteOrWrite(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        foreach (['create', 'delete', 'write'] as $type) {
            try {
                $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
                    id: 'columnGrantsWholeRow'.$type,
                    attributes: [Attribute::string(key: 'name', size: 64)],
                    permissions: [Permission::$type(Role::any(), 'name')],
                )));
                $this->fail('Expected createCollection() to reject a column-scoped "'.$type.'"');
            } catch (DatabaseException $e) {
                $this->assertStringContainsString(
                    'Permission "'.$type.'" cannot be scoped to a column, it applies to the whole row.',
                    $e->getMessage(),
                );
            }
        }
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

        $updated = $database->getCollection('columnGrantsUpdate')->declaredPermissions();

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

        $grants = $database->getCollection('database_1_collection_1')->declaredPermissions();
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

    /**
     * Asking for everything is not the same as naming a column you may not read.
     *
     * select(['*']) and a read with no select are the same request -- "whatever I am entitled
     * to" -- and neither names a column, so neither is refused; the caller is given what their
     * grants reach. Naming the column is what gets refused, even alongside one they may read,
     * because a query that names a column answers questions about it whether or not the value
     * comes back.
     */
    public function testSelectingEverythingIsNotRefusedButNamingAnUngrantedColumnIs(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsSelect',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
            ],
        )));

        $authorization->skip(fn () => $database->createDocument('columnGrantsSelect', new Document([
            '$id' => 'e1',
            'name' => 'Ada',
            'salary' => 120000,
        ])));

        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            // Asking for the lot, however it is spelled: not refused, but answered with only
            // what the grant reaches. Refusing '*' would make a plain read unusable, so the
            // answer is shaped instead -- the other half of the gate.
            foreach ([[Query::select(['*'])], []] as $queries) {
                $rows = $database->find('columnGrantsSelect', $queries);

                $this->assertCount(1, $rows);
                $this->assertSame('Ada', $rows[0]->getAttribute('name'));
                $this->assertFalse($rows[0]->offsetExists('salary'), 'select(*) returned an ungranted column');
            }

            // Naming only what the grant reaches.
            $this->assertCount(1, $database->find('columnGrantsSelect', [Query::select(['name'])]));

            // Naming one it does not, even beside one it does.
            foreach ([['salary'], ['name', 'salary']] as $selection) {
                try {
                    $database->find('columnGrantsSelect', [Query::select($selection)]);
                    $this->fail('Expected select('.\implode(', ', $selection).') to be refused');
                } catch (AuthorizationException $e) {
                    $this->assertStringContainsString('Missing "read" permission for column "salary"', $e->getMessage());
                }
            }
        } finally {
            $authorization->cleanRoles();
            foreach ($previous as $role) {
                $authorization->addRole($role);
            }
        }
    }

    /**
     * A query naming a column the caller cannot read is refused, wherever it names it.
     *
     * Returning the rows would answer the question the filter asks whether or not the value
     * comes back -- Query::greaterThan() turns that into a binary search -- and returning none
     * is no better, since an empty result is a signal too and a wrong answer besides. Ordering
     * leaks the order, and an aggregate leaks the aggregate.
     */
    public function testAQueryNamingAnUngrantedColumnIsRefused(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsQueries',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
            ],
        )));

        $authorization->skip(fn () => $database->createDocument('columnGrantsQueries', new Document([
            '$id' => 'e1',
            'name' => 'Ada',
            'salary' => 120000,
        ])));

        $refused = [
            'filter' => fn () => $database->find('columnGrantsQueries', [Query::equal('salary', [120000])]),
            'range filter' => fn () => $database->find('columnGrantsQueries', [Query::greaterThan('salary', 1)]),
            'order' => fn () => $database->find('columnGrantsQueries', [Query::orderDesc('salary')]),
            'select' => fn () => $database->find('columnGrantsQueries', [Query::select(['salary'])]),
            'count' => fn () => $database->count('columnGrantsQueries', [Query::equal('salary', [120000])]),
            'sum' => fn () => $database->sum('columnGrantsQueries', 'salary'),
        ];

        // An adapter without aggregates rejects max() as a method before the column is reached,
        // which is the right order: an unsupported query is not a permission question.
        if ($database->getAdapter()->supports(Capability::Aggregations)) {
            $refused['aggregate'] = fn () => $database->aggregate('columnGrantsQueries', [Query::max('salary')]);
        }

        $this->asRole($authorization, Role::any()->toString(), function () use ($refused, $database): void {
            foreach ($refused as $what => $run) {
                try {
                    $run();
                    $this->fail($what.' on an ungranted column was allowed');
                } catch (AuthorizationException $e) {
                    $this->assertStringContainsString('Missing "read" permission for column "salary"', $e->getMessage(), $what);
                }
            }

            // The column the grant does reach is queryable as usual.
            $this->assertCount(1, $database->find('columnGrantsQueries', [Query::equal('name', ['Ada'])]));
        });

        // And it is per role, not per column: hr reaches salary and not name.
        $this->asRole($authorization, Role::team('hr')->toString(), function () use ($database): void {
            $this->assertSame(120000, $database->sum('columnGrantsQueries', 'salary'));

            try {
                $database->find('columnGrantsQueries', [Query::equal('name', ['Ada'])]);
                $this->fail('hr filtered on a column it was not granted');
            } catch (AuthorizationException $e) {
                $this->assertStringContainsString('Missing "read" permission for column "name"', $e->getMessage());
            }
        });
    }

    /**
     * A write touching a column the caller may not change is refused, and refused whole.
     *
     * Applying the part they are allowed and dropping the rest would leave a document neither
     * the caller nor the collection asked for.
     */
    public function testAWriteTouchingAnUngrantedColumnIsRefused(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsWritesScoped',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any(), 'name'),
                Permission::update(Role::team('hr'), 'salary'),
            ],
        )));

        $authorization->skip(fn () => $database->createDocument('columnGrantsWritesScoped', new Document([
            '$id' => 'e1',
            'name' => 'Ada',
            'salary' => 120000,
        ])));

        $this->asRole($authorization, Role::any()->toString(), function () use ($database): void {
            $this->assertSame('Grace', $database->updateDocument('columnGrantsWritesScoped', 'e1', new Document([
                'name' => 'Grace',
            ]))->getAttribute('name'));

            foreach (['salary alone' => ['salary' => 1], 'both at once' => ['name' => 'X', 'salary' => 2]] as $what => $data) {
                try {
                    $database->updateDocument('columnGrantsWritesScoped', 'e1', new Document($data));
                    $this->fail($what.' was allowed');
                } catch (AuthorizationException $e) {
                    $this->assertStringContainsString('Missing "update" permission for column "salary"', $e->getMessage(), $what);
                }
            }
        });

        // Refused whole: the granted half of "both at once" was not applied either.
        $stored = $authorization->skip(fn () => $database->getDocument('columnGrantsWritesScoped', 'e1'));
        $this->assertSame('Grace', $stored->getAttribute('name'));
        $this->assertSame(120000, $stored->getAttribute('salary'));
    }

    /**
     * Run the callback holding one role and nothing else.
     */
    private function asRole(Authorization $authorization, string $role, callable $callback): void
    {
        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole($role);

        try {
            $callback();
        } finally {
            $authorization->cleanRoles();
            foreach ($previous as $held) {
                $authorization->addRole($held);
            }
        }
    }

    /**
     * A read that names nothing comes back holding what the caller was granted.
     *
     * The mirror of the refusal: naming an ungranted column throws, asking for everything does
     * not, so the answer is shaped instead. The key is removed rather than nulled, since null
     * would be indistinguishable from a column that genuinely holds null.
     */
    public function testAReadComesBackHoldingOnlyTheGrantedColumns(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsMasked',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
                Permission::read(Role::team('exec')),
            ],
        )));

        $authorization->skip(fn () => $database->createDocument('columnGrantsMasked', new Document([
            '$id' => 'e1',
            'name' => 'Ada',
            'salary' => 120000,
        ])));

        $columns = static fn (Document $document): array => \array_keys(\array_intersect_key(
            $document->getArrayCopy(),
            ['name' => true, 'salary' => true],
        ));

        // Granted one column: the other is gone, not nulled.
        $this->asRole($authorization, Role::any()->toString(), function () use ($database, $columns): void {
            $stored = $database->getDocument('columnGrantsMasked', 'e1');

            $this->assertSame(['name'], $columns($stored));
            $this->assertSame('Ada', $stored->getAttribute('name'));
            $this->assertFalse($stored->offsetExists('salary'), 'salary came back for a role granted only name');

            // find() and select('*') shape the same way, and the system attributes survive.
            $this->assertSame(['name'], $columns($database->find('columnGrantsMasked')[0]));
            $this->assertSame(['name'], $columns($database->find('columnGrantsMasked', [Query::select(['*'])])[0]));
            $this->assertSame('e1', $database->find('columnGrantsMasked')[0]->getId());
        });

        // A different role, a different column.
        $this->asRole($authorization, Role::team('hr')->toString(), function () use ($database, $columns): void {
            $this->assertSame(['salary'], $columns($database->getDocument('columnGrantsMasked', 'e1')));
        });

        // A whole-row grant names no column, so nothing is withheld from it.
        $this->asRole($authorization, Role::team('exec')->toString(), function () use ($database, $columns): void {
            $this->assertSame(['name', 'salary'], $columns($database->getDocument('columnGrantsMasked', 'e1')));
        });

        // Internal reads are not shaped, or the library would lose columns it needs to write.
        $this->assertSame(
            ['name', 'salary'],
            $columns($authorization->skip(fn () => $database->getDocument('columnGrantsMasked', 'e1'))),
        );
    }

    /**
     * A write hands its result back the same way a read does.
     *
     * An update reads the row to merge the patch onto it, so its response carries every column
     * the row holds -- including the ones the caller may not read. The write itself is judged
     * separately: a role may be allowed to change a column it cannot see, and then the change
     * lands while the answer still withholds it.
     */
    public function testAWriteComesBackHoldingOnlyTheGrantedColumns(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsWritten',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::update(Role::team('exec')),
                Permission::read(Role::team('exec')),
            ],
        )));

        $columns = static fn (Document $document): array => \array_keys(\array_intersect_key(
            $document->getArrayCopy(),
            ['name' => true, 'salary' => true],
        ));

        $this->asRole($authorization, Role::any()->toString(), function () use ($database, $columns): void {
            $created = $database->createDocument('columnGrantsWritten', new Document([
                '$id' => 'w1',
                'name' => 'Ada',
                'salary' => 120000,
            ]));
            $this->assertSame(['name'], $columns($created), 'createDocument() handed back an ungranted column');

            $updated = $database->updateDocument('columnGrantsWritten', 'w1', new Document([
                'name' => 'Ada Lovelace',
            ]));
            $this->assertSame(['name'], $columns($updated), 'updateDocument() handed back an ungranted column');
            $this->assertSame('Ada Lovelace', $updated->getAttribute('name'));

            if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
                $upserted = $database->upsertDocument('columnGrantsWritten', new Document([
                    '$id' => 'w1',
                    'name' => 'Ada King',
                ]));
                $this->assertSame(['name'], $columns($upserted), 'upsertDocument() handed back an ungranted column');
            }

            // A counter may be bumped by a role that cannot read it, and the answer still withholds it.
            $increased = $database->increaseDocumentAttribute('columnGrantsWritten', 'w1', 'salary', 1000);
            $this->assertSame(['name'], $columns($increased), 'increaseDocumentAttribute() handed back an ungranted column');

            $decreased = $database->decreaseDocumentAttribute('columnGrantsWritten', 'w1', 'salary', 500);
            $this->assertSame(['name'], $columns($decreased), 'decreaseDocumentAttribute() handed back an ungranted column');
        });

        // Shaping the answer is not editing the row: every write above landed in full.
        $stored = $authorization->skip(fn () => $database->getDocument('columnGrantsWritten', 'w1'));
        $this->assertSame(['name', 'salary'], $columns($stored));
        $this->assertSame(120500, $stored->getAttribute('salary'));

        // A whole-row grant names no column, so nothing is withheld from it.
        $this->asRole($authorization, Role::team('exec')->toString(), function () use ($database, $columns): void {
            $this->assertSame(
                ['name', 'salary'],
                $columns($database->updateDocument('columnGrantsWritten', 'w1', new Document(['name' => 'Ada']))),
            );
        });
    }

    /**
     * A batch write reports each row through onNext, which is the same way out.
     *
     * Both documents it is handed are shaped: the one just written and the one it replaced,
     * since the previous row is read from storage in full.
     */
    public function testABatchWriteHandsBackOnlyTheGrantedColumns(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsBatched',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
                Permission::read(Role::any(), 'name'),
            ],
        )));

        $columns = static fn (Document $document): array => \array_keys(\array_intersect_key(
            $document->getArrayCopy(),
            ['name' => true, 'salary' => true],
        ));

        // Each write gets its own collector, so what one reported cannot carry into the next.
        $shapesFrom = static function (callable $write) use ($columns): array {
            /** @var list<list<string>> $seen */
            $seen = [];

            $write(static function (Document $document, ?Document $previous) use (&$seen, $columns): void {
                $seen[] = $columns($document);

                if ($previous !== null) {
                    $seen[] = $columns($previous);
                }
            });

            return $seen;
        };

        $this->asRole($authorization, Role::any()->toString(), function () use ($database, $shapesFrom): void {
            $this->assertSame(
                [['name'], ['name']],
                $shapesFrom(static fn (callable $onNext) => $database->createDocuments('columnGrantsBatched', [
                    new Document(['$id' => 'b1', 'name' => 'Ada', 'salary' => 120000]),
                    new Document(['$id' => 'b2', 'name' => 'Grace', 'salary' => 130000]),
                ], onNext: $onNext)),
                'createDocuments() reported an ungranted column',
            );

            $this->assertSame(
                [['name'], ['name'], ['name'], ['name']],
                $shapesFrom(static fn (callable $onNext) => $database->updateDocuments(
                    'columnGrantsBatched',
                    new Document(['name' => 'Redacted']),
                    onNext: $onNext,
                )),
                'updateDocuments() reported an ungranted column',
            );

            if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
                $this->assertSame(
                    [['name'], ['name']],
                    $shapesFrom(static fn (callable $onNext) => $database->upsertDocuments('columnGrantsBatched', [
                        new Document(['$id' => 'b1', 'name' => 'Ada', 'salary' => 1]),
                    ], onNext: $onNext)),
                    'upsertDocuments() reported an ungranted column',
                );
            }

            $this->assertSame(
                [['name'], ['name'], ['name'], ['name']],
                $shapesFrom(static fn (callable $onNext) => $database->deleteDocuments('columnGrantsBatched', onNext: $onNext)),
                'deleteDocuments() reported an ungranted column',
            );
        });
    }

    /**
     * A role under a column restriction may not join.
     *
     * A join names columns in a second collection under an alias, and returns them beside the
     * base row. The gate reads those names against the base collection and the mask only knows
     * the base collection, so a join would be judged against the wrong schema and its columns
     * would come back unshaped. Refusing is the honest answer until both halves follow aliases.
     *
     * A whole-row grant is unrestricted and joins exactly as it always did.
     */
    public function testARestrictedRoleMayNotJoin(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->markTestSkipped('Adapter does not support joins');
        }

        $authorization = $database->getAuthorization();

        $database->createCollection(Collection::create(
            id: 'columnGrantsJoined',
            attributes: [
                Attribute::string(key: 'mainId', size: 64),
                Attribute::string(key: 'note', size: 64)
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                // asRole() replaces the role list, so exec holds nothing that "any" would cover.
                Permission::read(Role::team('exec')),
            ],
        ));

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsJoinBase',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('exec')),
            ],
        )));

        $authorization->skip(function () use ($database): void {
            $database->createDocument('columnGrantsJoinBase', new Document(['$id' => 'm1', 'name' => 'Ada', 'salary' => 120000]));
            $database->createDocument('columnGrantsJoined', new Document(['$id' => 's1', 'mainId' => 'm1', 'note' => 'hi']));
        });

        $this->asRole($authorization, Role::any()->toString(), function () use ($database): void {
            try {
                $database->find('columnGrantsJoinBase', [
                    Query::join('columnGrantsJoined', 'entry', [Query::on('$id', 'mainId')]),
                ]);
                $this->fail('a column-restricted role was allowed to join');
            } catch (QueryException $e) {
                $this->assertStringContainsString('Join queries are not supported', $e->getMessage());
            }

            // The refusal does not mention a column, because the join names none of the base ones.
            try {
                $database->find('columnGrantsJoinBase', [
                    Query::join('columnGrantsJoined', 'entry', [Query::on('$id', 'mainId')]),
                    Query::equal('entry.note', ['hi']),
                ]);
                $this->fail('a column-restricted role was allowed to join and filter the joined collection');
            } catch (QueryException $e) {
                $this->assertStringNotContainsString('columnGrantsJoined', $e->getMessage());
            }
        });

        // A whole-row grant is not restricted, so joins keep working for it.
        $this->asRole($authorization, Role::team('exec')->toString(), function () use ($database): void {
            $rows = $database->find('columnGrantsJoinBase', [
                Query::join('columnGrantsJoined', 'entry', [Query::on('$id', 'mainId')]),
            ]);

            $this->assertCount(1, $rows);
            $this->assertSame('Ada', $rows[0]->getAttribute('name'));
            $this->assertSame(120000, $rows[0]->getAttribute('salary'));
        });

        // The other direction: an open collection must not be used as a way into a restricted
        // one. The base names no column, so nothing about it is refused -- the join is.
        $database->createCollection(Collection::create(
            id: 'columnGrantsJoinOpen',
            attributes: [Attribute::string(key: 'baseId', size: 64), Attribute::string(key: 'label', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        $authorization->skip(fn () => $database->createDocument('columnGrantsJoinOpen', new Document([
            '$id' => 'o1',
            'baseId' => 'm1',
            'label' => 'row',
        ])));

        $this->asRole($authorization, Role::any()->toString(), function () use ($database): void {
            try {
                $database->find('columnGrantsJoinOpen', [
                    Query::join('columnGrantsJoinBase', 'secret', [Query::on('baseId', '$id')]),
                ]);
                $this->fail('an open collection was joined into a column-restricted one');
            } catch (QueryException $e) {
                $this->assertStringContainsString('Join queries are not supported', $e->getMessage());
            }

            // And filtering on the restricted column through the alias is refused with it.
            try {
                $database->find('columnGrantsJoinOpen', [
                    Query::join('columnGrantsJoinBase', 'secret', [Query::on('baseId', '$id')]),
                    Query::equal('secret.salary', [120000]),
                ]);
                $this->fail('a restricted column was filtered through a join alias');
            } catch (QueryException $e) {
                $this->assertStringContainsString('Join queries are not supported', $e->getMessage());
            }

            // Without the join the open collection reads as it always did.
            $this->assertCount(1, $database->find('columnGrantsJoinOpen'));
        });
    }

    /**
     * A related document is shaped by its own collection's grants.
     *
     * Population reads the related collection through the same find() and getDocument() the
     * caller would, so the mask reaches it and the grants that apply are the related
     * collection's own, not the parent's. Pinned because a population that ever read rows
     * directly from the adapter would hand back everything and nothing else would notice.
     */
    public function testARelatedDocumentIsMaskedByItsOwnCollectionsGrants(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->markTestSkipped('Adapter does not support relationships');
        }

        $authorization = $database->getAuthorization();

        // The parent names no column, so nothing of its own is withheld.
        $database->createCollection(Collection::create(
            id: 'columnGrantsParent',
            attributes: [Attribute::string(key: 'label', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                // asRole() replaces the role list, so exec holds nothing that "any" would cover.
                Permission::read(Role::team('exec')),
            ],
        ));

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsRelated',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::read(Role::team('exec')),
            ],
        )));

        $database->createRelationship(
            'columnGrantsParent',
            Relationship::oneToOne(relatedCollection: 'columnGrantsRelated', key: 'related'),
        );

        $authorization->skip(function () use ($database): void {
            $database->createDocument('columnGrantsRelated', new Document([
                '$id' => 'r1',
                'name' => 'Ada',
                'salary' => 120000,
            ]));
            $database->createDocument('columnGrantsParent', new Document([
                '$id' => 'p1',
                'label' => 'row',
                'related' => 'r1',
            ]));
        });

        $this->asRole($authorization, Role::any()->toString(), function () use ($database): void {
            foreach ([
                'getDocument' => $database->getDocument('columnGrantsParent', 'p1'),
                'find' => $database->find('columnGrantsParent')[0],
            ] as $what => $parent) {
                // The parent is untouched: it carries no column grant of its own.
                $this->assertSame('row', $parent->getAttribute('label'), $what);

                $related = $parent->getAttribute('related');
                $this->assertInstanceOf(Document::class, $related, $what);
                $this->assertSame('Ada', $related->getAttribute('name'), $what);
                $this->assertFalse(
                    $related->offsetExists('salary'),
                    $what.'() returned an ungranted column of the related collection',
                );
            }
        });

        // A whole-row grant on the related collection withholds nothing from it.
        $this->asRole($authorization, Role::team('exec')->toString(), function () use ($database): void {
            $related = $database->getDocument('columnGrantsParent', 'p1')->getAttribute('related');

            $this->assertInstanceOf(Document::class, $related);
            $this->assertSame(120000, $related->getAttribute('salary'));
        });
    }

    /**
     * Every write path refuses a column the caller may not change, not just updateDocument().
     *
     * An upsert onto a stored row is an update, and a counter changes one column like any other
     * write of it, so each is judged the same way. Pinned per path because the gate is a call at
     * each of them rather than one choke point: a new write path added without it would be a
     * hole nothing else notices.
     */
    public function testEveryWritePathRefusesAnUngrantedColumn(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsWriteGate',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any(), 'name'),
            ],
        )));

        $authorization->skip(fn () => $database->createDocument('columnGrantsWriteGate', new Document([
            '$id' => 'g1',
            'name' => 'Ada',
            'salary' => 100,
        ])));

        $refused = [
            'updateDocument' => fn () => $database->updateDocument('columnGrantsWriteGate', 'g1', new Document(['salary' => 1])),
            'updateDocuments' => fn () => $database->updateDocuments('columnGrantsWriteGate', new Document(['salary' => 2])),
            'increaseDocumentAttribute' => fn () => $database->increaseDocumentAttribute('columnGrantsWriteGate', 'g1', 'salary', 7),
            'decreaseDocumentAttribute' => fn () => $database->decreaseDocumentAttribute('columnGrantsWriteGate', 'g1', 'salary', 7),
        ];

        if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $refused['upsertDocument'] = fn () => $database->upsertDocument('columnGrantsWriteGate', new Document(['$id' => 'g1', 'salary' => 5]));
            $refused['upsertDocuments'] = fn () => $database->upsertDocuments('columnGrantsWriteGate', [new Document(['$id' => 'g1', 'salary' => 6])]);
        }

        $this->asRole($authorization, Role::any()->toString(), function () use ($refused, $database): void {
            foreach ($refused as $what => $run) {
                try {
                    $run();
                    $this->fail($what.' changed a column the caller was not granted');
                } catch (AuthorizationException $e) {
                    $this->assertStringContainsString('Missing "update" permission for column "salary"', $e->getMessage(), $what);
                }
            }

            // The granted column is writable through the same paths.
            $this->assertSame(
                'Ada Lovelace',
                $database->updateDocument('columnGrantsWriteGate', 'g1', new Document(['name' => 'Ada Lovelace']))->getAttribute('name'),
            );
        });

        // Nothing the refusals touched was stored.
        $stored = $authorization->skip(fn () => $database->getDocument('columnGrantsWriteGate', 'g1'));

        $this->assertSame(100, $stored->getAttribute('salary'));
        $this->assertSame('Ada Lovelace', $stored->getAttribute('name'));
    }

    /**
     * A column-scoped updater may not rewrite the row's permissions.
     *
     * The row's permissions are not a column, and rewriting them is a whole-row act. Left
     * open, a caller granted one column could grant itself delete through them and destroy the
     * row, turning a grant over one column into power over all of it. The mask would still
     * hold -- it reads the collection's grants, not the row's -- so the escalation is over the
     * row rather than its columns, which makes it no less an escalation.
     */
    public function testAColumnScopedUpdaterMayNotRewriteTheRowsPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsEscalation',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::update(Role::any(), 'name'),
                Permission::read(Role::team('exec')),
                Permission::update(Role::team('exec')),
            ],
            documentSecurity: true,
        )));

        $stored = [
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ];

        $authorization->skip(fn () => $database->createDocument('columnGrantsEscalation', new Document([
            '$id' => 'row',
            'name' => 'Ada',
            'salary' => 120000,
            '$permissions' => $stored,
        ])));

        $this->asRole($authorization, Role::any()->toString(), function () use ($database): void {
            // Granting itself delete is the escalation the refusal exists for.
            try {
                $database->updateDocument('columnGrantsEscalation', 'row', new Document([
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                ]));
                $this->fail('a column-scoped updater rewrote the row\'s permissions');
            } catch (AuthorizationException $e) {
                $this->assertStringContainsString('Missing "update" permission for the row', $e->getMessage());
            }

            // Refused even alongside a column it is allowed to change, since the write is
            // refused whole rather than applied in part.
            try {
                $database->updateDocument('columnGrantsEscalation', 'row', new Document([
                    'name' => 'Ada Lovelace',
                    '$permissions' => [Permission::read(Role::any())],
                ]));
                $this->fail('a column-scoped updater rewrote permissions beside a granted column');
            } catch (AuthorizationException $e) {
                $this->assertStringContainsString('Missing "update" permission for the row', $e->getMessage());
            }

            // The granted column alone still updates.
            $this->assertSame(
                'Ada L',
                $database->updateDocument('columnGrantsEscalation', 'row', new Document(['name' => 'Ada L']))->getAttribute('name'),
            );
        });

        // Nothing was escalated: the row keeps the permissions it was created with.
        $after = $authorization->skip(fn () => $database->getDocument('columnGrantsEscalation', 'row'));

        $this->assertSame($stored, $after->getPermissions());

        // A whole-row grant is not column-scoped, so it rewrites permissions as it always did.
        $this->asRole($authorization, Role::team('exec')->toString(), function () use ($database): void {
            $updated = $database->updateDocument('columnGrantsEscalation', 'row', new Document([
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::team('exec'))],
            ]));

            $this->assertContains('update("team:exec")', $updated->getPermissions());
        });
    }

    /**
     * A grant follows the column it names, and goes when the column goes.
     *
     * A grant names its column by key, so a rename would otherwise leave it pointing at a key
     * nothing has and a delete would leave it naming a column the collection no longer holds.
     * Either way the collection ends up with a grant createCollection() refuses outright, and
     * the role silently loses the access it was given. Both are kept consistent instead.
     */
    public function testAColumnGrantFollowsARenameAndGoesWithADelete(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsRenamed',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::update(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
            ],
        )));

        $authorization->skip(fn () => $database->createDocument('columnGrantsRenamed', new Document([
            '$id' => 'r1',
            'name' => 'Ada',
            'salary' => 120000,
        ])));

        $database->renameAttribute('columnGrantsRenamed', 'name', 'fullName');

        $declared = $database->getCollection('columnGrantsRenamed')->declaredPermissions() ?? [];

        $this->assertContains('read("any", "fullName")', $declared, 'the read grant did not follow the rename');
        $this->assertContains('update("any", "fullName")', $declared, 'the update grant did not follow the rename');
        $this->assertNotContains('read("any", "name")', $declared);

        // A grant on another column is untouched, and so is one naming no column.
        $this->assertContains('read("team:hr", "salary")', $declared);
        $this->assertContains('create("any")', $declared);

        // The grant still reaches the column it was given, under its new name.
        $this->asRole($authorization, Role::any()->toString(), function () use ($database): void {
            $stored = $database->getDocument('columnGrantsRenamed', 'r1');

            $this->assertSame('Ada', $stored->getAttribute('fullName'));
            $this->assertFalse($stored->offsetExists('salary'));

            // And it is writable, which a grant naming a missing column could not be.
            $this->assertSame(
                'Ada L',
                $database->updateDocument('columnGrantsRenamed', 'r1', new Document(['fullName' => 'Ada L']))->getAttribute('fullName'),
            );
        });

        // Deleting the column takes its grants with it, leaving the rest alone.
        $database->deleteAttribute('columnGrantsRenamed', 'fullName');

        $declared = $database->getCollection('columnGrantsRenamed')->declaredPermissions() ?? [];

        $this->assertNotContains('read("any", "fullName")', $declared, 'a grant outlived the column it named');
        $this->assertNotContains('update("any", "fullName")', $declared, 'a grant outlived the column it named');
        $this->assertContains('read("team:hr", "salary")', $declared);
        $this->assertContains('create("any")', $declared);
    }

    /**
     * A relationship is an attribute, so a grant may name it -- and goes with it.
     *
     * Both sides lose their key when the relationship is deleted, so both lose the grants that
     * named it. Left behind, such a grant would name a column the collection no longer has,
     * which createCollection() refuses outright, and it would reach nothing in the meantime.
     */
    public function testAGrantOnARelationshipGoesWithIt(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->markTestSkipped('Adapter does not support relationships');
        }

        foreach (['columnGrantsRelLeft', 'columnGrantsRelRight'] as $id) {
            $database->createCollection(Collection::create(
                id: $id,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
        }

        $database->createRelationship('columnGrantsRelLeft', Relationship::oneToOne(
            relatedCollection: 'columnGrantsRelRight',
            key: 'link',
            twoWay: true,
            twoWayKey: 'backlink',
        ));

        $database->withColumnPermissions(function () use ($database): void {
            $database->updateCollection('columnGrantsRelLeft', new CollectionUpdate(permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'link'),
                Permission::read(Role::any(), 'name'),
            ]));
            $database->updateCollection('columnGrantsRelRight', new CollectionUpdate(permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'backlink'),
                Permission::read(Role::any(), 'name'),
            ]));
        });

        $this->assertContains('read("any", "link")', $database->getCollection('columnGrantsRelLeft')->declaredPermissions() ?? []);
        $this->assertContains('read("any", "backlink")', $database->getCollection('columnGrantsRelRight')->declaredPermissions() ?? []);

        $database->deleteRelationship('columnGrantsRelLeft', 'link');

        $left = $database->getCollection('columnGrantsRelLeft')->declaredPermissions() ?? [];
        $right = $database->getCollection('columnGrantsRelRight')->declaredPermissions() ?? [];

        $this->assertNotContains('read("any", "link")', $left, 'a grant outlived the relationship it named');
        $this->assertNotContains('read("any", "backlink")', $right, 'a grant outlived the two-way key it named');

        // Grants on the columns that remain are untouched, on both sides.
        $this->assertContains('read("any", "name")', $left);
        $this->assertContains('read("any", "name")', $right);
        $this->assertContains('create("any")', $left);
    }

    /**
     * A grant follows a rename made through updateAttribute(), which is the rename a consumer
     * reaches: renameAttribute() is the dedicated call, but an attribute update carrying a new
     * key renames too, and that is the one an API exposes.
     *
     * Indexes already follow such a rename. A grant that did not would name a key nothing has:
     * unreachable, and a grant createCollection() refuses.
     */
    public function testAColumnGrantFollowsAnUpdateThatRenamesIt(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->withColumnPermissions(fn () => $database->createCollection(Collection::create(
            id: 'columnGrantsUpdateRenamed',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'salary'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'name'),
                Permission::update(Role::any(), 'name'),
                Permission::read(Role::team('hr'), 'salary'),
            ],
        )));

        $database->updateAttribute('columnGrantsUpdateRenamed', 'name', new AttributeUpdate(key: 'fullName'));

        $declared = $database->getCollection('columnGrantsUpdateRenamed')->declaredPermissions() ?? [];

        $this->assertContains('read("any", "fullName")', $declared, 'the read grant did not follow the rename');
        $this->assertContains('update("any", "fullName")', $declared, 'the update grant did not follow the rename');
        $this->assertNotContains('read("any", "name")', $declared);

        // Grants on other columns, and grants naming none, are untouched.
        $this->assertContains('read("team:hr", "salary")', $declared);
        $this->assertContains('create("any")', $declared);
    }

    /**
     * And a grant on a relationship follows a rename of its key, on either side.
     */
    public function testAGrantOnARelationshipFollowsARename(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->markTestSkipped('Adapter does not support relationships');
        }

        foreach (['columnGrantsRenRelLeft', 'columnGrantsRenRelRight'] as $id) {
            $database->createCollection(Collection::create(
                id: $id,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
        }

        $database->createRelationship('columnGrantsRenRelLeft', Relationship::oneToOne(
            relatedCollection: 'columnGrantsRenRelRight',
            key: 'link',
            twoWay: true,
            twoWayKey: 'backlink',
        ));

        $database->withColumnPermissions(function () use ($database): void {
            $database->updateCollection('columnGrantsRenRelLeft', new CollectionUpdate(permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'link'),
            ]));
            $database->updateCollection('columnGrantsRenRelRight', new CollectionUpdate(permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any(), 'backlink'),
            ]));
        });

        $database->updateRelationship(
            collection: 'columnGrantsRenRelLeft',
            key: 'link',
            update: new RelationshipUpdate(key: 'connection', twoWayKey: 'backConnection'),
        );

        $left = $database->getCollection('columnGrantsRenRelLeft')->declaredPermissions() ?? [];
        $right = $database->getCollection('columnGrantsRenRelRight')->declaredPermissions() ?? [];

        $this->assertContains('read("any", "connection")', $left, 'the grant did not follow the relationship rename');
        $this->assertNotContains('read("any", "link")', $left);

        $this->assertContains('read("any", "backConnection")', $right, 'the grant did not follow the two-way key rename');
        $this->assertNotContains('read("any", "backlink")', $right);
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
