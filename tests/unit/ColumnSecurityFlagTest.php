<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * The columnSecurity flag.
 *
 * A collection must opt in before any permission may be scoped to a column. Without
 * that, the column half of a permission is never written or enforced, so storing one
 * would record a restriction that does not apply -- and that would start applying if
 * the flag were later switched on.
 *
 * Runs on SQLite so the guards are exercised against the real SQL the other adapters
 * share, including the permissions table that has no _column at all.
 */
class ColumnSecurityFlagTest extends TestCase
{
    protected Authorization $authorization;

    protected Database $database;

    protected string $file;

    protected function setUp(): void
    {
        $this->file = \sys_get_temp_dir() . '/utopia_colflag_' . \uniqid() . '.sql';

        $pdo = new PDO('sqlite:' . $this->file, null, null, SQLite::getPDOAttributes());
        $adapter = new SQLite($pdo);
        $adapter->setEmulateMySQL(true);

        $this->authorization = new Authorization();

        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setAuthorization($this->authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('cf_' . \uniqid());

        $this->database->create();
    }

    protected function tearDown(): void
    {
        if (isset($this->file) && \file_exists($this->file)) {
            @\unlink($this->file);
        }
    }

    /**
     * @param array<string> $permissions
     */
    private function collection(string $id, bool $columnSecurity, array $permissions = []): void
    {
        $this->authorization->skip(function () use ($id, $columnSecurity, $permissions) {
            $this->database->createCollection(
                $id,
                documentSecurity: true,
                columnSecurity: $columnSecurity,
                permissions: $permissions
            );
            $this->database->createAttribute($id, 'name', Database::VAR_STRING, 128, false);
            $this->database->createAttribute($id, 'salary', Database::VAR_INTEGER, 8, false);
        });
    }

    public function testDefaultsToOff(): void
    {
        $this->collection('plain', false);

        $collection = $this->authorization->skip(fn () => $this->database->getCollection('plain'));

        $this->assertFalse($collection->getAttribute('columnSecurity'));
    }

    // ---------------------------------------------------------------- writes blocked

    public function testCreateDocumentWithColumnPermissionIsRejected(): void
    {
        $this->collection('plain', false);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('column security is not enabled');

        $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
            'name' => 'Bob',
            'salary' => 100000,
        ])));
    }

    public function testCreateDocumentsBatchWithColumnPermissionIsRejected(): void
    {
        $this->collection('plain', false);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('column security is not enabled');

        $this->authorization->skip(fn () => $this->database->createDocuments('plain', [
            new Document(['$id' => 'd1', '$permissions' => [], 'name' => 'Ann']),
            new Document([
                '$id' => 'd2',
                '$permissions' => [Permission::update(Role::any(), 'name')],
                'name' => 'Bob',
            ]),
        ]));
    }

    public function testUpdateDocumentIntroducingAColumnPermissionIsRejected(): void
    {
        $this->collection('plain', false);

        $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Bob',
            'salary' => 100000,
        ])));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('column security is not enabled');

        $this->authorization->skip(fn () => $this->database->updateDocument('plain', 'd1', new Document([
            '$permissions' => [Permission::read(Role::any()), Permission::read(Role::user('hr'), 'salary')],
        ])));
    }

    public function testBulkUpdateWithAColumnPermissionIsRejected(): void
    {
        $this->collection('plain', false);

        $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Bob',
        ])));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('column security is not enabled');

        $this->authorization->skip(fn () => $this->database->updateDocuments('plain', new Document([
            '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
        ])));
    }

    public function testCollectionPermissionScopedToAColumnIsRejected(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('column security is not enabled');

        $this->authorization->skip(fn () => $this->database->createCollection(
            'plain',
            documentSecurity: true,
            columnSecurity: false,
            permissions: [Permission::read(Role::any(), 'name')]
        ));
    }

    public function testUpdateCollectionIntroducingAColumnPermissionIsRejected(): void
    {
        $this->collection('plain', false);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('column security is not enabled');

        $this->authorization->skip(fn () => $this->database->updateCollection(
            'plain',
            [Permission::read(Role::any(), 'name')],
            true
        ));
    }

    // ---------------------------------------------------------------- writes allowed

    public function testOrdinaryPermissionsStillWorkWithTheFlagOff(): void
    {
        $this->collection('plain', false);

        $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::user('hr')), Permission::update(Role::any())],
            'name' => 'Bob',
            'salary' => 100000,
        ])));

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        $document = $this->database->getDocument('plain', 'd1');

        $this->assertSame('Bob', $document->getAttribute('name'));
        $this->assertSame(100000, $document->getAttribute('salary'));
    }

    public function testColumnPermissionIsAcceptedOnceEnabled(): void
    {
        $this->collection('secured', true);

        $this->authorization->skip(fn () => $this->database->createDocument('secured', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
            'name' => 'Bob',
            'salary' => 100000,
        ])));

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        $document = $this->database->getDocument('secured', 'd1');

        $this->assertSame(100000, $document->getAttribute('salary'));
        $this->assertNull($document->getAttribute('name'));
    }

    // ---------------------------------------------------------------- the transition

    public function testEnablingLaterPreparesTheTableAndThenAcceptsColumnPermissions(): void
    {
        $this->collection('plain', false);

        // rejected before
        try {
            $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
                '$id' => 'before',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
            ])));
            $this->fail('Expected the write to be rejected while the flag is off');
        } catch (DatabaseException) {
            // expected
        }

        $this->authorization->skip(fn () => $this->database->updateCollection('plain', [], true, true));

        // accepted after
        $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
            '$id' => 'after',
            '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
            'name' => 'Bob',
            'salary' => 100000,
        ])));

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        $this->assertSame(100000, $this->database->getDocument('plain', 'after')->getAttribute('salary'));
    }

    /**
     * Disabling would leave the column half of the permission unwritten and
     * unenforced, so the row filter -- which matches on the role alone -- would widen
     * it to the whole row. Refuse rather than silently escalate.
     */
    public function testDisablingIsRefusedWhileColumnPermissionsExist(): void
    {
        $this->collection('secured', true);

        $this->authorization->skip(fn () => $this->database->createDocument('secured', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
            'name' => 'Bob',
            'salary' => 100000,
        ])));

        $this->expectException(DependencyException::class);
        $this->expectExceptionMessage('Cannot disable column security');

        $this->authorization->skip(fn () => $this->database->updateCollection('secured', [], true, false));
    }

    public function testDisablingIsAllowedOnceTheyAreRemoved(): void
    {
        $this->collection('secured', true);

        $this->authorization->skip(function () {
            $this->database->createDocument('secured', new Document([
                '$id' => 'd1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 100000,
            ]));

            // narrow it back to an ordinary permission
            $this->database->updateDocument('secured', 'd1', new Document([
                '$permissions' => [Permission::read(Role::user('hr'))],
            ]));

            $this->database->updateCollection('secured', [], true, false);
        });

        $collection = $this->authorization->skip(fn () => $this->database->getCollection('secured'));

        $this->assertFalse($collection->getAttribute('columnSecurity'));
    }

    /**
     * A document that already carries a column-scoped permission must stay editable,
     * or it would be stranded the moment the flag changed.
     */
    public function testExistingColumnPermissionsDoNotBlockOrdinaryUpdates(): void
    {
        $this->collection('secured', true);

        $this->authorization->skip(function () {
            $this->database->createDocument('secured', new Document([
                '$id' => 'd1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 100000,
            ]));

            // same permissions, different value
            $this->database->updateDocument('secured', 'd1', new Document([
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Robert',
            ]));
        });

        $stored = $this->authorization->skip(fn () => $this->database->getDocument('secured', 'd1'));

        $this->assertSame('Robert', $stored->getAttribute('name'));

        // the grant came through the update intact: hr still reads salary, and the
        // scope is still a scope -- the name it was never granted stays masked
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        $document = $this->database->getDocument('secured', 'd1');

        $this->assertSame(100000, $document->getAttribute('salary'));
        $this->assertNull($document->getAttribute('name'));
    }

    /**
     * Regression: the permissions table's unique index and the ON CONFLICT target it
     * is resolved against have to name the same columns. Widening the index while
     * leaving the target alone made every skipDuplicates insert fail on Postgres with
     * "no unique or exclusion constraint matching the ON CONFLICT specification",
     * which is why the table's shape follows the flag rather than always carrying
     * _column.
     */
    public function testSkipDuplicatesWorksWithTheFlagOff(): void
    {
        $this->collection('plain', false);

        $write = fn () => $this->database->skipDuplicates(
            fn () => $this->database->createDocuments('plain', [
                new Document([
                    '$id' => 'd1',
                    '$permissions' => [Permission::read(Role::any())],
                    'name' => 'Bob',
                ]),
            ])
        );

        $this->authorization->skip($write);
        $this->authorization->skip($write); // same ids again -- the conflict path

        $this->assertSame(1, $this->authorization->skip(fn () => $this->database->count('plain')));
    }

    public function testSkipDuplicatesWorksWithTheFlagOn(): void
    {
        $this->collection('secured', true);

        $write = fn () => $this->database->skipDuplicates(
            fn () => $this->database->createDocuments('secured', [
                new Document([
                    '$id' => 'd1',
                    '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                    'name' => 'Bob',
                    'salary' => 1,
                ]),
            ])
        );

        $this->authorization->skip($write);
        $this->authorization->skip($write);

        $this->assertSame(1, $this->authorization->skip(fn () => $this->database->count('secured')));
    }

    // ---------------------------------------------------------------- reads unchanged

    public function testQueriesAreUnchangedWithTheFlagOff(): void
    {
        $this->collection('plain', false);

        $this->authorization->skip(fn () => $this->database->createDocument('plain', new Document([
            '$id' => 'd1',
            '$permissions' => [Permission::read(Role::user('hr'))],
            'name' => 'Bob',
            'salary' => 100000,
        ])));

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        // no column gate is emitted, so a filter on any column behaves as it always did
        $this->assertCount(1, $this->database->find('plain', [Query::greaterThan('salary', 1)]));
        $this->assertSame(100000, $this->database->sum('plain', 'salary'));
        $this->assertSame(1, $this->database->count('plain'));
    }
}
