<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Column-level permission enforcement.
 *
 * Runs on the in-memory adapter: masking and update enforcement are resolved from
 * the $permissions that already travel with the document, so they need no adapter
 * support and no running database.
 */
class ColumnPermissionEnforcementTest extends TestCase
{
    protected Authorization $authorization;

    protected Database $database;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();

        $this->database = new Database(new Memory(), new Cache(new NoCache()));
        $this->database
            ->setAuthorization($this->authorization)
            ->setDatabase('columnPermissions')
            ->setNamespace('cols_' . \uniqid());

        if (!$this->database->exists()) {
            $this->database->create();
        }

        $this->authorization->skip(function () {
            $this->database->createCollection('employees', permissions: [], documentSecurity: true, columnSecurity: true);

            foreach (['name', 'email', 'salary'] as $column) {
                $this->database->createAttribute('employees', $column, Database::VAR_STRING, 128, false);
            }

            $this->database->createDocument('employees', new Document([
                '$id' => 'e1',
                '$permissions' => [
                    // Reads and writes only the columns it is granted
                    Permission::read(Role::user('peer'), 'name'),
                    Permission::read(Role::user('peer'), 'email'),
                    Permission::update(Role::user('peer'), 'email'),
                    // Unscoped, so every column
                    Permission::read(Role::user('boss')),
                    Permission::update(Role::user('boss')),
                ],
                'name' => 'Bob',
                'email' => 'bob@example.com',
                'salary' => '100000',
            ]));
        });
    }

    /**
     * @return array<string>
     */
    private function columnsVisibleTo(string $role): array
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole($role);

        $document = $this->database->getDocument('employees', 'e1');

        return \array_values(\array_filter(
            \array_keys($document->getArrayCopy()),
            fn (string $key) => !\str_starts_with($key, '$')
        ));
    }

    public function testUnscopedRoleSeesEveryColumn(): void
    {
        $this->assertSame(['name', 'email', 'salary'], $this->columnsVisibleTo('user:boss'));
    }

    public function testColumnScopedRoleSeesOnlyGrantedColumns(): void
    {
        $this->assertSame(['name', 'email'], $this->columnsVisibleTo('user:peer'));
    }

    public function testRoleWithNoReadPermissionSeesNothing(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:stranger');

        $this->assertTrue($this->database->getDocument('employees', 'e1')->isEmpty());
    }

    public function testFindMasksColumnsToo(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:peer');

        $results = $this->database->find('employees');
        $this->assertCount(1, $results);

        $columns = \array_values(\array_filter(
            \array_keys($results[0]->getArrayCopy()),
            fn (string $key) => !\str_starts_with($key, '$')
        ));

        $this->assertSame(['name', 'email'], $columns);
    }

    public function testSkippedAuthorizationIsNotMasked(): void
    {
        $columns = $this->authorization->skip(function () {
            $document = $this->database->getDocument('employees', 'e1');

            return \array_values(\array_filter(
                \array_keys($document->getArrayCopy()),
                fn (string $key) => !\str_starts_with($key, '$')
            ));
        });

        $this->assertSame(['name', 'email', 'salary'], $columns);
    }

    /**
     * A column-scoped grant on the COLLECTION sets $skipAuth, because the roles-only
     * permission check cannot see the column. That flag means "may see every row",
     * never "may see every column", so masking must still apply.
     */
    public function testCollectionLevelColumnGrantIsStillMaskedInFind(): void
    {
        $this->authorization->skip(function () {
            $this->database->createCollection('public_employees', documentSecurity: true, columnSecurity: true, permissions: [
                Permission::read(Role::any(), 'name'),
            ]);

            foreach (['name', 'email', 'salary'] as $column) {
                $this->database->createAttribute('public_employees', $column, Database::VAR_STRING, 128, false);
            }

            $this->database->createDocument('public_employees', new Document([
                '$id' => 'pub1',
                '$permissions' => [],
                'name' => 'Bob',
                'email' => 'bob@example.com',
                'salary' => '100000',
            ]));
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('any');

        $results = $this->database->find('public_employees');
        $this->assertCount(1, $results);

        $columns = \array_values(\array_filter(
            \array_keys($results[0]->getArrayCopy()),
            fn (string $key) => !\str_starts_with($key, '$')
        ));

        $this->assertSame(['name'], $columns, 'find() must mask even when $skipAuth is set');
    }

    /**
     * Collection-level grants live on the collection document in _metadata, not in
     * the collection's _perms table, so they need their own repointing on rename and
     * their own cleanup on delete.
     */
    public function testCollectionLevelColumnGrantFollowsARename(): void
    {
        $this->authorization->skip(function () {
            $this->database->createCollection('scoped', documentSecurity: true, columnSecurity: true, permissions: [
                Permission::read(Role::any(), 'name'),
            ]);

            $this->database->createAttribute('scoped', 'name', Database::VAR_STRING, 128, false);

            $this->assertSame(
                ['read("any", "name")'],
                $this->database->getCollection('scoped')->getPermissions()
            );

            $this->database->updateAttribute('scoped', 'name', newKey: 'fullName');

            $this->assertSame(
                ['read("any", "fullName")'],
                $this->database->getCollection('scoped')->getPermissions()
            );

            $this->database->deleteAttribute('scoped', 'fullName');

            $this->assertSame([], $this->database->getCollection('scoped')->getPermissions());
        });
    }

    /**
     * renameAttribute() is a second rename path, separate from updateAttribute()'s
     * newKey. It has to run the same permission migration: without it the grant stays
     * on the old key, so the caller loses the renamed column -- and a column later
     * created under the old name inherits authority nobody granted it.
     */
    public function testGrantFollowsRenameAttributeAndDoesNotOutliveTheOldKey(): void
    {
        $this->authorization->skip(function () {
            $this->database->createCollection('renamed', documentSecurity: true, columnSecurity: true, permissions: []);
            $this->database->createAttribute('renamed', 'name', Database::VAR_STRING, 128, false);
            $this->database->createAttribute('renamed', 'salary', Database::VAR_INTEGER, 8, false);

            $this->database->createDocument('renamed', new Document([
                '$id' => 'r1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 5,
            ]));

            $this->database->renameAttribute('renamed', 'salary', 'pay');
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        // the grant moved with the column
        $this->assertSame(5, $this->database->getDocument('renamed', 'r1')->getAttribute('pay'));

        // ...and a column recreated under the old key inherits nothing
        $this->authorization->skip(function () {
            $this->database->createAttribute('renamed', 'salary', Database::VAR_INTEGER, 8, false);
            $this->database->updateDocument('renamed', 'r1', new Document(['salary' => 999]));
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        $document = $this->database->getDocument('renamed', 'r1');

        $this->assertSame(5, $document->getAttribute('pay'));
        $this->assertNull($document->getAttribute('salary'), 'stale grant authorized a recreated column');
    }

    /**
     * The flag controls enforcement, not storage: disabling it leaves scoped grants in
     * place, dormant. So the rename and delete migrations must run regardless -- gating
     * them on the flag lets a rename slip past a grant, and re-enabling would then
     * point it at a key that no longer exists, or at whatever column took that name.
     */
    public function testGrantMigrationsRunWhileColumnSecurityIsDisabled(): void
    {
        $this->authorization->skip(function () {
            $this->database->createCollection('dormant', documentSecurity: true, columnSecurity: true, permissions: []);
            $this->database->createAttribute('dormant', 'salary', Database::VAR_INTEGER, 8, false);

            $this->database->createDocument('dormant', new Document([
                '$id' => 'r1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'salary' => 5,
            ]));

            // grants stay in storage while the flag is off
            $this->database->updateCollection('dormant', [], true, false);
            $this->database->renameAttribute('dormant', 'salary', 'pay');

            $this->database->updateCollection('dormant', [], true, true);
            $this->database->createAttribute('dormant', 'salary', Database::VAR_INTEGER, 8, false);
            $this->database->updateDocument('dormant', 'r1', new Document(['salary' => 999]));
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:hr');

        $document = $this->database->getDocument('dormant', 'r1');

        $this->assertSame(5, $document->getAttribute('pay'), 'grant did not follow the rename');
        $this->assertNull($document->getAttribute('salary'), 'stale grant authorized a recreated column');
    }

    public function testUpdateOfGrantedColumnIsAllowed(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:peer');

        $this->database->updateDocument('employees', 'e1', new Document([
            'email' => 'new@example.com',
        ]));

        $stored = $this->authorization->skip(fn () => $this->database->getDocument('employees', 'e1'));
        $this->assertSame('new@example.com', $stored->getAttribute('email'));
    }

    public function testUpdateOfUngrantedColumnIsRejected(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:peer');

        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "update" permission for column "salary"');

        $this->database->updateDocument('employees', 'e1', new Document([
            'salary' => '999999',
        ]));
    }

    public function testUpdateIsRejectedWholesaleWhenOneColumnIsUngranted(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:peer');

        try {
            $this->database->updateDocument('employees', 'e1', new Document([
                'email' => 'allowed@example.com',
                'salary' => '999999',
            ]));
            $this->fail('Expected an AuthorizationException');
        } catch (AuthorizationException) {
            // The permitted column must not have been written either
        }

        $stored = $this->authorization->skip(fn () => $this->database->getDocument('employees', 'e1'));
        $this->assertSame('bob@example.com', $stored->getAttribute('email'));
        $this->assertSame('100000', $stored->getAttribute('salary'));
    }

    /**
     * Regression: a caller whose update access is limited to one column must not be
     * able to rewrite $permissions. Allowing it made column-level permissions
     * unenforceable -- "may update email" was enough to grant yourself read and
     * update on salary, then read and overwrite it.
     */
    public function testColumnScopedUpdaterCannotRewritePermissions(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:peer');

        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "update" permission to change $permissions');

        $this->database->updateDocument('employees', 'e1', new Document([
            '$permissions' => [
                Permission::read(Role::user('peer'), 'salary'),
                Permission::update(Role::user('peer'), 'salary'),
            ],
        ]));
    }

    public function testFailedEscalationLeavesTheHiddenColumnHidden(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:peer');

        try {
            $this->database->updateDocument('employees', 'e1', new Document([
                '$permissions' => [Permission::read(Role::user('peer'), 'salary')],
            ]));
            $this->fail('Expected an AuthorizationException');
        } catch (AuthorizationException) {
            // expected
        }

        $this->assertSame(['name', 'email'], $this->columnsVisibleTo('user:peer'));

        $stored = $this->authorization->skip(
            fn () => $this->database->getDocument('employees', 'e1')
        );
        $this->assertSame('100000', $stored->getAttribute('salary'));
    }

    public function testUnscopedUpdaterMayRewritePermissions(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:boss');

        $current = $this->database->getDocument('employees', 'e1')->getPermissions();

        $this->database->updateDocument('employees', 'e1', new Document([
            '$permissions' => [...$current, Permission::read(Role::team('audit'), 'salary')],
        ]));

        $stored = $this->authorization->skip(
            fn () => $this->database->getDocument('employees', 'e1')
        );

        $this->assertContains('read("team:audit", "salary")', $stored->getPermissions());
    }

    /**
     * Write scopes and read scopes are independent, so being allowed to change a
     * column says nothing about being allowed to see the rest of the row. The merged
     * document a write returns carries every stored column, so it has to go through
     * the same read masking a get would.
     */
    public function testUpdateResponseIsMaskedByReadPermissions(): void
    {
        $this->authorization->skip(function () {
            $this->database->createDocument('employees', new Document([
                '$id' => 'w1',
                '$permissions' => [
                    Permission::update(Role::user('ed'), 'name'),   // may write name
                    Permission::read(Role::user('ed'), 'email'),    // may read email
                ],
                'name' => 'Bob',
                'email' => 'bob@example.com',
                'salary' => '100000',
            ]));
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:ed');

        $returned = $this->database->updateDocument('employees', 'w1', new Document([
            'name' => 'Robert',
        ]));

        // readable, so returned
        $this->assertSame('bob@example.com', $returned->getAttribute('email'));

        // supplied in this very call, so returned -- the caller already has it
        $this->assertSame('Robert', $returned->getAttribute('name'));

        // neither readable nor supplied: this is the column the response used to leak
        $this->assertNull($returned->getAttribute('salary'), 'update response leaked a hidden column');

        // the write itself still landed
        $stored = $this->authorization->skip(
            fn () => $this->database->getDocument('employees', 'w1')
        );
        $this->assertSame('Robert', $stored->getAttribute('name'));
        $this->assertSame('100000', $stored->getAttribute('salary'));
    }

    public function testBulkUpdateCallbackPayloadIsMasked(): void
    {
        $this->authorization->skip(function () {
            $this->database->createDocument('employees', new Document([
                '$id' => 'w2',
                '$permissions' => [
                    Permission::update(Role::user('ed'), 'name'),
                    Permission::read(Role::user('ed'), 'email'),
                ],
                'name' => 'Bob',
                'email' => 'bob@example.com',
                'salary' => '100000',
            ]));
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:ed');

        $seen = [];

        $this->database->updateDocuments(
            'employees',
            new Document(['name' => 'Bobby']),
            [Query::equal('$id', ['w2'])],
            100,
            onNext: function (Document $document) use (&$seen) {
                $seen[] = \array_keys(\array_filter(
                    $document->getArrayCopy(),
                    fn (string $key) => !\str_starts_with($key, '$'),
                    ARRAY_FILTER_USE_KEY
                ));
            }
        );

        // `name` was supplied by this call, `email` is readable; `salary` is neither
        $this->assertSame([['name', 'email']], $seen, 'bulk callback leaked hidden columns');

        $stored = $this->authorization->skip(
            fn () => $this->database->getDocument('employees', 'w2')
        );
        $this->assertSame('Bobby', $stored->getAttribute('name'));
        $this->assertSame('100000', $stored->getAttribute('salary'));
    }

    public function testUnscopedRoleMayUpdateAnyColumn(): void
    {
        $this->authorization->cleanRoles();
        $this->authorization->addRole('user:boss');

        $this->database->updateDocument('employees', 'e1', new Document([
            'salary' => '123456',
        ]));

        $stored = $this->authorization->skip(fn () => $this->database->getDocument('employees', 'e1'));
        $this->assertSame('123456', $stored->getAttribute('salary'));
    }
}
