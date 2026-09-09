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
            $this->database->createCollection('employees', permissions: [], documentSecurity: true);

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
            $this->database->createCollection('public_employees', documentSecurity: true, permissions: [
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
