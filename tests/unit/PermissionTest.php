<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class PermissionTest extends TestCase
{
    public function testOutputFromString(): void
    {
        $permission = Permission::parse('read("any")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('read("users")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('read("user:123")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('user', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());

        $permission = Permission::parse('read("team:123/admin")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('team', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());
        $this->assertSame('admin', $permission->getDimension());

        $permission = Permission::parse('read("guests")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('create("any")');
        $this->assertSame('create', $permission->getPermission());
        $this->assertSame('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('create("users")');
        $this->assertSame('create', $permission->getPermission());
        $this->assertSame('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('create("user:123")');
        $this->assertSame('create', $permission->getPermission());
        $this->assertSame('user', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());

        $permission = Permission::parse('create("team:123/admin")');
        $this->assertSame('create', $permission->getPermission());
        $this->assertSame('team', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());
        $this->assertSame('admin', $permission->getDimension());

        $permission = Permission::parse('create("guests")');
        $this->assertSame('create', $permission->getPermission());
        $this->assertSame('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('update("any")');
        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('update("users")');
        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('update("user:123")');
        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('user', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());

        $permission = Permission::parse('update("team:123/admin")');
        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('team', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());
        $this->assertSame('admin', $permission->getDimension());

        $permission = Permission::parse('update("guests")');
        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('delete("any")');
        $this->assertSame('delete', $permission->getPermission());
        $this->assertSame('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('delete("users")');
        $this->assertSame('delete', $permission->getPermission());
        $this->assertSame('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('delete("user:123")');
        $this->assertSame('delete', $permission->getPermission());
        $this->assertSame('user', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());

        $permission = Permission::parse('delete("team:123/admin")');
        $this->assertSame('delete', $permission->getPermission());
        $this->assertSame('team', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());
        $this->assertSame('admin', $permission->getDimension());

        $permission = Permission::parse('delete("guests")');
        $this->assertSame('delete', $permission->getPermission());
        $this->assertSame('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('read("users/verified")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertSame('verified', $permission->getDimension());

        $permission = Permission::parse('read("users/unverified")');
        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertSame('unverified', $permission->getDimension());
    }

    public function testInputFromParameters(): void
    {
        $permission = new Permission('read', 'any');
        $this->assertSame('read("any")', $permission->toString());

        $permission = new Permission('read', 'users');
        $this->assertSame('read("users")', $permission->toString());

        $permission = new Permission('read', 'user', '123');
        $this->assertSame('read("user:123")', $permission->toString());

        $permission = new Permission('read', 'team', '123', 'admin');
        $this->assertSame('read("team:123/admin")', $permission->toString());

        $permission = new Permission('create', 'any');
        $this->assertSame('create("any")', $permission->toString());

        $permission = new Permission('create', 'users');
        $this->assertSame('create("users")', $permission->toString());

        $permission = new Permission('create', 'user', '123');
        $this->assertSame('create("user:123")', $permission->toString());

        $permission = new Permission('create', 'team', '123', 'admin');
        $this->assertSame('create("team:123/admin")', $permission->toString());

        $permission = new Permission('update', 'any');
        $this->assertSame('update("any")', $permission->toString());

        $permission = new Permission('update', 'users');
        $this->assertSame('update("users")', $permission->toString());

        $permission = new Permission('update', 'user', '123');
        $this->assertSame('update("user:123")', $permission->toString());

        $permission = new Permission('update', 'team', '123', 'admin');
        $this->assertSame('update("team:123/admin")', $permission->toString());

        $permission = new Permission('delete', 'any');
        $this->assertSame('delete("any")', $permission->toString());

        $permission = new Permission('delete', 'users');
        $this->assertSame('delete("users")', $permission->toString());

        $permission = new Permission('delete', 'user', '123');
        $this->assertSame('delete("user:123")', $permission->toString());

        $permission = new Permission('delete', 'team', '123', 'admin');
        $this->assertSame('delete("team:123/admin")', $permission->toString());
    }

    public function testInputFromRoles(): void
    {
        $permission = Permission::read(Role::any());
        $this->assertSame('read("any")', $permission);

        $permission = Permission::read(Role::users());
        $this->assertSame('read("users")', $permission);

        $permission = Permission::read(Role::user(ID::custom('123')));
        $this->assertSame('read("user:123")', $permission);

        $permission = Permission::read(Role::team(ID::custom('123'), 'admin'));
        $this->assertSame('read("team:123/admin")', $permission);

        $permission = Permission::read(Role::guests());
        $this->assertSame('read("guests")', $permission);

        $permission = Permission::create(Role::any());
        $this->assertSame('create("any")', $permission);

        $permission = Permission::create(Role::users());
        $this->assertSame('create("users")', $permission);

        $permission = Permission::create(Role::user(ID::custom('123')));
        $this->assertSame('create("user:123")', $permission);

        $permission = Permission::create(Role::team(ID::custom('123'), 'admin'));
        $this->assertSame('create("team:123/admin")', $permission);

        $permission = Permission::create(Role::guests());
        $this->assertSame('create("guests")', $permission);

        $permission = Permission::update(Role::any());
        $this->assertSame('update("any")', $permission);

        $permission = Permission::update(Role::users());
        $this->assertSame('update("users")', $permission);

        $permission = Permission::update(Role::user(ID::custom('123')));
        $this->assertSame('update("user:123")', $permission);

        $permission = Permission::update(Role::team(ID::custom('123'), 'admin'));
        $this->assertSame('update("team:123/admin")', $permission);

        $permission = Permission::update(Role::guests());
        $this->assertSame('update("guests")', $permission);

        $permission = Permission::delete(Role::any());
        $this->assertSame('delete("any")', $permission);

        $permission = Permission::delete(Role::users());
        $this->assertSame('delete("users")', $permission);

        $permission = Permission::delete(Role::user(ID::custom('123')));
        $this->assertSame('delete("user:123")', $permission);

        $permission = Permission::delete(Role::team(ID::custom('123'), 'admin'));
        $this->assertSame('delete("team:123/admin")', $permission);

        $permission = Permission::delete(Role::guests());
        $this->assertSame('delete("guests")', $permission);

        $permission = Permission::write(Role::any());
        $this->assertSame('write("any")', $permission);
    }

    public function testInvalidFormats(): void
    {
        try {
            Permission::parse('read');
            $this->fail('Failed to throw Exception');
        } catch (\Exception $e) {
            $this->assertSame('Invalid permission string format: "read".', $e->getMessage());
        }

        try {
            Permission::parse('read(("any")');
            $this->fail('Failed to throw Exception');
        } catch (\Exception $e) {
            $this->assertSame('Invalid permission type: "read(".', $e->getMessage());
        }

        try {
            Permission::parse('read("users/un/verified")');
            $this->fail('Failed to throw Exception');
        } catch (\Exception $e) {
            $this->assertSame('Only one dimension can be provided', $e->getMessage());
        }

        try {
            Permission::parse('read("users/")');
            $this->fail('Failed to throw Exception');
        } catch (\Exception $e) {
            $this->assertSame('Dimension must not be empty', $e->getMessage());
        }
    }

    /**
     * @throws \Exception
     */
    public function testAggregation(): void
    {
        $permissions = ['write("any")'];
        $parsed = Permission::aggregate($permissions);
        $this->assertSame(['create("any")', 'update("any")', 'delete("any")'], $parsed);

        $parsed = Permission::aggregate($permissions, [Database::PERMISSION_UPDATE, Database::PERMISSION_DELETE]);
        $this->assertSame(['update("any")', 'delete("any")'], $parsed);

        $permissions = [
            'read("any")',
            'read("user:123")',
            'read("user:123")',
            'write("user:123")',
            'update("user:123")',
            'delete("user:123")'
        ];

        $parsed = Permission::aggregate($permissions, Database::PERMISSIONS);
        $this->assertSame([
            'read("any")',
            'read("user:123")',
            'create("user:123")',
            'update("user:123")',
            'delete("user:123")',
        ], $parsed);
    }
}
