<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ID;
use Utopia\Database\Permission;
use Utopia\Database\Role;

class PermissionHelperTest extends TestCase
{
    public function testOutputFromString()
    {
        $permission = Permission::parse('read("any")');
        $this->assertEquals('read', $permission->getPermission());
        $this->assertEquals('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('read("users")');
        $this->assertEquals('read', $permission->getPermission());
        $this->assertEquals('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('read("user:123")');
        $this->assertEquals('read', $permission->getPermission());
        $this->assertEquals('user', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());

        $permission = Permission::parse('read("team:123/admin")');
        $this->assertEquals('read', $permission->getPermission());
        $this->assertEquals('team', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());
        $this->assertEquals('admin', $permission->getDimension());

        $permission = Permission::parse('read("guests")');
        $this->assertEquals('read', $permission->getPermission());
        $this->assertEquals('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('create("any")');
        $this->assertEquals('create', $permission->getPermission());
        $this->assertEquals('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('create("users")');
        $this->assertEquals('create', $permission->getPermission());
        $this->assertEquals('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('create("user:123")');
        $this->assertEquals('create', $permission->getPermission());
        $this->assertEquals('user', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());

        $permission = Permission::parse('create("team:123/admin")');
        $this->assertEquals('create', $permission->getPermission());
        $this->assertEquals('team', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());
        $this->assertEquals('admin', $permission->getDimension());

        $permission = Permission::parse('create("guests")');
        $this->assertEquals('create', $permission->getPermission());
        $this->assertEquals('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('update("any")');
        $this->assertEquals('update', $permission->getPermission());
        $this->assertEquals('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('update("users")');
        $this->assertEquals('update', $permission->getPermission());
        $this->assertEquals('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('update("user:123")');
        $this->assertEquals('update', $permission->getPermission());
        $this->assertEquals('user', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());

        $permission = Permission::parse('update("team:123/admin")');
        $this->assertEquals('update', $permission->getPermission());
        $this->assertEquals('team', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());
        $this->assertEquals('admin', $permission->getDimension());

        $permission = Permission::parse('update("guests")');
        $this->assertEquals('update', $permission->getPermission());
        $this->assertEquals('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('delete("any")');
        $this->assertEquals('delete', $permission->getPermission());
        $this->assertEquals('any', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('delete("users")');
        $this->assertEquals('delete', $permission->getPermission());
        $this->assertEquals('users', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());

        $permission = Permission::parse('delete("user:123")');
        $this->assertEquals('delete', $permission->getPermission());
        $this->assertEquals('user', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());

        $permission = Permission::parse('delete("team:123/admin")');
        $this->assertEquals('delete', $permission->getPermission());
        $this->assertEquals('team', $permission->getRole());
        $this->assertEquals('123', $permission->getIdentifier());
        $this->assertEquals('admin', $permission->getDimension());

        $permission = Permission::parse('delete("guests")');
        $this->assertEquals('delete', $permission->getPermission());
        $this->assertEquals('guests', $permission->getRole());
        $this->assertEmpty($permission->getIdentifier());
        $this->assertEmpty($permission->getDimension());
    }

    public function testInputFromParameters()
    {
        $permission = new Permission('read', 'any');
        $this->assertEquals('read("any")', $permission->toString());

        $permission = new Permission('read', 'users');
        $this->assertEquals('read("users")', $permission->toString());

        $permission = new Permission('read', 'user', '123');
        $this->assertEquals('read("user:123")', $permission->toString());

        $permission = new Permission('read', 'team', '123', 'admin');
        $this->assertEquals('read("team:123/admin")', $permission->toString());

        $permission = new Permission('create', 'any');
        $this->assertEquals('create("any")', $permission->toString());

        $permission = new Permission('create', 'users');
        $this->assertEquals('create("users")', $permission->toString());

        $permission = new Permission('create', 'user', '123');
        $this->assertEquals('create("user:123")', $permission->toString());

        $permission = new Permission('create', 'team', '123', 'admin');
        $this->assertEquals('create("team:123/admin")', $permission->toString());

        $permission = new Permission('update', 'any');
        $this->assertEquals('update("any")', $permission->toString());

        $permission = new Permission('update', 'users');
        $this->assertEquals('update("users")', $permission->toString());

        $permission = new Permission('update', 'user', '123');
        $this->assertEquals('update("user:123")', $permission->toString());

        $permission = new Permission('update', 'team', '123', 'admin');
        $this->assertEquals('update("team:123/admin")', $permission->toString());

        $permission = new Permission('delete', 'any');
        $this->assertEquals('delete("any")', $permission->toString());

        $permission = new Permission('delete', 'users');
        $this->assertEquals('delete("users")', $permission->toString());

        $permission = new Permission('delete', 'user', '123');
        $this->assertEquals('delete("user:123")', $permission->toString());

        $permission = new Permission('delete', 'team', '123', 'admin');
        $this->assertEquals('delete("team:123/admin")', $permission->toString());
    }

    public function testInputFromRoles()
    {
        $permission = Permission::read(Role::any());
        $this->assertEquals('read("any")', $permission);

        $permission = Permission::read(Role::users());
        $this->assertEquals('read("users")', $permission);

        $permission = Permission::read(Role::user(ID::custom('123')));
        $this->assertEquals('read("user:123")', $permission);

        $permission = Permission::read(Role::team(ID::custom('123'), 'admin'));
        $this->assertEquals('read("team:123/admin")', $permission);

        $permission = Permission::read(Role::guests());
        $this->assertEquals('read("guests")', $permission);

        $permission = Permission::create(Role::any());
        $this->assertEquals('create("any")', $permission);

        $permission = Permission::create(Role::users());
        $this->assertEquals('create("users")', $permission);

        $permission = Permission::create(Role::user(ID::custom('123')));
        $this->assertEquals('create("user:123")', $permission);

        $permission = Permission::create(Role::team(ID::custom('123'), 'admin'));
        $this->assertEquals('create("team:123/admin")', $permission);

        $permission = Permission::create(Role::guests());
        $this->assertEquals('create("guests")', $permission);

        $permission = Permission::update(Role::any());
        $this->assertEquals('update("any")', $permission);

        $permission = Permission::update(Role::users());
        $this->assertEquals('update("users")', $permission);

        $permission = Permission::update(Role::user(ID::custom('123')));
        $this->assertEquals('update("user:123")', $permission);

        $permission = Permission::update(Role::team(ID::custom('123'), 'admin'));
        $this->assertEquals('update("team:123/admin")', $permission);

        $permission = Permission::update(Role::guests());
        $this->assertEquals('update("guests")', $permission);

        $permission = Permission::delete(Role::any());
        $this->assertEquals('delete("any")', $permission);

        $permission = Permission::delete(Role::users());
        $this->assertEquals('delete("users")', $permission);

        $permission = Permission::delete(Role::user(ID::custom('123')));
        $this->assertEquals('delete("user:123")', $permission);

        $permission = Permission::delete(Role::team(ID::custom('123'), 'admin'));
        $this->assertEquals('delete("team:123/admin")', $permission);

        $permission = Permission::delete(Role::guests());
        $this->assertEquals('delete("guests")', $permission);
    }
}