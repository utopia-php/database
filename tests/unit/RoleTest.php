<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Role;

class RoleTest extends TestCase
{
    public function testOutputFromString(): void
    {
        $role = Role::parse('any');
        $this->assertSame('any', $role->getRole());
        $this->assertEmpty($role->getIdentifier());
        $this->assertEmpty($role->getDimension());

        $role = Role::parse('guests');
        $this->assertSame('guests', $role->getRole());
        $this->assertEmpty($role->getIdentifier());
        $this->assertEmpty($role->getDimension());

        $role = Role::parse('users');
        $this->assertSame('users', $role->getRole());
        $this->assertEmpty($role->getIdentifier());
        $this->assertEmpty($role->getDimension());

        $role = Role::parse('user:123');
        $this->assertSame('user', $role->getRole());
        $this->assertSame('123', $role->getIdentifier());
        $this->assertEmpty($role->getDimension());

        $role = Role::parse('team:123');
        $this->assertSame('team', $role->getRole());
        $this->assertSame('123', $role->getIdentifier());
        $this->assertEmpty($role->getDimension());

        $role = Role::parse('team:123/456');
        $this->assertSame('team', $role->getRole());
        $this->assertSame('123', $role->getIdentifier());
        $this->assertSame('456', $role->getDimension());

        $role = Role::parse('user:123/verified');
        $this->assertSame('user', $role->getRole());
        $this->assertSame('123', $role->getIdentifier());
        $this->assertSame('verified', $role->getDimension());

        $role = Role::parse('users/verified');
        $this->assertSame('users', $role->getRole());
        $this->assertEmpty($role->getIdentifier());
        $this->assertSame('verified', $role->getDimension());

        $role = Role::parse('label:vip');
        $this->assertSame('label', $role->getRole());
        $this->assertSame('vip', $role->getIdentifier());
        $this->assertEmpty($role->getDimension());
    }

    public function testInputFromParameters(): void
    {
        $role = new Role('any');
        $this->assertSame('any', $role->toString());

        $role = new Role('guests');
        $this->assertSame('guests', $role->toString());

        $role = new Role('users');
        $this->assertSame('users', $role->toString());

        $role = new Role('user', '123');
        $this->assertSame('user:123', $role->toString());

        $role = new Role('team', '123');
        $this->assertSame('team:123', $role->toString());

        $role = new Role('team', '123', '456');
        $this->assertSame('team:123/456', $role->toString());

        $role = new Role('label', 'vip');
        $this->assertSame('label:vip', $role->toString());
    }

    public function testInputFromRoles(): void
    {
        $role = Role::any();
        $this->assertSame('any', $role->toString());

        $role = Role::guests();
        $this->assertSame('guests', $role->toString());

        $role = Role::users();
        $this->assertSame('users', $role->toString());

        $role = Role::user(ID::custom('123'));
        $this->assertSame('user:123', $role->toString());

        $role = Role::team(ID::custom('123'));
        $this->assertSame('team:123', $role->toString());

        $role = Role::team(ID::custom('123'), '456');
        $this->assertSame('team:123/456', $role->toString());

        $role = Role::label('vip');
        $this->assertSame('label:vip', $role->toString());
    }

    public function testInputFromID(): void
    {
        $role = Role::user(ID::custom('123'));
        $this->assertSame('user:123', $role->toString());

        $role = Role::team(ID::custom('123'));
        $this->assertSame('team:123', $role->toString());

        $role = Role::team(ID::custom('123'), '456');
        $this->assertSame('team:123/456', $role->toString());
    }
}
