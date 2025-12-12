<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Roles;

class RolesTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    /**
     * @throws \Exception
     */
    public function testValidRole(): void
    {
        $object = new Roles();
        $this->assertTrue($object->isValid([Role::users()->toString()]));
    }

    public function testNotAnArray(): void
    {
        $object = new Roles();
        $this->assertFalse($object->isValid('not an array'));
        $this->assertSame('Roles must be an array of strings.', $object->getDescription());
    }

    public function testExceedLength(): void
    {
        $object = new Roles(2);
        $this->assertFalse($object->isValid([
            Role::users()->toString(),
            Role::users()->toString(),
            Role::users()->toString()
        ]));
        $this->assertSame('You can only provide up to 2 roles.', $object->getDescription());
    }

    public function testNotAllStrings(): void
    {
        $object = new Roles();
        $this->assertFalse($object->isValid([
            Role::users()->toString(),
            123
        ]));
        $this->assertSame('Every role must be of type string.', $object->getDescription());
    }

    public function testObsoleteWildcardRole(): void
    {
        $object = new Roles();
        $this->assertFalse($object->isValid(['*']));
        $this->assertSame('Wildcard role "*" has been replaced. Use "any" instead.', $object->getDescription());
    }

    public function testObsoleteRolePrefix(): void
    {
        $object = new Roles();
        $this->assertFalse($object->isValid(['read("role:123")']));
        $this->assertSame('Roles using the "role:" prefix have been removed. Use "users", "guests", or "any" instead.', $object->getDescription());
    }

    public function testDisallowedRoles(): void
    {
        $object = new Roles(allowed: [Roles::ROLE_USERS]);
        $this->assertFalse($object->isValid([Role::any()->toString()]));
        $this->assertSame('Role "any" is not allowed. Must be one of: users.', $object->getDescription());
    }

    public function testLabels(): void
    {
        $object = new Roles();
        $this->assertTrue($object->isValid(['label:123']));
        $this->assertFalse($object->isValid(['label:not-alphanumeric']));
    }
}
