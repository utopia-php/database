<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

class AuthorizationTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testValues(): void
    {
        Authorization::setRole(Role::any()->toString());

        $document = new Document([
            '$id' => ID::unique(),
            '$collection' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);
        $object = new Authorization(Database::PERMISSION_READ);

        $this->assertEquals($object->isValid($document->getRead()), false);
        $this->assertEquals($object->isValid(''), false);
        $this->assertEquals($object->isValid([]), false);
        $this->assertEquals($object->getDescription(), 'No permissions provided for action \'read\'');

        Authorization::setRole(Role::user('456')->toString());
        Authorization::setRole(Role::user('123')->toString());

        $this->assertEquals(Authorization::isRole(Role::user('456')->toString()), true);
        $this->assertEquals(Authorization::isRole(Role::user('457')->toString()), false);
        $this->assertEquals(Authorization::isRole(''), false);
        $this->assertEquals(Authorization::isRole(Role::any()->toString()), true);

        $this->assertEquals($object->isValid($document->getRead()), true);

        Authorization::cleanRoles();

        $this->assertEquals($object->isValid($document->getRead()), false);

        Authorization::setRole(Role::team('123')->toString());

        $this->assertEquals($object->isValid($document->getRead()), true);

        Authorization::cleanRoles();
        Authorization::disable();

        $this->assertEquals($object->isValid($document->getRead()), true);

        Authorization::reset();

        $this->assertEquals($object->isValid($document->getRead()), false);

        Authorization::setDefaultStatus(false);
        Authorization::disable();

        $this->assertEquals($object->isValid($document->getRead()), true);

        Authorization::reset();

        $this->assertEquals($object->isValid($document->getRead()), true);

        Authorization::enable();

        $this->assertEquals($object->isValid($document->getRead()), false);

        Authorization::setRole('textX');

        $this->assertContains('textX', Authorization::getRoles());

        Authorization::unsetRole('textX');

        $this->assertNotContains('textX', Authorization::getRoles());

        // Test skip method
        $this->assertEquals($object->isValid($document->getRead()), false);
        $this->assertEquals(Authorization::skip(function () use ($object, $document) {
            return $object->isValid($document->getRead());
        }), true);
    }

    public function testNestedSkips(): void
    {
        $this->assertEquals(true, Authorization::$status);

        Authorization::skip(function () {
            $this->assertEquals(false, Authorization::$status);

            Authorization::skip(function () {
                $this->assertEquals(false, Authorization::$status);

                Authorization::skip(function () {
                    $this->assertEquals(false, Authorization::$status);
                });

                $this->assertEquals(false, Authorization::$status);
            });

            $this->assertEquals(false, Authorization::$status);
        });

        $this->assertEquals(true, Authorization::$status);
    }

    public function testSetUserFromRoles(): void
    {
        $currentUserRole = Role::user("123");
        Authorization::setRole(Role::user("123")->toString());
        $this->assertEquals($currentUserRole->getIdentifier(), Authorization::getUser());

        $roles = [];
        $roles[] = Role::user("123");
        $roles[] = Role::user("123", 'verififed');
        $roles[] = Role::user("126", 'unverififed');
        $roles[] = Role::any();
        $roles[] = Role::users();

        foreach ($roles as $role) {
            Authorization::setRole($role->toString());
        }
        $expectedRole = Role::user("126", 'unverififed');
        $this->assertEquals($expectedRole->getIdentifier(), Authorization::getUser());
    }
}
