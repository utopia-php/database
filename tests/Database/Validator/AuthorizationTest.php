<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\ID;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use PHPUnit\Framework\TestCase;

class AuthorizationTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testValues()
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
        $this->assertEquals(Authorization::skip(function() use ($object, $document) {return $object->isValid($document->getRead());}), true);
    }
}