<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Document;
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
        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:123', 'team:123'],
            '$write' => ['role:all'],
        ]);
        $object = new Authorization('read');

        $this->assertEquals($object->isValid($document->getRead()), false);
        $this->assertEquals($object->isValid(''), false);
        $this->assertEquals($object->isValid([]), false);
        $this->assertEquals($object->getDescription(), 'No permissions provided for action \'read\'');
        
        Authorization::setRole('user:456');
        Authorization::setRole('user:123');
        
        $this->assertEquals(Authorization::isRole('user:456'), true);
        $this->assertEquals(Authorization::isRole('user:457'), false);
        $this->assertEquals(Authorization::isRole(''), false);
        $this->assertEquals(Authorization::isRole('role:all'), true);

        $this->assertEquals($object->isValid($document->getRead()), true);
        
        Authorization::cleanRoles();
        
        $this->assertEquals($object->isValid($document->getRead()), false);

        Authorization::setRole('team:123');
        
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