<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;
use PHPUnit\Framework\TestCase;

class AuthorizationTest extends TestCase
{
    /**
     * @var Authorization
     */
    protected $object = null;

    /**
     * @var Document
     */
    protected $document = null;

    public function setUp(): void
    {
        $this->object = new Authorization(new Document([
            '$id' => 'authorizationtest',
            '$read' => [],
            '$write' => [],
        ]), 'read');

        $this->document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:123', 'team:123'],
            '$write' => ['role:all'],
        ]);
    }

    public function tearDown(): void
    {
    }

    public function testValues()
    {
        $this->assertEquals($this->object->isValid($this->document->getRead()), false);
        $this->assertEquals($this->object->isValid(''), false);
        $this->assertEquals($this->object->isValid([]), false);
        $this->assertEquals($this->object->getDescription(), 'No permissions provided for action \'read\'');
        
        Authorization::setRole('user:456');
        Authorization::setRole('user:123');
        
        $this->assertEquals(Authorization::isRole('user:456'), true);
        $this->assertEquals(Authorization::isRole('user:457'), false);
        $this->assertEquals(Authorization::isRole(''), false);
        $this->assertEquals(Authorization::isRole('role:all'), true);

        $this->assertEquals($this->object->isValid($this->document->getRead()), true);
        
        Authorization::cleanRoles();
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), false);

        Authorization::setRole('team:123');
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), true);
        
        Authorization::cleanRoles();
        Authorization::disable();
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), true);

        Authorization::reset();
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), false);

        Authorization::setDefaultStatus(false);
        Authorization::disable();
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), true);

        Authorization::reset();
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), true);

        Authorization::enable();
        
        $this->assertEquals($this->object->isValid($this->document->getRead()), false);

        Authorization::setRole('textX');

        $this->assertContains('textX', Authorization::getRoles());

        Authorization::unsetRole('textX');

        $this->assertNotContains('textX', Authorization::getRoles());
    }

    public function testCollectionPermissions()
    {
        Authorization::cleanRoles();

        // test for collection-level permissions
        $this->object = new Authorization(new Document([
            '$id' => 'authorizationtest',
            '$read' => ['user:123', 'user:456'],
            '$write' => ['user:123'],
        ]), 'read');

        $this->document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:456'],
            '$write' => [],
        ]);

        $this->assertEquals(false, $this->object->isValid($this->document->getRead()));

        Authorization::setRole('role:123');

        $this->assertEquals(false, $this->object->isValid($this->document->getRead()));

        Authorization::setRole('user:456');

        $this->assertEquals(true, $this->object->isValid($this->document->getRead()));
    }

    public function testDocumentPermissions()
    {
        Authorization::cleanRoles();

        // test for collection-level permissions
        $this->object = new Authorization(new Document([
            '$id' => 'authorizationtest',
            '$read' => [],
            '$write' => [],
        ]), 'read');

        $this->document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:789'],
            '$write' => [],
        ]);

        $this->assertEquals(false, $this->object->isValid($this->document->getRead()));

        Authorization::setRole('role:789');

        $this->assertEquals(false, $this->object->isValid($this->document->getRead()));

        Authorization::cleanRoles();
        Authorization::setRole('user:789');

        $this->assertEquals(true, $this->object->isValid($this->document->getRead()));

        Authorization::cleanRoles();
        Authorization::setRole('role:all');

        $this->assertEquals(false, $this->object->isValid($this->document->getRead()));
    }
}