<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Document;
use Utopia\Database\Validator\Permissions;
use PHPUnit\Framework\TestCase;

class PermissionsTest extends TestCase
{

    public function setUp(): void
    {

    }

    public function tearDown(): void
    {
    }

    public function testValues()
    {
        $object = new Permissions();

        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:123', 'team:123'],
            '$write' => ['role:all'],
        ]);
        
        $this->assertEquals($object->isValid($document->getRead()), true);
        $this->assertEquals($object->isValid($document->getWrite()), true);
        
        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:123', 'team:123'],
            '$read' => ['member:123'],
        ]);
        
        $this->assertEquals($object->isValid($document->getRead()), true);
        $this->assertEquals($object->isValid($document->getWrite()), true);
        $this->assertEquals($object->isValid('sting'), false);
        
    }

    public function testInvalidPermissions()
    {
        $object = new Permissions();

        // Must be array of strings
        $this->assertEquals($object->isValid('role:all'), false);
        $this->assertEquals($object->getDescription(), 'Permissions roles must be an array of strings.');
        $this->assertEquals($object->isValid(false), false);
        $this->assertEquals($object->getDescription(), 'Permissions roles must be an array of strings.');
        $this->assertEquals($object->isValid(1.5), false);
        $this->assertEquals($object->getDescription(), 'Permissions roles must be an array of strings.');

        // Permissions role must be of type string
        $this->assertEquals($object->isValid([0]), false);
        $this->assertEquals($object->getDescription(), 'Permissions role must be of type string.');
        $this->assertEquals($object->isValid([false]), false);
        $this->assertEquals($object->getDescription(), 'Permissions role must be of type string.');
        $this->assertEquals($object->isValid([['a']]), false);
        $this->assertEquals($object->getDescription(), 'Permissions role must be of type string.');

    }
}