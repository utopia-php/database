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
        $this->assertEquals($object->isValid([0, 1.5]), false);
        $this->assertEquals($object->getDescription(), 'Permissions role must be of type string.');
        $this->assertEquals($object->isValid([false, []]), false);
        $this->assertEquals($object->getDescription(), 'Permissions role must be of type string.');
        $this->assertEquals($object->isValid([['a']]), false);
        $this->assertEquals($object->getDescription(), 'Permissions role must be of type string.');

        // Wildcard character deprecated
        $this->assertEquals($object->isValid(['*']), false);
        $this->assertEquals($object->getDescription(), 'Wildcard permission "*" deprecated. Use "role:all" instead.');

        // Only contains a single ':'
        $this->assertEquals($object->isValid(['user1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles must contain one and only one ":" character.');
        $this->assertEquals($object->isValid(['user::1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles must contain one and only one ":" character.');
        $this->assertEquals($object->isValid(['user:123:4']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles must contain one and only one ":" character.');

        // Permission role must begin with one of: member, role, team, user
        $this->assertEquals($object->isValid(['memmber:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
        $this->assertEquals($object->isValid(['rol:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
        $this->assertEquals($object->isValid(['tteam:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
        $this->assertEquals($object->isValid(['userr:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
    }
}