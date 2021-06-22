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

        // Split role into format {$type}:{$value}
        // Permission must have value
        $this->assertEquals($object->isValid(['member:']), false);
        $this->assertEquals($object->getDescription(), 'Permission role value must not be empty');
        $this->assertEquals($object->isValid(['role:']), false);
        $this->assertEquals($object->getDescription(), 'Permission role value must not be empty');
        $this->assertEquals($object->isValid(['team:']), false);
        $this->assertEquals($object->getDescription(), 'Permission role value must not be empty');
        $this->assertEquals($object->isValid(['user:']), false);
        $this->assertEquals($object->getDescription(), 'Permission role value must not be empty');

        // Permission role:$value must be one of: all, guest, member
        $this->assertEquals($object->isValid(['role:alll']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles must be one of: all, guest, member');
        $this->assertEquals($object->isValid(['role:gguest']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles must be one of: all, guest, member');
        $this->assertEquals($object->isValid(['role:memer']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles must be one of: all, guest, member');

        // team:$value, member:$value and user:$value must have valid Key for $value
        // No leading underscores
        $this->assertEquals($object->isValid(['member:_1234']), false);
        $this->assertEquals($object->getDescription(), 'Parameter must contain only letters with no spaces or special chars and be shorter than 32 chars');

        // No special characters
        $this->assertEquals($object->isValid(['member:12$4']), false);
        $this->assertEquals($object->getDescription(), 'Parameter must contain only letters with no spaces or special chars and be shorter than 32 chars');
        $this->assertEquals($object->isValid(['user:12&4']), false);
        $this->assertEquals($object->getDescription(), 'Parameter must contain only letters with no spaces or special chars and be shorter than 32 chars');
        $this->assertEquals($object->isValid(['team:ab(124']), false);
        $this->assertEquals($object->getDescription(), 'Parameter must contain only letters with no spaces or special chars and be shorter than 32 chars');

        // Shorter than 32 chars
        $this->assertEquals($object->isValid(['user:aaaaaaaabbbbbbbbccccccccdddddddd']), true);
        $this->assertEquals($object->isValid(['user:aaaaaaaabbbbbbbbccccccccdddddddde']), false);
        $this->assertEquals($object->getDescription(), 'Parameter must contain only letters with no spaces or special chars and be shorter than 32 chars');

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