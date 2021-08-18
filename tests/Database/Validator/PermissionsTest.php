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
            '$read' => ['member:123', 'team:123/edit'],
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
        // No leading special chars
        $this->assertEquals($object->isValid(['member:_1234']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
        $this->assertEquals($object->isValid(['member:-1234']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
        $this->assertEquals($object->isValid(['member:.1234']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');

        // No unsupported special characters
        $this->assertEquals($object->isValid(['member:12$4']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
        $this->assertEquals($object->isValid(['user:12&4']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
        $this->assertEquals($object->isValid(['member:ab(124']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');

        // Shorter than 36 chars
        $this->assertEquals($object->isValid(['user:aaaaaaaabbbbbbbbccccccccddddddddeeee']), true);
        $this->assertEquals($object->isValid(['user:aaaaaaaabbbbbbbbccccccccddddddddeeeee']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');

        // Permission role must begin with one of: member, role, team, user
        $this->assertEquals($object->isValid(['memmber:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
        $this->assertEquals($object->isValid(['rol:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
        $this->assertEquals($object->isValid(['tteam:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');
        $this->assertEquals($object->isValid(['userr:1234']), false);
        $this->assertEquals($object->getDescription(), 'Permission role must begin with one of: member, role, team, user');

        // Team permission
        $this->assertEquals($object->isValid(['team:_abcd']), false);
        $this->assertEquals($object->getDescription(), '[role:$id] $id must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
        $this->assertEquals($object->isValid(['team:abcd/']), false);
        $this->assertEquals($object->getDescription(), 'Team role must not be empty.');
        $this->assertEquals($object->isValid(['team:/abcd']), false);
        $this->assertEquals($object->getDescription(), 'Team ID must not be empty.');
        $this->assertEquals($object->isValid(['team:abcd//efgh']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles may contain at most one "/" character.');
        $this->assertEquals($object->isValid(['team:abcd/e/fgh']), false);
        $this->assertEquals($object->getDescription(), 'Permission roles may contain at most one "/" character.');
        $this->assertEquals($object->isValid(['team:ab&cd3/efgh']), false);
        $this->assertEquals($object->getDescription(), '[team:$teamId/$role] $teamID and $role must be valid keys: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
        $this->assertEquals($object->isValid(['team:abcd/ef*gh']), false);
        $this->assertEquals($object->getDescription(), '[team:$teamId/$role] $teamID and $role must be valid keys: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char');
    }
}