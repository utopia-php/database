<?php

namespace Utopia\Tests;

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
            '$permissions' => [
                'read' => ['user:123', 'team:123'],
                'write' => ['*'],
            ],
        ]);
        
        $this->assertEquals($object->isValid($document->getPermissions()), true);
        
        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$permissions' => [
                'read' => ['user:123', 'team:123'],
            ],
        ]);
        
        $this->assertEquals($object->isValid($document->getPermissions()), true);
        
        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$permissions' => [
                'read' => ['user:123', 'team:123'],
                'write' => ['*'],
                'unknown' => ['*'],
            ],
        ]);
        
        $this->assertEquals($object->isValid($document->getPermissions()), false);
        
        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$permissions' => [
                'read' => 'unknown',
                'write' => ['*'],
            ],
        ]);
        
        $this->assertEquals($object->isValid($document->getPermissions()), false);
        
    }
}