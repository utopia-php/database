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
        
        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$read' => ['user:123', 'team:123'],
        ]);
        
        $this->assertEquals($object->isValid($document->getRead()), true);
        $this->assertEquals($object->isValid('sting'), false);
        
    }
}