<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ID;
use Utopia\Database\Permission;
use Utopia\Database\Role;

class IDHelperTest extends TestCase
{

    public function testCustomID()
    {
        $id = ID::custom('test');
        $this->assertEquals('test', $id);
    }

    public function testUniqueID()
    {
        $id = ID::unique();
        $this->assertNotEmpty($id);
        $this->assertIsString($id);
    }
}