<?php

namespace Utopia\Tests;

use Utopia\Database\Query;
use PHPUnit\Framework\TestCase;

class QueryTest extends TestCase
{

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testParse()
    {
        $queries = [
            Query::parse('equal("One",3,[55.55,\'Works\',true],false,null)'),
            // Same query with random spaces
            Query::parse('equal("One" , 3 , [55.55, \'Works\',true], false, null)')
        ];

        foreach ($queries as $query) {
            $this->assertEquals('equal', $query->getMethod());
            $this->assertCount(5, $query->getParams());
    
            $this->assertIsString($query->getParams()[0]);
            $this->assertEquals('One', $query->getParams()[0]);
    
            $this->assertIsNumeric($query->getParams()[1]);
            $this->assertEquals(3, $query->getParams()[1]);

            $this->assertIsArray($query->getParams()[2]);
            $this->assertCount(3, $query->getParams()[2]);
            $this->assertIsNumeric($query->getParams()[2][0]);
            $this->assertEquals(55.55, $query->getParams()[2][0]);
            $this->assertIsString($query->getParams()[2][1]);
            $this->assertEquals('Works', $query->getParams()[2][1]);
            $this->assertTrue($query->getParams()[2][2]);
    
            $this->assertFalse($query->getParams()[3]);
    
            $this->assertNull($query->getParams()[4]);
        }
    }
}