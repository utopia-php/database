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
        $query = Query::parse('equal("One", "Two" , 3 ,false, null)');

        $this->assertEquals('equal', $query->getMethod());
        // $this->assertEquals('equal', $query->getParams());
    }
}