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
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('title', $query->getAttribute());
        $this->assertEquals('equal', $query->getOperator());
        $this->assertEquals('Iron Man', $query->getOperand());
        
        $query = Query::parse('year.lesser(2001)'); 
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals('lesser', $query->getOperator());
        $this->assertEquals(2001, $query->getOperand());
    }

    public function testParseExpression()
    {
        [$operator, $operand] = Query::parseExpression('equal("Spiderman")'); 
        $this->assertEquals('equal', $operator);
        $this->assertEquals('Spiderman', $operand);


        [$operator, $operand] = Query::parseExpression('lesser(2001)'); 
        $this->assertEquals('lesser', $operator);
        $this->assertEquals(2001, $operand);
    }

}