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
        $this->assertEquals('Iron Man', $query->getValue());
        
        $query = Query::parse('year.lesser(2001)'); 
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals('lesser', $query->getOperator());
        $this->assertEquals(2001, $query->getValue());
    }

    public function testParseExpression()
    {
        [$operator, $value] = Query::parseExpression('equal("Spiderman")'); 
        $this->assertEquals('equal', $operator);
        $this->assertEquals('Spiderman', $value);


        [$operator, $value] = Query::parseExpression('lesser(2001)'); 
        $this->assertEquals('lesser', $operator);
        $this->assertEquals(2001, $value);
    }

    public function testGetAttribute()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('title', $query->getAttribute());
    }

    public function testGetOperator()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('equal', $query->getOperator());
    }

    public function testGetValue()
    {
        $query = Query::parse('title.equal("Iron Man")');

        $this->assertEquals('Iron Man', $query->getValue());
    }

    public function testGetQuery()
    {
        $parsed = Query::parse('title.equal("Iron Man")');
        $query = $parsed->getQuery();

        $this->assertEquals('title', $query['attribute']);
        $this->assertEquals('equal', $query['operator']);
        $this->assertEquals('Iron Man', $query['value']);
    }

}