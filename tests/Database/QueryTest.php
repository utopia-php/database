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
        $this->assertContains('Iron Man', $query->getValues());
        
        $query = Query::parse('year.lesser(2001)'); 
        $this->assertEquals('year', $query->getAttribute());
        $this->assertEquals('lesser', $query->getOperator());
        $this->assertContains('2001', $query->getValues());
    }

    public function testParseExpression()
    {
        [$operator, $values] = Query::parseExpression('equal("Spiderman")'); 
        $this->assertEquals('equal', $operator);
        $this->assertContains('Spiderman', $values);


        [$operator, $values] = Query::parseExpression('lesser(2001)'); 
        $this->assertEquals('lesser', $operator);
        $this->assertContains('2001', $values);
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

        $this->assertContains('Iron Man', $query->getValues());
    }

    public function testGetQuery()
    {
        $parsed = Query::parse('title.equal("Iron Man")');
        $query = $parsed->getQuery();

        $this->assertEquals('title', $query['attribute']);
        $this->assertEquals('equal', $query['operator']);
        $this->assertContains('Iron Man', $query['values']);
    }

}