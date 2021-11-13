<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Database\QueryBuilder;

class QueryBuilderTest extends TestCase
{
    /**
     * @var QueryBuilder
     */
    protected $builder;

    public function setUp(): void
    {
        $this->builder = new QueryBuilder();
    }

    public function tearDown(): void
    {
        $this->builder->reset();
    }

    public function testCreateDatabase(): void
    {
        $this->builder->createDatabase('test')->execute();
        $this->assertEquals(QueryBuilder::TYPE_CREATE, $this->builder->getStatement());
        $this->assertEquals('CREATE DATABASE :name /*!40100 DEFAULT CHARACTER SET utf8mb4 */;', $this->builder->getTemplate());
        $this->assertArrayHasKey('name', $this->builder->getParams());
        $this->assertEquals('test', $this->builder->getParams()['name']);
    }

    public function testCreateTable(): void
    {
        $this->builder->createTable('test')->execute();
        $this->assertEquals(QueryBuilder::TYPE_CREATE, $this->builder->getStatement());
        $this->assertEquals('CREATE TABLE IF NOT EXISTS :name;', $this->builder->getTemplate());
        $this->assertArrayHasKey('name', $this->builder->getParams());
        $this->assertEquals('test', $this->builder->getParams()['name']);
    }

    public function testDropDatabase(): void
    {
        $this->builder->drop(QueryBuilder::TYPE_DATABASE, 'test')->execute();
        $this->assertEquals(QueryBuilder::TYPE_DROP, $this->builder->getStatement());
        $this->assertEquals('DROP DATABASE :name;', $this->builder->getTemplate());
        $this->assertArrayHasKey('name', $this->builder->getParams());
        $this->assertEquals('test', $this->builder->getParams()['name']);
    }

    public function testDropTable(): void
    {
        $this->builder->drop(QueryBuilder::TYPE_TABLE, 'test')->execute();
        $this->assertEquals(QueryBuilder::TYPE_DROP, $this->builder->getStatement());
        $this->assertEquals('DROP TABLE :name;', $this->builder->getTemplate());
        $this->assertArrayHasKey('name', $this->builder->getParams());
        $this->assertEquals('test', $this->builder->getParams()['name']);
    }

    public function testFrom(): void
    {
        $this->builder->from('test', 'somekey')->execute();
        $this->assertEquals(QueryBuilder::TYPE_SELECT, $this->builder->getStatement());
        $this->assertEquals('SELECT :key FROM :table;', $this->builder->getTemplate());
        $this->assertArrayHasKey('key', $this->builder->getParams());
        $this->assertEquals('somekey', $this->builder->getParams()['key']);
        $this->assertArrayHasKey('table', $this->builder->getParams());
        $this->assertEquals('test', $this->builder->getParams()['table']);
    }
}
