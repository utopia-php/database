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
        $this->builder->createDatabase('test');
        $this->assertEquals('CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;', $this->builder->getTemplate());
    }

    public function testCreateTable(): void
    {
        $this->builder->createTable('test');
        $this->assertEquals('CREATE TABLE IF NOT EXISTS `test`;', $this->builder->getTemplate());
    }

    public function testDropDatabase(): void
    {
        $this->builder->drop(QueryBuilder::TYPE_DATABASE, 'test');
        $this->assertEquals('DROP DATABASE test;', $this->builder->getTemplate());
    }

    public function testDropTable(): void
    {
        $this->builder->drop(QueryBuilder::TYPE_TABLE, 'test');
        $this->assertEquals('DROP TABLE test;', $this->builder->getTemplate());
    }

    public function testFrom(): void
    {
        $this->builder->from('test', ['somekey']);
        $this->assertEquals('SELECT `somekey` FROM test;', $this->builder->getTemplate());
    }
}
