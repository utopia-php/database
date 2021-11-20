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

    public function testDeleteFrom(): void
    {
        $this->builder->deleteFrom('test');
        $this->assertEquals('DELETE FROM test;', $this->builder->getTemplate());
    }

    public function testAddColumn(): void
    {
        $this->builder->addColumn('testKey', 'char(255)');
        $this->assertEquals(' ADD COLUMN `testKey` char(255);', $this->builder->getTemplate());
    }

    public function testDropColumn(): void
    {
        $this->builder->dropColumn('testKey');
        $this->assertEquals(' DROP COLUMN `testKey`;', $this->builder->getTemplate());
    }

    public function testDropIndex(): void
    {
        $this->builder->dropIndex('testKey');
        $this->assertEquals(' DROP INDEX `testKey`;', $this->builder->getTemplate());
    }

    public function testAlterTable(): void
    {
        $this->builder->alterTable('test');
        $this->assertEquals('ALTER TABLE test;', $this->builder->getTemplate());
    }

    public function testWhere(): void
    {
        $this->builder->where('key', \Utopia\Database\Query::TYPE_EQUAL, 'testValue');
        $this->assertEquals(' WHERE key = :value0;', $this->builder->getTemplate());
        $this->assertArrayHasKey(':value0', $this->builder->getParams());
        $this->assertEquals('testValue', $this->builder->getParams()[':value0']);
    }

    public function testLimit(): void
    {
        $this->builder
             ->from('test', ['somekey'])
             ->limit(10);

        $this->assertEquals('SELECT `somekey` FROM test LIMIT 10;', $this->builder->getTemplate());
    }
}
