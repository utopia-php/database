<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;

class NewQueryAdapterTest extends TestCase
{
    protected function getMockMariaDBAdapter(): MariaDB
    {
        // Create a mock MariaDB adapter to test SQL generation
        $mock = $this->getMockBuilder(MariaDB::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['getPDO', 'getInternalKeyForAttribute', 'filter', 'quote', 'escapeWildcards', 'getSupportForJSONOverlaps', 'getSQLOperator', 'getFulltextValue'])
            ->getMock();

        $mock->method('getInternalKeyForAttribute')->willReturnArgument(0);
        $mock->method('filter')->willReturnArgument(0);
        $mock->method('quote')->willReturnCallback(function($value) { return "`{$value}`"; });
        $mock->method('escapeWildcards')->willReturnArgument(0);
        $mock->method('getSupportForJSONOverlaps')->willReturn(true);
        $mock->method('getSQLOperator')->willReturn('LIKE');
        $mock->method('getFulltextValue')->willReturnArgument(0);

        return $mock;
    }

    protected function getMockPostgresAdapter(): Postgres
    {
        // Create a mock Postgres adapter to test SQL generation
        $mock = $this->getMockBuilder(Postgres::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['getPDO', 'getInternalKeyForAttribute', 'filter', 'quote', 'escapeWildcards', 'getSQLOperator', 'getFulltextValue'])
            ->getMock();

        $mock->method('getInternalKeyForAttribute')->willReturnArgument(0);
        $mock->method('filter')->willReturnArgument(0);
        $mock->method('quote')->willReturnCallback(function($value) { return "\"{$value}\""; });
        $mock->method('escapeWildcards')->willReturnArgument(0);
        $mock->method('getSQLOperator')->willReturn('LIKE');
        $mock->method('getFulltextValue')->willReturnArgument(0);

        return $mock;
    }

    public function testMariaDBNotSearchSQLGeneration(): void
    {
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notSearch('content', 'unwanted');
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT (MATCH', $result);
        $this->assertStringContainsString('AGAINST', $result);
        $this->assertArrayHasKey(':uid_0', $binds);
        $this->assertEquals('unwanted', $binds[':uid_0']);
    }

    public function testMariaDBNotBetweenSQLGeneration(): void
    {
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notBetween('score', 10, 20);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT BETWEEN', $result);
        $this->assertArrayHasKey(':uid_0', $binds);
        $this->assertArrayHasKey(':uid_1', $binds);
        $this->assertEquals(10, $binds[':uid_0']);
        $this->assertEquals(20, $binds[':uid_1']);
    }

    public function testMariaDBNotContainsArraySQLGeneration(): void
    {
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notContains('tags', ['unwanted', 'spam']);
        $query->setOnArray(true);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT (JSON_OVERLAPS', $result);
    }

    public function testMariaDBNotContainsStringSQLGeneration(): void
    {
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notContains('title', ['unwanted']);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT LIKE', $result);
        $this->assertStringContainsString('%unwanted%', array_values($binds)[0]);
    }

    public function testMariaDBNotStartsWithSQLGeneration(): void
    {
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notStartsWith('title', 'temp');
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT LIKE', $result);
        $this->assertStringContainsString('temp%', array_values($binds)[0]);
    }

    public function testMariaDBNotEndsWithSQLGeneration(): void
    {
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notEndsWith('filename', '.tmp');
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT LIKE', $result);
        $this->assertStringContainsString('%.tmp', array_values($binds)[0]);
    }

    public function testPostgresNotSearchSQLGeneration(): void
    {
        $adapter = $this->getMockPostgresAdapter();
        $query = Query::notSearch('content', 'unwanted');
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT (to_tsvector', $result);
        $this->assertStringContainsString('websearch_to_tsquery', $result);
        $this->assertArrayHasKey(':uid_0', $binds);
    }

    public function testPostgresNotBetweenSQLGeneration(): void
    {
        $adapter = $this->getMockPostgresAdapter();
        $query = Query::notBetween('score', 10, 20);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT BETWEEN', $result);
        $this->assertArrayHasKey(':uid_0', $binds);
        $this->assertArrayHasKey(':uid_1', $binds);
    }

    public function testPostgresNotContainsArraySQLGeneration(): void
    {
        $adapter = $this->getMockPostgresAdapter();
        $query = Query::notContains('tags', ['unwanted']);
        $query->setOnArray(true);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        $this->assertStringContainsString('NOT @>', $result);
    }

    public function testNotQueryUsesAndLogic(): void
    {
        // Test that NOT queries use AND logic instead of OR
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::notContains('tags', ['unwanted', 'spam']);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        // For NOT queries, multiple values should be combined with AND
        $this->assertStringContainsString(' AND ', $result);
    }

    public function testRegularQueryUsesOrLogic(): void
    {
        // Test that regular queries still use OR logic
        $adapter = $this->getMockMariaDBAdapter();
        $query = Query::contains('tags', ['wanted', 'good']);
        $binds = [];

        $result = $adapter->getSQLCondition($query, $binds);

        // For regular queries, multiple values should be combined with OR
        $this->assertStringContainsString(' OR ', $result);
    }
}