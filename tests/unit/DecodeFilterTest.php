<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;

class DecodeFilterTest extends TestCase
{
    private function createDatabase(): Database
    {
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getSupportForHostname')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getNamespace')->willReturn('test');
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('filter')->willReturnCallback(fn ($v) => $v);

        return new Database($adapter, new Cache(new None()));
    }

    public function testDecodeSkipsUnregisteredFilter(): void
    {
        $db = $this->createDatabase();

        $collection = new Document([
            'attributes' => [
                [
                    '$id' => 'wafRules',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                    'filters' => ['subQueryWafRules'],
                ],
            ],
        ]);

        $document = new Document([
            '$id' => 'test-doc',
            '$permissions' => [],
            'wafRules' => 'some-value',
        ]);

        $result = $db->decode($collection, $document);

        $this->assertEquals('some-value', $result->getAttribute('wafRules'));
    }

    public function testDecodeAppliesRegisteredFilterNormally(): void
    {
        $db = $this->createDatabase();

        Database::addFilter(
            'testUpperCase',
            fn ($value) => $value, // encode (not used here)
            fn ($value) => \strtoupper($value), // decode
        );

        $collection = new Document([
            'attributes' => [
                [
                    '$id' => 'name',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                    'filters' => ['testUpperCase'],
                ],
            ],
        ]);

        $document = new Document([
            '$id' => 'test-doc',
            '$permissions' => [],
            'name' => 'hello',
        ]);

        $result = $db->decode($collection, $document);

        $this->assertEquals('HELLO', $result->getAttribute('name'));
    }
}
