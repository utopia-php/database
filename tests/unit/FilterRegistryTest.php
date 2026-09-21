<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;

class FilterRegistryTest extends TestCase
{
    /**
     * @param array<float> $point
     */
    private function createDatabase(array $point = [0.0, 0.0]): Database
    {
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getSupportForHostname')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getNamespace')->willReturn('test');
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('decodePoint')->willReturn($point);

        return new Database($adapter, new Cache(new None()));
    }

    private function pointCollection(): Document
    {
        return new Document([
            '$id' => 'places',
            'attributes' => [
                new Document([
                    '$id' => 'location',
                    'type' => Database::VAR_POINT,
                    'array' => false,
                    'filters' => [Database::VAR_POINT],
                ]),
            ],
        ]);
    }

    public function testSpatialDecodeUsesTheCallingDatabaseAdapter(): void
    {
        $first = $this->createDatabase([1.0, 2.0]);
        $second = $this->createDatabase([9.0, 9.0]);

        $decoded = $first->decode(
            $this->pointCollection(),
            new Document(['$id' => 'a', 'location' => 'POINT(1 2)']),
        );

        $this->assertSame([1.0, 2.0], $decoded->getAttribute('location'));

        $decoded = $second->decode(
            $this->pointCollection(),
            new Document(['$id' => 'b', 'location' => 'POINT(9 9)']),
        );

        $this->assertSame([9.0, 9.0], $decoded->getAttribute('location'));
    }

    public function testDefaultFilterSignaturesSurviveLaterConstruction(): void
    {
        $first = $this->createDatabase();
        [, , $before] = $first->getCacheKeys('places', 'a');

        $this->createDatabase();

        [, , $after] = $first->getCacheKeys('places', 'a');

        $this->assertSame($before, $after);
    }

    public function testRegisteringAGlobalFilterInvalidatesCacheKeys(): void
    {
        $database = $this->createDatabase();
        [, , $before] = $database->getCacheKeys('places', 'a');

        $noop = fn (mixed $value) => $value;
        Database::addFilter(__FUNCTION__, $noop, $noop);

        [, , $after] = $database->getCacheKeys('places', 'a');

        $this->assertNotSame($before, $after);
    }
}
