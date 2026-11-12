<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Document;

/**
 * A pooled adapter serves many databases and namespaces for the life of the process, so
 * finding a collection's spatial columns must leave nothing behind per collection.
 */
final class SpatialMemoTest extends TestCase
{
    private const int NAMESPACES = 20_000;

    private const int GROWTH_LIMIT = 1_048_576;

    public function testResolvingSpatialColumnsAcrossNamespacesKeepsMemoryFlat(): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            /**
             * @return list<string>
             */
            public function spatialColumnsOf(Document $collection): array
            {
                return $this->getSpatialAttributes($collection);
            }
        };
        $collection = new Document([
            '$id' => 'places',
            'attributes' => [
                Attribute::string(key: 'name', size: 64),
                Attribute::point(key: 'position'),
            ],
        ]);

        $adapter->setNamespace('warmup');
        $this->assertSame(['position'], $adapter->spatialColumnsOf($collection));
        \gc_collect_cycles();
        $before = \memory_get_usage();

        for ($namespace = 0; $namespace < self::NAMESPACES; $namespace++) {
            $adapter->setNamespace('tenant_'.$namespace);
            $adapter->spatialColumnsOf($collection);
        }

        \gc_collect_cycles();
        $growth = \memory_get_usage() - $before;

        $this->assertLessThan(self::GROWTH_LIMIT, $growth, 'Resolving spatial columns for '.self::NAMESPACES.' namespaces grew memory by '.$growth.' bytes');
    }
}
