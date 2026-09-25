<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class ObjectFilterTest extends TestCase
{
    public function testObjectFilterPreservesEmptyObjectsAndArrays(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $collection = new Document([
            'attributes' => [new Document([
                '$id' => 'meta',
                'array' => false,
                'filters' => [ColumnType::Object->value],
            ])],
        ]);
        $document = new Document([
            'meta' => [
                'emptyObject' => new \stdClass(),
                'emptyArray' => [],
                'nested' => [new \stdClass(), ['value' => 1]],
            ],
        ]);

        $encoded = $database->encode($collection, $document);
        $this->assertSame('{"emptyObject":{},"emptyArray":[],"nested":[{},{"value":1}]}', $encoded->getAttribute('meta'));

        $decoded = $database->decode($collection, $encoded)->getAttribute('meta');
        $this->assertIsArray($decoded);
        $this->assertInstanceOf(\stdClass::class, $decoded['emptyObject']);
        $this->assertSame([], $decoded['emptyArray']);
        $nested = $decoded['nested'];
        $this->assertIsArray($nested);
        $this->assertInstanceOf(\stdClass::class, $nested[0]);
        $this->assertSame(['value' => 1], $nested[1]);
    }

    public function testObjectFilterEncodesTopLevelEmptyObject(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $collection = new Document([
            'attributes' => [new Document([
                '$id' => 'meta',
                'array' => false,
                'filters' => [ColumnType::Object->value],
            ])],
        ]);

        $encoded = $database->encode($collection, new Document(['meta' => new \stdClass()]));
        $this->assertSame('{}', $encoded->getAttribute('meta'));
        $this->assertInstanceOf(
            \stdClass::class,
            $database->decode($collection, $encoded)->getAttribute('meta'),
        );
    }
}
