<?php

namespace Tests\Unit;

use MongoDB\BSON\UTCDateTime;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;
use Utopia\Mongo\Client;

class AllocationTest extends TestCase
{
    public function testQueryValidatorsOnlyNeedAttributeNames(): void
    {
        $attributes = [new Document(['$id' => 'items', 'default' => range(1, 100_000)])];
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $select = new Select($attributes);
        $order = new Order($attributes);
        $allocated = memory_get_peak_usage() - $before;

        $this->assertLessThan(1024 * 1024, $allocated);
        $this->assertTrue($select->isValid(Query::select(['items'])));
        $this->assertFalse($select->isValid(Query::select(['missing'])));
        $this->assertTrue($order->isValid(Query::orderAsc('items')));
        $this->assertFalse($order->isValid(Query::orderAsc('missing')));
    }

    /**
     * @dataProvider transformations
     */
    public function testUnfilteredArraysAreNotCopied(string $method): void
    {
        $database = new Database(new Memory(), new Cache(new CacheMemory()));
        $collection = new Document(['attributes' => [new Document([
            '$id' => 'items', 'type' => Database::VAR_STRING, 'array' => true, 'filters' => [],
        ])]]);
        $document = new Document(['items' => array_fill(0, 100_000, 'value')]);
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $result = $database->$method($collection, $document);
        $allocated = memory_get_peak_usage() - $before;

        $this->assertLessThan(1024 * 1024, $allocated);
        $this->assertSame($document, $result);
        $this->assertCount(100_000, $result->getAttribute('items'));
        $this->assertSame('value', $result->getAttribute('items')[99_999]);
    }

    /** @return array<string, array{string}> */
    public static function transformations(): array
    {
        return ['encode' => ['encode'], 'decode' => ['decode'], 'casting' => ['casting']];
    }

    public function testFiltersDefaultsCastingAndUnknownRemovalStillApply(): void
    {
        $database = new Database(new Memory(), new Cache(new CacheMemory()), [
            'append-a' => ['encode' => fn ($value) => $value . 'a', 'decode' => fn ($value) => substr($value, 0, -1)],
            'wrap' => ['encode' => fn ($value) => '[' . $value . ']', 'decode' => fn ($value) => substr($value, 1, -1)],
        ]);
        $collection = new Document(['attributes' => [
            new Document(['$id' => 'items', 'type' => Database::VAR_STRING, 'array' => true, 'filters' => ['append-a', 'wrap']]),
            new Document(['$id' => 'count', 'type' => Database::VAR_INTEGER, 'default' => 7]),
            new Document(['$id' => 'strings', 'type' => Database::VAR_STRING, 'array' => true]),
        ]]);
        $document = new Document(['items' => [7 => 'x', 'second' => 'y'], 'strings' => []]);
        $database->encode($collection, $document);
        $this->assertSame([7 => '[xa]', 'second' => '[ya]'], $document->getAttribute('items'));
        $this->assertSame(7, $document->getAttribute('count'));
        $database->decode($collection, $document);
        $this->assertSame([7 => 'x', 'second' => 'y'], $document->getAttribute('items'));
        $document->setAttribute('count', '42');
        $document->setAttribute('strings', '[]');
        $database->casting($collection, $document);
        $this->assertSame(42, $document->getAttribute('count'));
        $this->assertSame([], $document->getAttribute('strings'));

        $database->setDropUnknownAttributes(true);
        $document->setAttribute('unknown', range(1, 100_000));
        $document->setAttribute('unknownNext', 'remove');
        $document->setAttribute('$id', 'keep');
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $database->encode($collection, $document);
        $allocated = memory_get_peak_usage() - $before;
        $this->assertLessThan(1024 * 1024, $allocated);
        $this->assertFalse($document->offsetExists('unknown'));
        $this->assertFalse($document->offsetExists('unknownNext'));
        $this->assertSame('keep', $document->getId());
        $this->assertSame(42, $document->getAttribute('count'));
    }

    public function testCastingKeepsJsonDecodingAndScalarConversions(): void
    {
        $database = new Database(new Memory(), new Cache(new CacheMemory()));
        $attributes = [];
        foreach (['id' => Database::VAR_ID, 'bool' => Database::VAR_BOOLEAN, 'int' => Database::VAR_INTEGER, 'float' => Database::VAR_FLOAT, 'big' => Database::VAR_BIGINT, 'text' => Database::VAR_STRING] as $key => $type) {
            $attributes[] = new Document(['$id' => $key, 'type' => $type, 'array' => true]);
        }
        $collection = new Document(['attributes' => $attributes]);
        $document = new Document([
            'id' => '[42]', 'bool' => '[0,1]', 'int' => '["12"]', 'float' => '["1.5"]',
            'big' => '["42","9223372036854775808"]', 'text' => '{"sparse":"value"}',
        ]);
        $database->casting($collection, $document);
        $this->assertSame(['42'], $document->getAttribute('id'));
        $this->assertSame([false, true], $document->getAttribute('bool'));
        $this->assertSame([12], $document->getAttribute('int'));
        $this->assertSame([1.5], $document->getAttribute('float'));
        $this->assertSame([42, '9223372036854775808'], $document->getAttribute('big'));
        $this->assertSame(['sparse' => 'value'], $document->getAttribute('text'));
    }

    public function testMongoCastingPreservesKeysAndConvertsValues(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('connect')->willReturnSelf();
        $mongo = new Mongo($client);
        $collection = new Document(['attributes' => [
            new Document(['$id' => 'dates', 'type' => Database::VAR_DATETIME, 'array' => true]),
            new Document(['$id' => 'objects', 'type' => Database::VAR_OBJECT, 'array' => true]),
            new Document(['$id' => 'integers', 'type' => Database::VAR_INTEGER, 'array' => true]),
        ], 'indexes' => []]);
        $document = new Document([
            'dates' => [7 => '2026-01-01T00:00:00.000+00:00'],
            'objects' => ['first' => '{"value":42}'],
            'integers' => [4 => '12'],
        ]);
        $mongo->castingBefore($collection, $document);
        $this->assertInstanceOf(UTCDateTime::class, $document->getAttribute('dates')[7]);
        $this->assertSame(42, $document->getAttribute('objects')['first']->value);
        $mongo->castingAfter($collection, $document);
        $this->assertSame([4 => 12], $document->getAttribute('integers'));
        $this->assertSame(['first' => ['value' => 42]], $document->getAttribute('objects'));
        $this->assertSame([7 => '2026-01-01 00:00:00.000'], $document->getAttribute('dates'));
    }
}
