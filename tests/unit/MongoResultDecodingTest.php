<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class MongoResultDecodingTest extends TestCase
{
    public function testCastingAfterCastsCollectionAndInternalAttributes(): void
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }
        };

        $collection = new Document([
            '$id' => 'movies',
            'attributes' => [
                new Document(['$id' => 'score', 'key' => 'score', 'type' => ColumnType::Integer->value, 'array' => false]),
                new Document(['$id' => 'price', 'key' => 'price', 'type' => ColumnType::Float->value, 'array' => false]),
                new Document(['$id' => 'active', 'key' => 'active', 'type' => ColumnType::Boolean->value, 'array' => false]),
                new Document(['$id' => 'tags', 'key' => 'tags', 'type' => ColumnType::String->value, 'array' => true]),
            ],
        ]);

        foreach ([['42', 42], ['7', 7]] as [$stored, $expected]) {
            $document = $adapter->castingAfter($collection, new Document([
                '$id' => 'movie1',
                '$sequence' => 5,
                '$permissions' => ['read("any")'],
                'score' => $stored,
                'price' => 3,
                'active' => 1,
                'tags' => ['a', 'b'],
            ]));

            $this->assertSame([
                '$id' => 'movie1',
                '$sequence' => '5',
                '$permissions' => ['read("any")'],
                'score' => $expected,
                'price' => 3.0,
                'active' => true,
                'tags' => ['a', 'b'],
            ], $document->getArrayCopy());
        }
    }

    public function testProjectionSkipsInternalAttributes(): void
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }

            /**
             * @param  array<string>  $selections
             */
            public function project(array $selections): mixed
            {
                return $this->getAttributeProjection($selections);
            }
        };

        $this->assertSame([
            'name' => 1,
            '_uid' => 1,
            '_id' => 1,
            '_createdAt' => 1,
            '_updatedAt' => 1,
            '_permissions' => 1,
        ], $adapter->project(['name', '$id', '$createdAt']));
    }
}
