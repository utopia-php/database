<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class MongoResultDecodingTest extends TestCase
{
    public function testStoredRecordKeysAreRestored(): void
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }

            /**
             * @param  array<string, mixed>  $record
             * @return array<string, mixed>
             */
            public function restore(array $record): array
            {
                return $this->replaceChars('_', '$', $record);
            }
        };

        $restored = $adapter->restore([
            '_uid' => 'movie1',
            '_id' => '17',
            '_permissions' => ['read("any")', 'update("user:1")'],
            '_createdAt' => '2026-01-01 00:00:00.000',
            'tags' => ['t1', 't2'],
            'profile__dot__name' => 'Ann',
            'matrix' => [['_uid' => 'nested', 'a__dot__b' => 1], ['x', 'y']],
        ]);

        $this->assertSame([
            'tags' => ['t1', 't2'],
            'matrix' => [['a.b' => 1, '$id' => 'nested'], ['x', 'y']],
            '$permissions' => ['read("any")', 'update("user:1")'],
            '$createdAt' => '2026-01-01 00:00:00.000',
            'profile.name' => 'Ann',
            '$sequence' => '17',
            '$id' => 'movie1',
        ], $restored);
    }

    public function testDocumentKeysAreStored(): void
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }

            /**
             * @param  array<string, mixed>  $document
             * @return array<string, mixed>
             */
            public function store(array $document): array
            {
                return $this->replaceChars('$', '_', $document);
            }
        };

        $stored = $adapter->store([
            '$id' => 'movie1',
            '$permissions' => ['read("any")'],
            'tags' => ['a.b', '$c'],
            'profile.name' => 'Ann',
            '$custom' => 'value',
        ]);

        $this->assertSame([
            'tags' => ['a.b', '$c'],
            '_permissions' => ['read("any")'],
            'profile__dot__name' => 'Ann',
            '_custom' => 'value',
            '_uid' => 'movie1',
        ], $stored);
    }

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
