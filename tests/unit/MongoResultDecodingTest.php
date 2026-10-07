<?php

namespace Tests\Unit;

use Closure;
use MongoDB\BSON\Int64;
use MongoDB\BSON\UTCDateTime;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
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
                return $this->replaceCharacters('_', '$', $record);
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
                return $this->replaceCharacters('$', '_', $document);
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

    public function testCastingAfterRewritesOnlyTheValuesItChanges(): void
    {
        $adapter = $this->castingAdapter();
        $typed = $adapter->castingAfter($this->castingCollection(), new SetRecordingDocument([
            '$id' => 'movie1',
            '$sequence' => '5',
            '$permissions' => ['read("any")'],
            'score' => 7,
            'price' => 1.5,
            'active' => false,
            'tags' => ['a'],
            'name' => 'n',
        ]));

        $this->assertInstanceOf(SetRecordingDocument::class, $typed);
        $this->assertSame(['$permissions'], $typed->sets);

        $stored = $adapter->castingAfter($this->castingCollection(), new SetRecordingDocument([
            '$id' => 'movie2',
            '$sequence' => 6,
            'score' => '42',
            'price' => 1.5,
            'tags' => 5,
            'name' => 12,
        ]));

        $this->assertInstanceOf(SetRecordingDocument::class, $stored);
        $this->assertSame(['$sequence', 'name', 'score', 'tags'], $this->sorted($stored->sets));
        $this->assertSame(['$id' => 'movie2', '$sequence' => '6', 'score' => 42, 'price' => 1.5, 'tags' => ['5'], 'name' => '12'], $stored->getArrayCopy());
    }

    public function testCastingAfterNormalisesPermissionsWrittenPastSetAttribute(): void
    {
        $document = new Document(['$id' => 'movie1']);
        $document->offsetSet('$permissions', ['read("any")', 'read("any")', 'update("any")']);

        $cast = $this->castingAdapter()->castingAfter($this->castingCollection(), $document);

        $this->assertSame(['read("any")', 'update("any")'], $cast->getArrayCopy()['$permissions']);
    }

    /**
     * @return iterable<string, array{Document}>
     */
    public static function castingDocuments(): iterable
    {
        $object = new stdClass();
        $object->a = 1;
        $object->b = (object) ['c' => [2, 3]];

        yield 'typed' => [new Document(['$id' => 'd1', '$sequence' => '1', '$permissions' => ['read("any")'], 'score' => 7, 'price' => 1.5, 'active' => true, 'tags' => ['a', 'b'], 'name' => 'n'])];
        yield 'stored as other types' => [new Document(['$id' => 'd2', '$sequence' => 2, 'score' => '42', 'price' => 3, 'active' => 0, 'tags' => 5, 'name' => 12, 'ratio' => '0.25'])];
        yield 'json list' => [new Document(['$id' => 'd3', 'tags' => '["x","y"]', 'scores' => '[1,"2",3.0]'])];
        yield 'numbers in lists' => [new Document(['$id' => 'd4', 'scores' => ['1', 2, 'x', 4.7], 'flags' => [0, 1, 'yes', '']])];
        yield 'object' => [new Document(['$id' => 'd5', 'meta' => $object, 'free' => $object])];
        yield 'datetime strings' => [new Document(['$id' => 'd6', '$createdAt' => '2026-01-01 00:00:00.000', 'when' => 'not a date'])];
        yield 'nulls and operators' => [new Document(['$id' => 'd7', 'score' => null, 'price' => Operator::increment(2), 'tags' => null])];
        yield 'empty' => [new Document()];
        yield 'internal only' => [new Document(['$id' => 'd8', '$tenant' => 3, '$collection' => 'movies'])];

        if (\class_exists(Int64::class)) {
            yield 'bson values' => [new Document([
                '$id' => 'd9',
                '$sequence' => new Int64('9'),
                '$createdAt' => new UTCDateTime(1760405478290),
                'score' => new Int64('12'),
                'scores' => [new Int64('1'), 2],
                'when' => new UTCDateTime(0),
                'free' => new UTCDateTime(1000),
            ])];
        }
    }

    #[DataProvider('castingDocuments')]
    public function testCastingAfterDocumentsCastsEachDocumentAsCastingAfterDoes(Document $document): void
    {
        $collection = $this->castingCollection();
        $one = $this->castingAdapter(defined: false);
        $many = $this->castingAdapter(defined: false);
        $expected = $one->castingAfter($collection, clone $document)->getArrayCopy();

        $documents = $many->castingAfterDocuments($collection, ['first' => clone $document, 7 => clone $document]);

        $this->assertSame(['first', 7], \array_keys($documents));
        $this->assertSame($expected, $documents['first']->getArrayCopy());
        $this->assertSame($expected, $documents[7]->getArrayCopy());
        $this->assertSame([], $many->castingAfterDocuments($collection, []));
    }

    #[DataProvider('castingDocuments')]
    public function testCastingAfterIsUnchangedForDefinedAttributes(Document $document): void
    {
        $collection = $this->castingCollection();
        $expected = $this->referenceCastingAfter($collection, clone $document)->getArrayCopy();

        $this->assertSame($expected, $this->castingAdapter()->castingAfter($collection, clone $document)->getArrayCopy());
        $this->assertSame($expected, $this->castingAdapter()->castingAfterDocuments($collection, [clone $document])[0]->getArrayCopy());
    }

    private function castingCollection(): Document
    {
        return new Document([
            '$id' => 'movies',
            'attributes' => [
                new Document(['$id' => 'score', 'key' => 'score', 'type' => ColumnType::Integer->value, 'array' => false]),
                new Document(['$id' => 'scores', 'key' => 'scores', 'type' => ColumnType::BigInteger->value, 'array' => true]),
                new Document(['$id' => 'price', 'key' => 'price', 'type' => ColumnType::Float->value, 'array' => false]),
                new Document(['$id' => 'ratio', 'key' => 'ratio', 'type' => ColumnType::Double->value, 'array' => false]),
                new Document(['$id' => 'active', 'key' => 'active', 'type' => ColumnType::Boolean->value, 'array' => false]),
                new Document(['$id' => 'flags', 'key' => 'flags', 'type' => ColumnType::Boolean->value, 'array' => true]),
                new Document(['$id' => 'tags', 'key' => 'tags', 'type' => ColumnType::String->value, 'array' => true]),
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'array' => false]),
                new Document(['$id' => 'meta', 'key' => 'meta', 'type' => ColumnType::Object->value, 'array' => false]),
                new Document(['$id' => 'when', 'key' => 'when', 'type' => ColumnType::Datetime->value, 'array' => false]),
                ['$id' => 'legacy', 'type' => 'bigint', 'array' => false],
            ],
        ]);
    }

    private function castingAdapter(bool $defined = true): Mongo
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }
        };
        $adapter->setSupportForAttributes($defined);

        return $adapter;
    }

    /**
     * castingAfter() as it was before it skipped unchanged values and cast many documents at once.
     */
    private function referenceCastingAfter(Document $collection, Document $document): Document
    {
        if ($document->isEmpty()) {
            return $document;
        }

        /** @var array<int, Document|array<string, mixed>> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        $internal = \array_map(
            fn (Attribute $attribute): array => ['$id' => $attribute->key, 'type' => $attribute->type, 'array' => $attribute->array],
            Database::internalAttributesFor(true)
        );

        foreach (\array_merge($attributes, \array_values($internal)) as $attribute) {
            $key = \is_string($attribute['$id'] ?? null) ? $attribute['$id'] : '';
            $rawType = $attribute['type'] ?? null;
            $type = $rawType instanceof ColumnType ? $rawType : (\is_string($rawType) ? Attribute::typeFromStored($rawType) : null);
            $array = (bool) ($attribute['array'] ?? false);
            $value = $document->getAttribute($key);
            if ($value === null || Operator::isOperator($value)) {
                continue;
            }

            if ($array) {
                if (\is_string($value)) {
                    $value = \json_decode($value, true);
                }
                if (! \is_array($value)) {
                    $value = [$value];
                }
            } else {
                $value = [$value];
            }

            foreach ($value as $index => $node) {
                $value[$index] = match ($type) {
                    ColumnType::BigInteger, ColumnType::Integer => \is_int($node) ? $node : ($node instanceof Int64 ? (int) (string) $node : (\is_numeric($node) ? (int) $node : 0)),
                    ColumnType::String, ColumnType::Id => \is_string($node) ? $node : (\is_scalar($node) ? (string) $node : $node),
                    ColumnType::Float, ColumnType::Double => \is_float($node) ? $node : (\is_numeric($node) ? (float) $node : 0.0),
                    ColumnType::Boolean => \is_scalar($node) ? (bool) $node : $node,
                    ColumnType::Datetime => $node instanceof UTCDateTime ? DateTime::format($node->toDateTime()) : $node,
                    ColumnType::Object => $node instanceof stdClass ? \json_decode((string) \json_encode($node), true) : $node,
                    default => $node,
                };
            }
            $document->setAttribute($key, $array ? $value : $value[0]);
        }

        return $document;
    }

    /**
     * @param  list<string>  $keys
     * @return list<string>
     */
    private function sorted(array $keys): array
    {
        \sort($keys);

        return $keys;
    }

    public function testFoundRecordsTurnNestedObjectsIntoArrays(): void
    {
        $empty = new stdClass();
        $nestedEmpty = new stdClass();
        $listedEmpty = new stdClass();
        $record = (object) [
            '_uid' => 'movie1',
            '_id' => '17',
            '_permissions' => ['read("any")'],
            'meta' => (object) ['a' => 1, 'b' => (object) ['c' => [1, (object) ['d' => 'x']]], 'e' => $nestedEmpty],
            'list' => [1, 'two', (object) ['k' => true], [3, $listedEmpty], null],
            'keyed' => ['x' => (object) ['y' => 2], 'z' => 1.5],
            'empty' => $empty,
            'none' => null,
            'score' => 4,
        ];

        $client = new class ([$record]) extends Client {
            /**
             * @param  list<stdClass>  $records
             */
            public function __construct(private readonly array $records)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                return (object) ['cursor' => (object) ['firstBatch' => $this->records, 'id' => 0]];
            }
        };

        $adapter = new Mongo($client);
        $adapter->setAuthorization(new Authorization());
        $found = $adapter->find(new Document(['$id' => 'movies']));

        $this->assertCount(1, $found);
        $this->assertSame([
            'meta' => ['a' => 1, 'b' => ['c' => [1, ['d' => 'x']]], 'e' => $nestedEmpty],
            'list' => [1, 'two', ['k' => true], [3, $listedEmpty], null],
            'keyed' => ['x' => ['y' => 2], 'z' => 1.5],
            'empty' => $empty,
            'none' => null,
            'score' => 4,
            '$permissions' => ['read("any")'],
            '$sequence' => '17',
            '$id' => 'movie1',
        ], $found[0]->getArrayCopy());
    }

    public function testProjectionSkipsInternalAttributes(): void
    {
        $projections = [];
        $client = new class (function (mixed $projection) use (&$projections): void {
            $projections[] = $projection;
        }) extends Client {
            /**
             * @param  Closure(mixed): void  $record
             */
            public function __construct(private readonly Closure $record)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                ($this->record)($options['projection'] ?? null);

                return (object) ['cursor' => (object) ['firstBatch' => [], 'id' => 0]];
            }
        };

        $adapter = new Mongo($client);
        $adapter->getDocument(new Document(['$id' => 'movies']), 'movie1', [Query::select(['name', '$id', '$createdAt'])]);

        $this->assertSame([[
            'name' => 1,
            '_uid' => 1,
            '_id' => 1,
            '_createdAt' => 1,
            '_updatedAt' => 1,
            '_permissions' => 1,
        ]], $projections);
    }
}
