<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;

final class MongoQueryFilterTest extends TestCase
{
    private const string COLLECTION = 'records';

    /**
     * @var array<string, list<array<mixed>>>
     */
    private array $calls = [];

    /**
     * @var list<stdClass>
     */
    private array $rows = [];

    private ?MongoException $aggregateError = null;

    public function testStartsWithIsAnchoredAtTheStartWithoutFlags(): void
    {
        $this->find([Query::startsWith('name', 'a.b$')]);

        $this->assertSame(
            [['name' => ['$regex' => '^a\.b\$']]],
            $this->calls['find'][0]['$and'] ?? null,
            'startsWith must anchor at the start, escape the value and stay case-sensitive',
        );
    }

    public function testEndsWithIsAnchoredAtTheEndWithoutFlags(): void
    {
        $this->find([Query::endsWith('name', 'a.b$')]);

        $this->assertSame(
            [['name' => ['$regex' => 'a\.b\$$']]],
            $this->calls['find'][0]['$and'] ?? null,
            'endsWith must anchor at the end, escape the value and stay case-sensitive',
        );
    }

    public function testContainsAllKeepsTheAllOperatorOnFind(): void
    {
        $query = Query::containsAll('tags', ['a', 'b']);
        $query->setOnArray(true);

        $this->find([$query]);

        $this->assertSame(
            [['tags' => ['$all' => ['a', 'b']]]],
            $this->calls['find'][0]['$and'] ?? null,
            'find() must send $all as count() does, not the rewritten _all',
        );
    }

    public function testCountRethrowsDriverErrors(): void
    {
        $this->aggregateError = new MongoException('invalid pipeline', 2);

        $this->expectException(MongoException::class);
        $this->expectExceptionMessage('invalid pipeline');

        $this->createAdapter()->count(new Document(['$id' => self::COLLECTION]));
    }

    public function testNullTenantIsReadAsTheTenantAttribute(): void
    {
        $this->rows = [(object) ['_uid' => 'first', '_tenant' => null]];

        $documents = $this->find([]);

        $this->assertCount(1, $documents);
        $stored = $documents[0]->getArrayCopy();
        $this->assertArrayNotHasKey('_tenant', $stored, 'A null _tenant must not leak as a storage key');
        $this->assertArrayHasKey('$tenant', $stored);
        $this->assertNull($stored['$tenant']);
    }

    public function testUniquePartialFiltersMatchTheAttributeTypes(): void
    {
        $adapter = $this->createAdapter();
        $types = [
            'count' => ColumnType::Integer->value,
            'total' => ColumnType::BigInteger->value,
            'price' => ColumnType::Float->value,
            'active' => ColumnType::Boolean->value,
            'seenAt' => ColumnType::Datetime->value,
            'name' => ColumnType::String->value,
        ];

        foreach (\array_keys($types) as $attribute) {
            $adapter->createIndex(self::COLLECTION, Index::unique(key: $attribute.'_key', attributes: [$attribute]), $types);
        }
        $adapter->createIndex(self::COLLECTION, Index::unique(key: 'name_count', attributes: ['name', 'count']), $types);

        $this->assertSame(
            [
                'count_key' => ['count' => ['$exists' => true, '$type' => ['int', 'long']]],
                'total_key' => ['total' => ['$exists' => true, '$type' => ['int', 'long']]],
                'price_key' => ['price' => ['$exists' => true, '$type' => ['double', 'int', 'long']]],
                'active_key' => ['active' => ['$exists' => true, '$type' => 'bool']],
                'seenAt_key' => ['seenAt' => ['$exists' => true, '$type' => 'date']],
                'name_key' => ['name' => ['$exists' => true, '$type' => 'string']],
                'name_count' => [
                    'name' => ['$exists' => true, '$type' => 'string'],
                    'count' => ['$exists' => true, '$type' => ['int', 'long']],
                ],
            ],
            $this->partialFilters(),
        );
    }

    public function testKeyIndexPartialFiltersRequireOnlyThatTheLeadingFieldExists(): void
    {
        $adapter = $this->createAdapter();
        $types = [
            'count' => ColumnType::Integer->value,
            'price' => ColumnType::Float->value,
            'active' => ColumnType::Boolean->value,
            'seenAt' => ColumnType::Datetime->value,
            'name' => ColumnType::String->value,
        ];

        $adapter->createIndex(self::COLLECTION, Index::key(key: 'count_key', attributes: ['count']), $types);
        $adapter->createIndex(self::COLLECTION, Index::key(key: 'name_seen', attributes: ['name', 'seenAt']), $types);
        $adapter->createCollection(
            'created',
            [
                Attribute::integer(key: 'count'),
                Attribute::double(key: 'price'),
                Attribute::boolean(key: 'active'),
            ],
            [
                Index::key(key: 'price_active', attributes: ['price', 'active']),
                Index::unique(key: 'count_unique', attributes: ['count']),
            ],
        );

        $this->assertSame(
            [
                'count_key' => ['count' => ['$exists' => true]],
                'name_seen' => ['name' => ['$exists' => true]],
                '_uid' => null,
                '_createdAt' => null,
                '_updatedAt' => null,
                '_permissions' => null,
                'price_active' => ['price' => ['$exists' => true]],
                'count_unique' => ['count' => ['$exists' => true, '$type' => ['int', 'long']]],
            ],
            $this->partialFilters(),
            'A key index must be usable by any filter on a value of its leading field, which implies $exists but never $type',
        );
    }

    public function testUniqueIndexOnAnIntegerFiltersOnTheIntegerTypes(): void
    {
        $this->createAdapter()->createIndex(
            self::COLLECTION,
            Index::unique(key: 'count_unique', attributes: ['count']),
            ['count' => ColumnType::Integer->value],
        );

        $this->assertSame(
            ['count_unique' => ['count' => ['$exists' => true, '$type' => ['int', 'long']]]],
            $this->partialFilters(),
        );
        $this->assertTrue($this->calls['createIndexes'][0]['unique'] ?? false);
    }

    public function testDottedAttributesAreRenamedAndDeletedByTheirStoredName(): void
    {
        $adapter = $this->createAdapter();

        $adapter->renameAttribute(self::COLLECTION, 'a.b', 'c.d');
        $adapter->deleteAttribute(self::COLLECTION, 'a.b');

        $this->assertSame(
            [
                ['$rename' => ['a__dot__b' => 'c__dot__d']],
                ['$unset' => ['a__dot__b' => '']],
            ],
            $this->calls['update'] ?? [],
            'Renames and deletes must address the __dot__ field name that documents are stored under',
        );
    }

    public function testSumEscapesDottedAttributes(): void
    {
        $collection = new Document([
            '$id' => self::COLLECTION,
            'attributes' => [
                new Document(['$id' => 'a.b', 'key' => 'a.b', 'type' => ColumnType::Integer->value]),
            ],
        ]);

        $this->createAdapter()->sum($collection, 'a.b', [Query::equal('a.b', [1])]);

        $this->assertSame(
            [
                ['$match' => ['$and' => [['a__dot__b' => ['$eq' => 1]]]]],
                ['$group' => ['_id' => null, 'total' => ['$sum' => '$a__dot__b']]],
            ],
            $this->calls['aggregate'][0] ?? null,
            'sum() must filter and sum the stored field name of a dotted attribute, as find() and count() do',
        );
    }

    public function testRandomOrderIsRejectedAsAQueryError(): void
    {
        $this->expectException(QueryException::class);

        $this->createAdapter()->find(
            new Document(['$id' => self::COLLECTION]),
            orderAttributes: [''],
            orderTypes: [OrderDirection::Random],
        );
    }

    public function testDocumentsValidatorRejectsRandomOrderWithoutTheCapability(): void
    {
        $unsupported = new Documents([], [], ColumnType::Integer->value, supportForOrderRandom: false);

        $this->assertFalse($unsupported->isValid([Query::orderRandom()]));
        $this->assertStringContainsString('Random order is not supported', $unsupported->getDescription());

        $supported = new Documents([], [], ColumnType::Integer->value);

        $this->assertTrue($supported->isValid([Query::orderRandom()]), $supported->getDescription());
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Document>
     */
    private function find(array $queries): array
    {
        return $this->createAdapter()->find(new Document(['$id' => self::COLLECTION]), $queries);
    }

    /**
     * @return array<string, mixed>
     */
    private function partialFilters(): array
    {
        $filters = [];
        foreach ($this->calls['createIndexes'] ?? [] as $index) {
            if (\is_string($index['name'] ?? null)) {
                $filters[$index['name']] = $index['partialFilterExpression'] ?? null;
            }
        }

        return $filters;
    }

    private function createAdapter(): Mongo
    {
        $record = function (string $call, array $payload): void {
            $this->calls[$call][] = $payload;
        };

        $client = new class ($record, $this->rows, $this->aggregateError) extends Client {
            /**
             * @var list<stdClass>
             */
            private array $created = [];

            /**
             * @param  Closure(string, array<mixed>): void  $record
             * @param  list<stdClass>  $rows
             */
            public function __construct(
                private readonly Closure $record,
                private readonly array $rows,
                private readonly ?MongoException $aggregateError,
            ) {
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
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createCollection(string $name, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                ($this->record)('find', $filters);

                return (object) ['cursor' => (object) ['firstBatch' => $this->rows, 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $pipeline
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function aggregate(string $collection, array $pipeline, array $options = []): stdClass
            {
                if ($this->aggregateError !== null) {
                    throw $this->aggregateError;
                }

                ($this->record)('aggregate', $pipeline);

                return (object) ['cursor' => (object) ['firstBatch' => []]];
            }

            /**
             * @param  array<mixed>  $where
             * @param  array<mixed>  $updates
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): int
            {
                ($this->record)('update', $updates);

                return 0;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                foreach ($indexes as $index) {
                    if (\is_array($index)) {
                        ($this->record)('createIndexes', $index);
                        $this->created[] = (object) ['name' => $index['name'] ?? null];
                    }
                }

                return true;
            }

            /**
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass
            {
                return (object) ['cursor' => (object) ['firstBatch' => $this->created]];
            }
        };

        $authorization = new Authorization();
        $authorization->disable();

        $adapter = new Mongo($client);
        $adapter->setAuthorization($authorization);
        $adapter->setNamespace('query_filter');

        return $adapter;
    }
}
