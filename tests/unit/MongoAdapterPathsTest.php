<?php

namespace Tests\Unit;

use Closure;
use MongoDB\BSON\Int64;
use MongoDB\BSON\UTCDateTime;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Query\Schema\ColumnType;

final class MongoAdapterPathsTest extends TestCase
{
    private const string NAMESPACE = 'paths';

    /**
     * @var list<array{0: string, 1: list<mixed>}>
     */
    private array $calls = [];

    /**
     * @var array<string, Closure(list<mixed>): mixed>
     */
    private array $replies = [];

    public function testManyToManyKeysAreRenamedInTheJunctionOfEitherSide(): void
    {
        $this->replies['find'] = fn (array $arguments): stdClass => self::batch(match (self::filterOf($arguments)[Storage::UID] ?? null) {
            'books' => [(object) [Storage::UID => 'books', Storage::SEQUENCE => '11']],
            'authors' => [(object) [Storage::UID => 'authors', Storage::SEQUENCE => '22']],
            default => [],
        });
        $adapter = $this->adapter();

        $adapter->updateRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'authors', twoWayKey: 'books'), RelationshipSide::Parent, new RelationshipUpdate(key: 'writers'));
        $adapter->updateRelationship('authors', Relationship::manyToMany(relatedCollection: 'books', twoWay: true, key: 'books', twoWayKey: 'authors'), RelationshipSide::Child, new RelationshipUpdate(key: 'titles'));

        $this->assertSame([
            [self::NAMESPACE.'__11_22', ['$rename' => ['authors' => 'writers']]],
            [self::NAMESPACE.'__11_22', ['$rename' => ['books' => 'titles']]],
        ], \array_map(static fn (array $arguments): array => [$arguments[0] ?? null, $arguments[2] ?? null], $this->argumentsOf('update')));
    }

    public function testFulltextIndexesAreCreatedWithoutTheCollation(): void
    {
        $adapter = $this->adapter();

        $adapter->createIndex('books', Index::fulltext(key: 'by_text', attributes: ['title']), [], ['locale' => 'en']);
        $adapter->createIndex('books', Index::key(key: 'by_title', attributes: ['title']), [], ['locale' => 'en']);

        $specifications = \array_map(static fn (array $arguments): mixed => \is_array($arguments[1] ?? null) ? ($arguments[1][0] ?? null) : null, $this->argumentsOf('createIndexes'));
        $this->assertCount(2, $specifications);
        $this->assertIsArray($specifications[0]);
        $this->assertArrayNotHasKey('collation', $specifications[0]);
        $this->assertIsArray($specifications[1]);
        $this->assertSame(['locale' => 'en', 'strength' => 1], $specifications[1]['collation'] ?? null);
    }

    public function testUniqueIndexCreationWaitsUntilTheBuildIsReady(): void
    {
        $listings = 0;
        $this->replies['query'] = function (array $arguments) use (&$listings): stdClass {
            $listings++;

            return (object) ['cursor' => (object) ['firstBatch' => [
                (object) ['name' => 'unique_title', 'buildState' => $listings === 1 ? 'building' : 'ready'],
            ]]];
        };

        $this->assertTrue($this->adapter()->createIndex('books', Index::unique(key: 'unique_title', attributes: ['title'])));
        $this->assertSame(2, $listings, 'The index list must be read again while the build is not ready');
    }

    public function testIndexCreationFailuresAreMapped(): void
    {
        $this->replies['createIndexes'] = static fn (): never => throw new MongoException('Index already exists with a different name', 85);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Index already exists');
        $this->adapter()->createIndex('books', Index::key(key: 'by_title', attributes: ['title']));
    }

    public function testRenamingAnIndexTheMetadataDoesNotListIsRefused(): void
    {
        $this->replies['find'] = static fn (): stdClass => self::batch([(object) [
            Storage::UID => 'books',
            'indexes' => \json_encode([['$id' => 'by_title', 'key' => 'by_title', 'type' => 'key', 'attributes' => ['title']]]),
            'attributes' => '[]',
        ]]);

        try {
            $this->adapter()->renameIndex('books', 'missing', 'renamed');
            $this->fail('Renaming an index the metadata does not list must be refused');
        } catch (DatabaseException $exception) {
            $this->assertSame('Index not found: missing', $exception->getMessage());
        }

        $this->assertSame([], $this->argumentsOf('dropIndexes'));
        $this->assertSame([], $this->argumentsOf('createIndexes'));
    }

    public function testRenamingAnIndexTheSchemaDoesNotHaveFailsWithTheDriverError(): void
    {
        $this->replyWithIndexMetadata();
        $this->replies['dropIndexes'] = static fn (): never => throw new MongoException('index not found with name [by_title]', 27);

        try {
            $this->adapter()->renameIndex('books', 'by_title', 'by_name');
            $this->fail('Renaming an index the schema does not have must fail');
        } catch (MongoException $exception) {
            $this->assertSame(27, $exception->getCode());
        }

        $this->assertSame([], $this->argumentsOf('createIndexes'));
    }

    public function testRenamingAnIndexTheSchemaHasRebuildsItUnderTheNewName(): void
    {
        $this->replyWithIndexMetadata();

        $this->assertTrue($this->adapter()->renameIndex('books', 'by_title', 'by_name'));
        $this->assertSame([[self::NAMESPACE.'_books', ['by_title'], []]], $this->argumentsOf('dropIndexes'));
        $this->assertSame(['by_name'], \array_map(static function (array $arguments): mixed {
            $indexes = $arguments[1] ?? null;
            $first = \is_array($indexes) ? ($indexes[0] ?? null) : null;

            return \is_array($first) ? ($first['name'] ?? null) : null;
        }, $this->argumentsOf('createIndexes')));
    }

    public function testANonNumericPowerExponentIsRefused(): void
    {
        try {
            $this->adapter()->updateDocument(new Document(['$id' => 'books']), 'first', new Document(['price' => Operator::power('two')]), true);
            $this->fail('A non-numeric power exponent must be refused');
        } catch (DatabaseException $exception) {
            $this->assertSame('Invalid numeric operand for operator power', $exception->getMessage());
        }

        $this->assertSame([], $this->argumentsOf('query'), 'No update may reach the server');
    }

    public function testSequencesAreReadBeyondTheFirstBatch(): void
    {
        $this->replies['find'] = static fn (): stdClass => (object) ['cursor' => (object) [
            'firstBatch' => [(object) [Storage::UID => 'first', Storage::SEQUENCE => 'one']],
            'id' => 7,
        ]];
        $this->replies['getMore'] = static fn (): stdClass => (object) ['cursor' => (object) [
            'nextBatch' => [(object) [Storage::UID => 'second', Storage::SEQUENCE => 'two']],
            'id' => 0,
        ]];

        $documents = $this->adapter()->getSequences('books', [new Document(['$id' => 'first']), new Document(['$id' => 'second'])]);

        $this->assertSame(['one', 'two'], \array_map(static fn (Document $document): ?string => $document->getSequence(), $documents));
        $this->assertSame([7], \array_map(static fn (array $arguments): mixed => $arguments[0] ?? null, $this->argumentsOf('getMore')));
    }

    public function testSequenceReadFailuresAreMapped(): void
    {
        $this->replies['find'] = static fn (): never => throw new MongoException('operation exceeded time limit', 50);

        $this->expectException(TimeoutException::class);
        $this->adapter()->getSequences('books', [new Document(['$id' => 'first'])]);
    }

    public function testIntegerOperatorOperandsAreCastBeforeTheWrite(): void
    {
        $adapter = $this->adapter();
        $collection = new Document(['attributes' => [['$id' => 'count', 'type' => ColumnType::Integer->value, 'array' => false]]]);

        $document = $adapter->castBefore($collection, new Document(['count' => Operator::increment('5', '100')]));

        $operator = $document->getAttribute('count');
        $this->assertInstanceOf(Operator::class, $operator);
        $this->assertSame([5, 100], $operator->getValues());

        $this->expectException(TypeException::class);
        $this->expectExceptionMessage('outside the signed 64-bit range');
        $adapter->castBefore($collection, new Document(['count' => Operator::increment('9223372036854775808')]));
    }

    public function testArrayAttributesAreDecodedOrWrappedBeforeTheWrite(): void
    {
        $collection = new Document(['attributes' => [
            ['$id' => 'tags', 'type' => ColumnType::String->value, 'array' => true],
            ['$id' => 'labels', 'type' => ColumnType::String->value, 'array' => true],
            ['$id' => 'meta', 'type' => ColumnType::Object->value, 'array' => false],
        ]]);

        $adapter = $this->adapter();
        $document = $adapter->castBefore($collection, new Document([
            'tags' => '["a","b"]',
            'labels' => 7,
            'meta' => '{"colour":"red"}',
        ]));

        $this->assertSame(['a', 'b'], $document->getAttribute('tags'));
        $this->assertSame([7], $document->getAttribute('labels'));
        $this->assertEquals((object) ['colour' => 'red'], $document->getAttribute('meta'));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Failed to decode JSON for attribute tags');
        $adapter->castBefore($collection, new Document(['tags' => 'not json']));
    }

    public function testStoredValuesAreReadBackByTheirAttributeType(): void
    {
        $adapter = $this->adapter();
        $collection = new Document(['attributes' => [
            ['$id' => 'tags', 'type' => ColumnType::String->value, 'array' => true],
            ['$id' => 'labels', 'type' => ColumnType::String->value, 'array' => true],
            ['$id' => 'count', 'type' => ColumnType::Integer->value, 'array' => false],
        ]]);

        $empty = new Document();
        $this->assertSame($empty, $adapter->castAfter($collection, [$empty])[0]);

        $document = $adapter->castAfter($collection, [new Document([
            'tags' => '["a","b"]',
            'labels' => 7,
            'count' => '42',
        ])])[0];

        $this->assertSame(['a', 'b'], $document->getAttribute('tags'));
        $this->assertSame(['7'], $document->getAttribute('labels'));
        $this->assertSame(42, $document->getAttribute('count'));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Failed to decode JSON for attribute tags');
        $adapter->castAfter($collection, [new Document(['tags' => 'not json'])]);
    }

    public function testSchemalessCastingKeepsStringsThatAreNotDates(): void
    {
        $adapter = $this->adapter();
        $adapter->setSupportForAttributes(false);
        $collection = new Document(['indexes' => [['$id' => 'expiry', 'type' => 'ttl', 'attributes' => ['expiresAt']]]]);

        $document = $adapter->castBefore($collection, new Document([
            'expiresAt' => '2026-13-45T99:99:99Z',
            'label' => 'plain',
        ]));

        $this->assertSame('2026-13-45T99:99:99Z', $document->getAttribute('expiresAt'));
        $this->assertSame('plain', $document->getAttribute('label'));
    }

    #[RequiresPhpExtension('mongodb')]
    public function testNumericDatesAndInt64ValuesAreConverted(): void
    {
        $adapter = $this->adapter();

        $before = $adapter->castBefore(
            new Document(['attributes' => [['$id' => 'when', 'type' => ColumnType::Datetime->value, 'array' => false]]]),
            new Document(['when' => '1700000000000']),
        );
        $when = $before->getAttribute('when');
        $this->assertInstanceOf(UTCDateTime::class, $when);
        $this->assertSame('1700000000000', (string) $when);

        $after = $adapter->castAfter(
            new Document(['attributes' => [['$id' => 'count', 'type' => ColumnType::BigInteger->value, 'array' => false]]]),
            [new Document(['count' => new Int64('9007199254740993')])],
        )[0];
        $this->assertSame(9007199254740993, $after->getAttribute('count'));
    }

    public function testReconnectReconnectsTheClient(): void
    {
        $adapter = $this->adapter();
        $connections = \count($this->argumentsOf('connect'));

        $adapter->reconnect();

        $this->assertCount($connections + 1, $this->argumentsOf('connect'));
    }

    public function testOperatorsRefuseOperandsOfTheWrongType(): void
    {
        $cases = [
            'dateAddDays' => [new Operator(OperatorType::DateAddDays, '', ['5']), 'Invalid integer operand for operator dateAddDays'],
            'arrayInsert' => [new Operator(OperatorType::ArrayInsert, '', ['1', 'x']), 'Invalid integer operand for operator arrayInsert'],
            'arrayFilter' => [new Operator(OperatorType::ArrayFilter, '', [5]), 'Invalid string operand for operator arrayFilter'],
        ];

        foreach ($cases as $case => [$operator, $message]) {
            try {
                $this->adapter()->updateDocument(new Document(['$id' => 'books']), 'first', new Document(['value' => $operator]), true);
                $this->fail("{$case}: an operand of the wrong type must be refused");
            } catch (DatabaseException $exception) {
                $this->assertSame($message, $exception->getMessage(), $case);
            }
        }

        $this->assertSame([], $this->argumentsOf('query'), 'No update may reach the server');
    }

    public function testAReadEndingOnAFullBatchKillsItsCursor(): void
    {
        $this->replies['find'] = static fn (): stdClass => (object) ['cursor' => (object) [
            'firstBatch' => \array_map(static fn (int $index): object => (object) [Storage::UID => 'row'.$index], \range(1, 3)),
            'id' => 7,
        ]];
        $this->replies['getMore'] = static fn (): stdClass => (object) ['cursor' => (object) ['nextBatch' => [], 'id' => 7]];

        $documents = $this->adapter()->find(new Document(['$id' => 'books']), limit: null);

        $this->assertCount(3, $documents);
        $this->assertSame([['killCursors' => self::NAMESPACE.'_books', 'cursors' => [7]]], \array_map(static fn (array $arguments): mixed => $arguments[0] ?? null, $this->argumentsOf('query')));
    }

    public function testResponsesWithoutACursorIdEndTheRead(): void
    {
        $this->replies['find'] = static fn (): stdClass => (object) ['cursor' => (object) ['firstBatch' => [(object) [Storage::UID => 'first']]]];

        $this->assertCount(1, $this->adapter()->find(new Document(['$id' => 'books']), limit: null));
        $this->assertSame([], $this->argumentsOf('getMore'));

        $this->calls = [];
        $this->replies['find'] = static fn (): stdClass => (object) ['cursor' => (object) ['firstBatch' => [(object) [Storage::UID => 'first']], 'id' => 7]];
        $this->replies['getMore'] = static fn (): stdClass => (object) ['cursor' => (object) ['nextBatch' => [(object) [Storage::UID => 'second']]]];

        $this->assertCount(2, $this->adapter()->find(new Document(['$id' => 'books']), limit: null));
        $this->assertCount(1, $this->argumentsOf('getMore'));
        $this->assertSame([], $this->argumentsOf('query'), 'A cursor the server closed must not be killed');
    }

    public function testDollarPrefixedUserKeyRoundTrips(): void
    {
        $this->replies['find'] = static fn (): stdClass => self::batch([(object) [Storage::UID => 'first', '_custom' => 'x']]);

        $created = $this->adapter()->createDocument(new Document(['$id' => 'books']), new Document(['$id' => 'first', '$permissions' => [], '$custom' => 'x']));

        $inserted = $this->argumentsOf('insert')[0][1] ?? null;
        $this->assertIsArray($inserted);
        $this->assertSame('x', $inserted['_custom'] ?? null);
        $this->assertArrayNotHasKey('$custom', $inserted);
        $this->assertSame('x', $created->getAttribute('$custom'));
    }

    /**
     * @param  array<mixed>  $arguments
     * @return array<mixed>
     */
    private static function filterOf(array $arguments): array
    {
        $filter = $arguments[1] ?? [];

        return \is_array($filter) ? $filter : [];
    }

    /**
     * @param  list<object>  $documents
     */
    private static function batch(array $documents): stdClass
    {
        return (object) ['cursor' => (object) ['firstBatch' => $documents, 'id' => 0]];
    }

    /**
     * @return list<list<mixed>>
     */
    private function argumentsOf(string $method): array
    {
        $calls = \array_filter($this->calls, static fn (array $call): bool => $call[0] === $method);

        return \array_values(\array_map(static fn (array $call): array => $call[1], $calls));
    }

    /**
     * @param  list<mixed>  $arguments
     */
    private function record(string $method, array $arguments): mixed
    {
        $this->calls[] = [$method, $arguments];
        $reply = $this->replies[$method] ?? null;

        return $reply !== null ? $reply($arguments) : null;
    }

    private function replyWithIndexMetadata(): void
    {
        $this->replies['find'] = static fn (): stdClass => self::batch([(object) [
            Storage::UID => 'books',
            'indexes' => \json_encode([['$id' => 'by_title', 'key' => 'by_title', 'type' => 'key', 'attributes' => ['title']]]),
            'attributes' => \json_encode([['$id' => 'title', 'key' => 'title', 'type' => 'string']]),
        ]]);
    }

    private function adapter(): Mongo
    {
        $client = new class ($this->record(...)) extends Client {
            /**
             * @param  Closure(string, list<mixed>): mixed  $record
             */
            public function __construct(private readonly Closure $record)
            {
            }

            #[\Override]
            public function connect(): self
            {
                ($this->record)('connect', []);

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
                $reply = ($this->record)('find', [$collection, $filters, $options]);

                return $reply instanceof stdClass ? $reply : (object) ['cursor' => (object) ['firstBatch' => [], 'id' => 0]];
            }

            #[\Override]
            public function getMore(int $cursorId, string $collection, int $batchSize = 25): stdClass
            {
                $reply = ($this->record)('getMore', [$cursorId, $collection, $batchSize]);

                return $reply instanceof stdClass ? $reply : (object) ['cursor' => (object) ['nextBatch' => [], 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $where
             * @param  array<mixed>  $updates
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): int
            {
                $reply = ($this->record)('update', [$collection, $where, $updates, $options, $multi]);

                return \is_int($reply) ? $reply : 1;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                $reply = ($this->record)('createIndexes', [$collection, $indexes, $options]);

                return \is_bool($reply) ? $reply : true;
            }

            /**
             * @param  array<mixed>  $document
             * @param  array<mixed>  $options
             * @return array<mixed>
             */
            #[\Override]
            public function insert(string $collection, array $document, array $options = []): array
            {
                ($this->record)('insert', [$collection, $document, $options]);

                return $document;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function dropIndexes(string $collection, array $indexes, array $options = []): self
            {
                ($this->record)('dropIndexes', [$collection, $indexes, $options]);

                return $this;
            }

            /**
             * @param  array<mixed>  $command
             * @return stdClass|array<mixed>|int
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass|array|int
            {
                $reply = ($this->record)('query', [$command, $db]);

                return $reply instanceof stdClass || \is_array($reply) || \is_int($reply) ? $reply : 1;
            }
        };

        $authorization = new Authorization();
        $authorization->disable();

        $adapter = new Mongo($client);
        $adapter->setAuthorization($authorization);
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter;
    }
}
