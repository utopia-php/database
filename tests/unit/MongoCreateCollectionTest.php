<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Throwable;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Index;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;

final class MongoCreateCollectionTest extends TestCase
{
    private const string SERVER_EXISTS = 'Collection utopiaTests.engine_orders already exists.';

    /**
     * @return array<string, array{0: MongoException, 1: string, 2: bool}>
     */
    public static function existingCollectionProvider(): array
    {
        return [
            'the client finds the collection' => [new MongoException('Collection Exists', 48), 'orders', false],
            'the client finds the collection, shared tables' => [new MongoException('Collection Exists', 48), 'orders', true],
            'an older client finds the collection' => [new MongoException('Collection Exists'), 'orders', false],
            'the server answers 48, shared tables' => [new MongoException(self::SERVER_EXISTS, 48), 'orders', true],
            'the server answers 48 for the metadata collection' => [new MongoException(self::SERVER_EXISTS, 48), Database::METADATA, false],
        ];
    }

    #[DataProvider('existingCollectionProvider')]
    public function testCreatingAnExistingCollectionSucceeds(MongoException $error, string $name, bool $sharedTables): void
    {
        $this->assertTrue($this->adapter($error, $sharedTables)->createCollection($name));
    }

    public function testServerAnsweringThatTheCollectionExistsIsDuplicateOutsideSharedTables(): void
    {
        $error = new MongoException(self::SERVER_EXISTS, 48);

        $failure = $this->createFailure($this->adapter($error, false), 'orders');

        $this->assertInstanceOf(DuplicateException::class, $failure);
        $this->assertSame('Collection already exists', $failure->getMessage());
        $this->assertSame($error, $failure->getPrevious());
    }

    public function testOtherErrorsAreRethrown(): void
    {
        $error = new MongoException('not authorized on utopiaTests to execute command', 13);

        $this->assertSame($error, $this->createFailure($this->adapter($error, true), 'orders'));
    }

    public function testAFailureCreatingTheInternalIndexesIsMapped(): void
    {
        $error = new MongoException('operation exceeded time limit', 50);

        $failure = $this->createFailure($this->indexFailingAdapter($error, failingCall: 1), 'orders');

        $this->assertInstanceOf(TimeoutException::class, $failure);
        $this->assertSame($error, $failure->getPrevious());
    }

    public function testAFailureCreatingTheDeclaredIndexesIsMapped(): void
    {
        $error = new MongoException('Index with name: title already exists with different options', 85);
        $adapter = $this->indexFailingAdapter($error, failingCall: 2);

        try {
            $adapter->createCollection('orders', [Attribute::string(key: 'title', size: 64)], [Index::key(key: 'title', attributes: ['title'])]);
            $this->fail('The collection was created');
        } catch (DuplicateException $failure) {
            $this->assertSame('Index already exists', $failure->getMessage());
            $this->assertSame($error, $failure->getPrevious());
        }
    }

    /**
     * @return array<string, array{Closure(int): bool, list<Index>}>
     */
    public static function indexesNotCreated(): array
    {
        $title = [Index::key(key: 'title', attributes: ['title'])];

        return [
            'the internal indexes are not created' => [static fn (int $call): bool => false, []],
            'the internal indexes fail' => [static fn (int $call): never => throw new MongoException('not authorized', 13), []],
            'the declared indexes are not created' => [static fn (int $call): bool => $call === 1, $title],
            'the declared indexes fail' => [static fn (int $call): bool => $call === 1 ? true : throw new MongoException('not authorized', 13), $title],
            'a declared index of an unsupported type' => [static fn (int $call): bool => true, [Index::spatial(key: 'title', attribute: 'title')]],
        ];
    }

    /**
     * @param  Closure(int): bool  $createIndexes
     * @param  list<Index>  $indexes
     */
    #[DataProvider('indexesNotCreated')]
    public function testACollectionWhoseIndexesAreNotCreatedIsDropped(Closure $createIndexes, array $indexes): void
    {
        $client = new class ($createIndexes) extends Client {
            /**
             * @var list<string>
             */
            public array $dropped = [];

            private int $calls = 0;

            /**
             * @param  Closure(int): bool  $createIndexes
             */
            public function __construct(private readonly Closure $createIndexes)
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
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createCollection(string $name, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                return ($this->createIndexes)(++$this->calls);
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function dropCollection(string $name, array $options = []): bool
            {
                $this->dropped[] = $name;

                return true;
            }
        };
        $adapter = new Mongo($client);
        $adapter->setNamespace('engine');

        try {
            $this->assertFalse($adapter->createCollection('orders', [Attribute::string(key: 'title', size: 64)], $indexes));
        } catch (Throwable) {
            // A failure is reported as itself; the collection must be dropped either way.
        }

        $this->assertSame(['engine_orders'], $client->dropped, 'A collection this call created without its indexes must not be left behind');
    }

    private function createFailure(Mongo $adapter, string $name): Throwable
    {
        try {
            $adapter->createCollection($name);
        } catch (Throwable $failure) {
            return $failure;
        }

        $this->fail('The collection was created');
    }

    private function indexFailingAdapter(MongoException $error, int $failingCall): Mongo
    {
        $client = new class ($error, $failingCall) extends Client {
            private int $calls = 0;

            public function __construct(private readonly MongoException $error, private readonly int $failingCall)
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
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createCollection(string $name, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                if (++$this->calls === $this->failingCall) {
                    throw $this->error;
                }

                return true;
            }
        };

        $adapter = new Mongo($client);
        $adapter->setNamespace('engine');

        return $adapter;
    }

    private function adapter(MongoException $error, bool $sharedTables): Mongo
    {
        $client = new class ($error) extends Client {
            public function __construct(private readonly MongoException $error)
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
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass
            {
                return (object) ['cursor' => (object) ['firstBatch' => [], 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createCollection(string $name, array $options = []): bool
            {
                throw $this->error;
            }
        };

        $adapter = new Mongo($client);
        $adapter->setNamespace('engine');
        $adapter->setSharedTables($sharedTables);

        return $adapter;
    }
}
