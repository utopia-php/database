<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Throwable;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
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

    private function createFailure(Mongo $adapter, string $name): Throwable
    {
        try {
            $adapter->createCollection($name);
        } catch (Throwable $failure) {
            return $failure;
        }

        $this->fail('The collection was created');
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
