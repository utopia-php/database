<?php

namespace Tests\Unit;

use ArrayObject;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoNameFilterTest extends TestCase
{
    public function testCollectionWithPeriodInItsIdExistsAfterCreation(): void
    {
        $adapter = self::adapter(self::client());

        $adapter->createCollection('audit.logs');

        $this->assertTrue($adapter->exists('app', 'audit.logs'));
    }

    public function testCollectionWithNulByteInItsIdExistsAfterCreation(): void
    {
        $adapter = self::adapter(self::client());

        $adapter->createCollection("audit\0logs");

        $this->assertTrue($adapter->exists('app', "audit\0logs"));
    }

    public function testCollectionThatWasNeverCreatedDoesNotExist(): void
    {
        $adapter = self::adapter(self::client());

        $adapter->createCollection('audit.logs');

        $this->assertFalse($adapter->exists('app', 'audit.events'));
    }

    public function testDeleteDropsTheDatabaseThatSetDatabaseSelects(): void
    {
        $droppedDatabases = new ArrayObject();
        $adapter = self::adapter(self::client($droppedDatabases));
        $adapter->setDatabase("tenant.data\0");

        $this->assertTrue($adapter->delete("tenant.data\0"));
        $this->assertSame([$adapter->getDatabase()], $droppedDatabases->getArrayCopy());
        $this->assertSame(['tenantdata'], $droppedDatabases->getArrayCopy());
    }

    public function testDeleteKeepsAnAlreadyValidDatabaseName(): void
    {
        $droppedDatabases = new ArrayObject();
        $adapter = self::adapter(self::client($droppedDatabases));

        $adapter->delete('tenant_data-1');

        $this->assertSame(['tenant_data-1'], $droppedDatabases->getArrayCopy());
    }

    private static function adapter(Client $client): Mongo
    {
        $adapter = new Mongo($client);
        $adapter->setAuthorization(new Authorization());
        $adapter->setNamespace('names');

        return $adapter;
    }

    /**
     * @param  ArrayObject<int, string|null>  $droppedDatabases
     */
    private static function client(ArrayObject $droppedDatabases = new ArrayObject()): Client
    {
        return new class ($droppedDatabases) extends Client {
            /** @var array<string, true> */
            private array $collections = [];

            /**
             * @param  ArrayObject<int, string|null>  $droppedDatabases
             */
            public function __construct(private readonly ArrayObject $droppedDatabases)
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
                $this->collections[$name] = true;

                return true;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function dropDatabase(array $options = [], ?string $db = null): bool
            {
                $this->droppedDatabases->append($db);

                return true;
            }

            /**
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass|array|int
            {
                /** @var array{name?: string} $filter */
                $filter = $command['filter'] ?? [];
                $name = $filter['name'] ?? null;
                $matches = $name !== null && isset($this->collections[$name]) ? [(object) ['name' => $name]] : [];

                return (object) ['cursor' => (object) ['firstBatch' => $matches, 'id' => 0]];
            }
        };
    }
}
