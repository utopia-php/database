<?php

namespace Tests\Unit\Adapter;

use Closure;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Index;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;

/**
 * A later tenant's createCollection() under shared tables finds the collection another tenant
 * created. It must create the indexes its own definition declares, and report the collection
 * as existing so a failed metadata write never drops the collection every tenant shares.
 */
final class MongoSharedCollectionTest extends TestCase
{
    /**
     * @var list<array{collection: string, name: string, key: array<string, mixed>}>
     */
    private array $createdIndexes = [];

    private int $collectionsCreated = 0;

    public function testALaterTenantCreatesItsDeclaredIndexesAndReportsTheCollection(): void
    {
        $adapter = $this->createAdapter(exists: true);

        try {
            $adapter->createCollection('users', [Attribute::string(key: 'title', size: 64)], [Index::unique(key: 'byTitle', attributes: ['title'])]);
            $this->fail('A collection another tenant created must be reported as existing');
        } catch (DuplicateException $error) {
            $this->assertSame('Collection already exists', $error->getMessage());
        }

        $this->assertSame(0, $this->collectionsCreated);
        $this->assertSame(
            [['collection' => 'scope_users', 'name' => 'byTitle', 'key' => [Storage::TENANT => 1, 'title' => 1]]],
            $this->createdIndexes,
        );
    }

    public function testAnIndexTheCollectionHasUnderAnotherDefinitionIsLeft(): void
    {
        $adapter = $this->createAdapter(exists: true, conflicting: ['byTitle']);

        try {
            $adapter->createCollection('users', [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'age')], [
                Index::unique(key: 'byTitle', attributes: ['title']),
                Index::key(key: 'byAge', attributes: ['age']),
            ]);
            $this->fail('A collection another tenant created must be reported as existing');
        } catch (DuplicateException) {
            $this->addToAssertionCount(1);
        }

        $this->assertSame(['byAge'], \array_column($this->createdIndexes, 'name'));
    }

    public function testTheMetadataCollectionIsStillAcceptedAsIs(): void
    {
        $adapter = $this->createAdapter(exists: true);

        $this->assertTrue($adapter->createCollection(Database::METADATA));
        $this->assertSame([], $this->createdIndexes);
    }

    /**
     * @param  list<string>  $conflicting  Index names the collection already has under another definition
     */
    private function createAdapter(bool $exists, array $conflicting = []): Mongo
    {
        $recordIndex = function (string $collection, string $name, array $key): void {
            $this->createdIndexes[] = ['collection' => $collection, 'name' => $name, 'key' => $key];
        };
        $recordCollection = function (): void {
            $this->collectionsCreated++;
        };

        $client = new class ($exists, $conflicting, $recordIndex, $recordCollection) extends Client {
            /**
             * @param  list<string>  $conflicting
             * @param  Closure(string, string, array<string, mixed>): void  $recordIndex
             * @param  Closure(): void  $recordCollection
             */
            public function __construct(
                private readonly bool $exists,
                private readonly array $conflicting,
                private readonly Closure $recordIndex,
                private readonly Closure $recordCollection,
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
             * @param  array<mixed>  $command
             * @return stdClass|array<mixed>|int
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass|array|int
            {
                return (object) ['cursor' => (object) ['firstBatch' => $this->exists ? [(object) ['name' => 'existing']] : [], 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createCollection(string $name, array $options = []): bool
            {
                ($this->recordCollection)();

                return true;
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                foreach ($indexes as $index) {
                    /** @var array{name: string, key: array<string, mixed>} $index */
                    if (\in_array($index['name'], $this->conflicting, true)) {
                        throw new MongoException('An existing index has the same name as the requested index', 86);
                    }
                    ($this->recordIndex)($collection, $index['name'], $index['key']);
                }

                return true;
            }
        };

        $adapter = new Mongo($client);
        $adapter->setAuthorization(new Authorization());
        $adapter->setNamespace('scope');
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        return $adapter;
    }
}
