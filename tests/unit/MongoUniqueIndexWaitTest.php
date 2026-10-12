<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index;
use Utopia\Mongo\Client;

/**
 * After creating a unique index, the Mongo adapter polls listIndexes until the index is built, so the constraint
 * holds once createIndex() returns. A listing that fails is retried until the polls run out.
 */
final class MongoUniqueIndexWaitTest extends TestCase
{
    public function testAListingThatFailsOnceIsRetriedUntilTheIndexIsReady(): void
    {
        $listings = 0;
        $adapter = $this->adapter(static function () use (&$listings): stdClass {
            if (++$listings === 1) {
                throw new RuntimeException('the listing timed out');
            }

            return (object) ['cursor' => (object) ['firstBatch' => [(object) ['name' => 'unique_email', 'buildState' => 'ready']]]];
        });

        $this->assertTrue($adapter->createIndex('users', Index::unique(key: 'unique_email', attributes: ['email'])));
        $this->assertSame(2, $listings);
    }

    public function testAListingThatKeepsFailingTimesOutWithItsLastError(): void
    {
        $listings = 0;
        $adapter = $this->adapter(static function () use (&$listings): stdClass {
            $listings++;

            throw new RuntimeException('the listing timed out');
        });

        try {
            $adapter->createIndex('users', Index::unique(key: 'unique_email', attributes: ['email']));
            $this->fail('an index whose build cannot be confirmed must not be reported as created');
        } catch (DatabaseException $error) {
            $this->assertSame('Timeout waiting for index creation: the listing timed out', $error->getMessage());
            $this->assertInstanceOf(RuntimeException::class, $error->getPrevious());
        }

        $this->assertSame(10, $listings);
    }

    /**
     * @param  Closure(): stdClass  $listIndexes
     */
    private function adapter(Closure $listIndexes): Mongo
    {
        $client = new class ($listIndexes) extends Client {
            /**
             * @param  Closure(): stdClass  $listIndexes
             */
            public function __construct(private readonly Closure $listIndexes)
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
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass
            {
                return ($this->listIndexes)();
            }
        };

        $adapter = new Mongo($client);
        $adapter->setNamespace('index_wait');

        return $adapter;
    }
}
