<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoTimeoutTest extends TestCase
{
    private const string COLLECTION = 'records';

    private const string MAX_TIME = 'maxTimeMS';

    /**
     * @var list<array<mixed>>
     */
    private array $options = [];

    private Mongo $adapter;

    #[\Override]
    protected function setUp(): void
    {
        $this->options = [];
        $this->adapter = $this->createAdapter();
    }

    public function testAnEventTimeoutBoundsOnlyThatEvent(): void
    {
        $this->adapter->setTimeout(500, Event::DocumentCount);

        $this->assertArrayNotHasKey(self::MAX_TIME, $this->findOptions(), 'a count timeout must not bound find()');
        $this->assertSame(500, $this->countOptions()[self::MAX_TIME] ?? null, 'count() must carry its own timeout');
        $this->assertArrayNotHasKey(self::MAX_TIME, $this->sumOptions(), 'a count timeout must not bound sum()');
    }

    public function testEachReadCarriesItsOwnEventTimeout(): void
    {
        $this->adapter->setTimeout(100, Event::DocumentFind);
        $this->adapter->setTimeout(200, Event::DocumentCount);
        $this->adapter->setTimeout(300, Event::DocumentSum);

        $this->assertSame(100, $this->findOptions()[self::MAX_TIME] ?? null);
        $this->assertSame(200, $this->countOptions()[self::MAX_TIME] ?? null);
        $this->assertSame(300, $this->sumOptions()[self::MAX_TIME] ?? null);
    }

    public function testAnEventTimeoutOverridesTheGlobalTimeout(): void
    {
        $this->adapter->setTimeout(1000);
        $this->adapter->setTimeout(50, Event::DocumentFind);

        $this->assertSame(50, $this->findOptions()[self::MAX_TIME] ?? null);
        $this->assertSame(1000, $this->countOptions()[self::MAX_TIME] ?? null);
    }

    public function testClearingOneEventKeepsTheGlobalTimeout(): void
    {
        $this->adapter->setTimeout(1000);
        $this->adapter->setTimeout(50, Event::DocumentFind);
        $this->adapter->clearTimeout(Event::DocumentFind);

        $this->assertSame(1000, $this->findOptions()[self::MAX_TIME] ?? null, 'clearing find must fall back to the global timeout');
        $this->assertSame(1000, $this->countOptions()[self::MAX_TIME] ?? null, 'clearing find must leave count bounded');
    }

    public function testClearingAnEventWithoutItsOwnTimeoutKeepsTheGlobalTimeout(): void
    {
        $this->adapter->setTimeout(1000);
        $this->adapter->clearTimeout(Event::DocumentCount);

        $this->assertSame(1000, $this->countOptions()[self::MAX_TIME] ?? null);
        $this->assertSame(1000, $this->findOptions()[self::MAX_TIME] ?? null);
    }

    public function testAZeroTimeoutClearsThatEvent(): void
    {
        $this->adapter->setTimeout(1000);
        $this->adapter->setTimeout(50, Event::DocumentCount);
        $this->adapter->setTimeout(0, Event::DocumentCount);

        $this->assertSame(1000, $this->countOptions()[self::MAX_TIME] ?? null, 'a zero count timeout must fall back to the global timeout');
        $this->assertSame(1000, $this->findOptions()[self::MAX_TIME] ?? null, 'a zero count timeout must not touch find()');
        $this->assertSame(1000, $this->adapter->getTimeout(Event::DocumentCount));
    }

    public function testClearingTheGlobalTimeoutClearsEveryEvent(): void
    {
        $this->adapter->setTimeout(1000);
        $this->adapter->setTimeout(50, Event::DocumentFind);
        $this->adapter->clearTimeout();

        $this->assertArrayNotHasKey(self::MAX_TIME, $this->findOptions());
        $this->assertArrayNotHasKey(self::MAX_TIME, $this->countOptions());
        $this->assertArrayNotHasKey(self::MAX_TIME, $this->sumOptions());
    }

    /**
     * @return array<mixed>
     */
    private function findOptions(): array
    {
        $this->adapter->find(new Document(['$id' => self::COLLECTION]));

        return $this->lastOptions();
    }

    /**
     * @return array<mixed>
     */
    private function countOptions(): array
    {
        $this->adapter->count(new Document(['$id' => self::COLLECTION]));

        return $this->lastOptions();
    }

    /**
     * @return array<mixed>
     */
    private function sumOptions(): array
    {
        $this->adapter->sum(new Document(['$id' => self::COLLECTION]), 'price');

        return $this->lastOptions();
    }

    /**
     * @return array<mixed>
     */
    private function lastOptions(): array
    {
        $this->assertNotEmpty($this->options, 'the read must reach the client');

        return $this->options[\array_key_last($this->options)];
    }

    private function createAdapter(): Mongo
    {
        $record = function (array $options): void {
            $this->options[] = $options;
        };

        $client = new class ($record) extends Client {
            /**
             * @param  Closure(array<mixed>): void  $record
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
                ($this->record)($options);

                return (object) ['cursor' => (object) ['firstBatch' => [], 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $pipeline
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function aggregate(string $collection, array $pipeline, array $options = []): stdClass
            {
                ($this->record)($options);

                return (object) ['cursor' => (object) ['firstBatch' => []]];
            }
        };

        $authorization = new Authorization();
        $authorization->disable();

        $adapter = new Mongo($client);
        $adapter->setAuthorization($authorization);
        $adapter->setNamespace('timeouts');

        return $adapter;
    }
}
