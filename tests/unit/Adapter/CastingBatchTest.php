<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Pools\Pool as UtopiaPool;

final class CastingBatchTest extends TestCase
{
    private const string NAMESPACE = 'casting';

    private const string COLLECTION = 'posts';

    private const int PAGE = 25;

    private const string STORED_WHEN = '1760405478290';

    public function testAMongoPageReadThroughAPoolComesBackWithEveryAttributeCast(): void
    {
        $documents = $this->database()->find(self::COLLECTION, [Query::limit(self::PAGE)]);

        $this->assertCount(self::PAGE, $documents);
        foreach ($documents as $position => $document) {
            $this->assertSame('post'.$position, $document->getId());
            $this->assertSame((string) ($position + 1), $document->getSequence());
            $this->assertSame($position, $document->getAttribute('score'));
            $this->assertSame((float) $position, $document->getAttribute('price'));
            $this->assertSame($position % 2 === 1, $document->getAttribute('active'));
            $this->assertSame(['a', (string) $position], $document->getAttribute('tags'));
            $this->assertSame((string) $position, $document->getAttribute('name'));
            $this->assertSame('2025-10-14T01:31:18.290+00:00', $document->getAttribute('when'));
        }
    }

    public function testAPoolOverMongoCastsAPageItIsHandedUnderItsKeys(): void
    {
        $pool = $this->pool();
        $collection = $this->collectionRecord();
        $page = ['first' => $this->stored(3), 9 => $this->stored(4)];

        $this->assertTrue($pool->hasFeature(Feature\Casting::class));

        $cast = $pool->castAfter(new Document([Document::ID => self::COLLECTION, 'attributes' => $collection['attributes']]), $page);

        $this->assertSame(['first', 9], \array_keys($cast));
        $this->assertSame(3, $cast['first']->getAttribute('score'));
        $this->assertSame(4.0, $cast[9]->getAttribute('price'));
        $this->assertSame(['a', '4'], $cast[9]->getAttribute('tags'));
        $this->assertSame([], $pool->castAfter(new Document([Document::ID => self::COLLECTION]), []));
    }

    public function testAnAdapterWithoutCastingLeavesItToTheLibrary(): void
    {
        $pool = new Pool($this->connections(new Memory()));
        $pool->setAuthorization(new Authorization());

        $this->assertFalse($pool->hasFeature(Feature\Casting::class));
    }

    private function stored(int $position): Document
    {
        return new Document([
            Document::ID => 'post'.$position,
            Document::SEQUENCE => $position + 1,
            'score' => (string) $position,
            'price' => $position,
            'active' => $position % 2,
            'tags' => \json_encode(['a', (string) $position]),
            'name' => $position,
            'when' => ['$date' => ['$numberLong' => self::STORED_WHEN]],
        ]);
    }

    private function database(): Database
    {
        $database = new Database($this->pool(), new Cache(new None()));
        $database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE);
        $database->getAuthorization()->disable();

        return $database;
    }

    private function pool(): Pool
    {
        $authorization = new Authorization();
        $authorization->disable();

        $adapter = new Mongo($this->client());
        $adapter->setAuthorization($authorization);

        $pool = new Pool($this->connections($adapter));
        $pool->setAuthorization($authorization);

        return $pool;
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($adapter));

        return $connections;
    }

    /**
     * @return array<string, mixed>
     */
    private function collectionRecord(): array
    {
        return [
            Storage::UID => self::COLLECTION,
            Storage::SEQUENCE => '1',
            'name' => self::COLLECTION,
            'documentSecurity' => false,
            Storage::PERMISSIONS => [],
            'attributes' => \json_encode(\array_map(
                static fn (Attribute $attribute): array => $attribute->toDocument()->getArrayCopy(),
                [
                    Attribute::integer(key: 'score'),
                    Attribute::double(key: 'price'),
                    Attribute::boolean(key: 'active'),
                    Attribute::string(key: 'tags', size: 16, array: true),
                    Attribute::string(key: 'name', size: 16),
                    Attribute::datetime(key: 'when'),
                ],
            )),
            'indexes' => '[]',
        ];
    }

    private function client(): Client
    {
        $page = [];
        for ($position = 0; $position < self::PAGE; $position++) {
            $page[] = (object) [
                Storage::UID => 'post'.$position,
                Storage::SEQUENCE => $position + 1,
                'score' => (string) $position,
                'price' => $position,
                'active' => $position % 2,
                'tags' => \json_encode(['a', (string) $position]),
                'name' => $position,
                'when' => (object) ['$date' => (object) ['$numberLong' => self::STORED_WHEN]],
            ];
        }

        return new class ((object) $this->collectionRecord(), $page) extends Client {
            /**
             * @param  list<stdClass>  $page
             */
            public function __construct(private readonly stdClass $collection, private readonly array $page)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function getHost(): string
            {
                return 'mongo';
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
                $documents = \str_ends_with($collection, Database::METADATA) ? [$this->collection] : $this->page;

                return (object) ['cursor' => (object) ['firstBatch' => $documents, 'id' => 0]];
            }
        };
    }
}
