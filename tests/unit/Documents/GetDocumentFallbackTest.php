<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\StderrCapture;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;

final class GetDocumentFallbackTest extends TestCase
{
    private const string COLLECTION = 'sessions';

    public function testAnEmptyCollectionIdIsNotFound(): void
    {
        $database = $this->database(new Memory(), new Cache(new MemoryCache()));

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $database->getDocument('', 'session');
    }

    public function testAJoinOnAnAdapterWithoutJoinsIsRefused(): void
    {
        $database = $this->database(new Memory(), new Cache(new MemoryCache()));

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query method: join');

        $database->getDocument(self::COLLECTION, 'session', [Query::join(self::COLLECTION, 'twin', [Query::on('owner', 'owner')])]);
    }

    public function testACacheThatCannotBeReadOrWrittenFallsBackToTheDatabase(): void
    {
        [$cache, $fail] = $this->failingCache();
        $database = $this->database(new Memory(), new Cache($cache));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'session', 'owner' => 'ada']));
        $fail();

        $document = null;
        $missing = null;
        $log = StderrCapture::during(function () use ($database, &$document, &$missing): void {
            $document = $database->getDocument(self::COLLECTION, 'session');
            $missing = $database->getDocument(self::COLLECTION, 'absent');
        });

        $this->assertInstanceOf(Document::class, $document);
        $this->assertSame('ada', $document->getAttribute('owner'));
        $this->assertInstanceOf(Document::class, $missing);
        $this->assertTrue($missing->isEmpty());
        $this->assertStringContainsString('Warning: Failed to get document from cache: the cache refused load', $log);
        $this->assertStringContainsString('Warning: Failed to get cache generation: the cache refused getGeneration', $log);
        $this->assertStringContainsString('Failed to save document to cache: the cache refused saveWithLease', $log);
        $this->assertStringContainsString('Failed to save empty document to cache: the cache refused saveWithLease', $log);
    }

    public function testACachedDocumentPastItsTimeToLiveReadsAsEmpty(): void
    {
        $database = $this->database($this->ttlMemory(), new Cache(new MemoryCache()));
        $database->createIndex(self::COLLECTION, Index::ttl(key: 'expiry', attribute: 'startedAt', ttl: 1));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'session', 'owner' => 'ada', 'startedAt' => DateTime::now()]));

        $this->assertSame('ada', $database->getDocument(self::COLLECTION, 'session')->getAttribute('owner'));
        \sleep(2);

        $this->assertTrue($database->getDocument(self::COLLECTION, 'session')->isEmpty(), 'the cached copy is past its time to live');
    }

    private function ttlMemory(): Memory
    {
        return new class () extends Memory {
            #[\Override]
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::IndexTtl];
            }
        };
    }

    /**
     * @return array{MemoryCache, Closure(): void}
     */
    private function failingCache(): array
    {
        $cache = new class (self::COLLECTION) extends MemoryCache implements Leasable {
            public bool $failing = false;

            public function __construct(private readonly string $collection)
            {
            }

            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                $this->refuse('load', $key);

                return parent::load($key, $ttl, $hash);
            }

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                return parent::save($key, $data, $hash);
            }

            public function getGeneration(string $key): string
            {
                $this->refuse('getGeneration', $key);

                return '0';
            }

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
            {
                $this->refuse('saveWithLease', $key);

                return parent::save($key, $data, $hash);
            }

            private function refuse(string $operation, string $key): void
            {
                if ($this->failing && ! \str_contains($key, '#') && \str_contains($key, ':'.$this->collection.':')) {
                    throw new RuntimeException("the cache refused {$operation}");
                }
            }
        };

        return [$cache, static function () use ($cache): void {
            $cache->failing = true;
        }];
    }

    private function database(Memory $adapter, Cache $cache): Database
    {
        $database = new Database($adapter, $cache);
        $database->setDatabase('fallback')->setNamespace('fallback_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'owner', size: 32), Attribute::datetime(key: 'startedAt')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
