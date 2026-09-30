<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
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
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

final class WithCacheTest extends TestCase
{
    private const string COLLECTION = 'reports';

    private const string KEY = 'reports:summary';

    private int $calls = 0;

    public function testACachedValueIsRefusedToACallerWhoCannotReadTheCollection(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::user('reader')->toString());
        $database = $this->database(new Memory(), new Cache(new MemoryCache()), $authorization, [Permission::read(Role::user('reader'))]);
        $database->getAuthorization()->skip(fn (): Document => $database->createDocument(self::COLLECTION, new Document([Document::ID => 'q3', 'title' => 'Q3'])));

        $this->assertSame('Q3', $this->cached($database, fn (): Document => $database->getDocument(self::COLLECTION, 'q3'))->getAttribute('title'));
        $this->assertSame('Q3', $this->cached($database, fn (): Document => $database->getDocument(self::COLLECTION, 'q3'))->getAttribute('title'));
        $this->assertSame(1, $this->calls);

        $authorization->removeRole(Role::user('reader')->toString());
        $this->expectException(AuthorizationException::class);
        $this->cached($database, fn (): Document => $database->getDocument(self::COLLECTION, 'q3'));
    }

    public function testACachedDocumentTheCallerCannotReadIsRecomputed(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::user('owner')->toString());
        $database = $this->database(new Memory(), new Cache(new MemoryCache()), $authorization, [], documentSecurity: true);
        $database->getAuthorization()->skip(fn (): Document => $database->createDocument(self::COLLECTION, new Document([
            Document::ID => 'q3',
            Document::PERMISSIONS => [Permission::read(Role::user('owner'))],
            'title' => 'Q3',
        ])));

        $this->assertSame('Q3', $this->cached($database, fn (): Document => $database->getDocument(self::COLLECTION, 'q3'))->getAttribute('title'));
        $authorization->removeRole(Role::user('owner')->toString());
        $authorization->addRole(Role::user('stranger')->toString());

        $this->assertTrue($this->cached($database, fn (): Document => $database->getDocument(self::COLLECTION, 'q3'))->isEmpty());
        $this->assertSame(2, $this->calls, 'the stranger\'s read runs the callback instead of serving the owner\'s copy');
    }

    public function testACachedValueOfADeletedCollectionIsRecomputed(): void
    {
        $database = $this->database(new Memory(), new Cache(new MemoryCache()));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'q3', 'title' => 'Q3']));

        $this->cached($database, fn (): array => $database->find(self::COLLECTION));
        $database->deleteCollection(self::COLLECTION);

        $this->assertSame('recomputed', $this->cached($database, fn (): string => 'recomputed'));
        $this->assertSame(2, $this->calls);
    }

    public function testACachedDocumentPastItsTimeToLiveIsRecomputed(): void
    {
        $adapter = new class () extends Memory {
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::TTLIndexes];
            }
        };
        $database = $this->database($adapter, new Cache(new MemoryCache()));
        $database->createIndex(self::COLLECTION, Index::ttl(key: 'expiry', attributes: ['publishedAt'], ttl: 1));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'q3', 'title' => 'Q3', 'publishedAt' => DateTime::now()]));

        $this->cached($database, fn (): array => $database->find(self::COLLECTION));
        $this->cached($database, fn (): array => $database->find(self::COLLECTION));
        $this->assertSame(1, $this->calls);
        \sleep(2);

        $this->cached($database, fn (): array => $database->find(self::COLLECTION));
        $this->assertSame(2, $this->calls, 'a cached document past its time to live is not served');
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function cacheFailures(): array
    {
        return [
            'the epoch cannot be read' => ['loadEpoch', 'Warning: Failed to load cache epoch: the cache refused loadEpoch'],
            'the epoch cannot be written' => ['saveEpoch', ''],
            'the epoch is not a string' => ['listEpoch', ''],
            'the value cannot be read' => ['loadValue', 'Warning: Failed to load cache value: the cache refused loadValue'],
            'a rejected value cannot be purged' => ['purge', 'Warning: Failed to purge rejected cache value: the cache refused purge'],
            'the generation cannot be read' => ['getGeneration', 'Warning: Failed to get cache generation: the cache refused getGeneration'],
            'the value cannot be written' => ['saveWithLease', 'Warning: Failed to save cache value: the cache refused saveWithLease'],
        ];
    }

    #[DataProvider('cacheFailures')]
    public function testACacheFailureFallsBackToTheCallback(string $failure, string $warning): void
    {
        $cache = $this->failingCache($failure);
        $database = $this->database(new Memory(), new Cache($cache));
        if ($failure === 'listEpoch') {
            $cache->save(self::KEY.'#epoch', ['not', 'an', 'epoch']);
        }
        if ($failure === 'purge') {
            $cache->save(self::KEY.'#epoch', 'fixed');
            $cache->save(self::KEY.'#fixed:', 'not a cache entry');
        }

        $value = null;
        $log = StderrCapture::during(function () use ($database, &$value): void {
            $value = $this->cached($database, fn (): string => 'computed');
        });

        $this->assertSame('computed', $value);
        $this->assertSame(1, $this->calls);
        if ($warning !== '') {
            $this->assertStringContainsString($warning, $log);
        }
    }

    /**
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    private function cached(Database $database, callable $callback): mixed
    {
        return $database->withCache(self::KEY, function () use ($callback) {
            $this->calls++;

            return $callback();
        });
    }

    private function failingCache(string $failure): MemoryCache
    {
        return new class ($failure) extends MemoryCache implements Leasable {
            public function __construct(private readonly string $failure)
            {
            }

            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                $epoch = \str_ends_with($key, '#epoch');
                if (\str_starts_with($key, 'reports:summary') && (($epoch && $this->failure === 'loadEpoch') || (! $epoch && $this->failure === 'loadValue'))) {
                    throw new RuntimeException("the cache refused {$this->failure}");
                }

                return parent::load($key, $ttl, $hash);
            }

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if ($this->failure === 'saveEpoch' && $key === 'reports:summary#epoch') {
                    return false;
                }

                return parent::save($key, $data, $hash);
            }

            public function purge(string $key, string $hash = ''): bool
            {
                if ($this->failure === 'purge' && \str_starts_with($key, 'reports:summary#')) {
                    throw new RuntimeException('the cache refused purge');
                }

                return parent::purge($key, $hash);
            }

            public function getGeneration(string $key): string
            {
                if ($this->failure === 'getGeneration' && \str_starts_with($key, 'reports:summary#')) {
                    throw new RuntimeException('the cache refused getGeneration');
                }

                return '0';
            }

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
            {
                if ($this->failure === 'saveWithLease' && \str_starts_with($key, 'reports:summary#')) {
                    throw new RuntimeException('the cache refused saveWithLease');
                }

                return parent::save($key, $data, $hash);
            }
        };
    }

    /**
     * @param  list<string>|null  $permissions
     */
    private function database(Memory $adapter, Cache $cache, ?Authorization $authorization = null, ?array $permissions = null, bool $documentSecurity = false): Database
    {
        $database = new Database($adapter, $cache);
        if ($authorization !== null) {
            $database->setAuthorization($authorization);
        }
        $database->setDatabase('with_cache')->setNamespace('with_cache_'.\uniqid());
        $database->create();
        $database->getAuthorization()->skip(fn (): mixed => $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 32), Attribute::datetime(key: 'publishedAt')],
            permissions: $permissions ?? [Permission::create(Role::any()), Permission::read(Role::any()), Permission::delete(Role::any())],
            documentSecurity: $documentSecurity,
        )));

        return $database;
    }
}
