<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

class ListCacheTest extends TestCase
{
    /**
     * @param array<string, array{encode: callable, decode: callable}> $filters
     */
    private function createDatabase(Adapter $cache, array $filters = [], ?DatabaseMemory $adapter = null): Database
    {
        $database = new Database($adapter ?? new DatabaseMemory(), new Cache($cache), $filters);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('list_cache_' . \uniqid());

        $database->create();

        return $database;
    }

    public function testWithCacheUsesCallbackOnMissAndCachesResult(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);

        $callbackCalls = 0;

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): array {
                $callbackCalls++;
                return ['value' => 'fresh'];
            },
        );

        $this->assertSame(['value' => 'fresh'], $value);
        $this->assertSame(1, $callbackCalls);

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): array {
                $callbackCalls++;
                return ['value' => 'new'];
            },
        );

        $this->assertSame(['value' => 'fresh'], $value);
        $this->assertSame(1, $callbackCalls);
    }

    public function testWithCacheCachesEmptyValues(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);

        $callbackCalls = 0;

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): array {
                $callbackCalls++;
                return [];
            },
        );

        $this->assertSame([], $value);

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): array {
                $callbackCalls++;
                return ['value' => 'miss'];
            },
        );

        $this->assertSame([], $value);
        $this->assertSame(1, $callbackCalls);
    }

    public function testWithCacheCachesNullValues(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);

        $callbackCalls = 0;

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): mixed {
                $callbackCalls++;
                return null;
            },
        );

        $this->assertNull($value);

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): string {
                $callbackCalls++;
                return 'miss';
            },
        );

        $this->assertNull($value);
        $this->assertSame(1, $callbackCalls);
    }

    public function testWithCacheSeparatesPayloadsByHashField(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);

        $firstCalls = 0;
        $secondCalls = 0;

        $first = $database->withCache(
            'key',
            function () use (&$firstCalls): array {
                $firstCalls++;
                return ['value' => 'first'];
            },
            'first-field',
        );

        $second = $database->withCache(
            'key',
            function () use (&$secondCalls): array {
                $secondCalls++;
                return ['value' => 'second'];
            },
            'second-field',
        );

        $cachedFirst = $database->withCache(
            'key',
            function () use (&$firstCalls): array {
                $firstCalls++;
                return ['value' => 'miss'];
            },
            'first-field',
        );

        $this->assertSame(['value' => 'first'], $first);
        $this->assertSame(['value' => 'second'], $second);
        $this->assertSame(['value' => 'first'], $cachedFirst);
        $this->assertSame(1, $firstCalls);
        $this->assertSame(1, $secondCalls);
        $this->assertSame(['first-field', 'second-field'], $cache->list('key'));
    }

    public function testWithCacheDoesNotCacheFalseValues(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);

        $callbackCalls = 0;

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): bool {
                $callbackCalls++;
                return false;
            },
        );

        $this->assertFalse($value);
        $this->assertSame([], $cache->list('key'));

        $value = $database->withCache(
            'key',
            function () use (&$callbackCalls): string {
                $callbackCalls++;
                return 'fresh';
            },
        );

        $this->assertSame('fresh', $value);
        $this->assertSame(2, $callbackCalls);
    }

    public function testCachedFindUsesCacheUntilPurged(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $database->createCollection('wafRules', [
            new Document([
                '$id' => 'projectId',
                'type' => Database::VAR_STRING,
                'size' => 255,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createDocument('wafRules', new Document([
            '$id' => 'rule-a',
            'projectId' => 'project-a',
        ]));

        $queries = [
            Query::equal('projectId', ['project-a']),
            Query::orderAsc('$id'),
            Query::limit(25),
        ];

        $first = $database->cachedFind('wafRules', $queries, '_39');
        $this->assertCount(1, $first);
        $this->assertSame('rule-a', $first[0]->getId());

        $database->createDocument('wafRules', new Document([
            '$id' => 'rule-b',
            'projectId' => 'project-a',
        ]));

        $cached = $database->cachedFind('wafRules', $queries, '_39');
        $this->assertCount(1, $cached);
        $this->assertSame('rule-a', $cached[0]->getId());

        $this->assertTrue($database->purgeCachedFind('wafRules', '_39'));

        $fresh = $database->cachedFind('wafRules', $queries, '_39');
        $this->assertCount(2, $fresh);
        $this->assertSame(['rule-a', 'rule-b'], \array_map(
            static fn (Document $document): string => $document->getId(),
            $fresh,
        ));
    }

    public function testCachedFindSeparatesEntriesByPermissionMode(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $database->createCollection('wafRules', [
            new Document([
                '$id' => 'projectId',
                'type' => Database::VAR_STRING,
                'size' => 255,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]);

        $database->createDocument('wafRules', new Document([
            '$id' => 'rule-a',
            'projectId' => 'project-a',
        ]));

        $queries = [
            Query::equal('projectId', ['project-a']),
            Query::orderAsc('$id'),
            Query::limit(25),
        ];

        $database->cachedFind('wafRules', $queries, '_39', forPermission: Database::PERMISSION_READ);

        $database->createDocument('wafRules', new Document([
            '$id' => 'rule-b',
            'projectId' => 'project-a',
        ]));

        $cached = $database->cachedFind('wafRules', $queries, '_39', forPermission: Database::PERMISSION_READ);
        $this->assertCount(1, $cached);

        $permissionSeparated = $database->cachedFind('wafRules', $queries, '_39', forPermission: Database::PERMISSION_UPDATE);
        $this->assertCount(2, $permissionSeparated);
    }

    public function testCachedFindRecastsCacheHits(): void
    {
        $cache = new JsonHashMemoryCache();
        $database = $this->createDatabase($cache);
        $database->createCollection('metrics', [
            new Document([
                '$id' => 'value',
                'type' => Database::VAR_FLOAT,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createDocument('metrics', new Document([
            '$id' => 'metric-a',
            'value' => 1.0,
        ]));

        $queries = [
            Query::orderAsc('$id'),
            Query::limit(25),
        ];

        $fresh = $database->cachedFind('metrics', $queries, '_39');
        $cached = $database->cachedFind('metrics', $queries, '_39');

        $this->assertSame(1.0, $fresh[0]->getAttribute('value'));
        $this->assertSame(1.0, $cached[0]->getAttribute('value'));
    }

    public function testCachedFindDoesNotDoubleDecodeCustomFilters(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache, [
            'wrapped' => [
                'encode' => static fn (mixed $value): string => 'encoded:' . $value,
                'decode' => static fn (mixed $value): string => \str_starts_with((string) $value, 'encoded:')
                    ? \substr((string) $value, 8)
                    : 'double:' . $value,
            ],
        ]);
        $database->createCollection('secrets', [
            new Document([
                '$id' => 'secret',
                'type' => Database::VAR_STRING,
                'size' => 255,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => ['wrapped'],
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createDocument('secrets', new Document([
            '$id' => 'secret-a',
            'secret' => 'value',
        ]));

        $queries = [
            Query::orderAsc('$id'),
            Query::limit(25),
        ];

        $fresh = $database->cachedFind('secrets', $queries, '_39');
        $cached = $database->cachedFind('secrets', $queries, '_39');

        $this->assertSame('value', $fresh[0]->getAttribute('secret'));
        $this->assertSame('value', $cached[0]->getAttribute('secret'));
    }

    public function testCachedFindBypassesCacheForRandomOrder(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $database->createCollection('wafRules', [
            new Document([
                '$id' => 'projectId',
                'type' => Database::VAR_STRING,
                'size' => 255,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        $database->createDocument('wafRules', new Document([
            '$id' => 'rule-a',
            'projectId' => 'project-a',
        ]));

        $queries = [
            Query::equal('projectId', ['project-a']),
            Query::orderRandom(),
            Query::limit(25),
        ];

        $first = $database->cachedFind('wafRules', $queries, '_39');
        $this->assertCount(1, $first);

        $database->createDocument('wafRules', new Document([
            '$id' => 'rule-b',
            'projectId' => 'project-a',
        ]));

        $second = $database->cachedFind('wafRules', $queries, '_39');
        $this->assertCount(2, $second);
    }

    public function testCachedFindReliesOnPurgeForDocumentSecurityCollections(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createCollection('secureRules', [
                new Document([
                    '$id' => 'projectId',
                    'type' => Database::VAR_STRING,
                    'size' => 255,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
            ], permissions: [
                Permission::create(Role::any()),
            ], documentSecurity: true);

            $database->createDocument('secureRules', new Document([
                '$id' => 'rule-a',
                '$permissions' => [
                    Permission::read(Role::user('user-1')),
                    Permission::update(Role::any()),
                ],
                'projectId' => 'project-a',
            ]));
        });

        $database->getAuthorization()->addRole(Role::user('user-1')->toString());

        $queries = [
            Query::equal('projectId', ['project-a']),
            Query::orderAsc('$id'),
            Query::limit(25),
        ];

        $first = $database->cachedFind('secureRules', $queries, '_39');
        $this->assertCount(1, $first);

        $database->getAuthorization()->skip(function () use ($database): void {
            $database->updateDocument('secureRules', 'rule-a', new Document([
                '$permissions' => [
                    Permission::read(Role::user('user-2')),
                    Permission::update(Role::any()),
                ],
            ]));
        });

        $cached = $database->cachedFind('secureRules', $queries, '_39');

        $this->assertCount(1, $cached);

        $this->assertTrue($database->purgeCachedFind('secureRules', '_39'));

        $fresh = $database->cachedFind('secureRules', $queries, '_39');
        $this->assertSame([], $fresh);
    }

    public function testCachedFindRehydratesNestedDocumentPayloads(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $database->createCollection('parents', permissions: [
            Permission::read(Role::any()),
        ]);

        $queries = [
            Query::limit(25),
        ];

        $collection = $database->getCollection('parents');
        $cache->save(
            $database->getFindCacheKey($collection->getId(), '_39'),
            [
                'value' => [
                    [
                        '$id' => 'parent-a',
                        'child' => [
                            '$id' => 'child-a',
                            '$collection' => 'children',
                            'name' => 'Child A',
                        ],
                    ],
                ],
            ],
            $database->getFindCacheField($collection, $queries),
        );

        $parents = $database->cachedFind('parents', $queries, '_39');

        $this->assertCount(1, $parents);
        $this->assertInstanceOf(Document::class, $parents[0]->getAttribute('child'));
        $this->assertSame('child-a', $parents[0]->getAttribute('child')->getId());
    }
}

class HashMemoryCache implements Adapter
{
    /**
     * @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>>
     */
    private array $store = [];

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $hash = $hash === '' ? $key : $hash;
        $saved = $this->store[$key][$hash] ?? null;
        if ($saved === null) {
            return false;
        }

        return ($saved['time'] + $ttl > \time()) ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '' || empty($data)) {
            return false;
        }

        $hash = $hash === '' ? $key : $hash;
        $this->store[$key][$hash] = [
            'time' => \time(),
            'data' => $data,
        ];

        return $data;
    }

    public function touch(string $key, string $hash = ''): bool
    {
        $hash = $hash === '' ? $key : $hash;
        if (!isset($this->store[$key][$hash])) {
            return false;
        }

        $this->store[$key][$hash]['time'] = \time();

        return true;
    }

    /**
     * @return array<string>
     */
    public function list(string $key): array
    {
        return \array_keys($this->store[$key] ?? []);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        if ($hash !== '') {
            unset($this->store[$key][$hash]);
            return true;
        }

        unset($this->store[$key]);

        return true;
    }

    public function flush(): bool
    {
        $this->store = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->store);
    }

    public function getName(?string $key = null): string
    {
        return 'hash-memory';
    }
}

class JsonHashMemoryCache implements Adapter
{
    /**
     * @var array<string, array<string, array{time: int, data: string}>>
     */
    private array $store = [];

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $hash = $hash === '' ? $key : $hash;
        $saved = $this->store[$key][$hash] ?? null;
        if ($saved === null || $saved['time'] + $ttl <= \time()) {
            return false;
        }

        return \json_decode($saved['data'], true);
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '' || empty($data)) {
            return false;
        }

        $hash = $hash === '' ? $key : $hash;
        $this->store[$key][$hash] = [
            'time' => \time(),
            'data' => \json_encode($data) ?: '',
        ];

        return $data;
    }

    public function touch(string $key, string $hash = ''): bool
    {
        $hash = $hash === '' ? $key : $hash;
        if (!isset($this->store[$key][$hash])) {
            return false;
        }

        $this->store[$key][$hash]['time'] = \time();

        return true;
    }

    /**
     * @return array<string>
     */
    public function list(string $key): array
    {
        return \array_keys($this->store[$key] ?? []);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        if ($hash !== '') {
            unset($this->store[$key][$hash]);
            return true;
        }

        unset($this->store[$key]);

        return true;
    }

    public function flush(): bool
    {
        $this->store = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->store);
    }

    public function getName(?string $key = null): string
    {
        return 'json-hash-memory';
    }
}
