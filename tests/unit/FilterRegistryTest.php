<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Filter\Reversed;
use Tests\Unit\Filter\Rot13;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Filter;
use Utopia\Database\Filter\Callback;
use Utopia\Database\Filter\Codec;
use Utopia\Database\Filter\Registry;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class FilterRegistryTest extends TestCase
{
    private DatabaseMemory $adapter;

    private Cache $cache;

    private string $namespace;

    private Database $database;

    /**
     * @var array<string, array{encode: callable, decode: callable, signature: string}>
     */
    private array $registry = [];

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
        $this->cache = new Cache(new HashAwareMemoryCache());
        $this->namespace = 'filter_registry_'.\uniqid();

        $this->database = $this->createDatabase();

        // Snapshot once the constructor has registered the built-ins, so the
        // restore in tearDown puts back a populated registry rather than an
        // empty one.
        $this->registry = FilterRegistry::filters();

        $this->database->create();
        $this->database->createCollection(Collection::create(id: 'projects'));
        $this->database->createAttribute('projects', Attribute::string(key: 'name', size: 255));
        $this->database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'cached',
        ]));
    }

    protected function tearDown(): void
    {
        // addFilter() writes to a static registry with no removal API, so a test
        // registering one would otherwise leak into every later test.
        FilterRegistry::restore($this->registry, true);
    }

    public function testRegisteredCodecsAreFoundByName(): void
    {
        $registry = new Registry();
        $reversed = new Reversed();
        $rot13 = new Rot13();

        $this->assertSame($registry, $registry->register($reversed)->register($rot13));
        $this->assertSame($reversed, $registry->get('reversed'));
        $this->assertSame($rot13, $registry->get('rot13'));
        $this->assertTrue($registry->has('reversed'));
    }

    public function testAnUnknownNameIsNeitherFoundNorHeld(): void
    {
        $registry = (new Registry())->register(new Reversed());

        $this->assertNull($registry->get('rot13'));
        $this->assertFalse($registry->has('rot13'));
    }

    public function testRegisteringANameAgainReplacesTheCodec(): void
    {
        $replacement = new Rot13('reversed');
        $registry = (new Registry())->register(new Reversed())->register($replacement);

        $this->assertSame($replacement, $registry->get('reversed'));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function builtInFilters(): iterable
    {
        foreach (Filter::cases() as $filter) {
            yield $filter->value => [$filter->value];
        }
    }

    #[DataProvider('builtInFilters')]
    public function testBuiltInFilterNamesAreRejected(string $name): void
    {
        $registry = new Registry();

        try {
            $registry->register(new Reversed($name));
            $this->fail("registering a codec named \"{$name}\" must be rejected");
        } catch (DuplicateException $exception) {
            $this->assertStringContainsString("\"{$name}\"", $exception->getMessage());
        }

        $this->assertFalse($registry->has($name));
    }

    public function testDefaultFiltersNameEveryBuiltInFilter(): void
    {
        FilterRegistry::clear();
        new Database(new DatabaseMemory(), new Cache(new HashAwareMemoryCache()));

        $names = \array_keys(FilterRegistry::filters());
        $expected = Database::DEFAULT_FILTERS;
        $cases = \array_map(static fn (Filter $filter): string => $filter->value, Filter::cases());
        \sort($names);
        \sort($expected);
        \sort($cases);

        $this->assertSame($expected, $names);
        $this->assertSame($expected, $cases);
    }

    public function testCallbackRunsItsClosures(): void
    {
        $callback = new Callback(
            'wrapped',
            static fn (mixed $value): mixed => \is_string($value) ? '['.$value.']' : $value,
            static fn (mixed $value): mixed => \is_string($value) ? \trim($value, '[]') : $value,
        );

        $this->assertSame('wrapped', $callback->name());
        $this->assertSame('[value]', $callback->encode('value'));
        $this->assertSame('value', $callback->decode('[value]'));
    }

    public function testRegisteringOnARegistryLeavesTheGlobalFiltersUntouched(): void
    {
        (new Registry())->register(new Reversed());

        $this->expectException(NotFoundException::class);
        $this->createDatabase()->decode($this->notes('reversed'), new Document(['body' => 'olleh']));
    }

    public function testRegisteringAGlobalFilterStopsStaleEntriesBeingServed(): void
    {
        $this->assertSame('cached', $this->read());

        $this->writeBehindTheCache('fresh');
        $this->assertSame('cached', $this->read(), 'read should still be served from cache');

        $noop = fn (mixed $value) => $value;
        Database::addFilter(__FUNCTION__, $noop, $noop);

        $this->assertSame(
            'fresh',
            $this->read(),
            'a document cached under the previous filter set must not be served after it changes',
        );
    }

    public function testRegisteringOnASharedRegistryStopsStaleEntriesBeingServed(): void
    {
        $registry = new Registry();
        $database = $this->createDatabase()->setFilters($registry);

        $this->assertSame('cached', $this->read($database));

        $this->writeBehindTheCache('fresh');
        $this->assertSame('cached', $this->read($database), 'read should still be served from cache');

        $registry->register(new Reversed('probe'));

        $this->assertSame(
            'fresh',
            $this->read($database),
            'a codec registered after the handle cached a document must not keep the previous entry served',
        );
    }

    public function testOverridingABuiltInFilterBeforeTheFirstInstanceStillWins(): void
    {
        // A fresh process: nothing has constructed a Database yet, so the
        // built-ins are not in the registry.
        FilterRegistry::clear();

        $identity = fn (mixed $value) => $value;
        Database::addFilter('datetime', $identity, $identity);

        $decoded = $this->createDatabase()->decode(
            new Document([
                '$id' => 'events',
                'attributes' => [
                    new Document([
                        '$id' => 'occurredAt',
                        'type' => ColumnType::Datetime->value,
                        'array' => false,
                        'filters' => ['datetime'],
                    ]),
                ],
            ]),
            new Document(['$id' => 'event', 'occurredAt' => '2026-09-21 10:00:00.000']),
        );

        // The built-in decode would hand back '2026-09-21T10:00:00.000+00:00'.
        $this->assertSame(
            '2026-09-21 10:00:00.000',
            $decoded->getAttribute('occurredAt'),
            'the override registered before the first instance must be the filter that runs',
        );
    }

    public function testInstancesSharingAConfigShareCachedDocuments(): void
    {
        $this->assertSame('cached', $this->read());

        $this->writeBehindTheCache('fresh');

        $this->assertSame(
            'cached',
            $this->read($this->createDatabase()),
            'a later instance with the same config must hit the entry the first one cached',
        );
    }

    public function testFilterEncodeFailureIsADatabaseExceptionWithTheOriginalAsPrevious(): void
    {
        $failure = new \InvalidArgumentException('cannot encode the probe', 7);
        Database::addFilter(
            'failingEncode',
            static fn (mixed $value) => throw $failure,
            static fn (mixed $value) => $value,
        );

        $this->assertEncodeFailureWrapped($this->database, 'failingEncode', $failure);
    }

    public function testCodecEncodeFailureIsADatabaseExceptionWithTheOriginalAsPrevious(): void
    {
        $failure = new \DomainException('cannot encode the custom probe', 11);
        $codec = new class ($failure) implements Codec {
            public function __construct(private readonly \DomainException $failure)
            {
            }

            public function name(): string
            {
                return 'failingCodec';
            }

            public function encode(mixed $value): mixed
            {
                throw $this->failure;
            }

            public function decode(mixed $value): mixed
            {
                return $value;
            }
        };

        $database = new Database($this->adapter, $this->cache, [$codec]);

        $this->assertEncodeFailureWrapped($database, 'failingCodec', $failure);
    }

    /**
     * @return array<string, array{callable, callable}>
     */
    public static function nonClosureCallables(): array
    {
        $first = new class () {
            public function transform(mixed $value): mixed
            {
                return $value;
            }
        };
        $second = new class () {
            public function transform(mixed $value): mixed
            {
                return $value;
            }
        };

        return [
            'string callables' => ['trim', 'strtolower'],
            'static array callables' => [[self::class, 'identity'], [self::class, 'passthrough']],
            'instance array callables' => [[$first, 'transform'], [$second, 'transform']],
        ];
    }

    #[DataProvider('nonClosureCallables')]
    public function testReplacingANonClosureFilterStopsStaleEntriesBeingServed(callable $original, callable $replacement): void
    {
        Database::addFilter('replaceable', $original, $original);
        $this->assertSame('cached', $this->read());

        $this->writeBehindTheCache('fresh');
        $this->assertSame('cached', $this->read(), 'read should still be served from cache');

        Database::addFilter('replaceable', $replacement, $replacement);

        $this->assertSame(
            'fresh',
            $this->read(),
            'a filter replaced by another callable under the same name must not keep serving the previous entry',
        );
    }

    public static function identity(mixed $value): mixed
    {
        return $value;
    }

    public static function passthrough(mixed $value): mixed
    {
        return $value;
    }

    private function createDatabase(): Database
    {
        $database = new Database($this->adapter, $this->cache);

        return $database
            ->setDatabase('utopiaTests')
            ->setNamespace($this->namespace);
    }

    /**
     * Write through the adapter, bypassing Database and therefore the cache
     * purge, so the cache holds a copy the source no longer agrees with. A read
     * returning 'cached' was served from the cache; one returning 'fresh' missed
     * and went to the adapter.
     */
    private function writeBehindTheCache(string $value): void
    {
        $collection = $this->database->getCollection('projects');
        $document = $this->adapter->getDocument($collection, 'project');
        $document->setAttribute('name', $value);
        $this->adapter->updateDocument($collection, 'project', $document, true);
    }

    private function read(?Database $database = null): mixed
    {
        return ($database ?? $this->database)
            ->getDocument('projects', 'project')
            ->getAttribute('name');
    }

    private function notes(string $filter): Document
    {
        return new Document([
            '$id' => 'notes',
            'attributes' => [
                new Document([
                    '$id' => 'body',
                    'type' => ColumnType::String->value,
                    'array' => false,
                    'filters' => [$filter],
                ]),
            ],
        ]);
    }

    private function assertEncodeFailureWrapped(Database $database, string $filter, \Throwable $failure): void
    {
        $collection = new Document([
            '$id' => 'probes',
            'attributes' => [new Document([
                '$id' => 'probe',
                'type' => ColumnType::String->value,
                'array' => false,
                'filters' => [$filter],
            ])],
        ]);

        try {
            $database->encode($collection, new Document(['$id' => 'probe', 'probe' => 'value']));
            $this->fail('encode() must rethrow the failure of '.$filter);
        } catch (DatabaseException $error) {
            $this->assertSame(DatabaseException::class, $error::class);
            $this->assertSame($failure->getMessage(), $error->getMessage());
            $this->assertSame($failure->getCode(), $error->getCode());
            $this->assertSame($failure, $error->getPrevious());
        }
    }
}
