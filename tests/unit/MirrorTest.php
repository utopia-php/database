<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Event\RecordingLifecycle;
use Tests\Unit\Support\CountingMemory;
use Throwable;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Mirror;
use Utopia\Database\Query;
use Utopia\Database\Type\TypeRegistry;

class MirrorTest extends TestCase
{
    private const string COLLECTION = 'posts';

    public function testSetDatabaseUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setDatabase('utopiaTests');

        $this->assertSame('utopiaTests', $mirror->getDatabase());
        $this->assertSame('utopiaTests', $source->getDatabase());
        $this->assertSame('utopiaTests', $destination->getDatabase());
    }

    public function testSetNamespaceUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setNamespace('myapp');

        $this->assertSame('myapp', $mirror->getNamespace());
        $this->assertSame('myapp', $source->getNamespace());
        $this->assertSame('myapp', $destination->getNamespace());
    }

    public function testSetTenantUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setTenant(7);

        $this->assertSame(7, $mirror->getTenant());
        $this->assertSame(7, $source->getTenant());
        $this->assertSame(7, $destination->getTenant());
    }

    public function testSetSharedTablesUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setSharedTables(true);

        $this->assertTrue($mirror->getSharedTables());
        $this->assertTrue($source->getSharedTables());
        $this->assertTrue($destination->getSharedTables());
    }

    public function testCreateCreatesMetadataOnDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror
            ->setDatabase('utopiaTests')
            ->setNamespace('myapp')
            ->create();

        $this->assertTrue($source->exists('utopiaTests'));
        $this->assertTrue($source->exists('utopiaTests', Database::METADATA));
        $this->assertTrue($destination->exists('utopiaTests'));
        $this->assertTrue($destination->exists('utopiaTests', Database::METADATA));
    }

    public function testListCollectionsHidesSourceOnlyUpgrades(): void
    {
        [$mirror, $source] = $this->pair();

        $mirror
            ->setDatabase('utopiaTests')
            ->setNamespace('myapp')
            ->create();

        $mirror->createCollection(new Collection(id: 'actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));

        $listed = $mirror->listCollections();
        $ids = \array_map(static fn ($collection): string => $collection->getId(), $listed);

        $this->assertSame(['actors'], $ids);
        $this->assertFalse($source->getCollection('upgrades')->isEmpty());
    }

    public function testSkipValidationRestoresSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $this->assertTrue($mirror->isValidationEnabled());
        $this->assertTrue($source->isValidationEnabled());
        $this->assertTrue($destination->isValidationEnabled());

        $mirror->skipValidation(function () use ($mirror, $source, $destination) {
            $this->assertFalse($mirror->isValidationEnabled());
            $this->assertFalse($source->isValidationEnabled());
            $this->assertFalse($destination->isValidationEnabled());
        });

        $this->assertTrue($mirror->isValidationEnabled());
        $this->assertTrue($source->isValidationEnabled());
        $this->assertTrue($destination->isValidationEnabled());
    }

    public function testDisableValidationDelegatesToSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->disableValidation();

        $this->assertFalse($mirror->isValidationEnabled());
        $this->assertFalse($source->isValidationEnabled());
        $this->assertFalse($destination->isValidationEnabled());

        $mirror->enableValidation();

        $this->assertTrue($mirror->isValidationEnabled());
        $this->assertTrue($source->isValidationEnabled());
        $this->assertTrue($destination->isValidationEnabled());
    }

    public function testCreateThrowsWhenDestinationCreateFails(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new class () extends Memory {
            public function create(string $name): bool
            {
                throw new RuntimeException('destination create failed');
            }
        }, new Cache(new None()));
        $mirror = new Mirror($source, $destination);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('destination create failed');

        $mirror->setDatabase('utopiaTests')->setNamespace('myapp')->create();
    }

    public function testUpdateThroughMirrorInvalidatesItsQueryCache(): void
    {
        $adapter = new CountingMemory();
        $mirror = $this->seed(new Mirror(
            new Database($adapter, new Cache(new None())),
            new Database(new Memory(), new Cache(new None())),
        ));
        $mirror->setQueryCache(new QueryCache(new Cache(new MemoryCache())));

        $this->assertSame('first', $this->title($mirror));
        $finds = $adapter->finds;
        $this->assertSame('first', $this->title($mirror));
        $this->assertSame($finds, $adapter->finds, 'The repeated read must be served from the query cache');

        $mirror->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated']));

        $this->assertSame('updated', $this->title($mirror));
    }

    public function testTriggerInvalidatesTheMirrorQueryCacheAndDispatchesOnce(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $mirror = new class ($source) extends Mirror {
            public function fire(Event $event, mixed $data): void
            {
                $this->trigger($event, $data);
            }
        };
        $this->seed($mirror);
        $mirror->setQueryCache(new QueryCache(new Cache(new MemoryCache())));
        $source->setQueryCache(null);

        $this->assertSame('first', $this->title($mirror));
        $source->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated']));
        $this->assertSame('first', $this->title($mirror), 'Only the mirror holds the query cache, so the source write must not reach it');

        $updated = $source->getDocument(self::COLLECTION, 'first');
        $recorder = new RecordingLifecycle();
        $mirror->addHook($recorder);
        $mirror->fire(Event::DocumentUpdate, $updated);

        $this->assertSame([Event::DocumentUpdate], $recorder->getEvents());
        $this->assertSame('updated', $this->title($mirror));
    }

    /**
     * @return iterable<string, array{Closure(Mirror, Invalidator): mixed}>
     */
    public static function invalidatorRegistrations(): iterable
    {
        yield 'addHook' => [
            static fn (Mirror $mirror, Invalidator $invalidator): mixed => $mirror->addHook($invalidator),
        ];
        yield 'addLifecycleHook' => [
            static fn (Mirror $mirror, Invalidator $invalidator): mixed => $mirror->addLifecycleHook($invalidator),
        ];
    }

    /**
     * @param  Closure(Mirror, Invalidator): mixed  $register
     */
    #[DataProvider('invalidatorRegistrations')]
    public function testInvalidatorAddedThroughMirrorInvalidatesOnPurge(Closure $register): void
    {
        $mirror = $this->seed(new Mirror(new Database(new Memory(), new Cache(new None()))));
        $queryCache = new QueryCache(new Cache(new MemoryCache()));
        $register($mirror, new Invalidator($queryCache));

        $this->assertStaleUntilPurgedThroughMirror($mirror, $this->sibling($mirror)->setQueryCache($queryCache));
    }

    public function testPurgeThroughMirrorInvalidatesTheSourceQueryCache(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $mirror = $this->seed(new Mirror($source));
        $source->setQueryCache(new QueryCache(new Cache(new MemoryCache())));

        $this->assertStaleUntilPurgedThroughMirror($mirror, $source);
    }

    public function testUpdateThroughMirrorPurgesDocumentsCachedUnderItsCacheName(): void
    {
        $mirror = $this->seed(new Mirror(
            new Database(new Memory(), new Cache(new MemoryCache())),
            new Database(new Memory(), new Cache(new None())),
        ));
        $mirror->setCacheName('mirrored');

        $this->assertSame('first', $mirror->getDocument(self::COLLECTION, 'first')->getAttribute('title'));

        $mirror->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated']));

        $this->assertSame('updated', $mirror->getDocument(self::COLLECTION, 'first')->getAttribute('title'));
    }

    /**
     * @return iterable<string, array{Closure(Mirror): mixed, Closure(Database): mixed, mixed}>
     */
    public static function forwardedSetters(): iterable
    {
        $queryCache = new QueryCache(new Cache(new None()));
        $typeRegistry = new TypeRegistry();
        $meta = self::meta(...);

        yield 'setQueryCache' => [
            static fn (Mirror $mirror): mixed => $mirror->setQueryCache($queryCache),
            static fn (Database $database): mixed => $database->getQueryCache(),
            $queryCache,
        ];
        yield 'setCacheName' => [
            static fn (Mirror $mirror): mixed => $mirror->setCacheName('mirrored'),
            static fn (Database $database): mixed => $database->getCacheName(),
            'mirrored',
        ];
        yield 'setGlobalCollections' => [
            static fn (Mirror $mirror): mixed => $mirror->setGlobalCollections(['projects']),
            static fn (Database $database): mixed => $database->getGlobalCollections(),
            ['projects'],
        ];
        yield 'resetGlobalCollections' => [
            static function (Mirror $mirror): void {
                self::onEach($mirror, static fn (Database $database): mixed => $database->setGlobalCollections(['projects']))->resetGlobalCollections();
            },
            static fn (Database $database): mixed => $database->getGlobalCollections(),
            [],
        ];
        yield 'setTenantPerDocument' => [
            static fn (Mirror $mirror): mixed => $mirror->setTenantPerDocument(true),
            static fn (Database $database): mixed => $database->getTenantPerDocument(),
            true,
        ];
        yield 'setTimeout' => [
            static fn (Mirror $mirror): mixed => $mirror->setTimeout(500),
            static fn (Database $database): mixed => $database->getAdapter()->getTimeout(),
            500,
        ];
        yield 'clearTimeout' => [
            static function (Mirror $mirror): void {
                self::onEach($mirror, static fn (Database $database): mixed => $database->setTimeout(500))->clearTimeout();
            },
            static fn (Database $database): mixed => $database->getAdapter()->getTimeout(),
            0,
        ];
        yield 'setMetadata' => [
            static fn (Mirror $mirror): mixed => $mirror->setMetadata('request', 'mirrored'),
            static fn (Database $database): mixed => $database->getMetadata(),
            ['request' => 'mirrored'],
        ];
        yield 'resetMetadata' => [
            static function (Mirror $mirror): void {
                self::onEach($mirror, static fn (Database $database): mixed => $database->setMetadata('request', 'mirrored'))->resetMetadata();
            },
            static fn (Database $database): mixed => $database->getMetadata(),
            [],
        ];
        yield 'disableFilters' => [
            static fn (Mirror $mirror): mixed => $mirror->disableFilters(),
            $meta,
            '{"filtered":true}',
        ];
        yield 'enableFilters' => [
            static fn (Mirror $mirror): mixed => self::onEach($mirror, static fn (Database $database): mixed => $database->disableFilters())->enableFilters(),
            $meta,
            ['filtered' => true],
        ];
        yield 'enableLocks' => [
            static fn (Mirror $mirror): mixed => $mirror->enableLocks(true),
            static fn (Database $database): mixed => $database->getAdapter()->getAlterLocks(),
            true,
        ];
        yield 'enableProfiling' => [
            static fn (Mirror $mirror): mixed => $mirror->enableProfiling(),
            static fn (Database $database): mixed => $database->getProfiler()?->isEnabled(),
            true,
        ];
        yield 'disableProfiling' => [
            static fn (Mirror $mirror): mixed => self::onEach($mirror, static fn (Database $database): mixed => $database->enableProfiling())->disableProfiling(),
            static fn (Database $database): mixed => $database->getProfiler()?->isEnabled(),
            false,
        ];
        yield 'setMigrating' => [
            static fn (Mirror $mirror): mixed => $mirror->setMigrating(true),
            static fn (Database $database): mixed => $database->isMigrating(),
            true,
        ];
        yield 'setTypeRegistry' => [
            static fn (Mirror $mirror): mixed => $mirror->setTypeRegistry($typeRegistry),
            static fn (Database $database): mixed => $database->getTypeRegistry(),
            $typeRegistry,
        ];
    }

    /**
     * @param  Closure(Mirror): mixed  $configure
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('forwardedSetters')]
    public function testSetterReachesSourceAndDestination(Closure $configure, Closure $read, mixed $expected): void
    {
        $source = new Database(self::configurableAdapter(), new Cache(new None()));
        $destination = new Database(self::configurableAdapter(), new Cache(new None()));
        $mirror = new Mirror($source, $destination);

        $configure($mirror);

        $this->assertSame($expected, $read($mirror), 'mirror');
        $this->assertSame($expected, $read($source), 'source');
        $this->assertSame($expected, $read($destination), 'destination');
    }

    /**
     * @return iterable<string, array{array<string>|null}>
     */
    public static function skippedFilters(): iterable
    {
        yield 'every filter' => [null];
        yield 'named filters' => [['json']];
    }

    /**
     * @param  array<string>|null  $filters
     */
    #[DataProvider('skippedFilters')]
    public function testSkipFiltersRestoresSourceAndDestination(?array $filters): void
    {
        [$mirror, $source, $destination] = $this->pair();
        $databases = [$mirror, $source, $destination];

        $skipped = $mirror->skipFilters(
            static fn (): array => \array_map(self::meta(...), $databases),
            $filters,
        );

        $this->assertSame(\array_fill(0, 3, '{"filtered":true}'), $skipped);
        $this->assertSame(\array_fill(0, 3, ['filtered' => true]), \array_map(self::meta(...), $databases));
    }

    public function testProfilingThroughMirrorRecordsIntoTheProfilerItReturns(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->enableProfiling();

        $this->assertNotNull($mirror->getProfiler());
        $this->assertSame($source->getProfiler(), $mirror->getProfiler());
        $this->assertSame($mirror->getProfiler(), $mirror->getAdapter()->getProfiler());
        $this->assertNotNull($destination->getProfiler());
        $this->assertSame($destination->getProfiler(), $destination->getAdapter()->getProfiler());

        $mirror->disableProfiling();

        $this->assertNull($mirror->getAdapter()->getProfiler());
        $this->assertNull($destination->getAdapter()->getProfiler());
    }

    /**
     * @return iterable<string, array{string, Closure(Mirror): mixed, Closure(Database): mixed, mixed}>
     */
    public static function timeoutCalls(): iterable
    {
        yield 'setTimeout' => [
            'setTimeout',
            static fn (Mirror $mirror): mixed => $mirror->setTimeout(500),
            static fn (Database $database): mixed => $database->getAdapter()->getTimeout(),
            500,
        ];
        yield 'clearTimeout' => [
            'clearTimeout',
            static function (Mirror $mirror): void {
                $mirror->clearTimeout();
            },
            static fn (Database $database): mixed => $database->getAdapter()->getTimeout(),
            0,
        ];
    }

    /**
     * @param  Closure(Mirror): mixed  $call
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('timeoutCalls')]
    public function testDestinationTimeoutFailureIsReportedNotThrown(string $action, Closure $call, Closure $read, mixed $expected): void
    {
        $source = new Database(self::configurableAdapter(), new Cache(new None()));
        $destination = new Database(new class () extends Memory {
            public function setTimeout(int $milliseconds, Event $event = Event::All): void
            {
                throw new RuntimeException('destination unreachable');
            }

            public function clearTimeout(Event $event = Event::All): void
            {
                throw new RuntimeException('destination unreachable');
            }
        }, new Cache(new None()));
        $mirror = new Mirror($source, $destination);
        $errors = [];
        $mirror->onError(static function (string $failed, Throwable $error) use (&$errors): void {
            $errors[] = [$failed, $error->getMessage()];
        });

        $call($mirror);

        $this->assertSame($expected, $read($source));
        $this->assertSame([[$action, 'destination unreachable']], $errors);
    }

    private function assertStaleUntilPurgedThroughMirror(Mirror $mirror, Database $reader): void
    {
        $this->assertSame('first', $this->title($reader));
        $this->sibling($mirror)->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated']));
        $this->assertSame('first', $this->title($reader), 'A write that invalidates nothing must leave the cached read in place');

        $mirror->purgeCachedDocument(self::COLLECTION, 'first');

        $this->assertSame('updated', $this->title($reader));
    }

    /**
     * A database on the mirror's adapter and authorization that shares none of its caches or hooks.
     */
    private function sibling(Mirror $mirror): Database
    {
        return (new Database($mirror->getAdapter(), new Cache(new None())))->setAuthorization($mirror->getAuthorization());
    }

    private function title(Database $database): mixed
    {
        return $database->findOne(self::COLLECTION, [Query::equal(Document::ID, ['first'])])->getAttribute('title');
    }

    /**
     * Applies $apply to the wrapped databases directly, then to the mirror, so an undo through
     * the mirror has state to clear on each of them.
     *
     * @param  Closure(Database): mixed  $apply
     */
    private static function onEach(Mirror $mirror, Closure $apply): Mirror
    {
        $apply($mirror->getSource());
        $destination = $mirror->getDestination();
        if ($destination !== null) {
            $apply($destination);
        }
        $apply($mirror);

        return $mirror;
    }

    private static function meta(Database $database): mixed
    {
        $collection = new Collection(id: self::COLLECTION, attributes: [
            Attribute::string(key: 'meta', size: 64, filters: ['json']),
        ]);

        return $database->decode($collection, new Document(['meta' => '{"filtered":true}']))->getAttribute('meta');
    }

    private function seed(Mirror $mirror): Mirror
    {
        $mirror
            ->setDatabase('mirror')
            ->setNamespace('mirror_'.\uniqid())
            ->create();

        $mirror->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64),
                Attribute::integer(key: 'views'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ));

        $mirror->createDocument(self::COLLECTION, new Document([
            Document::ID => 'first',
            'title' => 'first',
            'views' => 1,
        ]));

        return $mirror;
    }

    private static function configurableAdapter(): Memory
    {
        return new class () extends Memory {
            /**
             * @return array<Capability>
             */
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::AlterLock];
            }

            public function setTimeout(int $milliseconds, Event $event = Event::All): void
            {
                $this->setTimeoutState($milliseconds, $event);
            }

            public function clearTimeout(Event $event = Event::All): void
            {
                $this->clearTimeoutState($event);
            }
        };
    }

    /**
     * @return array{0: Mirror, 1: Database, 2: Database}
     */
    private function pair(): array
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new Memory(), new Cache(new None()));

        return [new Mirror($source, $destination), $source, $destination];
    }
}
