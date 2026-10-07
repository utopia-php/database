<?php

namespace Tests\Unit;

use ArrayObject;
use Closure;
use DateTime;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Runtime;
use Tests\Unit\Event\RecordingLifecycle;
use Tests\Unit\Support\CountingMemory;
use Throwable;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Adapter\Timeout;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Capability;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Filter\Registry;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Decorator;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Mirror;
use Utopia\Database\Mirror\Failure;
use Utopia\Database\Mirroring\Filter;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

use function Swoole\Coroutine\run;

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

        $this->assertTrue($mirror->hasSharedTables());
        $this->assertTrue($source->hasSharedTables());
        $this->assertTrue($destination->hasSharedTables());
    }

    public function testCreateCreatesMetadataOnDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror
            ->setDatabase('utopiaTests')
            ->setNamespace('myapp')
            ->create();

        $this->assertTrue($source->exists('utopiaTests'));
        $this->assertTrue($source->collectionExists(Database::METADATA, 'utopiaTests'));
        $this->assertTrue($destination->exists('utopiaTests'));
        $this->assertTrue($destination->collectionExists(Database::METADATA, 'utopiaTests'));
    }

    public function testListCollectionsHidesSourceOnlyUpgrades(): void
    {
        [$mirror, $source] = $this->pair();

        $mirror
            ->setDatabase('utopiaTests')
            ->setNamespace('myapp')
            ->create();

        $mirror->createCollection(Collection::create(id: 'actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));

        $listed = $mirror->listCollections();
        $ids = \array_map(static fn ($collection): string => $collection->getId(), $listed);

        $this->assertSame(['actors'], $ids);
        $this->assertNotNull($source->findCollection('upgrades'));
    }

    public function testSkipValidationRestoresSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $this->assertTrue($mirror->isValidating());
        $this->assertTrue($source->isValidating());
        $this->assertTrue($destination->isValidating());

        $mirror->skipValidation(function () use ($mirror, $source, $destination) {
            $this->assertFalse($mirror->isValidating());
            $this->assertFalse($source->isValidating());
            $this->assertFalse($destination->isValidating());
        });

        $this->assertTrue($mirror->isValidating());
        $this->assertTrue($source->isValidating());
        $this->assertTrue($destination->isValidating());
    }

    public function testDisableValidationDelegatesToSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setValidation(false);

        $this->assertFalse($mirror->isValidating());
        $this->assertFalse($source->isValidating());
        $this->assertFalse($destination->isValidating());

        $mirror->setValidation(true);

        $this->assertTrue($mirror->isValidating());
        $this->assertTrue($source->isValidating());
        $this->assertTrue($destination->isValidating());
    }

    public function testWrappingKeepsTheAuthorizationOfTheSourceAndDestination(): void
    {
        $sourceAuthorization = new Authorization();
        $destinationAuthorization = new Authorization();
        $source = self::sqlite()->setAuthorization($sourceAuthorization);
        $destination = self::sqlite()->setAuthorization($destinationAuthorization);

        $mirror = new Mirror($source, $destination);

        $this->assertSame($sourceAuthorization, $source->getAuthorization());
        $this->assertSame($destinationAuthorization, $destination->getAuthorization());
        $this->assertSame($sourceAuthorization, $mirror->getAuthorization());
    }

    public function testARoleGrantedOnTheSourcesAuthorizationAfterWrappingAppliesToTheMirror(): void
    {
        $authorization = new Authorization();
        $source = self::sqlite()->setAuthorization($authorization);
        $mirror = new Mirror($source, self::sqlite());
        $mirror->setDatabase('utopiaTests')->setNamespace('wrapped_'.\uniqid())->create();
        $authorization->skip(function () use ($mirror): void {
            $mirror->createCollection(Collection::create(
                id: self::COLLECTION,
                attributes: [Attribute::string(key: 'title', size: 64)],
                permissions: [Permission::create(Role::any())],
                documentSecurity: true,
            ));
            $mirror->createDocument(self::COLLECTION, new Document([
                Document::ID => 'owned',
                'title' => 'owned',
                Document::PERMISSIONS => [Permission::read(Role::user('alice'))],
            ]));
        });

        $hidden = $mirror->getDocument(self::COLLECTION, 'owned');
        $authorization->addRole(Role::user('alice')->toString());
        $granted = $mirror->getDocument(self::COLLECTION, 'owned');
        $authorization->skip(fn (): bool => $mirror->deleteDocument(self::COLLECTION, 'owned'));

        $this->assertTrue($hidden->isEmpty());
        $this->assertSame('owned', $granted->getAttribute('title'));
        $this->assertTrue($source->getDocument(self::COLLECTION, 'owned')->isEmpty());
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

    public function testAnEventThroughTheMirrorInvalidatesTheMirrorQueryCacheAndIsDispatchedOnce(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $mirror = new class ($source) extends Mirror {
            public function fire(Document $document): void
            {
                $this->invalidate(Event::DocumentUpdate, $document);
                $listeners = $this->listens(Event::DocumentUpdate);
                if ($listeners !== []) {
                    $this->dispatch(new Event\Document\Updated($document->getCollection(), $document), $listeners);
                }
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
        $mirror->fire($updated);

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
        $filters = new Registry();
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
            static fn (Database $database): mixed => $database->isTenantPerDocument(),
            true,
        ];
        yield 'setTimeout' => [
            static fn (Mirror $mirror): mixed => $mirror->setTimeout(500),
            static fn (Database $database): mixed => self::timeout($database),
            500,
        ];
        yield 'clearTimeout' => [
            static function (Mirror $mirror): void {
                self::onEach($mirror, static fn (Database $database): mixed => $database->setTimeout(500))->clearTimeout();
            },
            static fn (Database $database): mixed => self::timeout($database),
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
        yield 'setFiltering(false)' => [
            static fn (Mirror $mirror): mixed => $mirror->setFiltering(false),
            $meta,
            '{"filtered":true}',
        ];
        yield 'setFiltering(true)' => [
            static fn (Mirror $mirror): mixed => self::onEach($mirror, static fn (Database $database): mixed => $database->setFiltering(false))->setFiltering(true),
            $meta,
            ['filtered' => true],
        ];
        yield 'setLocks' => [
            static fn (Mirror $mirror): mixed => $mirror->setLocks(true),
            static fn (Database $database): mixed => self::locking($database),
            true,
        ];
        yield 'setProfiling(true)' => [
            static fn (Mirror $mirror): mixed => $mirror->setProfiling(true),
            static fn (Database $database): mixed => $database->getProfiler()?->isEnabled(),
            true,
        ];
        yield 'setProfiling(false)' => [
            static fn (Mirror $mirror): mixed => self::onEach($mirror, static fn (Database $database): mixed => $database->setProfiling(true))->setProfiling(false),
            static fn (Database $database): mixed => $database->getProfiler()?->isEnabled(),
            false,
        ];
        yield 'setMigrating' => [
            static fn (Mirror $mirror): mixed => $mirror->setMigrating(true),
            static fn (Database $database): mixed => $database->isMigrating(),
            true,
        ];
        yield 'setFilters' => [
            static fn (Mirror $mirror): mixed => $mirror->setFilters($filters),
            static fn (Database $database): mixed => $database->getFilters(),
            $filters,
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

    /**
     * @return iterable<string, array{Closure(Mirror, Closure(): mixed): mixed, Closure(Database): mixed, mixed, mixed, mixed}>
     */
    public static function scopedSetters(): iterable
    {
        yield 'withPreserveDates' => [
            static fn (Mirror $mirror, Closure $callback): mixed => $mirror->withPreserveDates(true, $callback),
            static fn (Database $database): mixed => $database->isPreservingDates(),
            true,
            false,
            true,
        ];
        yield 'withPreserveSequence' => [
            static fn (Mirror $mirror, Closure $callback): mixed => $mirror->withPreserveSequence(true, $callback),
            static fn (Database $database): mixed => $database->isPreservingSequence(),
            true,
            false,
            true,
        ];
        yield 'withTenant' => [
            static fn (Mirror $mirror, Closure $callback): mixed => $mirror->withTenant(7, $callback),
            static fn (Database $database): mixed => $database->getTenant(),
            7,
            null,
            7,
        ];
        yield 'skipRelationships' => [
            static fn (Mirror $mirror, Closure $callback): mixed => $mirror->skipRelationships($callback),
            static fn (Database $database): mixed => $database->getRelationshipHook()?->isEnabled(),
            false,
            true,
            true,
        ];
        yield 'skipRelationshipsExistCheck' => [
            static fn (Mirror $mirror, Closure $callback): mixed => $mirror->skipRelationshipsExistCheck($callback),
            static fn (Database $database): mixed => $database->getRelationshipHook()?->shouldCheckExist(),
            false,
            true,
            true,
        ];
    }

    /**
     * @param  Closure(Mirror, Closure(): mixed): mixed  $scope
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('scopedSetters')]
    public function testScopedSetterAppliesToTheSourceOnce(Closure $scope, Closure $read, mixed $inside, mixed $outside, mixed $destinationInside): void
    {
        [$mirror, $source, $destination] = $this->pair();
        $mirror->addHook(new Relationships());
        $databases = [$mirror, $source, $destination];
        $observed = [];

        $scope($mirror, static function () use (&$observed, $databases, $read): void {
            $observed[] = \array_map($read, $databases);
        });

        $this->assertSame([[$inside, $inside, $destinationInside]], $observed);
        $this->assertSame(\array_fill(0, 3, $outside), \array_map($read, $databases));
    }

    public function testRequestTimestampThroughMirrorRunsTheCallbackOnce(): void
    {
        [$mirror] = $this->pair();
        $runs = 0;

        $mirror->withRequestTimestamp(new DateTime(), static function () use (&$runs): void {
            $runs++;
        });

        $this->assertSame(1, $runs);
    }

    public function testSingleUpsertThroughMirrorHonoursTheRequestTimestamp(): void
    {
        $mirror = $this->seed(new Mirror(self::sqlite(), self::sqlite()));

        $this->expectException(ConflictException::class);

        self::inCoroutine(static fn (): mixed => $mirror->withRequestTimestamp(
            new DateTime('-1 hour'),
            static fn (): mixed => $mirror->upsertDocument(self::COLLECTION, new Document([Document::ID => 'first', 'title' => 'late'])),
        ));
    }

    public function testSingleUpsertThroughMirrorKeepsScopedPreservedDates(): void
    {
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->seed(new Mirror($source, $destination));
        $createdAt = '2001-02-03T04:05:06.000+00:00';

        self::inCoroutine(static fn (): mixed => $mirror->withPreserveDates(
            true, static fn (): mixed => $mirror->upsertDocument(self::COLLECTION, new Document([
                Document::ID => 'dated',
                '$createdAt' => $createdAt,
                '$updatedAt' => $createdAt,
                'title' => 'dated',
                'views' => 1,
            ])),
        ));

        $expected = (new DateTime($createdAt))->getTimestamp();
        foreach (['source' => $source, 'destination' => $destination] as $name => $database) {
            $stored = $database->getDocument(self::COLLECTION, 'dated')->getCreatedAt();
            $this->assertNotNull($stored, $name);
            $this->assertSame($expected, (new DateTime($stored))->getTimestamp(), $name);
        }
    }

    public function testProfilingThroughMirrorRecordsIntoTheProfilerItReturns(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setProfiling(true);

        $this->assertNotNull($mirror->getProfiler());
        $this->assertSame($source->getProfiler(), $mirror->getProfiler());
        $this->assertSame($mirror->getProfiler(), $mirror->getAdapter()->getProfiler());
        $this->assertNotNull($destination->getProfiler());
        $this->assertSame($destination->getProfiler(), $destination->getAdapter()->getProfiler());

        $mirror->setProfiling(false);

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
            static fn (Database $database): mixed => self::timeout($database),
            500,
        ];
        yield 'clearTimeout' => [
            'clearTimeout',
            static function (Mirror $mirror): void {
                $mirror->clearTimeout();
            },
            static fn (Database $database): mixed => self::timeout($database),
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
        $destination = new Database(new class () extends Memory implements Feature\Timeouts {
            use Timeout;

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
        $mirror->onError(static function (Failure $failure) use (&$errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });

        $call($mirror);

        $this->assertSame($expected, $read($source));
        $this->assertSame([[$action, 'destination unreachable']], $errors);
    }

    /**
     * @return iterable<string, array{Closure(Mirror, Document): mixed}>
     */
    public static function upserts(): iterable
    {
        yield 'upsertDocument' => [
            static fn (Mirror $mirror, Document $document): mixed => $mirror->upsertDocument(self::COLLECTION, $document),
        ];
        yield 'upsertDocuments' => [
            static fn (Mirror $mirror, Document $document): mixed => $mirror->upsertDocuments(self::COLLECTION, [$document]),
        ];
        yield 'upsertDocuments with an increase' => [
            static fn (Mirror $mirror, Document $document): mixed => $mirror->upsertDocuments(self::COLLECTION, [$document], increase: 'views'),
        ];
    }

    /**
     * @return iterable<string, array{Closure(Mirror, Document): mixed, Event}>
     */
    public static function upsertEvents(): iterable
    {
        $events = [
            'upsertDocument' => Event::DocumentUpsert,
            'upsertDocuments' => Event::DocumentsUpsert,
            'upsertDocuments with an increase' => Event::DocumentsUpsert,
        ];

        foreach (self::upserts() as $label => [$upsert]) {
            yield $label => [$upsert, $events[$label]];
        }
    }

    /**
     * @param  Closure(Mirror, Document): mixed  $upsert
     */
    #[DataProvider('upsertEvents')]
    public function testUpsertThroughMirrorFiresEachEventOnce(Closure $upsert, Event $event): void
    {
        $mirror = $this->seed(new Mirror(self::sqlite(), self::sqlite()));
        $recorder = new RecordingLifecycle();
        $mirror->addHook($recorder);

        self::inCoroutine(static fn (): mixed => $upsert($mirror, new Document([Document::ID => 'upserted', 'title' => 'upserted', 'views' => 2])));

        $this->assertSame([Event::DocumentPurge, $event], $recorder->getEvents());
    }

    /**
     * @param  Closure(Mirror, Document): mixed  $upsert
     */
    #[DataProvider('upserts')]
    public function testUpsertThroughMirrorReachesTheDestination(Closure $upsert): void
    {
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->seed(new Mirror($source, $destination));
        $errors = [];
        $mirror->onError(static function (Failure $failure) use (&$errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });

        self::inCoroutine(static fn (): mixed => $upsert($mirror, new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 2])));

        $this->assertSame([], $errors);
        $upserted = $source->getDocument(self::COLLECTION, 'first');
        $mirrored = $destination->getDocument(self::COLLECTION, 'first');
        $this->assertNotSame(1, $upserted->getAttribute('views'));
        $this->assertSame(
            [$upserted->getAttribute('title'), $upserted->getAttribute('views')],
            [$mirrored->getAttribute('title'), $mirrored->getAttribute('views')],
        );
    }

    /**
     * @return iterable<string, array{Closure(Mirror, Document): mixed, string}>
     */
    public static function upsertActions(): iterable
    {
        yield 'upsertDocument' => [
            static fn (Mirror $mirror, Document $document): mixed => $mirror->upsertDocument(self::COLLECTION, $document),
            'upsertDocument',
        ];
        yield 'upsertDocuments' => [
            static fn (Mirror $mirror, Document $document): mixed => $mirror->upsertDocuments(self::COLLECTION, [$document]),
            'upsertDocuments',
        ];
        yield 'upsertDocuments with an increase' => [
            static fn (Mirror $mirror, Document $document): mixed => $mirror->upsertDocuments(self::COLLECTION, [$document], increase: 'views'),
            'upsertDocuments',
        ];
    }

    /**
     * @param  Closure(Mirror, Document): mixed  $upsert
     */
    #[DataProvider('upsertActions')]
    public function testUpsertReplicationFailureIsReportedUnderItsAction(Closure $upsert, string $action): void
    {
        $destination = new Database(new class (new PDO('sqlite::memory:')) extends SQLite {
            /**
             * @param  array<Change>  $changes
             * @return array<Document>
             */
            public function upsertDocuments(Document $collection, array $changes, ?string $increase = null): array
            {
                throw new RuntimeException('destination unreachable');
            }
        }, new Cache(new None()));
        $mirror = $this->seed(new Mirror(self::sqlite(), $destination));
        $errors = [];
        $mirror->onError(static function (Failure $failure) use (&$errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });

        self::inCoroutine(static fn (): mixed => $upsert($mirror, new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 2])));

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
        $collection = Collection::create(id: self::COLLECTION, attributes: [
            Attribute::string(key: 'meta', size: 64, filters: [\Utopia\Database\Filter::Json]),
        ]);

        return $database->decode($collection, new Document(['meta' => '{"filtered":true}']))->getAttribute('meta');
    }

    private function seed(Mirror $mirror): Mirror
    {
        $mirror
            ->setDatabase('mirror')
            ->setNamespace('mirror_'.\uniqid())
            ->create();

        $mirror->createCollection(Collection::create(
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

    /**
     * Runs $callback in a coroutine scheduler, as a Swoole server does, so the mirror's
     * asynchronous replication finishes inside it; a failure is rethrown outside, where
     * PHPUnit can report it.
     *
     * @param  Closure(): mixed  $callback
     */
    private static function inCoroutine(Closure $callback): void
    {
        $failure = null;
        $hookFlags = Runtime::getHookFlags();

        try {
            run(static function () use ($callback, &$failure): void {
                try {
                    $callback();
                } catch (Throwable $error) {
                    $failure = $error;
                }
            });
        } finally {
            Runtime::setHookFlags($hookFlags);
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    private static function sqlite(): Database
    {
        return new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
    }

    private static function configurableAdapter(): Memory
    {
        return new class () extends Memory implements Feature\Timeouts {
            use Timeout;

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

            public function isLocking(): bool
            {
                return $this->locks;
            }
        };
    }

    private static function timeout(Database $database): int
    {
        $adapter = $database->getAdapter();
        self::assertInstanceOf(Feature\Timeouts::class, $adapter);

        return $adapter->getTimeout();
    }

    private static function locking(Database $database): bool
    {
        $adapter = $database->getAdapter();
        self::assertTrue(\method_exists($adapter, 'isLocking'));
        $locking = $adapter->isLocking();
        self::assertIsBool($locking);

        return $locking;
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

    public function testSetLocksFailureOnTheDestinationReachesOnError(): void
    {
        $source = new Database(self::configurableAdapter(), new Cache(new None()));
        $destination = new Database(new class () extends Memory {
            /**
             * @return array<Capability>
             */
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::AlterLock];
            }

            public function setLocks(bool $locks): static
            {
                throw new RuntimeException('destination unreachable');
            }
        }, new Cache(new None()));
        $mirror = new Mirror($source, $destination);
        $errors = [];
        $mirror->onError(static function (Failure $failure) use (&$errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });

        $mirror->setLocks(true);

        $this->assertTrue(self::locking($source));
        $this->assertSame([['setLocks', 'destination unreachable']], $errors);
    }

    public function testCacheWriterTimeoutReachesSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setCacheWriterTimeout(30);

        $this->assertSame(
            [30, 30, 30],
            [$mirror->getCacheWriterTimeout(), $source->getCacheWriterTimeout(), $destination->getCacheWriterTimeout()],
        );
    }

    /**
     * @return iterable<string, array{Closure(Mirror): array<Document>}>
     */
    public static function writesReturningDocuments(): iterable
    {
        yield 'createDocument' => [
            static fn (Mirror $mirror): array => [$mirror->createDocument(self::COLLECTION, new Document([Document::ID => 'written', 'title' => 'written']))],
        ];
        yield 'updateDocument' => [
            static fn (Mirror $mirror): array => [$mirror->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'written']))],
        ];
        yield 'upsertDocument' => [
            static fn (Mirror $mirror): array => [$mirror->upsertDocument(self::COLLECTION, new Document([Document::ID => 'first', 'title' => 'written']))],
        ];
        yield 'createDocuments' => [
            static function (Mirror $mirror): array {
                $returned = [];
                $mirror->createDocuments(
                    self::COLLECTION,
                    [new Document([Document::ID => 'written', 'title' => 'written'])],
                    onNext: static function (Document $document) use (&$returned): void {
                        $returned[] = $document;
                    },
                );

                return $returned;
            },
        ];
        yield 'updateDocuments' => [
            static function (Mirror $mirror): array {
                $returned = [];
                $mirror->updateDocuments(
                    self::COLLECTION,
                    new Document(['title' => 'written']),
                    [Query::equal(Document::ID, ['first'])],
                    onNext: static function (Document $document) use (&$returned): void {
                        $returned[] = $document;
                    },
                );

                return $returned;
            },
        ];
        yield 'upsertDocuments' => [
            static function (Mirror $mirror): array {
                $returned = [];
                $mirror->upsertDocuments(
                    self::COLLECTION,
                    [new Document([Document::ID => 'first', 'title' => 'written'])],
                    onNext: static function (Document $document) use (&$returned): void {
                        $returned[] = $document;
                    },
                );

                return $returned;
            },
        ];
    }

    /**
     * @param  Closure(Mirror): array<Document>  $write
     */
    #[DataProvider('writesReturningDocuments')]
    public function testDecoratorsApplyToDocumentsReturnedByWrites(Closure $write): void
    {
        $destination = self::sqlite();
        $mirror = $this->seed(new Mirror(self::sqlite(), $destination));
        $mirror->addHook(new class () implements Decorator {
            public function decorate(Event $event, Document $collection, Document $document): Document
            {
                return $document->setAttribute('decoratedFor', $collection->getId());
            }
        });
        $errors = [];
        $mirror->onError(static function (Failure $failure) use (&$errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });
        /** @var ArrayObject<int, Document> $returned */
        $returned = new ArrayObject();

        self::inCoroutine(static function () use ($mirror, $write, $returned): void {
            $returned->exchangeArray($write($mirror));
        });

        $this->assertCount(1, $returned);
        $first = $returned[0] ?? null;
        $this->assertInstanceOf(Document::class, $first);
        $this->assertSame(self::COLLECTION, $first->getAttribute('decoratedFor'));
        $this->assertSame([], $errors, 'A decorated document must not reach the destination');
        $replicated = $destination->getDocument(self::COLLECTION, $first->getId());
        $this->assertSame('written', $replicated->getAttribute('title'));
        $this->assertNull($replicated->getAttribute('decoratedFor'));
    }

    /**
     * @return iterable<string, array{Closure(Mirror): mixed, mixed}>
     */
    public static function destinationlessCalls(): iterable
    {
        yield 'setTimeout' => [
            static function (Mirror $mirror): mixed {
                $mirror->setTimeout(500);

                return self::timeout($mirror->getSource());
            },
            500,
        ];
        yield 'setValidation(false)' => [
            static function (Mirror $mirror): mixed {
                $mirror->setValidation(false);

                return [$mirror->isValidating(), $mirror->getSource()->isValidating()];
            },
            [false, false],
        ];
        yield 'collectionExists' => [
            static fn (Mirror $mirror): mixed => $mirror->collectionExists(self::COLLECTION, 'mirror'),
            true,
        ];
        yield 'increaseDocumentAttribute' => [
            static fn (Mirror $mirror): mixed => [
                $mirror->increaseDocumentAttribute(self::COLLECTION, 'first', 'views', 2)->getAttribute('views'),
                $mirror->getSource()->getDocument(self::COLLECTION, 'first')->getAttribute('views'),
            ],
            [3, 3],
        ];
    }

    /**
     * @param  Closure(Mirror): mixed  $call
     */
    #[DataProvider('destinationlessCalls')]
    public function testMirrorWithoutDestinationDelegatesToTheSource(Closure $call, mixed $expected): void
    {
        $mirror = $this->seed(new Mirror(new Database(self::configurableAdapter(), new Cache(new None()))));
        $errors = [];
        $mirror->onError(static function (Failure $failure) use (&$errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });

        $this->assertSame($expected, $call($mirror));
        $this->assertSame([], $errors);
    }

    public function testSkipValidationWithoutDestinationRunsOnTheSource(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror($source);

        $inside = $mirror->skipValidation(static fn (): array => [$mirror->isValidating(), $source->isValidating()]);

        $this->assertSame([false, false], $inside);
        $this->assertSame([true, true], [$mirror->isValidating(), $source->isValidating()]);
    }

    public function testCreateCollectionRunsWriteFilters(): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $destination = self::sqlite();
        $mirror = $this->filtered(
            [self::recordingFilter($calls, static fn (string $hook, ?Document $collection): ?Document => $collection === null ? null : (clone $collection)->setAttribute('filtered', true))],
            self::sqlite(),
            $destination,
        );

        $created = $mirror->createCollection(Collection::create(id: 'filtered', attributes: [Attribute::string(key: 'title', size: 64)]));

        $this->assertSame([['beforeCreateCollection', 'filtered', 'filtered']], $calls->getArrayCopy());
        $this->assertTrue($created->getAttribute('filtered'), 'The filtered collection is what the caller receives');
        $this->assertNotNull($destination->findCollection('filtered'));
        $this->assertSame('upgraded', self::upgradeStatus($mirror, 'filtered'));
    }

    public function testCreateCollectionFilterReturningNullSkipsTheDestination(): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter($calls, static fn (): ?Document => null)], $source, $destination);
        $errors = self::errors($mirror);

        $created = $mirror->createCollection(Collection::create(id: 'skipped', attributes: [Attribute::string(key: 'title', size: 64)]));

        $this->assertSame([['beforeCreateCollection', 'skipped', 'skipped']], $calls->getArrayCopy());
        $this->assertSame('skipped', $created->getId());
        $this->assertNotNull($source->findCollection('skipped'));
        $this->assertNull($destination->findCollection('skipped'));
        $this->assertNull(self::upgradeStatus($mirror, 'skipped'), 'Documents of a collection the destination lacks must not be replicated');
        $this->assertSame([], $errors->getArrayCopy());
    }

    public function testUpdateCollectionRunsWriteFilters(): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $destination = self::sqlite();
        $mirror = $this->filtered(
            [self::recordingFilter($calls, static fn (string $hook, ?Document $collection): ?Document => $collection === null ? null : (clone $collection)->setAttribute('filtered', true))],
            self::sqlite(),
            $destination,
        );

        $updated = $mirror->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::users())], documentSecurity: false));

        $this->assertSame([['beforeUpdateCollection', self::COLLECTION, self::COLLECTION]], $calls->getArrayCopy());
        $this->assertNull($updated->getAttribute('filtered'), 'The caller receives the collection the source stored');
        $this->assertSame([Permission::read(Role::users())], $destination->getCollection(self::COLLECTION)->getPermissions());
        $this->assertFalse($destination->getCollection(self::COLLECTION)->getAttribute('documentSecurity'));
    }

    public function testUpdateCollectionFilterReturningNullSkipsTheDestination(): void
    {
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter(new ArrayObject(), static fn (): ?Document => null)], $source, $destination);
        $permissions = $destination->getCollection(self::COLLECTION)->getPermissions();

        $updated = $mirror->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::users())], documentSecurity: false));

        $this->assertFalse($updated->getAttribute('documentSecurity'));
        $this->assertSame([Permission::read(Role::users())], $source->getCollection(self::COLLECTION)->getPermissions());
        $this->assertSame($permissions, $destination->getCollection(self::COLLECTION)->getPermissions());
        $this->assertTrue($destination->getCollection(self::COLLECTION)->getAttribute('documentSecurity'));
    }

    public function testUpdateCollectionReplicationFailureIsReportedNotThrown(): void
    {
        $source = self::sqlite();
        $mirror = $this->filtered([], $source, self::sqlite());
        $errors = self::errors($mirror);
        $source->createCollection(Collection::create(id: 'sourceOnly', attributes: [Attribute::string(key: 'title', size: 64)]));

        $updated = $mirror->updateCollection('sourceOnly', new CollectionUpdate(permissions: [Permission::read(Role::any())], documentSecurity: false));

        $this->assertFalse($updated->getAttribute('documentSecurity'));
        $this->assertSame([Permission::read(Role::any())], $source->getCollection('sourceOnly')->getPermissions());
        $this->assertSame([['updateCollection', 'Collection not found']], $errors->getArrayCopy());
    }

    /**
     * The mirror seeded over $source and $destination, as a mirror with $filters over the same two databases.
     *
     * @param  array<Filter>  $filters
     */
    private function filtered(array $filters, Database $source, Database $destination): Mirror
    {
        $seeded = $this->seed(new Mirror($source, $destination));

        return (new Mirror($source, $destination, $filters))
            ->setDatabase($seeded->getDatabase())
            ->setNamespace($seeded->getNamespace());
    }

    private static function upgradeStatus(Mirror $mirror, string $collection): mixed
    {
        $source = $mirror->getSource();
        if ($source->findCollection('upgrades') === null) {
            return null;
        }

        return $source->getAuthorization()->skip(
            static fn (): mixed => $source->getDocument('upgrades', $collection)->getAttribute('status'),
        );
    }

    /**
     * @return ArrayObject<int, array{string, string}>
     */
    private static function errors(Mirror $mirror): ArrayObject
    {
        /** @var ArrayObject<int, array{string, string}> $errors */
        $errors = new ArrayObject();
        $mirror->onError(static function (Failure $failure) use ($errors): void {
            $errors[] = [$failure->method, $failure->error->getMessage()];
        });

        return $errors;
    }

    /**
     * A write filter that records every hook it runs as [hook, collection id, subject] and returns what $transform
     * makes of the document the hook receives; hooks without a document pass null and ignore the result.
     *
     * @param  ArrayObject<int, array{string, string, mixed}>  $calls
     * @param  Closure(string, ?Document): ?Document  $transform
     */
    private static function recordingFilter(ArrayObject $calls, Closure $transform): Filter
    {
        return new class ($calls, $transform) extends Filter {
            /**
             * @param  ArrayObject<int, array{string, string, mixed}>  $calls
             * @param  Closure(string, ?Document): ?Document  $transform
             */
            public function __construct(
                private readonly ArrayObject $calls,
                private readonly Closure $transform,
            ) {
            }

            public function beforeCreateCollection(Database $source, Database $destination, string $collectionId, ?Document $collection = null): ?Document
            {
                return $this->run(__FUNCTION__, $collectionId, $collection?->getId(), $collection);
            }

            public function beforeUpdateCollection(Database $source, Database $destination, string $collectionId, ?Document $collection = null): ?Document
            {
                return $this->run(__FUNCTION__, $collectionId, $collection?->getId(), $collection);
            }

            public function beforeDeleteCollection(Database $source, Database $destination, string $collectionId): void
            {
                $this->run(__FUNCTION__, $collectionId, $collectionId);
            }

            public function beforeCreateAttribute(Database $source, Database $destination, string $collectionId, string $attributeId, ?Document $attribute = null): ?Document
            {
                return $this->run(__FUNCTION__, $collectionId, $attributeId, $attribute);
            }

            public function beforeUpdateAttribute(Database $source, Database $destination, string $collectionId, string $attributeId, ?Document $attribute = null): ?Document
            {
                return $this->run(__FUNCTION__, $collectionId, $attributeId, $attribute);
            }

            public function beforeDeleteAttribute(Database $source, Database $destination, string $collectionId, string $attributeId): void
            {
                $this->run(__FUNCTION__, $collectionId, $attributeId);
            }

            public function beforeCreateIndex(Database $source, Database $destination, string $collectionId, string $indexId, ?Document $index = null): ?Document
            {
                return $this->run(__FUNCTION__, $collectionId, $indexId, $index);
            }

            public function beforeDeleteIndex(Database $source, Database $destination, string $collectionId, string $indexId): void
            {
                $this->run(__FUNCTION__, $collectionId, $indexId);
            }

            public function beforeCreateDocument(Database $source, Database $destination, string $collectionId, Document $document): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $document->getId(), $document) ?? $document;
            }

            public function afterCreateDocument(Database $source, Database $destination, string $collectionId, Document $document): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $document->getId(), $document) ?? $document;
            }

            public function beforeUpdateDocument(Database $source, Database $destination, string $collectionId, Document $document): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $document->getId(), $document) ?? $document;
            }

            public function afterUpdateDocument(Database $source, Database $destination, string $collectionId, Document $document): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $document->getId(), $document) ?? $document;
            }

            public function beforeUpdateDocuments(Database $source, Database $destination, string $collectionId, Document $updates, array $queries): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $updates->getAttribute('title'), $updates) ?? $updates;
            }

            public function afterUpdateDocuments(Database $source, Database $destination, string $collectionId, Document $updates, array $queries): void
            {
                $this->run(__FUNCTION__, $collectionId, $updates->getAttribute('title'));
            }

            public function beforeDeleteDocument(Database $source, Database $destination, string $collectionId, string $documentId): void
            {
                $this->run(__FUNCTION__, $collectionId, $documentId);
            }

            public function afterDeleteDocument(Database $source, Database $destination, string $collectionId, string $documentId): void
            {
                $this->run(__FUNCTION__, $collectionId, $documentId);
            }

            public function beforeDeleteDocuments(Database $source, Database $destination, string $collectionId, array $queries): void
            {
                $this->run(__FUNCTION__, $collectionId, \count($queries));
            }

            public function afterDeleteDocuments(Database $source, Database $destination, string $collectionId, array $queries): void
            {
                $this->run(__FUNCTION__, $collectionId, \count($queries));
            }

            public function beforeCreateOrUpdateDocument(Database $source, Database $destination, string $collectionId, Document $document): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $document->getId(), $document) ?? $document;
            }

            public function afterCreateOrUpdateDocument(Database $source, Database $destination, string $collectionId, Document $document): Document
            {
                return $this->run(__FUNCTION__, $collectionId, $document->getId(), $document) ?? $document;
            }

            private function run(string $hook, string $collectionId, mixed $subject, ?Document $document = null): ?Document
            {
                $this->calls[] = [$hook, $collectionId, $subject];

                return ($this->transform)($hook, $document);
            }
        };
    }

    /**
     * @return iterable<string, array{Closure(string, ?Document): ?Document, int|null}>
     */
    public static function attributeFilters(): iterable
    {
        yield 'resized' => [
            static fn (string $hook, ?Document $attribute): ?Document => $attribute === null ? null : (clone $attribute)->setAttribute('size', 128),
            128,
        ];
        yield 'skipped' => [static fn (): ?Document => null, null];
    }

    /**
     * @param  Closure(string, ?Document): ?Document  $transform
     */
    #[DataProvider('attributeFilters')]
    public function testCreateAttributeRunsWriteFilters(Closure $transform, ?int $size): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter($calls, $transform)], $source, $destination);
        $errors = self::errors($mirror);

        $mirror->createAttribute(self::COLLECTION, Attribute::string(key: 'summary', size: 64));

        $this->assertSame([['beforeCreateAttribute', self::COLLECTION, 'summary']], $calls->getArrayCopy());
        $this->assertSame(64, self::attribute($source, 'summary')?->size);
        $this->assertSame($size, self::attribute($destination, 'summary')?->size);
        $this->assertSame([], $errors->getArrayCopy());
    }

    public function testCreateAttributesRunsWriteFiltersPerAttribute(): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter(
            $calls,
            static fn (string $hook, ?Document $attribute): ?Document => $attribute === null || $attribute->getAttribute('key') === 'dropped'
                ? null
                : (clone $attribute)->setAttribute('size', 128),
        )], $source, $destination);
        $errors = self::errors($mirror);

        $mirror->createAttributes(self::COLLECTION, [
            Attribute::string(key: 'dropped', size: 64),
            Attribute::string(key: 'resized', size: 64),
        ]);

        $this->assertSame([
            ['beforeCreateAttribute', self::COLLECTION, 'dropped'],
            ['beforeCreateAttribute', self::COLLECTION, 'resized'],
        ], $calls->getArrayCopy());
        $this->assertSame([64, 64], [self::attribute($source, 'dropped')?->size, self::attribute($source, 'resized')?->size]);
        $this->assertSame([null, 128], [self::attribute($destination, 'dropped')?->size, self::attribute($destination, 'resized')?->size]);
        $this->assertSame([], $errors->getArrayCopy());
    }

    /**
     * @param  Closure(string, ?Document): ?Document  $transform
     */
    #[DataProvider('attributeFilters')]
    public function testUpdateAttributeRunsWriteFilters(Closure $transform, ?int $size): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter($calls, $transform)], $source, $destination);
        $errors = self::errors($mirror);

        $updated = $mirror->updateAttribute(self::COLLECTION, 'title', new AttributeUpdate(size: 100));

        $this->assertSame([['beforeUpdateAttribute', self::COLLECTION, 'title']], $calls->getArrayCopy());
        $this->assertSame(100, $updated->size, 'The caller receives the definition the source stored');
        $this->assertSame(100, self::attribute($source, 'title')?->size);
        $this->assertSame($size ?? 64, self::attribute($destination, 'title')?->size);
        $this->assertSame([], $errors->getArrayCopy());
    }

    /**
     * @return iterable<string, array{Closure(string, ?Document): ?Document, array<string>|null}>
     */
    public static function indexFilters(): iterable
    {
        yield 'retargeted' => [
            static fn (string $hook, ?Document $index): ?Document => $index === null ? null : (clone $index)->setAttribute('attributes', ['views']),
            ['views'],
        ];
        yield 'skipped' => [static fn (): ?Document => null, null];
    }

    /**
     * @param  Closure(string, ?Document): ?Document  $transform
     * @param  array<string>|null  $attributes
     */
    #[DataProvider('indexFilters')]
    public function testCreateIndexRunsWriteFilters(Closure $transform, ?array $attributes): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter($calls, $transform)], $source, $destination);
        $errors = self::errors($mirror);

        $mirror->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']));

        $this->assertSame([['beforeCreateIndex', self::COLLECTION, 'titles']], $calls->getArrayCopy());
        $this->assertSame(['title'], self::index($source, 'titles')?->attributes);
        $this->assertSame($attributes, self::index($destination, 'titles')?->attributes);
        $this->assertSame([], $errors->getArrayCopy());
    }

    /**
     * @return iterable<string, array{string, Closure(Mirror, Database, Database): mixed}>
     */
    public static function schemaReplicationFailures(): iterable
    {
        yield 'createAttribute' => [
            'createAttribute',
            static fn (Mirror $mirror): Attribute => $mirror->createAttribute(self::COLLECTION, Attribute::string(key: 'summary', size: 64)),
        ];
        yield 'createAttributes' => [
            'createAttributes',
            static fn (Mirror $mirror): array => $mirror->createAttributes(self::COLLECTION, [Attribute::string(key: 'summary', size: 64)]),
        ];
        yield 'deleteAttribute' => [
            'deleteAttribute',
            static function (Mirror $mirror, Database $source): void {
                $source->createAttribute(self::COLLECTION, Attribute::string(key: 'summary', size: 64));
                $mirror->deleteAttribute(self::COLLECTION, 'summary');
            },
        ];
        yield 'createIndex' => [
            'createIndex',
            static function (Mirror $mirror, Database $source, Database $destination): Index {
                $destination->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']));

                return $mirror->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']));
            },
        ];
        yield 'deleteIndex' => [
            'deleteIndex',
            static function (Mirror $mirror, Database $source): void {
                $source->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']));
                $mirror->deleteIndex(self::COLLECTION, 'titles');
            },
        ];
    }

    /**
     * @param  Closure(Mirror, Database, Database): mixed  $change
     */
    #[DataProvider('schemaReplicationFailures')]
    public function testSchemaReplicationFailureIsReportedNotThrown(string $action, Closure $change): void
    {
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([], $source, $destination);
        $errors = self::errors($mirror);
        if ($action !== 'createIndex') {
            $destination->deleteCollection(self::COLLECTION);
        }

        $change($mirror, $source, $destination);

        $this->assertCount(1, $errors);
        $this->assertSame([$action], \array_column($errors->getArrayCopy(), 0));
    }

    /**
     * Each case: the write, its onError action, the document it touches, the filter hooks it runs, then the title the
     * source and the destination hold afterwards, and the one the destination keeps when a filter fails.
     *
     * @return iterable<string, array{Closure(Mirror): mixed, string, string, list<array{string, string, mixed}>, ?string, ?string, ?string}>
     */
    public static function documentWrites(): iterable
    {
        yield 'createDocument' => [
            static fn (Mirror $mirror): mixed => $mirror->createDocument(self::COLLECTION, new Document([Document::ID => 'second', 'title' => 'second'])),
            'createDocument',
            'second',
            [['beforeCreateDocument', self::COLLECTION, 'second'], ['afterCreateDocument', self::COLLECTION, 'second']],
            'second',
            'filtered',
            null,
        ];
        yield 'createDocuments' => [
            static fn (Mirror $mirror): mixed => $mirror->createDocuments(self::COLLECTION, [new Document([Document::ID => 'second', 'title' => 'second'])]),
            'createDocuments',
            'second',
            [['beforeCreateDocument', self::COLLECTION, 'second'], ['afterCreateDocument', self::COLLECTION, 'second']],
            'second',
            'filtered',
            null,
        ];
        yield 'updateDocument' => [
            static fn (Mirror $mirror): mixed => $mirror->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated'])),
            'updateDocument',
            'first',
            [['beforeUpdateDocument', self::COLLECTION, 'first'], ['afterUpdateDocument', self::COLLECTION, 'first']],
            'updated',
            'filtered',
            'first',
        ];
        yield 'updateDocuments' => [
            static fn (Mirror $mirror): mixed => $mirror->updateDocuments(self::COLLECTION, new Document(['title' => 'updated']), [Query::equal(Document::ID, ['first'])]),
            'updateDocuments',
            'first',
            [['beforeUpdateDocuments', self::COLLECTION, 'updated'], ['afterUpdateDocuments', self::COLLECTION, 'filtered']],
            'updated',
            'filtered',
            'first',
        ];
        yield 'upsertDocuments' => [
            static fn (Mirror $mirror): mixed => $mirror->upsertDocuments(self::COLLECTION, [new Document([Document::ID => 'first', 'title' => 'upserted'])]),
            'upsertDocuments',
            'first',
            [['beforeCreateOrUpdateDocument', self::COLLECTION, 'first'], ['afterCreateOrUpdateDocument', self::COLLECTION, 'first']],
            'upserted',
            'filtered',
            'first',
        ];
        yield 'upsertDocuments with an increase' => [
            static fn (Mirror $mirror): mixed => $mirror->upsertDocuments(self::COLLECTION, [new Document([Document::ID => 'second', 'title' => 'upserted', 'views' => 1])], increase: 'views'),
            'upsertDocuments',
            'second',
            [['beforeCreateOrUpdateDocument', self::COLLECTION, 'second'], ['afterCreateOrUpdateDocument', self::COLLECTION, 'second']],
            'upserted',
            'filtered',
            null,
        ];
        yield 'deleteDocument' => [
            static fn (Mirror $mirror): mixed => $mirror->deleteDocument(self::COLLECTION, 'first'),
            'deleteDocument',
            'first',
            [['beforeDeleteDocument', self::COLLECTION, 'first'], ['afterDeleteDocument', self::COLLECTION, 'first']],
            null,
            null,
            'first',
        ];
        yield 'deleteDocuments' => [
            static fn (Mirror $mirror): mixed => $mirror->deleteDocuments(self::COLLECTION, [Query::equal(Document::ID, ['first'])]),
            'deleteDocuments',
            'first',
            [['beforeDeleteDocuments', self::COLLECTION, 1], ['afterDeleteDocuments', self::COLLECTION, 1]],
            null,
            null,
            'first',
        ];
    }

    /**
     * @param  Closure(Mirror): mixed  $write
     * @param  list<array{string, string, mixed}>  $hooks
     */
    #[DataProvider('documentWrites')]
    public function testDocumentWritesRunWriteFilters(Closure $write, string $action, string $id, array $hooks, ?string $sourceTitle, ?string $destinationTitle, ?string $unchangedTitle): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter(
            $calls,
            static fn (string $hook, ?Document $document): ?Document => $document !== null && \str_starts_with($hook, 'before')
                ? (clone $document)->setAttribute('title', 'filtered')
                : $document,
        )], $source, $destination);
        $errors = self::errors($mirror);

        self::inCoroutine(static fn (): mixed => $write($mirror));

        $this->assertSame([], $errors->getArrayCopy());
        $this->assertSame($hooks, $calls->getArrayCopy());
        $this->assertSame([$sourceTitle, $destinationTitle], [self::storedTitle($source, $id), self::storedTitle($destination, $id)]);
    }

    /**
     * @param  Closure(Mirror): mixed  $write
     * @param  list<array{string, string, mixed}>  $hooks
     */
    #[DataProvider('documentWrites')]
    public function testDocumentWriteFilterFailureIsReportedNotThrown(Closure $write, string $action, string $id, array $hooks, ?string $sourceTitle, ?string $destinationTitle, ?string $unchangedTitle): void
    {
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter(
            new ArrayObject(),
            static fn (string $hook): ?Document => throw new RuntimeException('filter failed in '.$hook),
        )], $source, $destination);
        $errors = self::errors($mirror);

        self::inCoroutine(static fn (): mixed => $write($mirror));

        $this->assertSame([[$action, 'filter failed in '.$hooks[0][0]]], $errors->getArrayCopy());
        $this->assertSame([$sourceTitle, $unchangedTitle], [self::storedTitle($source, $id), self::storedTitle($destination, $id)]);
    }

    /**
     * @param  Closure(Mirror): mixed  $write
     * @param  list<array{string, string, mixed}>  $hooks
     */
    #[DataProvider('documentWrites')]
    public function testDocumentReplicationFailureIsReportedNotThrown(Closure $write, string $action, string $id, array $hooks, ?string $sourceTitle, ?string $destinationTitle, ?string $unchangedTitle): void
    {
        $source = self::sqlite();
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public bool $unreachable = false;

            public function createDocument(Document $collection, Document $document): Document
            {
                $this->reach();

                return parent::createDocument($collection, $document);
            }

            public function createDocuments(Document $collection, array $documents): array
            {
                $this->reach();

                return parent::createDocuments($collection, $documents);
            }

            public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
            {
                $this->reach();

                return parent::updateDocument($collection, $id, $document, $skipPermissions);
            }

            public function updateDocuments(Document $collection, Document $updates, array $documents, array $skipPermissions = []): int
            {
                $this->reach();

                return parent::updateDocuments($collection, $updates, $documents, $skipPermissions);
            }

            /**
             * @param  array<Change>  $changes
             * @return array<Document>
             */
            public function upsertDocuments(Document $collection, array $changes, ?string $increase = null): array
            {
                $this->reach();

                return parent::upsertDocuments($collection, $changes, $increase);
            }

            public function deleteDocument(Document $collection, string $id): bool
            {
                $this->reach();

                return parent::deleteDocument($collection, $id);
            }

            public function deleteDocuments(Document $collection, array $sequences, array $permissionIds): int
            {
                $this->reach();

                return parent::deleteDocuments($collection, $sequences, $permissionIds);
            }

            private function reach(): void
            {
                if ($this->unreachable) {
                    throw new RuntimeException('destination unreachable');
                }
            }
        };
        $destination = new Database($adapter, new Cache(new None()));
        $mirror = $this->filtered([], $source, $destination);
        $errors = self::errors($mirror);
        $adapter->unreachable = true;

        self::inCoroutine(static fn (): mixed => $write($mirror));

        $this->assertSame([[$action, 'destination unreachable']], $errors->getArrayCopy());
        $this->assertSame($sourceTitle, self::storedTitle($source, $id));
        $this->assertSame($unchangedTitle, self::storedTitle($destination, $id));
        $this->assertFalse($destination->isPreservingDates(), 'A failed replication must not leave the destination preserving dates');
    }

    public function testReplicationKeepsTheDestinationsPreserveDatesSetting(): void
    {
        $destination = self::sqlite();
        $mirror = $this->filtered([], self::sqlite(), $destination);
        $mirror->setPreserveDates(true);

        $mirror->createDocument(self::COLLECTION, new Document([Document::ID => 'second', 'title' => 'second']));
        $mirror->updateDocument(self::COLLECTION, 'second', new Document(['title' => 'updated']));

        $this->assertTrue($destination->isPreservingDates());
    }

    public function testWritesThroughAMirrorWhoseSourceHasNoUpgradesCollectionAreNotReplicated(): void
    {
        $source = self::sqlite();
        $destination = self::sqlite();
        $namespace = 'mirror_'.\uniqid();
        foreach ([$source, $destination] as $database) {
            $database->setDatabase('mirror')->setNamespace($namespace)->create();
            $database->createCollection(Collection::create(
                id: self::COLLECTION,
                attributes: [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'views')],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
            ));
            $database->createDocument(self::COLLECTION, new Document([Document::ID => 'first', 'title' => 'first']));
            $database->createDocument(self::COLLECTION, new Document([Document::ID => 'third', 'title' => 'third']));
        }
        $mirror = (new Mirror($source, $destination))->setDatabase('mirror')->setNamespace($namespace);
        $errors = self::errors($mirror);

        self::inCoroutine(static function () use ($mirror): void {
            $mirror->createDocument(self::COLLECTION, new Document([Document::ID => 'second', 'title' => 'second']));
            $mirror->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated']));
            $mirror->deleteDocument(self::COLLECTION, 'third');
            $mirror->createDocuments(self::COLLECTION, [new Document([Document::ID => 'fourth', 'title' => 'fourth'])]);
        });

        $this->assertNull($source->findCollection('upgrades'));
        $this->assertSame(['updated', 'second', null, 'fourth'], \array_map(static fn (string $id): ?string => self::storedTitle($source, $id), ['first', 'second', 'third', 'fourth']));
        $this->assertSame(['first', null, 'third', null], \array_map(static fn (string $id): ?string => self::storedTitle($destination, $id), ['first', 'second', 'third', 'fourth']));
        $this->assertSame([], $errors->getArrayCopy());
    }

    public function testUpdateCollectionWithoutDestinationReturnsTheSourceCollection(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $mirror = $this->seed(new Mirror($source));
        $errors = self::errors($mirror);

        $updated = $mirror->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::users())], documentSecurity: false));

        $this->assertSame([Permission::read(Role::users())], $updated->getPermissions());
        $this->assertFalse($updated->getAttribute('documentSecurity'));
        $this->assertSame([Permission::read(Role::users())], $source->getCollection(self::COLLECTION)->getPermissions());
        $this->assertSame([], $errors->getArrayCopy());
    }

    /**
     * @return iterable<string, array{Closure(Mirror, Database): array{mixed, mixed}, array{mixed, mixed}}>
     */
    public static function destinationlessReplicatedWrites(): iterable
    {
        yield 'createAttribute' => [
            static fn (Mirror $mirror, Database $source): array => [
                $mirror->createAttribute(self::COLLECTION, Attribute::string(key: 'summary', size: 64))->key,
                self::attribute($source, 'summary')?->size,
            ],
            ['summary', 64],
        ];
        yield 'createAttributes' => [
            static fn (Mirror $mirror, Database $source): array => [
                \array_map(
                    static fn (Attribute $attribute): string => $attribute->key,
                    $mirror->createAttributes(self::COLLECTION, [Attribute::string(key: 'summary', size: 64), Attribute::string(key: 'subtitle', size: 32)]),
                ),
                [self::attribute($source, 'summary')?->size, self::attribute($source, 'subtitle')?->size],
            ],
            [['summary', 'subtitle'], [64, 32]],
        ];
        yield 'updateAttribute' => [
            static fn (Mirror $mirror, Database $source): array => [
                $mirror->updateAttribute(self::COLLECTION, 'title', new AttributeUpdate(size: 100))->size,
                self::attribute($source, 'title')?->size,
            ],
            [100, 100],
        ];
        yield 'createIndex' => [
            static fn (Mirror $mirror, Database $source): array => [
                $mirror->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']))->key,
                self::index($source, 'titles')?->attributes,
            ],
            ['titles', ['title']],
        ];
        yield 'upsertDocuments without an increase' => [
            static fn (Mirror $mirror, Database $source): array => [
                $mirror->upsertDocuments(self::COLLECTION, [new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 1])]),
                self::storedTitle($source, 'first'),
            ],
            [1, 'upserted'],
        ];
        yield 'deleteAttribute' => [
            static function (Mirror $mirror, Database $source): array {
                $mirror->deleteAttribute(self::COLLECTION, 'views');

                return [self::attribute($source, 'views'), self::attribute($source, 'title')?->key];
            },
            [null, 'title'],
        ];
        yield 'deleteIndex' => [
            static function (Mirror $mirror, Database $source): array {
                $source->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']));

                $mirror->deleteIndex(self::COLLECTION, 'titles');

                return [self::index($source, 'titles'), self::attribute($source, 'title')?->key];
            },
            [null, 'title'],
        ];
        yield 'updateDocument' => [
            static fn (Mirror $mirror, Database $source): array => [
                $mirror->updateDocument(self::COLLECTION, 'first', new Document(['title' => 'updated']))->getAttribute('title'),
                self::storedTitle($source, 'first'),
            ],
            ['updated', 'updated'],
        ];
    }

    /**
     * A mirror built without a destination over a source whose collection an earlier mirror upgraded writes the
     * source only: its write filters do not run and nothing is reported.
     *
     * @param  Closure(Mirror, Database): array{mixed, mixed}  $write
     * @param  array{mixed, mixed}  $expected
     */
    #[DataProvider('destinationlessReplicatedWrites')]
    public function testWriteWithoutDestinationOverAnUpgradedCollectionStaysOnTheSource(Closure $write, array $expected): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $seeded = $this->seed(new Mirror($source, self::sqlite()));
        $mirror = (new Mirror($source, null, [self::recordingFilter($calls, static fn (string $hook, ?Document $document): ?Document => $document)]))
            ->setDatabase($seeded->getDatabase())
            ->setNamespace($seeded->getNamespace());
        $errors = self::errors($mirror);

        $this->assertSame('upgraded', self::upgradeStatus($mirror, self::COLLECTION));
        $this->assertSame($expected, $write($mirror, $source));
        $this->assertSame([], $calls->getArrayCopy());
        $this->assertSame([], $errors->getArrayCopy());
    }

    public function testDeleteAttributeAndIndexRunWriteFilters(): void
    {
        /** @var ArrayObject<int, array{string, string, mixed}> $calls */
        $calls = new ArrayObject();
        $source = self::sqlite();
        $destination = self::sqlite();
        $mirror = $this->filtered([self::recordingFilter($calls, static fn (string $hook, ?Document $document): ?Document => $document)], $source, $destination);
        $errors = self::errors($mirror);
        $mirror->createIndex(self::COLLECTION, Index::key(key: 'titles', attributes: ['title']));
        $calls->exchangeArray([]);

        $mirror->deleteIndex(self::COLLECTION, 'titles');
        $mirror->deleteAttribute(self::COLLECTION, 'views');

        $this->assertSame([
            ['beforeDeleteIndex', self::COLLECTION, 'titles'],
            ['beforeDeleteAttribute', self::COLLECTION, 'views'],
        ], $calls->getArrayCopy());
        $this->assertSame([null, null], [self::index($destination, 'titles'), self::attribute($destination, 'views')]);
        $this->assertSame([], $errors->getArrayCopy());
    }

    private static function storedTitle(Database $database, string $id): ?string
    {
        $title = $database->getDocument(self::COLLECTION, $id)->getAttribute('title');

        return \is_string($title) ? $title : null;
    }

    private static function attribute(Database $database, string $key): ?Attribute
    {
        foreach ($database->getCollection(self::COLLECTION)->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        return null;
    }

    private static function index(Database $database, string $key): ?Index
    {
        foreach ($database->getCollection(self::COLLECTION)->indexes() as $index) {
            if ($index->key === $key) {
                return $index;
            }
        }

        return null;
    }
}
