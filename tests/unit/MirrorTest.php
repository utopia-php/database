<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Event\RecordingLifecycle;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Mirror;
use Utopia\Database\Query;

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
