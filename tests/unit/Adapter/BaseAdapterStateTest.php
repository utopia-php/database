<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\PreserveGlobalState;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\Tenancy;
use Utopia\Database\Validator\Authorization;

final class BaseAdapterStateTest extends TestCase
{
    private const string NAMESPACE = 'state';

    private const int FILTERED_KEY_CACHE_LIMIT = 4096;

    public function testSetMetadataStoresEntriesAndResetMetadataClearsThem(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));

        $this->assertSame([], $adapter->getMetadata());
        $this->assertSame($adapter, $adapter->setMetadata('a', 1));
        $this->assertSame($adapter, $adapter->setMetadata('b', ['nested' => true]));
        $this->assertSame($adapter, $adapter->setMetadata('a', 2));
        $this->assertSame(['a' => 2, 'b' => ['nested' => true]], $adapter->getMetadata());

        $adapter->resetMetadata();
        $this->assertSame([], $adapter->getMetadata());
    }

    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testAKeyThePatternEngineCannotFilterIsAnError(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $jit = \ini_get('pcre.jit');
        $limit = \ini_get('pcre.backtrack_limit');
        \ini_set('pcre.jit', '0');
        \ini_set('pcre.backtrack_limit', '0');

        try {
            $adapter->filter('unfilterable_'.\uniqid().' !@#');
            $this->fail('a key the pattern engine fails on must not pass as filtered');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to filter key', $error->getMessage());
        } finally {
            \ini_set('pcre.jit', (string) $jit);
            \ini_set('pcre.backtrack_limit', (string) $limit);
        }
    }

    public function testTenantHookIsFoundAmongTheOtherWriteHooks(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $adapter->addWriteHook(new class () extends Interceptor {
        });

        $this->assertNull($adapter->getTenantHook());

        $tenancy = new Tenancy();
        $adapter->addWriteHook($tenancy);

        $this->assertSame($tenancy, $adapter->getTenantHook());

        $adapter->removeWriteHook(Tenancy::class);

        $this->assertNull($adapter->getTenantHook());
        $this->assertCount(1, $adapter->getWriteHooks());
    }

    public function testRemovingAWriteHookInstanceKeepsAnotherOfItsClass(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $kept = new Tenancy();
        $removed = new Tenancy('owner');
        $adapter->addWriteHook($kept);
        $adapter->addWriteHook($removed);

        $adapter->removeWriteHook($removed);

        $this->assertSame([$kept], $adapter->getWriteHooks());
    }

    public function testTenantHookFollowsSharedTablesOnAWrite(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection($this->notes('own'));
        $database->createDocument('own', new Document(['$id' => 'first', 'body' => 'one']));

        $this->assertNull($adapter->getTenantHook());

        $database->setSharedTables(true)->setTenant(7);
        $database->setNamespace(self::NAMESPACE . '_shared');
        $database->create();
        $database->createCollection($this->notes('shared'));
        $database->createDocument('shared', new Document(['$id' => 'second', 'body' => 'two']));

        $this->assertNotNull($adapter->getTenantHook());
        $this->assertSame(7, $database->getDocument('shared', 'second')->getTenant());

        $database->setSharedTables(false)->setTenant(null);
        $database->setNamespace(self::NAMESPACE);
        $database->createDocument('own', new Document(['$id' => 'third', 'body' => 'three']));

        $this->assertNull($adapter->getTenantHook());
    }

    public function testClearingTheGlobalTimeoutForgetsEveryEvent(): void
    {
        $adapter = new TimeoutRecordingAdapter();
        $adapter->setTimeout(100);
        $adapter->setTimeout(50, Event::DocumentFind);

        $this->assertSame(100, $adapter->getTimeout());
        $this->assertSame(50, $adapter->getTimeout(Event::DocumentFind));

        $adapter->clearTimeout();

        $this->assertSame(0, $adapter->getTimeout());
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentCreate));
    }

    public function testFilterStaysCorrectPastTheCacheLimit(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $count = self::FILTERED_KEY_CACHE_LIMIT + 4;

        for ($index = 0; $index < $count; $index++) {
            $this->assertSame('key_' . $index . '-x', $adapter->filter('key_' . $index . '-x !@#'));
        }

        for ($index = 0; $index < $count; $index++) {
            $this->assertSame('key_' . $index . '-x', $adapter->filter('key_' . $index . '-x !@#'));
        }
    }

    private function notes(string $id): Collection
    {
        return Collection::create(
            id: $id,
            attributes: [Attribute::string('body', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        );
    }
}
