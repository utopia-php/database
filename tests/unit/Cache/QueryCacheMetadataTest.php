<?php

namespace Tests\Unit\Cache;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;

final class QueryCacheMetadataTest extends TestCase
{
    public function testCollectionListingsFollowCreatedAndDeletedCollections(): void
    {
        $database = new Database(new DatabaseMemory(), new Cache(new None()));
        $database
            ->setDatabase('metadata')
            ->setNamespace('metadata_'.\uniqid());
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->setQueryCache(new QueryCache(new Cache(new LeasableHashCache())));
        $database->createCollection(new Collection(id: 'first', permissions: $this->permissions()));

        $this->assertSame(['first'], $this->listCollectionIds($database));

        $database->createCollection(new Collection(id: 'second', permissions: $this->permissions()));
        $this->assertSame(['first', 'second'], $this->listCollectionIds($database));

        $database->deleteCollection('first');
        $this->assertSame(['second'], $this->listCollectionIds($database));
    }

    public function testCollectionListingsFollowDefinitionsSharedWithEveryTenant(): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setDatabase('metadata')
            ->setNamespace('metadata_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(1);
        $database->addHook(new Permissions());
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->setQueryCache(new QueryCache(new Cache(new LeasableHashCache())));
        $database->createCollection(new Collection(id: 'owned', permissions: $this->permissions()));

        $this->assertSame(['owned'], $this->listCollectionIds($database));

        $database->withTenant(null, fn (): Document => $database->createCollection(new Collection(id: 'shared', permissions: $this->permissions())));

        $this->assertSame(
            ['owned', 'shared'],
            $this->listCollectionIds($database),
            'A tenant-less definition is listed by every tenant, so no tenant\'s invalidation can refresh a cached listing',
        );
    }

    /** @return array<string> */
    private function permissions(): array
    {
        return [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ];
    }

    /** @return array<string> */
    private function listCollectionIds(Database $database): array
    {
        $ids = \array_map(
            static fn (Document $collection): string => $collection->getId(),
            $database->listCollections(),
        );
        \sort($ids);

        return $ids;
    }
}
