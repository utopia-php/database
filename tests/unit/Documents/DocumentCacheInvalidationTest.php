<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\CountingCache;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter as DatabaseAdapter;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

final class DocumentCacheInvalidationTest extends TestCase
{
    private const int DOCUMENTS = 5;

    public function testRepeatedWritesAndReadsKeepTheKeyCountBounded(): void
    {
        $cache = new RedisLeasableCache();
        $database = $this->createDatabase(new CountingMemory(), $cache);
        for ($index = 0; $index < self::DOCUMENTS; $index++) {
            $database->createDocument('webhooks', $this->hook('hook'.$index));
        }

        $keysAfterFirstRound = 0;
        for ($round = 1; $round <= 20; $round++) {
            for ($index = 0; $index < self::DOCUMENTS; $index++) {
                $database->updateDocument('webhooks', 'hook'.$index, new Document(['name' => 'round '.$round]));
                $this->assertSame('round '.$round, $database->getDocument('webhooks', 'hook'.$index)->getAttribute('name'));
            }

            if ($round === 1) {
                $keysAfterFirstRound = \count($cache->keys());
            }
        }

        $this->assertSame(
            $keysAfterFirstRound,
            \count($cache->keys()),
            'A purged key stays behind in Redis with no expiry, so writes and reads of the same documents must not add keys',
        );
    }

    public function testACacheWithoutFieldsServesEachSelectionItsOwnCopy(): void
    {
        $database = $this->createDatabase(new CountingMemory(), new MemoryCache());
        $database->createDocument('webhooks', $this->hook('hook'));

        for ($round = 0; $round < 2; $round++) {
            $plain = $database->getDocument('webhooks', 'hook');
            $this->assertSame('description', $plain->getAttribute('description'), 'A read without a selection must get every attribute');

            $projected = $database->getDocument('webhooks', 'hook', [Query::select(['name'])]);
            $this->assertSame('hook', $projected->getAttribute('name'));
            $this->assertFalse($projected->offsetExists('description'), 'A projected read must get only what it selected');
        }
    }

    public function testACachedMissUnderOneCasingDoesNotHideAnotherCasing(): void
    {
        $database = $this->createDatabase($this->caseSensitiveAdapter(), new MemoryCache());
        $database->createDocument('webhooks', $this->hook('Hook'));

        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty(), 'The adapter stores ids case-sensitively');
        $this->assertSame('Hook', $database->getDocument('webhooks', 'Hook')->getId(), 'A cached miss for one casing must not answer another');
        $this->assertTrue($database->getDocument('webhooks', 'hook')->isEmpty(), 'A cached document must not answer another casing of its id');
        $this->assertSame('Hook', $database->getDocument('webhooks', 'Hook')->getId());
    }

    public function testAnUpdateInvalidatesTheCacheOnceLikeADelete(): void
    {
        $cache = new CountingCache(new RedisLeasableCache());
        $database = $this->createDatabase(new CountingMemory(), $cache);
        $database->createDocument('webhooks', $this->hook('updated'));
        $database->createDocument('webhooks', $this->hook('deleted'));
        $database->getDocument('webhooks', 'updated');
        $database->getDocument('webhooks', 'deleted');

        $cache->resetOperations();
        $database->updateDocument('webhooks', 'updated', new Document(['name' => 'renamed']));
        $update = $cache->getOperations();

        $cache->resetOperations();
        $database->deleteDocument('webhooks', 'deleted');
        $delete = $cache->getOperations();

        $this->assertSame($delete, $update, 'updateDocument() must invalidate its document once, as deleteDocument() does');
    }

    private function hook(string $id): Document
    {
        return new Document([
            '$id' => $id,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'hook',
            'description' => 'description',
        ]);
    }

    private function caseSensitiveAdapter(): DatabaseMemory
    {
        return new class () extends DatabaseMemory {
            #[\Override]
            protected function documentKey(string $id, int|string|null $tenant = null): string
            {
                return $this->sharedTables ? ($tenant ?? $this->getTenant()).'|'.$id : $id;
            }
        };
    }

    private function createDatabase(DatabaseAdapter $adapter, CacheAdapter $cache): Database
    {
        $database = new Database($adapter, new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('document_cache_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(new Collection(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
            Attribute::string(key: 'description'),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        return $database;
    }
}
