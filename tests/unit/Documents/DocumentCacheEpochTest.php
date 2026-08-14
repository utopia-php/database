<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class DocumentCacheEpochTest extends TestCase
{
    public function testPurgeCachedCollectionDoesNotThrowWhenEpochPurgeReturnsFalse(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->purgeCachedCollection('webhooks'));

        [$collectionKey] = $database->getCacheKeys('webhooks');
        $epochKey = $collectionKey.'#epoch';
        $before = $database->getCache()->load($epochKey, Database::TTL);
        $this->assertIsString($before);
        $this->assertNotSame('', $before);

        $adapter->failPurges();

        $this->assertTrue($database->purgeCachedCollection('webhooks'));
        $this->assertTrue($database->purgeCachedDocument('webhooks', 'hook'));

        $after = $database->getCache()->load($epochKey, Database::TTL);
        $this->assertIsString($after);
        $this->assertNotSame($before, $after);
    }

    public function testCreateDocumentDoesNotFailWhenEpochPurgeReturnsFalse(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->purgeCachedCollection('webhooks'));
        $adapter->failPurges();

        $document = $database->createDocument('webhooks', new Document([
            '$id' => 'hook',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $this->assertSame('hook', $document->getId());
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getId());
    }

    /**
     * @return array{Database, FailPurgeMemory}
     */
    private function createDatabase(): array
    {
        $adapter = new FailPurgeMemory();
        $database = new Database(new DatabaseMemory(), new Cache($adapter));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('epoch_'.\uniqid());
        $database->create();
        $database->createCollection('webhooks', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]);

        return [$database, $adapter];
    }
}

final class FailPurgeMemory extends MemoryCache
{
    private bool $failing = false;

    public function failPurges(): void
    {
        $this->failing = true;
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->failing && \str_ends_with($key, '#epoch')) {
            return false;
        }

        return parent::purge($key, $hash);
    }
}
