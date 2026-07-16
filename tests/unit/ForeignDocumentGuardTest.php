<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

/**
 * Regression guard for the by-id read identity invariant: getDocument($id) must
 * never return, or keep serving, a document whose $id differs from $id.
 *
 * Reproduces a production incident where a by-id read cached a sibling row's
 * body under the requested key, so every subsequent read served the wrong
 * document for ~90s until the cache entry expired (DAT-1904).
 */
class ForeignDocumentGuardTest extends TestCase
{
    private DatabaseMemory $adapter;

    private CacheMemory $cacheAdapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
        $this->cacheAdapter = new CacheMemory();
        $this->database = new Database($this->adapter, new Cache($this->cacheAdapter));
        $this->database
            ->setDatabase('utopiaTests')
            ->setNamespace('foreign_doc_' . \uniqid());

        $this->database->create();
        $this->database->createCollection('projects');
        $this->database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);

        foreach (['victim', 'sibling'] as $id) {
            $this->database->createDocument('projects', new Document([
                '$id' => $id,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => $id,
            ]));
        }
    }

    /**
     * Overwrite the requested key's cached body with the sibling row's body,
     * exactly as a mismatched read would have persisted it under this key.
     */
    private function poisonCache(string $requestedId, string $foreignId): void
    {
        $this->database->getDocument('projects', $requestedId);

        [, $documentKey] = $this->database->getCacheKeys('projects', $requestedId);
        $foreign = $this->adapter->getDocument($this->database->getCollection('projects'), $foreignId);

        $this->assertArrayHasKey($documentKey, $this->cacheAdapter->store, 'read should have populated the cache');
        $this->cacheAdapter->store[$documentKey]['data'] = $foreign->getArrayCopy();
    }

    public function testGetDocumentPurgesPoisonedCacheEntryAndRefetches(): void
    {
        $this->poisonCache('victim', 'sibling');

        $document = $this->database->getDocument('projects', 'victim');

        // The poisoned entry must never be served: the correct row is refetched.
        $this->assertSame('victim', $document->getId());
        $this->assertSame('victim', $document->getAttribute('name'));

        // ...and the poison is gone, so a later read stays correct without a write.
        [, $documentKey] = $this->database->getCacheKeys('projects', 'victim');
        $this->assertSame('victim', $this->cacheAdapter->store[$documentKey]['data']['$id']);
        $this->assertSame('victim', $this->database->getDocument('projects', 'victim')->getId());
    }

    public function testGetDocumentThrowsWhenSourceReturnsForeignRow(): void
    {
        // A source read whose row carries the wrong $id (e.g. a result-set
        // delivered on the wrong pooled connection) is an identity violation:
        // it must fail loudly, never be cached, and never be served.
        $adapter = new class () extends DatabaseMemory {
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                $document = parent::getDocument($collection, $id, $queries, $forUpdate);

                if ($collection->getId() === 'projects' && $id === 'victim' && !$document->isEmpty()) {
                    $document->setAttribute('$id', 'sibling');
                }

                return $document;
            }
        };

        $database = new Database($adapter, new Cache(new CacheMemory()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('foreign_src_' . \uniqid());
        $database->create();
        $database->createCollection('projects');
        $database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $database->createDocument('projects', new Document([
            '$id' => 'victim',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'victim',
        ]));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage("mismatched \$id 'sibling'");

        $database->getDocument('projects', 'victim');
    }

    public function testGetDocumentServesMatchingCachedDocument(): void
    {
        // The guard must not disturb the normal cache hit path.
        $first = $this->database->getDocument('projects', 'victim');
        $second = $this->database->getDocument('projects', 'victim');

        $this->assertSame('victim', $first->getId());
        $this->assertSame('victim', $second->getId());
        $this->assertSame('victim', $second->getAttribute('name'));
    }
}
