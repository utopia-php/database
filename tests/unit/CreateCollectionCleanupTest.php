<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\StderrCapture;
use Throwable;
use Utopia\Cache\Adapter\Memory as MemoryCache;
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
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

final class CreateCollectionCleanupTest extends TestCase
{
    /**
     * The definition row committed and only the cache invalidation after the commit failed:
     * the collection exists, so its table must stay and the failure reaches the caller as it
     * was raised.
     */
    public function testCreateCollectionKeepsItsTableWhenTheInvalidationAfterTheCommitFails(): void
    {
        $failure = new RuntimeException('cache unavailable');
        $cache = new class ($failure) extends MemoryCache {
            public bool $failing = false;

            public function __construct(private readonly RuntimeException $failure)
            {
            }

            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if ($this->failing) {
                    throw $this->failure;
                }

                return parent::save($key, $data, $hash);
            }

            #[\Override]
            public function purge(string $key, string $hash = ''): bool
            {
                if ($this->failing) {
                    throw $this->failure;
                }

                return parent::purge($key, $hash);
            }
        };
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public ?Closure $afterCommit = null;

            #[\Override]
            public function commitTransaction(): bool
            {
                $committed = parent::commitTransaction();
                if (! $this->inTransaction()) {
                    $this->afterCommit?->__invoke();
                }

                return $committed;
            }
        };
        $database = (new Database($adapter, new Cache($cache)))
            ->setAuthorization(new Authorization())
            ->setDatabase('cleanup')
            ->setNamespace('cleanup_'.\uniqid());
        $database->create();
        $database->getAuthorization()->addRole(Role::any()->toString());

        $adapter->afterCommit = static function () use ($cache): void {
            $cache->failing = true;
        };

        $error = null;
        try {
            $database->createCollection(new Collection(
                id: 'logs',
                attributes: [Attribute::string(key: 'message', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
        } catch (Throwable $thrown) {
            $error = $thrown;
        }

        $adapter->afterCommit = null;
        $cache->failing = false;

        $this->assertNotNull($error, 'A failed invalidation after the commit must reach the caller');
        $this->assertTrue($adapter->exists('cleanup', 'logs'), 'A collection whose definition committed must keep its table');
        $this->assertSame('logs', $database->getCollection('logs')->getId());
        $this->assertSame($failure, $error, 'The failure after the commit must reach the caller as it was raised');

        $database->createDocument('logs', new Document(['message' => 'kept']));
        $this->assertSame(1, $database->count('logs'));
    }

    /**
     * A definition row that never committed leaves the table behind it without a
     * collection, so the table is dropped.
     */
    public function testCreateCollectionDropsItsTableWhenTheDefinitionIsNotStored(): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public bool $refuseDefinitions = false;

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                if ($this->refuseDefinitions && $collection->getId() === Database::METADATA) {
                    throw new RuntimeException('refused');
                }

                return parent::createDocument($collection, $document);
            }
        };
        $database = (new Database($adapter, new Cache(new MemoryCache())))
            ->setAuthorization(new Authorization())
            ->setDatabase('cleanup')
            ->setNamespace('cleanup_'.\uniqid());
        $database->create();
        $adapter->refuseDefinitions = true;

        $error = null;
        try {
            $database->createCollection(new Collection(
                id: 'logs',
                attributes: [Attribute::string(key: 'message', size: 64)],
            ));
        } catch (Throwable $thrown) {
            $error = $thrown;
        }

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertFalse($adapter->exists('cleanup', 'logs'), 'A table without a stored definition must be dropped');
        $this->assertTrue($database->getCollection('logs')->isEmpty());
    }

    public function testACleanupThatFailsKeepsTheOriginalErrorAndLogsTheCleanupFailure(): void
    {
        $indexFailure = new RuntimeException('the index could not be built');
        $adapter = new class (new PDO('sqlite::memory:'), $indexFailure) extends SQLite {
            public function __construct(PDO $pdo, private readonly RuntimeException $indexFailure)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = [], Event $event = Event::IndexCreate): bool
            {
                throw $this->indexFailure;
            }

            #[\Override]
            public function deleteCollection(string $id): bool
            {
                throw new RuntimeException('database is locked');
            }
        };
        $adapter->setDatabase('cleanup');
        $adapter->setNamespace('cleanup_'.\uniqid());

        $error = null;
        $log = StderrCapture::during(function () use ($adapter, &$error): void {
            try {
                $adapter->createCollection('logs', [Attribute::string(key: 'message', size: 64)], [
                    Index::key(key: 'byMessage', attributes: ['message']),
                ]);
            } catch (Throwable $caught) {
                $error = $caught;
            }
        });

        $this->assertSame($indexFailure, $error, 'the index failure reaches the caller, not the failed drop');
        $this->assertStringContainsString("Failed to rollback collection 'logs': database is locked", $log, 'the failed cleanup is logged');
    }
}
