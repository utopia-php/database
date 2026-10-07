<?php

namespace Tests\Unit\Documents;

use DomainException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\UncachedTwin;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class InvalidationRestoreFailureTest extends TestCase
{
    private const string COLLECTION = 'ledgers';

    private bool $failing = false;

    public function testAFailedWriteKeepsItsErrorWhenRestoringTheCacheEpochsFails(): void
    {
        $cache = $this->failingEpochCache();
        $queryCache = new QueryCache(new Cache(new MemoryCache()));
        $database = new Database(new Memory(), new Cache($cache));
        $database->setDatabase('restore')->setNamespace('restore_'.\uniqid());
        $database->setQueryCache($queryCache);
        $database->addHook($this->failingInvalidator($queryCache));
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'entry', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'kept', 'entry' => 'kept']));
        $this->assertSame('kept', $database->getDocument(self::COLLECTION, 'kept')->getAttribute('entry'));

        $failure = new DomainException('the write failed');
        try {
            $database->withTransaction(function () use ($database, $failure): never {
                $database->updateDocuments(self::COLLECTION, new Document(['entry' => 'rolled back']));
                $this->failing = true;

                throw $failure;
            });
        } catch (\Throwable $error) {
            $this->assertSame($failure, $error, 'the write\'s own error reaches the caller, not the failed restore');
        } finally {
            $this->failing = false;
        }

        $this->assertSame('kept', $database->getDocument(self::COLLECTION, 'kept')->getAttribute('entry'));

        UncachedTwin::of($database)->updateDocument(self::COLLECTION, 'kept', new Document(['entry' => 'changed']));
        $this->assertSame('changed', $database->getDocument(self::COLLECTION, 'kept')->getAttribute('entry'), 'a restore that failed leaves the document cache fail-closed');
    }

    private function failingEpochCache(): MemoryCache
    {
        $failing = fn (): bool => $this->failing;

        return new class ($failing) extends MemoryCache {
            public function __construct(private readonly \Closure $failing)
            {
            }

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if (($this->failing)() && \str_ends_with($key, '#epoch') && \is_string($data) && ! \str_starts_with($data, 'blocked:')) {
                    throw new RuntimeException('the cache refused the epoch');
                }

                return parent::save($key, $data, $hash);
            }
        };
    }

    private function failingInvalidator(QueryCache $queryCache): Invalidator
    {
        $failing = fn (): bool => $this->failing;

        return new class ($queryCache, $failing) extends Invalidator {
            public function __construct(QueryCache $queryCache, private readonly \Closure $failing)
            {
                parent::__construct($queryCache);
            }

            /**
             * @param  array<string, string>  $tokens
             */
            #[\Override]
            public function activate(array $tokens): void
            {
                if (($this->failing)()) {
                    throw new RuntimeException('the query cache refused the activation');
                }

                parent::activate($tokens);
            }
        };
    }
}
