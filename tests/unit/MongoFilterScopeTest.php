<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PermissionType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoFilterScopeTest extends TestCase
{
    private const int TENANT = 7;

    private const string COLLECTION = 'counters';

    /**
     * @var array<string, list<array<mixed>>>
     */
    private array $filters = [];

    private Authorization $authorization;

    public function testWritesScopeByTenantWithoutReadPermission(): void
    {
        $adapter = $this->createAdapter();
        $collection = new Document(['$id' => self::COLLECTION]);

        $adapter->updateDocument($collection, 'first', new Document(['count' => 1]), true);
        $adapter->updateDocuments($collection, new Document(['count' => 42]), [
            new Document(['$id' => 'first', '$sequence' => 'sequence-first']),
        ]);
        $adapter->upsertDocuments($collection, '', [
            new Change(new Document(), new Document([
                '$id' => 'second',
                '$createdAt' => '2026-01-01T00:00:00.000+00:00',
                '$updatedAt' => '2026-01-01T00:00:00.000+00:00',
                '$permissions' => [],
                'count' => 1,
            ])),
        ]);
        $adapter->deleteDocument(self::COLLECTION, 'first');
        $adapter->deleteDocuments(self::COLLECTION, ['sequence-first'], []);

        $this->assertCount(2, $this->filters['update'] ?? []);
        $this->assertCount(1, $this->filters['upsert'] ?? []);
        $this->assertCount(2, $this->filters['delete'] ?? []);
        $this->assertTenantScopeOnly(['update', 'upsert', 'delete']);
    }

    #[RequiresPhpExtension('mongodb')]
    public function testIncrementsScopeByTenantWithoutReadPermission(): void
    {
        $adapter = $this->createAdapter();

        $adapter->increaseDocumentAttribute(self::COLLECTION, 'first', 'count', 5, '2026-01-01 00:00:00.000');
        $adapter->increaseDocumentAttribute(self::COLLECTION, 'first', 'count', -2, '2026-01-01 00:00:00.000', min: 0);

        $this->assertCount(2, $this->filters['update'] ?? []);
        $this->assertTenantScopeOnly(['update']);
    }

    public function testGetDocumentScopesByTenantWithoutReadPermission(): void
    {
        $adapter = $this->createAdapter();

        $adapter->getDocument(new Document(['$id' => self::COLLECTION]), 'first');

        $this->assertSame(
            [[Storage::UID => 'first', Storage::TENANT => self::TENANT]],
            $this->filters['find'] ?? [],
        );
    }

    public function testFindFiltersByTheRequestedPermission(): void
    {
        $adapter = $this->createAdapter();

        $adapter->find(new Document(['$id' => self::COLLECTION]), forPermission: PermissionType::Update);

        $filters = $this->filters['find'][0] ?? [];
        $this->assertSame(self::TENANT, $filters[Storage::TENANT] ?? null);
        $this->assertSame(
            ['$in' => ['update("any")', 'update("users")', 'update("user:bob")']],
            $filters[Storage::PERMISSIONS] ?? null,
        );
    }

    public function testCountAndSumFilterByReadPermission(): void
    {
        $adapter = $this->createAdapter();
        $collection = new Document(['$id' => self::COLLECTION]);

        $adapter->count($collection);
        $adapter->sum($collection, 'count');

        $this->assertCount(2, $this->filters['aggregate'] ?? []);
        foreach ($this->filters['aggregate'] as $filters) {
            $this->assertSame(self::TENANT, $filters[Storage::TENANT] ?? null);
            $this->assertSame(
                ['$in' => ['read("any")', 'read("users")', 'read("user:bob")']],
                $filters[Storage::PERMISSIONS] ?? null,
            );
        }
    }

    public function testMetadataReadsFilterDefinitionsByReadPermission(): void
    {
        $adapter = $this->createAdapter();
        $metadata = new Document(['$id' => Database::METADATA]);

        $adapter->find($metadata);
        $adapter->count($metadata);
        $adapter->sum($metadata, 'count');

        $reads = [...($this->filters['find'] ?? []), ...($this->filters['aggregate'] ?? [])];
        $this->assertCount(3, $reads);
        foreach ($reads as $filters) {
            $this->assertSame(['$in' => [self::TENANT, null]], $filters[Storage::TENANT] ?? null);
            $this->assertSame(
                ['$in' => ['read("any")', 'read("users")', 'read("user:bob")']],
                $filters[Storage::PERMISSIONS] ?? null,
                'Collection definitions are listed under their own read permissions',
            );
        }
    }

    public function testReadsSkipThePermissionFilterWhileAuthorizationIsDisabled(): void
    {
        $adapter = $this->createAdapter();
        $collection = new Document(['$id' => self::COLLECTION]);

        $this->authorization->skip(function () use ($adapter, $collection): void {
            $adapter->find($collection);
            $adapter->count($collection);
        });

        $this->assertArrayNotHasKey(Storage::PERMISSIONS, $this->filters['find'][0] ?? []);
        $this->assertArrayNotHasKey(Storage::PERMISSIONS, $this->filters['aggregate'][0] ?? []);
    }

    /**
     * @param  list<string>  $operations
     */
    private function assertTenantScopeOnly(array $operations): void
    {
        foreach ($operations as $operation) {
            foreach ($this->filters[$operation] ?? [] as $filters) {
                $this->assertArrayNotHasKey(
                    Storage::PERMISSIONS,
                    $filters,
                    "The {$operation} filter must not require read permission: Database authorizes writes before they reach the adapter",
                );
                $this->assertSame(self::TENANT, $filters[Storage::TENANT] ?? null, "The {$operation} filter must stay inside the tenant");
            }
        }
    }

    private function createAdapter(): Mongo
    {
        $record = function (string $operation, array $filters): void {
            $this->filters[$operation][] = $filters;
        };

        $client = new class ($record) extends Client {
            /**
             * @param  Closure(string, array<mixed>): void  $record
             */
            public function __construct(private readonly Closure $record)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                ($this->record)('find', $filters);

                return (object) ['cursor' => (object) ['firstBatch' => [], 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $where
             * @param  array<mixed>  $updates
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): int
            {
                ($this->record)('update', $where);

                return 1;
            }

            /**
             * @param  array<mixed>  $operations
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function upsert(string $collection, array $operations, array $options = []): int
            {
                foreach ($operations as $operation) {
                    if (\is_array($operation) && \is_array($operation['filter'] ?? null)) {
                        ($this->record)('upsert', $operation['filter']);
                    }
                }

                return \count($operations);
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $deleteOptions
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function delete(string $collection, array $filters = [], int $limit = 1, array $deleteOptions = [], array $options = []): int
            {
                ($this->record)('delete', $filters);

                return 1;
            }

            /**
             * @param  array<mixed>  $pipeline
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function aggregate(string $collection, array $pipeline, array $options = []): stdClass
            {
                $stage = $pipeline[0] ?? null;
                $match = \is_array($stage) ? ($stage['$match'] ?? null) : null;
                ($this->record)('aggregate', \is_array($match) || $match instanceof stdClass ? (array) $match : []);

                return (object) ['cursor' => (object) ['firstBatch' => []]];
            }
        };

        $this->authorization = new Authorization();
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::any()->toString());
        $this->authorization->addRole(Role::users()->toString());
        $this->authorization->addRole(Role::user('bob')->toString());

        $adapter = new Mongo($client);
        $adapter->setAuthorization($this->authorization);
        $adapter->setNamespace('scope');
        $adapter->setSharedTables(true);
        $adapter->setTenant(self::TENANT);
        $adapter->addWriteHook(new Permissions());

        return $adapter;
    }
}
