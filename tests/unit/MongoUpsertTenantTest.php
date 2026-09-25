<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Database\Storage;
use Utopia\Mongo\Client;

final class MongoUpsertTenantTest extends TestCase
{
    private const string COLLECTION = 'orders';

    /**
     * @var array<int, array<string, mixed>>
     */
    private array $operations = [];

    public function testUpsertStampsAndMatchesTheDocumentsOwnTenant(): void
    {
        $adapter = $this->createAdapter(sharedTables: true, tenant: 6);

        $adapter->upsertDocuments($this->collection(), '', [$this->change('shared', tenant: 5)]);

        $this->assertSame(5, $this->filter(0)[Storage::TENANT]);
        $this->assertSame(5, $this->set(0)[Storage::TENANT]);
    }

    public function testUpsertFallsBackToTheSelectedTenant(): void
    {
        $adapter = $this->createAdapter(sharedTables: true, tenant: 3);

        $adapter->upsertDocuments($this->collection(), '', [$this->change('own', tenant: null)]);

        $this->assertSame(3, $this->filter(0)[Storage::TENANT]);
        $this->assertSame(3, $this->set(0)[Storage::TENANT]);
    }

    public function testUpsertKeepsEachDocumentUnderItsTenant(): void
    {
        $adapter = $this->createAdapter(sharedTables: true, tenant: null);

        $adapter->upsertDocuments($this->collection(), '', [
            $this->change('shared', tenant: 1),
            $this->change('shared', tenant: 2),
        ]);

        $this->assertSame([1, 2], [$this->filter(0)[Storage::TENANT], $this->filter(1)[Storage::TENANT]]);
    }

    public function testUpsertWithoutSharedTablesStampsNoTenant(): void
    {
        $adapter = $this->createAdapter(sharedTables: false, tenant: null);

        $adapter->upsertDocuments($this->collection(), '', [$this->change('single', tenant: null)]);

        $this->assertArrayNotHasKey(Storage::TENANT, $this->filter(0));
        $this->assertArrayNotHasKey(Storage::TENANT, $this->set(0));
    }

    /**
     * @return array<string, mixed>
     */
    private function filter(int $index): array
    {
        /** @var array<string, mixed> $filter */
        $filter = $this->operations[$index]['filter'];

        return $filter;
    }

    /**
     * @return array<string, mixed>
     */
    private function set(int $index): array
    {
        /** @var array<string, array<string, mixed>> $update */
        $update = $this->operations[$index]['update'];

        return $update['$set'];
    }

    private function collection(): Document
    {
        return new Document([Document::ID => self::COLLECTION]);
    }

    private function change(string $id, ?int $tenant): Change
    {
        $document = new Document([
            Document::ID => $id,
            Document::CREATED_AT => '2026-01-01 00:00:00.000',
            Document::UPDATED_AT => '2026-01-01 00:00:00.000',
            Document::PERMISSIONS => [],
            'name' => 'renamed',
        ]);

        if ($tenant !== null) {
            $document->setAttribute(Document::TENANT, $tenant);
        }

        return new Change(new Document(), $document);
    }

    private function createAdapter(bool $sharedTables, ?int $tenant): Mongo
    {
        $record = function (array $operations): void {
            /** @var array<int, array<string, mixed>> $typed */
            $typed = $operations;
            $this->operations = $typed;
        };

        $client = new class ($record) extends Client {
            /**
             * @param  Closure(array<int, array<string, mixed>>): void  $record
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
             * @param  array<int, array<string, mixed>>  $operations
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function upsert(string $collection, array $operations, array $options = []): int
            {
                ($this->record)($operations);

                return \count($operations);
            }
        };

        $adapter = new Mongo($client);
        $adapter->setNamespace('tenants');
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant($tenant);

        return $adapter;
    }
}
