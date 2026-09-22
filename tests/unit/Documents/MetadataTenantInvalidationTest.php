<?php

namespace Tests\Unit\Documents;

use Override;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

/**
 * A collection definition is the one row tenant-per-document lets through with
 * no tenant, so purging "under the document's tenant" aimed every _metadata
 * invalidation at a tenant-less cache key while every reader resolves the same
 * entry under the adapter's tenant. Nothing the writer did could reach what the
 * readers held.
 */
final class MetadataTenantInvalidationTest extends TestCase
{
    private const TENANT = 7;

    public function testCreatingACollectionClearsAMissCachedUnderTheAdapterTenant(): void
    {
        [$writer, $reader] = $this->databases();

        $this->assertTrue(
            $reader->getCollection('targets')->isEmpty(),
            'The collection does not exist yet, so the read must miss',
        );

        $writer->createCollection(new Collection(id: 'targets', attributes: [
            Attribute::string(key: 'name', size: 64),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        $this->assertFalse(
            $reader->getCollection('targets')->isEmpty(),
            'A miss cached before provisioning must not outlive the collection being created',
        );
    }

    public function testACollectionIsReadableByItsOwnWriterAfterCreation(): void
    {
        [$writer] = $this->databases();

        $writer->createCollection(new Collection(id: 'targets', attributes: [
            Attribute::string(key: 'name', size: 64),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        $this->assertCount(
            1,
            $this->attributesOf($writer),
            'createCollection() reads the collection first, and that miss must not survive the create',
        );

        $writer->createAttribute('targets', Attribute::string(key: 'provider', size: 64));

        $this->assertCount(
            2,
            $this->attributesOf($writer),
            'A schema change must not be hidden by a stale collection definition',
        );
    }

    /**
     * @return array<mixed>
     */
    private function attributesOf(Database $database): array
    {
        $attributes = $database->getCollection('targets')->getAttribute('attributes', []);
        $this->assertIsArray($attributes);

        return $attributes;
    }

    /**
     * @return array{Database, Database}
     */
    private function databases(): array
    {
        $adapter = new DatabaseMemory();
        $cache = new Cache(new LeasedMemoryCacheAdapter());
        $namespace = 'metadata_tenant_'.\uniqid();

        $databases = [];
        foreach ([0, 1] as $ignored) {
            $database = new Database($adapter, $cache);
            $database
                ->setDatabase('utopiaTests')
                ->setNamespace($namespace)
                ->setSharedTables(true)
                ->setTenantPerDocument(true)
                ->setTenant(self::TENANT);
            $database->getAuthorization()->addRole(Role::any()->toString());
            $databases[] = $database;
        }

        $databases[0]->create();

        return [$databases[0], $databases[1]];
    }
}

final class LeasedMemoryCacheAdapter extends MemoryCache implements Leasable
{
    /** @var array<string, int> */
    private array $generations = [];

    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    /**
     * @param  array<int|string, mixed>|string  $data
     * @return bool|string|array<int|string, mixed>
     */
    #[Override]
    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    #[Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;

        return parent::purge($key, $hash);
    }

    #[Override]
    public function flush(): bool
    {
        $this->generations = [];

        return parent::flush();
    }
}
