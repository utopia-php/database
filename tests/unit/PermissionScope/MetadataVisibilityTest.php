<?php

namespace Tests\Unit\PermissionScope;

use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\PermissionFilter;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

/**
 * Collection definitions are documents of the metadata collection and are
 * listed, counted and found under their own read permissions.
 */
final class MetadataVisibilityTest extends TestCase
{
    public function testTenantlessDefinitionsStayReadableFromEveryTenantOfASharedPool(): void
    {
        $database = $this->database();
        $database->setSharedTables(true)->setTenant(null);
        $database->create();
        $database->createCollection(new Collection(id: 'pooled', permissions: [Permission::read(Role::any())]));

        $database->setTenant(1);
        $database->createCollection(new Collection(id: 'owned', permissions: [Permission::read(Role::any())]));

        $database->setTenant(990);
        $pooled = [Query::equal('$id', ['pooled'])];

        $this->assertSame(1, $database->count(Database::METADATA, $pooled), 'A tenantless definition carries tenantless permission rows');
        $this->assertSame(['pooled'], $this->ids($database->find(Database::METADATA, $pooled)));
        $this->assertSame(['pooled'], $this->ids($database->listCollections()), 'Another tenant\'s definition must stay invisible');
    }

    public function testMetadataPermissionSubqueryMatchesTenantlessRows(): void
    {
        $adapter = new MySQL($this->createStub(PDO::class));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(990);

        $hook = new ReflectionMethod(MySQL::class, 'newPermissionHook');

        $metadata = $hook->invoke($adapter, Database::METADATA, ['any']);
        $this->assertInstanceOf(PermissionFilter::class, $metadata);
        $this->assertStringContainsString(
            Storage::TENANT.' IS NULL',
            $metadata->filter('table_main')->expression,
            'The metadata permissions table holds tenantless rows for pooled definitions',
        );

        $collection = $hook->invoke($adapter, 'orders', ['any']);
        $this->assertInstanceOf(PermissionFilter::class, $collection);
        $this->assertStringNotContainsString(
            Storage::TENANT.' IS NULL',
            $collection->filter('table_main')->expression,
            'A project collection\'s permission rows must stay strictly tenanted',
        );
    }

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database
            ->setDatabase('metadata_visibility')
            ->setNamespace('metadata_visibility_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());

        return $database;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $documents);
        \sort($ids);

        return $ids;
    }
}
