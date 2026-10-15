<?php

namespace Tests\Unit\PermissionScope;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

/**
 * Collection definitions are documents of the metadata collection and are
 * listed, counted and found under their own read permissions.
 */
final class MetadataVisibilityTest extends TestCase
{
    public function testListCollectionsReturnsOnlyReadableDefinitions(): void
    {
        $database = $this->database();
        $database->create();
        $database->createCollection(Collection::create(id: 'public', permissions: [Permission::read(Role::any())]));
        $database->createCollection(Collection::create(id: 'private', permissions: [Permission::read(Role::user('admin'))]));
        $database->createCollection(Collection::create(id: 'unlisted', permissions: [Permission::create(Role::any())]));

        $authorization = $database->getAuthorization();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        $this->assertSame(['public'], $this->ids($database->listCollections()));
        $this->assertSame(['public'], $this->ids($database->find(Database::METADATA)));
        $this->assertSame(1, $database->count(Database::METADATA), 'count() and find() must agree on the metadata collection');

        $authorization->addRole(Role::user('admin')->toString());

        $this->assertSame(['private', 'public'], $this->ids($database->listCollections()));
        $this->assertSame(2, $database->count(Database::METADATA));

        $everything = $authorization->skip(fn (): array => $database->listCollections());
        $this->assertSame(['private', 'public', 'unlisted'], $this->ids($everything));
    }

    public function testTenantlessDefinitionsStayReadableFromEveryTenantOfASharedPool(): void
    {
        $database = $this->database();
        $database->setSharedTables(true)->setTenant(null);
        $database->create();
        $database->createCollection(Collection::create(id: 'pooled', permissions: [Permission::read(Role::any())]));

        $database->setTenant(1);
        $database->createCollection(Collection::create(id: 'owned', permissions: [Permission::read(Role::any())]));

        $database->setTenant(990);
        $pooled = [Query::equal('$id', ['pooled'])];

        $this->assertSame(1, $database->count(Database::METADATA, $pooled), 'A tenantless definition carries tenantless permission rows');
        $this->assertSame(['pooled'], $this->ids($database->find(Database::METADATA, $pooled)));
        $this->assertSame(['pooled'], $this->ids($database->listCollections()), 'Another tenant\'s definition must stay invisible');
    }

    public function testMetadataPermissionSubqueryMatchesTenantlessRows(): void
    {
        $this->assertStringContainsString(
            Storage::TENANT.' IS NULL',
            $this->permissionSubquery(new Document(['$id' => Database::METADATA])),
            'The metadata permissions table holds tenantless rows for pooled definitions',
        );

        $this->assertStringNotContainsString(
            Storage::TENANT.' IS NULL',
            $this->permissionSubquery(new Document(['$id' => 'orders', 'documentSecurity' => true])),
            'A project collection\'s permission rows must stay strictly tenanted',
        );
    }

    private function permissionSubquery(Document $collection): string
    {
        $statement = self::createStub(\PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);

        $sql = '';
        $pdo = self::createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
            $sql = $query;

            return $statement;
        });

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(990);
        $adapter->setAuthorization(new Authorization());

        $adapter->find($collection);

        $subquery = \strpos($sql, '_perms`');
        $this->assertNotFalse($subquery, $sql);

        return \substr($sql, $subquery);
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
