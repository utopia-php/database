<?php

namespace Tests\E2E\Adapter\SharedTables;

use Exception;
use Redis;
use Tests\E2E\Adapter\Base;
use Tests\E2E\Adapter\Scopes\MongoReadFilterTests;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Mongo\Client;

class MongoDBTest extends Base
{
    use MongoReadFilterTests;

    public static ?Database $database = null;

    protected static string $namespace;

    /**
     * @var array<string, array<int, string|null>>
     */
    private array $emittedSequences = [];

    /**
     * Return name of adapter
     */
    public static function getAdapterName(): string
    {
        return 'mongodb';
    }

    /**
     * @throws Exception
     */
    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(11);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $schema = $this->testDatabase;
        $client = new Client(
            $schema,
            'mongo',
            27017,
            'root',
            'password',
            false
        );

        $database = new Database(new Mongo($client), $cache);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($schema)
            ->setSharedTables(true)
            ->setTenant(999)
            ->setNamespace(static::$namespace = 'st_'.static::getTestToken());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    /**
     * @throws Exception
     */
    public function testCreateExistsDelete(): void
    {
        $database = $this->getDatabase();

        $this->assertTrue($database->create());
        $this->assertTrue($database->exists($this->testDatabase));
        $this->assertFalse($database->exists($this->testDatabase.'Absent'));
        $this->assertTrue($database->delete($this->testDatabase));
        $this->assertFalse($database->exists($this->testDatabase));
        $this->assertTrue($database->create());
        $this->assertTrue($database->exists($this->testDatabase));
        $this->assertSame($database, $database->setDatabase($this->testDatabase));
    }

    public function testSkipDuplicatesKeepsEachTenantsSequence(): void
    {
        $database = $this->getDatabase();
        $tenant = $database->getTenant();
        $tenantPerDocument = $database->getTenantPerDocument();
        $collection = 'tenantSequences';

        $documents = fn (string $id): array => [
            new Document(['$id' => $id, '$tenant' => 1, 'name' => 'tenant one']),
            new Document(['$id' => $id, '$tenant' => 2, 'name' => 'tenant two']),
        ];

        try {
            $database->setTenant(null)->setTenantPerDocument(true);

            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64, required: true)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                ],
                documentSecurity: false,
            ));

            $database->createDocuments($collection, $documents('existing'));

            foreach (['existing', 'inserted'] as $id) {
                $database
                    ->setTenant(null)
                    ->setTenantPerDocument(true)
                    ->skipDuplicates(fn () => $database->createDocuments(
                        $collection,
                        $documents($id),
                        onNext: function (Document $document): void {
                            $this->emittedSequences[$document->getId()][(int) $document->getTenant()] = $document->getSequence();
                        },
                    ));

                foreach ([1 => 'tenant one', 2 => 'tenant two'] as $documentTenant => $name) {
                    $stored = $database
                        ->setTenantPerDocument(false)
                        ->setTenant($documentTenant)
                        ->getDocument($collection, $id);

                    $this->assertSame($name, $stored->getAttribute('name'));
                    $this->assertNotEmpty($stored->getSequence());
                    $this->assertSame(
                        $id === 'existing' ? null : $stored->getSequence(),
                        $this->emittedSequences[$id][$documentTenant] ?? null,
                        "Tenant {$documentTenant}'s {$id} document must carry its own \$sequence, and a skipped one is not emitted",
                    );
                }
            }
        } finally {
            $database->setTenant($tenant)->setTenantPerDocument($tenantPerDocument);
        }
    }

    public function testPooledDefinitionsAreListedUnderTheirReadPermissions(): void
    {
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();
        $tenant = $database->getTenant();
        $roles = $authorization->getRoles();

        try {
            $database->setTenant(null);
            $database->createCollection(Collection::create(id: 'pooledDefinition', permissions: [Permission::read(Role::any())]));
            $database->createCollection(Collection::create(id: 'pooledAdminDefinition', permissions: [Permission::read(Role::user('admin'))]));

            $database->setTenant(1);
            $database->createCollection(Collection::create(id: 'ownedDefinition', permissions: [Permission::read(Role::any())]));

            $database->setTenant(990);
            $authorization->cleanRoles();
            $authorization->addRole(Role::any()->toString());
            $queries = [Query::equal('$id', ['pooledDefinition', 'pooledAdminDefinition', 'ownedDefinition'])];

            $this->assertSame(
                ['pooledDefinition'],
                \array_map(fn (Document $definition) => $definition->getId(), $database->find(Database::METADATA, $queries)),
            );
            $this->assertSame(1, $database->count(Database::METADATA, $queries));
        } finally {
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }
            $database->setTenant($tenant);
        }
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }
}
