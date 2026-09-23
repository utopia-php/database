<?php

namespace Tests\E2E\Adapter;

use Exception;
use Redis;
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
    public static ?Database $database = null;

    protected static string $namespace;

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
        $redis->select(4);
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
        $database->getAdapter()->setSupportForAttributes(true);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($schema)
            ->setNamespace(static::$namespace = 'myapp_'.uniqid());

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
        // Mongo creates databases on the fly, so exists would always pass. So we override this test to remove the exists check.
        $this->assertTrue($this->getDatabase()->create());
        $this->assertTrue($this->getDatabase()->delete($this->testDatabase));
        $this->assertTrue($this->getDatabase()->create());
        $this->assertSame($this->getDatabase(), $this->getDatabase()->setDatabase($this->testDatabase));
    }

    public function testCollectionGrantsAuthorizeWritesWithoutReadPermission(): void
    {
        $database = $this->getDatabase();
        $collection = 'collectionGrantedWrites';

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [Attribute::integer(key: 'count', required: true)],
            permissions: [
                Permission::read(Role::any()),
                Permission::update(Role::users()),
                Permission::delete(Role::users()),
            ],
            documentSecurity: false,
        ));

        $database->getAuthorization()->skip(function () use ($database, $collection): void {
            foreach (['first', 'second', 'third'] as $id) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    '$permissions' => [],
                    'count' => 0,
                ]));
            }
        });

        $this->actAs('bob', function () use ($database, $collection): void {
            $database->increaseDocumentAttribute($collection, 'first', 'count', 5);
            $this->assertSame(5, $database->getDocument($collection, 'first')->getAttribute('count'));

            $database->decreaseDocumentAttribute($collection, 'first', 'count', 2);
            $this->assertSame(3, $database->getDocument($collection, 'first')->getAttribute('count'));

            $this->assertSame(3, $database->updateDocuments($collection, new Document(['count' => 42])));
            $this->assertSame(
                [42, 42, 42],
                \array_map(fn (Document $document) => $document->getAttribute('count'), $database->find($collection)),
            );

            $this->assertSame(3, $database->deleteDocuments($collection));
            $this->assertSame(0, $database->count($collection));
        });
    }

    public function testDocumentGrantsAuthorizeWritesWithoutReadPermission(): void
    {
        $database = $this->getDatabase();
        $collection = 'documentGrantedWrites';

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [Attribute::integer(key: 'count', required: true)],
            permissions: [],
            documentSecurity: true,
        ));

        $database->getAuthorization()->skip(function () use ($database, $collection): void {
            foreach (['first', 'second'] as $id) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    '$permissions' => [
                        Permission::update(Role::user('bob')),
                        Permission::delete(Role::user('bob')),
                    ],
                    'count' => 0,
                ]));
            }
        });

        $stored = fn (): array => $database->getAuthorization()->skip(fn () => \array_map(
            fn (Document $document) => $document->getAttribute('count'),
            $database->find($collection),
        ));

        $this->actAs('bob', fn () => $database->increaseDocumentAttribute($collection, 'first', 'count', 5));
        $this->assertSame([5, 0], $stored());

        $this->actAs('bob', fn () => $this->assertSame(2, $database->updateDocuments($collection, new Document(['count' => 42]))));
        $this->assertSame([42, 42], $stored());

        $this->actAs('bob', fn () => $this->assertSame(2, $database->deleteDocuments($collection)));
        $this->assertSame([], $stored());
    }

    public function testDeniedReaderDoesNotHideADocumentFromItsReader(): void
    {
        $database = $this->getDatabase();
        $collection = 'deniedReaderProfiles';

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [Permission::read(Role::user('alice'))],
            documentSecurity: false,
        ));

        $database->getAuthorization()->skip(fn () => $database->createDocument($collection, new Document([
            '$id' => 'alice',
            '$permissions' => [Permission::read(Role::user('alice'))],
            'name' => 'Alice',
        ])));

        $this->actAs('bob', fn () => $this->assertTrue($database->getDocument($collection, 'alice')->isEmpty()));

        $this->actAs('alice', fn () => $this->assertSame(
            'Alice',
            $database->getDocument($collection, 'alice')->getAttribute('name'),
            'A reader denied the document must not leave a negative cache entry for a reader who may see it',
        ));
    }

    public function testListCollectionsReturnsOnlyReadableDefinitions(): void
    {
        $database = $this->getDatabase();
        $definitions = ['adminDefinition', 'listedDefinition', 'unlistedDefinition'];

        $database->createCollection(new Collection(id: 'listedDefinition', permissions: [Permission::read(Role::any())]));
        $database->createCollection(new Collection(id: 'adminDefinition', permissions: [Permission::read(Role::user('admin'))]));
        $database->createCollection(new Collection(id: 'unlistedDefinition', permissions: [Permission::create(Role::any())]));

        $listed = fn (): array => \array_values(\array_intersect(
            $definitions,
            \array_map(fn (Collection $collection) => $collection->getId(), $database->listCollections(100)),
        ));
        $queries = [Query::equal('$id', $definitions)];

        $this->actAs('bob', function () use ($database, $listed, $queries): void {
            $this->assertSame(['listedDefinition'], $listed());
            $this->assertCount(1, $database->find(Database::METADATA, $queries));
            $this->assertSame(1, $database->count(Database::METADATA, $queries), 'count() and find() must agree on the metadata collection');
        });

        $this->actAs('admin', function () use ($database, $listed, $queries): void {
            $this->assertSame(['adminDefinition', 'listedDefinition'], $listed());
            $this->assertSame(2, $database->count(Database::METADATA, $queries));
        });

        $this->assertSame($definitions, $database->getAuthorization()->skip($listed));
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }

    private function actAs(string $user, callable $callback): void
    {
        $authorization = $this->getDatabase()->getAuthorization();
        $roles = $authorization->getRoles();

        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());
        $authorization->addRole(Role::users()->toString());
        $authorization->addRole(Role::user($user)->toString());

        try {
            $callback();
        } finally {
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }
        }
    }
}
