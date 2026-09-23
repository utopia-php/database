<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

/**
 * Base registers Hook\Permissions on every lane's shared Database, so these tests build their own
 * Database without it: MongoDB stores permissions on the document and must filter by them regardless.
 */
trait MongoReadFilterTests
{
    public function testReadsWithoutThePermissionsHookReturnOnlyReadableDocuments(): void
    {
        $database = $this->createDatabaseWithoutThePermissionsHook();
        $collection = $this->createAliceOnlyCollection($database);

        $this->assumeRolesOf($database, 'bob');
        $this->assertSame([], $database->find($collection));
        $this->assertSame(0, $database->count($collection));
        $this->assertSame(0, $database->sum($collection, 'count'));

        $this->assumeRolesOf($database, 'alice');
        $this->assertSame(['alice'], \array_map(fn (Document $document) => $document->getId(), $database->find($collection)));
        $this->assertSame(1, $database->count($collection));
        $this->assertSame(5, $database->sum($collection, 'count'));
    }

    public function testBulkWritesWithoutThePermissionsHookSkipDocumentsTheCallerCannotChange(): void
    {
        $database = $this->createDatabaseWithoutThePermissionsHook();
        $collection = $this->createAliceOnlyCollection($database);

        $this->assumeRolesOf($database, 'bob');
        $this->assertSame(0, $database->updateDocuments($collection, new Document(['count' => 42])));
        $this->assertSame(0, $database->deleteDocuments($collection));

        $this->assertSame(
            [5],
            $database->getAuthorization()->skip(fn (): array => \array_map(
                fn (Document $document) => $document->getAttribute('count'),
                $database->find($collection),
            )),
        );
    }

    private function createDatabaseWithoutThePermissionsHook(): Database
    {
        $lane = $this->getDatabase();

        $adapter = new Mongo(new Client($this->testDatabase, 'mongo', 27017, 'root', 'password', false));
        $adapter->setSupportForAttributes($lane->getAdapter()->supports(Capability::DefinedAttributes));

        $database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization(new Authorization())
            ->setDatabase($this->testDatabase)
            ->setSharedTables($lane->getSharedTables())
            ->setTenant($lane->getTenant())
            ->setNamespace('unhooked_'.\uniqid());

        $database->create();

        $this->assertFalse($adapter->hasPermissionHook());

        return $database;
    }

    private function createAliceOnlyCollection(Database $database): string
    {
        $collection = 'aliceOnly';

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [Attribute::integer(key: 'count', required: true)],
            permissions: [],
            documentSecurity: true,
        ));

        $database->getAuthorization()->skip(fn () => $database->createDocument($collection, new Document([
            '$id' => 'alice',
            '$permissions' => [
                Permission::read(Role::user('alice')),
                Permission::update(Role::user('alice')),
                Permission::delete(Role::user('alice')),
            ],
            'count' => 5,
        ])));

        return $collection;
    }

    private function assumeRolesOf(Database $database, string $user): void
    {
        $authorization = $database->getAuthorization();

        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());
        $authorization->addRole(Role::users()->toString());
        $authorization->addRole(Role::user($user)->toString());
    }
}
