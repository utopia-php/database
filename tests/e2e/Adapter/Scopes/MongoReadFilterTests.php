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
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;

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

    public function testStartsWithAndEndsWithAreAnchored(): void
    {
        $database = $this->getDatabase();
        $collection = $this->createNamesCollection($database, ['foobar', 'barfoo', 'Foobar', 'barfoobar']);

        $this->assertSame(['foobar'], $this->namesOf($database->find($collection, [Query::startsWith('name', 'foo')])));
        $this->assertSame(['barfoo'], $this->namesOf($database->find($collection, [Query::endsWith('name', 'foo')])));
        $this->assertSame(1, $database->count($collection, [Query::startsWith('name', 'foo')]));

        $database->deleteCollection($collection);
    }

    public function testContainsAllWorksOnFind(): void
    {
        $database = $this->getDatabase();
        $collection = $this->createNamesCollection($database, ['foobar', 'barfoo', 'foobaz']);

        $this->assertSame(['barfoo', 'foobar'], $this->namesOf($database->find($collection, [Query::containsAll('tags', ['foo', 'bar'])])));
        $this->assertSame(2, $database->count($collection, [Query::containsAll('tags', ['foo', 'bar'])]));

        $database->deleteCollection($collection);
    }

    public function testCountReportsDriverErrors(): void
    {
        $database = $this->getDatabase();
        $collection = $this->createNamesCollection($database, ['foobar']);

        try {
            $database->getAdapter()->count($database->getCollection($collection), [Query::regex('name', '(')]);
            $this->fail('count() must report the driver error for an invalid regular expression instead of 0');
        } catch (MongoException $e) {
            $this->assertNotSame(0, $e->getCode());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testDottedAttributesSurviveRenameAndDelete(): void
    {
        $database = $this->getDatabase();
        $collection = 'dotted_'.\uniqid();

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [
                Attribute::string(key: 'a.b', size: 16),
                Attribute::string(key: 'x.y', size: 16),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument($collection, new Document(['$id' => 'first', 'a.b' => 'renamed', 'x.y' => 'deleted']));

        $database->updateAttribute($collection, 'a.b', newKey: 'c');
        $this->assertSame('renamed', $database->getDocument($collection, 'first')->getAttribute('c'));

        $database->deleteAttribute($collection, 'x.y');
        $database->createAttribute($collection, Attribute::string(key: 'x.y', size: 16));
        $this->assertNull($database->getDocument($collection, 'first')->getAttribute('x.y'));

        $database->deleteCollection($collection);
    }

    /**
     * @param  list<string>  $names
     */
    private function createNamesCollection(Database $database, array $names): string
    {
        $collection = 'names_'.\uniqid();

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::string(key: 'tags', size: 16, array: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        foreach ($names as $name) {
            $database->createDocument($collection, new Document(['name' => $name, 'tags' => \str_split($name, 3)]));
        }

        return $collection;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function namesOf(array $documents): array
    {
        $names = [];
        foreach ($documents as $document) {
            $name = $document->getAttribute('name');
            $this->assertIsString($name);
            $names[] = $name;
        }
        \sort($names);

        return $names;
    }
}
