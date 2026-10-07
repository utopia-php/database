<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
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
        $adapter->setSchemaless(! $lane->getAdapter()->supports(Capability::DefinedAttributes));

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

        $database->createCollection(Collection::create(
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

        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [
                Attribute::string(key: 'a.b', size: 16),
                Attribute::string(key: 'x.y', size: 16),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument($collection, new Document(['$id' => 'first', 'a.b' => 'renamed', 'x.y' => 'deleted']));

        $database->updateAttribute($collection, 'a.b', new AttributeUpdate(key: 'c'));
        $this->assertSame('renamed', $database->getDocument($collection, 'first')->getAttribute('c'));

        $database->deleteAttribute($collection, 'x.y');
        $database->createAttribute($collection, Attribute::string(key: 'x.y', size: 16));
        $this->assertNull($database->getDocument($collection, 'first')->getAttribute('x.y'));

        $database->deleteCollection($collection);
    }

    public function testOrderRandomIsRejectedAsAQueryError(): void
    {
        $database = $this->getDatabase();
        $collection = $this->createNamesCollection($database, ['foobar']);

        try {
            foreach ([
                fn (): array => $database->find($collection, [Query::orderRandom()]),
                fn (): array => $database->skipValidation(fn (): array => $database->find($collection, [Query::orderRandom()])),
            ] as $find) {
                try {
                    $find();
                    $this->fail('orderRandom() must be rejected as a query error where the adapter cannot order by random');
                } catch (QueryException $e) {
                    $this->assertStringContainsString('Random order is not supported', $e->getMessage());
                }
            }
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testSumOnADottedAttributeMatchesTheCountedRows(): void
    {
        $database = $this->getDatabase();
        $collection = 'dotted_sum_'.\uniqid();

        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [
                Attribute::integer(key: 'score.value'),
                Attribute::string(key: 'group.name', size: 16),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        foreach ([[1, 'a'], [2, 'a'], [4, 'b']] as [$score, $group]) {
            $database->createDocument($collection, new Document(['score.value' => $score, 'group.name' => $group]));
        }

        $queries = [Query::equal('group.name', ['a'])];

        $this->assertSame(2, $database->count($collection, $queries));
        $this->assertSame(3, $database->sum($collection, 'score.value', $queries));
        $this->assertSame(7, $database->sum($collection, 'score.value'));

        $database->deleteCollection($collection);
    }

    /**
     * @param  list<string>  $names
     */
    private function createNamesCollection(Database $database, array $names): string
    {
        $collection = 'names_'.\uniqid();

        $database->createCollection(Collection::create(
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

    public function testFiltersMatchALiteralDollarWord(): void
    {
        $database = $this->getDatabase();
        $collection = 'dollar_words';
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::string(key: 'label', size: 64, required: true)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        try {
            foreach (['lead' => '$USD 10', 'tail' => '10 $USD', 'plain' => 'plain'] as $id => $label) {
                $database->createDocument($collection, new Document(['$id' => $id, 'label' => $label]));
            }

            $idsOf = function (Query $query) use ($database, $collection): array {
                $ids = \array_map(fn (Document $document): string => $document->getId(), $database->find($collection, [$query]));
                \sort($ids);

                return $ids;
            };

            $this->assertSame(['lead', 'tail'], $idsOf(Query::contains('label', ['$USD'])));
            $this->assertSame(['plain'], $idsOf(Query::notContains('label', ['$USD'])));
            $this->assertSame(['plain', 'tail'], $idsOf(Query::notStartsWith('label', '$USD')));
            $this->assertSame(['lead', 'plain'], $idsOf(Query::notEndsWith('label', '$USD')));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testContainsFamilyMatchesLikeTheOtherEngines(): void
    {
        $database = $this->getDatabase();
        $collection = 'contains_family';
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [
                Attribute::string(key: 'name', size: 64, required: true),
                Attribute::string(key: 'tags', size: 32, required: false, array: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        try {
            $documents = [
                'captain' => ['name' => 'Captain America', 'tags' => ['comics', 'action']],
                'work' => ['name' => 'Work in Progress', 'tags' => ['drama']],
                'kids' => ['name' => 'Frozen', 'tags' => ['kids']],
                'untagged' => ['name' => 'Untitled', 'tags' => []],
            ];
            foreach ($documents as $id => $attributes) {
                $database->createDocument($collection, new Document(['$id' => $id, ...$attributes]));
            }

            $idsOf = function (Query $query) use ($database, $collection): array {
                $ids = \array_map(fn (Document $document): string => $document->getId(), $database->find($collection, [$query]));
                \sort($ids);

                return $ids;
            };

            $this->assertSame(['captain', 'kids'], $idsOf(Query::contains('tags', ['comics', 'kids'])));
            $this->assertSame(['captain', 'kids'], $idsOf(Query::containsAny('tags', ['comics', 'kids'])));
            $this->assertSame(['kids', 'untagged', 'work'], $idsOf(Query::notContains('tags', ['comics'])));
            $this->assertSame(['captain', 'work'], $idsOf(Query::contains('name', ['Captain', 'Work'])));
            $this->assertSame(['captain', 'work'], $idsOf(Query::containsAny('name', ['Captain', 'Work'])));
            $this->assertSame(['kids', 'untagged', 'work'], $idsOf(Query::notContains('name', ['Captain'])));
            $this->assertSame(['kids', 'untagged'], $database->skipValidation(fn (): array => $idsOf(Query::notEqual('name', ['Captain America', 'Work in Progress']))));
        } finally {
            $database->deleteCollection($collection);
        }
    }
}
