<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class DottedAttributeKeyTest extends TestCase
{
    private const string PEOPLE = 'people';

    private const string ORDERS = 'orders';

    public function testCountMatchesFindForAFilterOnADottedKey(): void
    {
        $database = $this->database();
        $queries = [Query::equal('dots.name', ['v'])];

        $this->assertSame(2, \count($database->find(self::PEOPLE, $queries)));
        $this->assertSame(2, $database->count(self::PEOPLE, $queries));
        $this->assertSame(2, $database->count(self::PEOPLE, $queries, 10));
        $this->assertSame(1, $database->count(self::PEOPLE, $queries, 1));
        $this->assertSame(0, $database->count(self::PEOPLE, [Query::equal('dots.name', ['missing'])]));
    }

    public function testCountReadsADottedKeyInsideALogicalGroup(): void
    {
        $database = $this->database();
        $queries = [Query::or([Query::equal('dots.name', ['w']), Query::greaterThan('dots.score', 2)])];

        $this->assertSame(['b', 'c'], $this->ids($database->find(self::PEOPLE, $queries)));
        $this->assertSame(2, $database->count(self::PEOPLE, $queries));
    }

    public function testCountWithAnOrderOnADottedKey(): void
    {
        $database = $this->database();

        $this->assertSame(3, $database->count(self::PEOPLE, [
            Query::isNotNull('dots.name'),
            Query::orderDesc('dots.score'),
        ]));
    }

    public function testSumFiltersOnADottedKey(): void
    {
        $database = $this->database();
        $queries = [Query::equal('dots.name', ['v'])];

        $this->assertSame(5, $database->sum(self::PEOPLE, 'dots.score', $queries));
        $this->assertSame(5, $database->sum(self::PEOPLE, 'dots.score', $queries, 10));
        $this->assertSame(10, $database->sum(self::PEOPLE, 'dots.score'));
        $this->assertSame(0, $database->sum(self::PEOPLE, 'dots.score', [Query::equal('dots.name', ['missing'])]));
    }

    public function testExistsAndNotExistsReadADottedKey(): void
    {
        $database = $this->database();

        $this->assertSame(['a', 'b', 'c'], $this->ids($database->find(self::PEOPLE, [Query::exists(['dots.name'])])));
        $this->assertSame(['d'], $this->ids($database->find(self::PEOPLE, [Query::notExists(['dots.name'])])));
        $this->assertSame(3, $database->count(self::PEOPLE, [Query::exists(['dots.name'])]));
        $this->assertSame(1, $database->count(self::PEOPLE, [Query::notExists(['dots.name'])]));
        $this->assertSame(10, $database->sum(self::PEOPLE, 'dots.score', [Query::exists(['dots.score'])]));
    }

    public function testJoinAliasesStillQualifyAJoinedColumn(): void
    {
        $database = $this->database();
        $join = Query::join(self::ORDERS, '$id', 'personId', '=', 'ord');

        $this->assertSame(['a'], $this->ids($database->find(self::PEOPLE, [$join, Query::equal('dots.name', ['v'])])));
        $this->assertSame(1, $database->count(self::PEOPLE, [$join, Query::equal('dots.name', ['v'])]));
        $this->assertSame(7, $database->sum(self::PEOPLE, 'ord.total', [$join, Query::equal('dots.name', ['v'])]));
        $this->assertSame(1, $database->count(self::PEOPLE, [$join, Query::greaterThan('ord.total', 5)]));
        $this->assertSame(0, $database->count(self::PEOPLE, [$join, Query::greaterThan('ord.total', 7)]));
    }

    public function testExistsQualifiesAJoinedInternalAttribute(): void
    {
        $database = $this->database();
        $join = Query::join(self::ORDERS, '$id', 'personId', '=', 'ord');
        $leftJoin = Query::leftJoin(self::ORDERS, '$id', 'personId', '=', 'ord');

        $this->assertSame(['a'], $this->ids($database->find(self::PEOPLE, [$join, Query::exists(['ord.$id'])])));
        $this->assertSame(['b', 'c', 'd'], $this->ids($database->find(self::PEOPLE, [$leftJoin, Query::notExists(['ord.$id'])])));
        $this->assertSame(3, $database->count(self::PEOPLE, [$leftJoin, Query::notExists(['ord.$createdAt'])]));
        $this->assertSame(1, $database->count(self::PEOPLE, [$join, Query::exists(['dots.name'])]));
    }

    public function testGroupsAndHavingReadADottedKey(): void
    {
        $database = $this->database();

        $groups = $database->aggregate(self::PEOPLE, [
            Query::count('*', 'people'),
            Query::sum('dots.score', 'score'),
            Query::groupBy(['dots.name']),
            Query::exists(['dots.name']),
            Query::orderDesc('people'),
        ]);

        $this->assertSame([[2, 5], [1, 5]], \array_map(
            static fn (array $group): array => [$group['people'], $group['score']],
            $groups,
        ));

        $having = $database->aggregate(self::PEOPLE, [
            Query::count('*', 'people'),
            Query::groupBy(['dots.name']),
            Query::having([Query::greaterThan('people', 1)]),
        ]);

        $this->assertCount(1, $having);
        $this->assertSame(2, $having[0]['people']);
    }

    public function testSearchReadsADottedKey(): void
    {
        $database = $this->database();
        $database->createIndex(self::PEOPLE, Index::fulltext(key: 'names', attributes: ['dots.name']));

        $this->assertSame(['a', 'b'], $this->ids($database->find(self::PEOPLE, [Query::search('dots.name', 'v')])));
        $this->assertSame(2, $database->count(self::PEOPLE, [Query::search('dots.name', 'v')]));
        $this->assertSame(1, $database->count(self::PEOPLE, [
            Query::join(self::ORDERS, '$id', 'personId', '=', 'ord'),
            Query::search('dots.name', 'v'),
        ]));
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

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database
            ->setAuthorization($authorization)
            ->setDatabase('dotted')
            ->setNamespace('dotted_'.\uniqid());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: self::PEOPLE,
            attributes: [
                Attribute::string(key: 'dots.name', size: 64),
                Attribute::integer(key: 'dots.score'),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(Collection::create(
            id: self::ORDERS,
            attributes: [
                Attribute::string(key: 'personId', size: 64, required: true),
                Attribute::integer(key: 'total', required: true),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));

        $database->createDocument(self::PEOPLE, new Document(['$id' => 'a', 'dots.name' => 'v', 'dots.score' => 2]));
        $database->createDocument(self::PEOPLE, new Document(['$id' => 'b', 'dots.name' => 'v', 'dots.score' => 3]));
        $database->createDocument(self::PEOPLE, new Document(['$id' => 'c', 'dots.name' => 'w', 'dots.score' => 5]));
        $database->createDocument(self::PEOPLE, new Document(['$id' => 'd']));
        $database->createDocument(self::ORDERS, new Document(['$id' => 'o1', 'personId' => 'a', 'total' => 7]));

        return $database;
    }
}
