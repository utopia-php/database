<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * An internal attribute under a join alias groups the joined rows as it groups the main collection's
 * rows: by its column, over every join.
 */
final class JoinInternalColumnsTest extends TestCase
{
    private const string TENANT = '1';

    private Database $database;

    protected function setUp(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')));
    }

    /**
     * Each join of the customers to their notes, with the number of rows it returns: c3 has no note
     * and n4 no customer.
     *
     * @return iterable<string, array{Method, bool, int}>
     */
    public static function joins(): iterable
    {
        yield 'inner join' => [Method::Join, false, 3];
        yield 'left join' => [Method::LeftJoin, false, 4];
        yield 'right join' => [Method::RightJoin, false, 4];
        yield 'emulated full outer join' => [Method::FullOuterJoin, false, 5];
        yield 'native full outer join' => [Method::FullOuterJoin, true, 5];
    }

    /**
     * @return iterable<string, array{Method, bool, int, string}>
     */
    public static function internalAttributesUnderEveryJoin(): iterable
    {
        foreach (self::joins() as $label => [$join, $native, $rows]) {
            foreach ([Document::ID, Document::SEQUENCE, Document::CREATED_AT, Document::UPDATED_AT, Document::PERMISSIONS] as $attribute) {
                yield $attribute.' over a '.$label => [$join, $native, $rows, $attribute];
            }
        }
    }

    #[DataProvider('internalAttributesUnderEveryJoin')]
    public function testInternalAttributeUnderAnAliasGroupsTheJoinedRows(Method $join, bool $native, int $rows, string $attribute): void
    {
        if ($native) {
            $this->useDatabase(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $groups = $this->database->find('customers', [
            $this->join($join),
            Query::count('*', 'rows'),
            Query::groupBy(['note.'.$attribute]),
        ]);

        $total = 0;
        foreach ($groups as $group) {
            $this->assertArrayHasKey(Storage::column($attribute), $group->getArrayCopy(), 'a group comes back under its column, as on the main collection');
            $count = $group->getAttribute('rows');
            $this->assertIsInt($count);
            $total += $count;
        }
        $this->assertSame($rows, $total);
    }

    /**
     * @return iterable<string, array{Method, bool, list<string|null>}>
     */
    public static function joinedIdGroups(): iterable
    {
        yield 'inner join' => [Method::Join, false, ['n1', 'n2', 'n3']];
        yield 'left join' => [Method::LeftJoin, false, [null, 'n1', 'n2', 'n3']];
        yield 'right join' => [Method::RightJoin, false, ['n1', 'n2', 'n3', 'n4']];
        yield 'emulated full outer join' => [Method::FullOuterJoin, false, [null, 'n1', 'n2', 'n3', 'n4']];
        yield 'native full outer join' => [Method::FullOuterJoin, true, [null, 'n1', 'n2', 'n3', 'n4']];
    }

    /**
     * @param  list<string|null>  $expected
     */
    #[DataProvider('joinedIdGroups')]
    public function testJoinedIdGroupsOneRowPerJoinedDocument(Method $join, bool $native, array $expected): void
    {
        if ($native) {
            $this->useDatabase(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $groups = $this->database->find('customers', [
            $this->join($join),
            Query::count('*', 'rows'),
            Query::groupBy(['note.$id']),
            Query::orderAsc('note.$id'),
        ]);

        $this->assertSame($expected, \array_map(static fn (Document $group): mixed => $group->getAttribute(Storage::UID), $groups));
    }

    public function testTenantIsReadUnderSharedTables(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')), sharedTables: true);
        $note = $this->join(Method::Join);

        $this->assertSame([['total' => 3]], $this->rows($this->database->find('customers', [Query::count('$tenant', 'total')])));
        $this->assertSame([['rows' => 3, Storage::TENANT => 1]], $this->rows($this->database->find('customers', [Query::count('*', 'rows'), Query::groupBy(['$tenant'])])));
        $this->assertSame([['total' => 3]], $this->rows($this->database->find('customers', [$note, Query::count('note.$tenant', 'total')])));
        $this->assertSame([['rows' => 3, Storage::TENANT => 1]], $this->rows($this->database->find('customers', [$note, Query::count('*', 'rows'), Query::groupBy(['note.$tenant'])])));

        $customers = $this->database->find('customers', [$note, Query::select(['name', 'note.$tenant']), Query::orderAsc('note.$id')]);
        $this->assertSame([self::TENANT, self::TENANT, self::TENANT], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('note.$tenant'), $customers));
    }

    private function join(Method $method): Query
    {
        return new Query($method, 'notes', ['$id', '=', 'customerId', 'note']);
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array<string, mixed>>
     */
    private function rows(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): array => $document->getArrayCopy(), $documents));
    }

    private function useDatabase(SQLite $adapter, bool $sharedTables = false): void
    {
        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase('join_internal_columns')
            ->setNamespace('join_internal_columns_'.\uniqid())
            ->setAuthorization(new Authorization());
        if ($sharedTables) {
            $this->database->setSharedTables(true)->setTenant(null);
        }
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection('customers', [
            Attribute::string(key: 'name', size: 64),
            Attribute::integer(key: 'score'),
            Attribute::string(key: 'tags', size: 16, array: true),
        ]);
        $this->createCollection('notes', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'body', size: 256),
            Attribute::integer(key: 'score'),
            Attribute::string(key: 'tags', size: 16, array: true),
        ]);

        if ($sharedTables) {
            $this->database->setTenant(self::TENANT);
        }

        $this->createDocument('customers', 'c1', ['name' => 'One', 'score' => 10, 'tags' => ['a']]);
        $this->createDocument('customers', 'c2', ['name' => 'Two', 'score' => 20, 'tags' => ['b']]);
        $this->createDocument('customers', 'c3', ['name' => 'Three', 'score' => 30, 'tags' => []]);
        $this->createDocument('notes', 'n1', ['customerId' => 'c1', 'body' => 'first', 'score' => 1, 'tags' => ['x']]);
        $this->createDocument('notes', 'n2', ['customerId' => 'c1', 'body' => 'second', 'score' => 2, 'tags' => []]);
        $this->createDocument('notes', 'n3', ['customerId' => 'c2', 'body' => 'third', 'score' => 3, 'tags' => []]);
        $this->createDocument('notes', 'n4', ['customerId' => 'ghost', 'body' => 'stray', 'score' => 4, 'tags' => []]);
    }

    /**
     * @param  array<Attribute>  $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(new Collection(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createDocument(string $collection, string $id, array $attributes): void
    {
        $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            ...$attributes,
        ]));
    }
}
