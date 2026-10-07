<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * An order on a bare name only one join declares reads that join's attribute, and a cursor pages
 * along it as it pages along the qualified name: the cursor is a row the read returned, which holds
 * the value under `alias.name`.
 */
final class BareJoinedOrderCursorTest extends TestCase
{
    private const string MAIN = 'stores';

    private const string JOINED = 'items';

    private const string OTHER = 'offers';

    private const string ALIAS = 'it';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('bare_order')
            ->setNamespace('bare_order_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection(self::MAIN, [Attribute::string(key: 'name', size: 16)]);
        $this->createCollection(self::JOINED, [
            Attribute::string(key: 'store', size: 16),
            Attribute::string(key: 'name', size: 16),
            Attribute::integer(key: 'price', required: false),
        ]);
        $this->createCollection(self::OTHER, [
            Attribute::string(key: 'store', size: 16),
            Attribute::integer(key: 'price', required: false),
        ]);

        foreach (['s1', 's2', 's3'] as $id) {
            $this->createDocument(self::MAIN, $id, ['name' => 'store '.$id]);
        }

        foreach ([
            'i1' => ['s1', 10],
            'i2' => ['s1', 10],
            'i3' => ['s2', 20],
            'i4' => ['s2', null],
            'i5' => ['s1', 5],
            'i6' => ['zz', 30],
        ] as $id => [$store, $price]) {
            $this->createDocument(self::JOINED, $id, ['store' => $store, 'name' => 'item '.$id, 'price' => $price]);
        }

        $this->createDocument(self::OTHER, 'o1', ['store' => 's1', 'price' => 1]);
    }

    /**
     * @return iterable<string, array{Method, bool}>
     */
    public static function reads(): iterable
    {
        foreach (['inner join' => Method::Join, 'left join' => Method::LeftJoin] as $joinName => $join) {
            foreach (['ascending' => true, 'descending' => false] as $directionName => $ascending) {
                yield "{$joinName}, {$directionName}" => [$join, $ascending];
            }
        }
    }

    #[DataProvider('reads')]
    public function testCursorPagesAlongABareJoinedOrderAsAlongTheQualifiedOne(Method $join, bool $ascending): void
    {
        $bare = [$this->join($join), $ascending ? Query::orderAsc('price') : Query::orderDesc('price')];
        $qualified = [$this->join($join), $ascending ? Query::orderAsc(self::ALIAS.'.price') : Query::orderDesc(self::ALIAS.'.price')];

        $expected = $this->keys($qualified);
        $rows = $join === Method::LeftJoin ? 6 : 5;
        $this->assertCount($rows, $expected, 'every joined row, the tied i1/i2, the null i4 and for a left join s3 without an item');
        $this->assertSame($expected, $this->keys($bare), 'the bare name orders by the joined attribute');

        $this->assertPagesEveryRowOnce($bare, $expected);
    }

    public function testCursorKeyedByTheBareNameFollowsTheQualifiedOrder(): void
    {
        $queries = [$this->join(Method::Join), Query::orderAsc('price')];
        $expected = $this->keys($queries);

        $cursor = $this->database->find(self::MAIN, [...$queries, Query::limit(1)])[0];
        $cursor->setAttribute('price', $cursor->getAttribute(self::ALIAS.'.price'));
        $cursor->removeAttribute(self::ALIAS.'.price');

        $this->assertSame(\array_slice($expected, 1), $this->keys([...$queries, Query::cursorAfter($cursor)]));
    }

    public function testNameTheMainCollectionDeclaresKeepsOrderingTheMainTable(): void
    {
        $queries = [$this->join(Method::Join), Query::orderDesc('name')];
        $rows = \array_values($this->database->find(self::MAIN, [...$queries, Query::limit(100)]));

        $this->assertSame(['s2', 's2', 's1', 's1', 's1'], \array_map(static fn (Document $row): string => $row->getId(), $rows));
        $this->assertPagesEveryRowOnce($queries, \array_map($this->key(...), $rows));
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function validation(): iterable
    {
        yield 'validation on' => [true];
        yield 'validation off' => [false];
    }

    #[DataProvider('validation')]
    public function testBareNameSeveralJoinsDeclareIsRefusedForACursorRead(bool $validate): void
    {
        $joins = [$this->join(Method::LeftJoin), Query::leftJoin(self::OTHER, '$id', 'store', '=', 'of')];
        $cursor = $this->database->find(self::MAIN, [...$joins, Query::orderAsc(self::ALIAS.'.price'), Query::limit(1)])[0];

        if (! $validate) {
            $this->database->disableValidation();
        }

        try {
            $this->database->find(self::MAIN, [...$joins, Query::orderAsc('price'), Query::cursorAfter($cursor)]);
            $this->fail('A bare name two joins declare must be refused, not read from one of them');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('Attribute "price" is ambiguous across joins; qualify it with a join alias', $exception->getMessage());
        }
    }

    private function join(Method $method): Query
    {
        return new Query($method, self::JOINED, ['$id', '=', 'store', self::ALIAS]);
    }

    /**
     * After and before each row the rest of the read in order, then a walk in pages of two in both
     * directions that returns each row exactly once.
     *
     * @param  list<Query>  $queries
     * @param  list<string>  $expected
     */
    private function assertPagesEveryRowOnce(array $queries, array $expected): void
    {
        $all = \array_values($this->database->find(self::MAIN, [...$queries, Query::limit(100)]));
        $this->assertSame($expected, \array_map($this->key(...), $all));

        foreach ($all as $index => $row) {
            $this->assertSame(\array_slice($expected, $index + 1), $this->keys([...$queries, Query::cursorAfter($row)]), "after {$expected[$index]}");
            $this->assertSame(\array_slice($expected, 0, $index), $this->keys([...$queries, Query::cursorBefore($row)]), "before {$expected[$index]}");
        }

        $forward = [];
        $cursor = null;
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = \array_values($this->database->find(self::MAIN, [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]));
            if ($rows === []) {
                break;
            }
            \array_push($forward, ...\array_map($this->key(...), $rows));
            $cursor = $rows[\count($rows) - 1];
        }
        $this->assertSame($expected, $forward, 'pages of two forward');

        $backward = [];
        $cursor = $all[\count($all) - 1];
        $backward[] = $this->key($cursor);
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = \array_values($this->database->find(self::MAIN, [...$queries, Query::limit(2), Query::cursorBefore($cursor)]));
            if ($rows === []) {
                break;
            }
            \array_unshift($backward, ...\array_map($this->key(...), $rows));
            $cursor = $rows[0];
        }
        $this->assertSame($expected, $backward, 'pages of two backward');
    }

    /**
     * @param  list<Query>  $queries
     * @return list<string>
     */
    private function keys(array $queries): array
    {
        return \array_values(\array_map($this->key(...), $this->database->find(self::MAIN, [...$queries, Query::limit(100)])));
    }

    private function key(Document $row): string
    {
        $joined = $row->getAttribute(self::ALIAS.'.$id');

        return $row->getId().'/'.(\is_string($joined) && $joined !== '' ? $joined : '-');
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(Collection::create(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
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
