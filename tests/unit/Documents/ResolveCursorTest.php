<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;

/**
 * resolveCursor() turns the ID of a document into the cursor a list pages from: after the document a page starts
 * behind the last row the list pairs with it, before it a page ends ahead of the first. The document is read whether
 * or not the caller can read it; a joined order takes its value from the joined rows the caller can read.
 */
final class ResolveCursorTest extends TestCase
{
    private const string CUSTOMERS = 'customers';

    private const string ORDERS = 'orders';

    private const string PRODUCTS = 'products';

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::user('reader')->toString());

        $this->database = new Database(new SQLite(new PDO('sqlite::memory:', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION])), new Cache(new None()));
        $this->database
            ->setAuthorization($authorization)
            ->setDatabase('resolve_cursor')
            ->setNamespace('resolve_cursor_'.\uniqid());
        $this->database->addHook(new Relationships());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection(self::CUSTOMERS, [Attribute::string(key: 'name', size: 64)]);
        $this->createCollection(self::ORDERS, [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'productId', size: 64),
            Attribute::string(key: 'status', size: 16),
            Attribute::integer(key: 'amount'),
            Attribute::datetime(key: 'placedAt'),
        ]);
        $this->createCollection(self::PRODUCTS, [Attribute::string(key: 'name', size: 64), Attribute::integer(key: 'price')]);
    }

    public function testCursorWithoutJoinPagesPastADocumentTheCallerCannotRead(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: false);
        $this->customer('carol', readable: true);

        $queries = [Query::orderAsc('name')];

        $this->assertSame(['carol'], $this->ids($this->page($queries, 'bob', CursorDirection::After)), 'a caller who cannot read the cursor document still pages past it');
        $this->assertSame(['alice'], $this->ids($this->page($queries, 'bob', CursorDirection::Before)));
        $this->assertWalksEveryRowOnce($queries, ['alice', 'carol']);
    }

    public function testCursorWithJoinPagesByAnOrderTheSelectLeavesOut(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->customer('carol', readable: true);
        $this->order('a-only', 'alice', 10, readable: true);
        $this->order('b-only', 'bob', 15, readable: true);
        $this->order('c-only', 'carol', 20, readable: true);

        $queries = [$this->ordersJoin(), Query::select(['$id']), Query::orderAsc('name')];

        $this->assertSame(['carol'], $this->ids($this->page($queries, 'bob', CursorDirection::After)), 'the select shapes the listed rows, never the cursor the page starts after');
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorWithJoinOrderTakesItsValueFromAJoinedRowTheCallerCanRead(): void
    {
        $this->customer('alice', readable: true);
        $this->order('a-hidden', 'alice', 35, readable: false);
        $this->order('b-visible', 'alice', 10, readable: true);

        $queries = [$this->ordersJoin(), Query::orderAsc('ord.amount')];
        $cursor = $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries);

        $this->assertSame('alice', $cursor->getId());
        $this->assertSame(10, $cursor->getAttribute('ord.amount'), 'the page boundary comes from the order the caller can read, not the one the list hides');
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorWithJoinOrderResolvesJoinedValuesOfADocumentTheCallerCannotRead(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: false);
        $this->order('a-visible', 'alice', 25, readable: true);
        $this->order('c-hidden', 'bob', 30, readable: false);
        $this->order('d-visible', 'bob', 20, readable: true);

        $queries = [$this->ordersJoin(), Query::orderAsc('ord.amount')];
        $cursor = $this->database->resolveCursor(self::CUSTOMERS, 'bob', CursorDirection::After, $queries);

        $this->assertSame('bob', $cursor->getId(), 'a caller who cannot read the cursor document still pages past it');
        $this->assertSame(20, $cursor->getAttribute('ord.amount'));
        $this->assertSame([['alice', 'a-visible']], $this->rows($this->page($queries, 'bob', CursorDirection::After)));
    }

    public function testCursorWithJoinOrderSkipsJoinedRowsTheListFiltersOut(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-open', 'alice', 50, readable: true, status: 'open');
        $this->order('b-paid', 'alice', 10, readable: true, status: 'paid');
        $this->order('c-paid', 'bob', 30, readable: true, status: 'paid');

        $queries = [$this->ordersJoin(), Query::equal('ord.status', ['paid']), Query::orderAsc('ord.amount')];
        $cursor = $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries);

        $this->assertSame(10, $cursor->getAttribute('ord.amount'), 'the list only pairs the customer with its paid order');
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorWithJoinOrderSkipsJoinedRowsTheJoinConditionFiltersOut(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-open', 'alice', 50, readable: true, status: 'open');
        $this->order('b-paid', 'alice', 10, readable: true, status: 'paid');
        $this->order('c-paid', 'bob', 30, readable: true, status: 'paid');

        $queries = [
            Query::join(self::ORDERS, 'ord', [Query::on('$id', 'customerId'), Query::equal('ord.status', ['paid'])]),
            Query::orderAsc('ord.amount'),
        ];

        $this->assertSame(10, $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries)->getAttribute('ord.amount'));
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorAfterADocumentWithSeveralJoinedRowsStartsBehindItsLastRow(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-first', 'alice', 10, readable: true);
        $this->order('b-second', 'alice', 25, readable: true);
        $this->order('c-only', 'bob', 15, readable: true);

        $ascending = [$this->ordersJoin(), Query::orderAsc('ord.amount')];
        $descending = [$this->ordersJoin(), Query::orderDesc('ord.amount')];

        $this->assertSame(25, $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $ascending)->getAttribute('ord.amount'));
        $this->assertSame(10, $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $descending)->getAttribute('ord.amount'));
        $this->assertSame(10, $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::Before, $ascending)->getAttribute('ord.amount'), 'the page before a document ends ahead of its first row');
        $this->assertSame([], $this->rows($this->page($ascending, 'alice', CursorDirection::After)));
        $this->assertSame([], $this->rows($this->page($ascending, 'alice', CursorDirection::Before)));
        $this->assertCursorSplitsTheList($ascending);
        $this->assertCursorSplitsTheList($descending);
    }

    public function testCursorWithJoinOrderPassesJoinedDatetimesForTheLibraryToEncode(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-visible', 'alice', 10, readable: true, placedAt: '2024-05-01T10:00:00.000+00:00');
        $this->order('b-visible', 'bob', 10, readable: true, placedAt: '2024-06-01T10:00:00.000+00:00');

        $queries = [$this->ordersJoin(), Query::orderAsc('ord.placedAt')];

        $this->assertSame('2024-05-01T10:00:00.000+00:00', $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries)->getAttribute('ord.placedAt'));
        $this->assertSame([['bob', 'b-visible']], $this->rows($this->page($queries, 'alice', CursorDirection::After)), 'the joined datetime is encoded as the cursor document\'s own attributes are');
    }

    public function testCursorWithJoinOrderFollowsAChainOfJoins(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-visible', 'alice', 10, readable: true, productId: 'lamp');
        $this->order('b-visible', 'bob', 10, readable: true, productId: 'desk');
        $this->product('lamp', 'Lamp');
        $this->product('desk', 'Desk');

        $queries = [
            $this->ordersJoin(),
            Query::join(self::PRODUCTS, 'prod', [Query::on('ord.productId', '$id')]),
            Query::orderAsc('prod.name'),
        ];

        $this->assertSame('Lamp', $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries)->getAttribute('prod.name'));
        $this->assertSame(['bob'], $this->ids($this->page($queries, 'alice', CursorDirection::Before)));
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorComparesTheJoinedColumnAsTheJoinConditionDoes(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-only', 'alice', 20, readable: true);
        $this->order('b-only', 'bob', 40, readable: true);
        $this->product('lamp', 'Lamp', 10);
        $this->product('desk', 'Desk', 30);

        $queries = [
            $this->ordersJoin(),
            Query::join(self::PRODUCTS, 'prod', [Query::on('ord.amount', 'price', '>=')]),
            Query::orderDesc('prod.name'),
        ];

        $this->assertSame('Lamp', $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries)->getAttribute('prod.name'), 'an order of 20 pairs only with the product priced at most 20');
        $this->assertSame('Desk', $this->database->resolveCursor(self::CUSTOMERS, 'bob', CursorDirection::After, $queries)->getAttribute('prod.name'), 'the last of the products an order of 40 pairs with');
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorWithoutAReadableJoinedRowHasNoJoinedOrderValue(): void
    {
        $this->customer('alice', readable: true);
        $this->order('a-hidden', 'alice', 35, readable: false);

        $cursor = $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, [$this->ordersJoin(), Query::orderAsc('ord.amount')]);

        $this->assertTrue($cursor->offsetExists('ord.amount'));
        $this->assertNull($cursor->getAttribute('ord.amount'), 'without a row the caller can read the order value is null, as it is in the list');
    }

    public function testCursorOnAOneToManyJoinPagesBehindTheDocumentsLastJoinedRow(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-first', 'alice', 10, readable: true);
        $this->order('a-second', 'alice', 25, readable: true);
        $this->order('b-only', 'bob', 15, readable: true);

        $queries = [$this->ordersJoin()];

        $this->assertSame([['bob', 'b-only']], $this->rows($this->page($queries, 'alice', CursorDirection::After)), 'the page after a document starts behind every row the list pairs with it');
        $this->assertSame([['alice', 'a-first'], ['alice', 'a-second']], $this->rows($this->page($queries, 'bob', CursorDirection::Before)), 'the page before a document ends ahead of its first row');
        $this->assertWalksEveryRowOnce($queries, ['alice', 'alice', 'bob'], 2);
    }

    public function testCursorOrderedByAJoinedAttributeWithoutAReadableRowPagesAsTheListDoes(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-hidden', 'alice', 35, readable: false);
        $this->order('b-only', 'bob', 15, readable: true);

        $queries = [Query::leftJoin(self::ORDERS, 'ord', [Query::on('$id', 'customerId')]), Query::orderAsc('ord.amount')];

        $this->assertSame([['bob', 'b-only']], $this->rows($this->page($queries, 'alice', CursorDirection::After)), 'a row the join did not match is a page boundary with null joined values');
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorOrderedByABareJoinedNamePagesAsTheListDoes(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->order('a-first', 'alice', 10, readable: true);
        $this->order('a-second', 'alice', 25, readable: true);
        $this->order('b-only', 'bob', 15, readable: true);

        $queries = [$this->ordersJoin(), Query::orderAsc('amount')];

        $this->assertSame([['alice', 'a-second']], $this->rows($this->page($queries, 'bob', CursorDirection::After)), 'a bare order name only the joined collection declares reads the joined row');
        $this->assertSame(15, $this->database->resolveCursor(self::CUSTOMERS, 'bob', CursorDirection::After, $queries)->getAttribute('ord.amount'));
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorOnAJoinOfTheJoinedIdNeedsNoJoinedRow(): void
    {
        $this->customer('alice', readable: true);
        $this->order('a-first', 'alice', 10, readable: true);
        $this->order('a-second', 'alice', 25, readable: true);

        $queries = [Query::join(self::CUSTOMERS, 'cus', [Query::on('customerId', '$id')])];
        $cursor = $this->database->resolveCursor(self::ORDERS, 'a-first', CursorDirection::After, $queries);

        $this->assertFalse($cursor->offsetExists('cus.$id'), 'a join on the joined $id pairs at most one row, so no joined value is needed');
        $this->assertSame(['a-second'], $this->ids($this->database->find(self::ORDERS, [...$queries, Query::cursorAfter($cursor)])));
    }

    public function testCursorOverAJoinOnAOneToManyRelationshipPagesEveryRowOnce(): void
    {
        $this->database->createRelationship(self::CUSTOMERS, Relationship::oneToMany(relatedCollection: self::ORDERS, key: 'orders', twoWayKey: 'customer'));
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $this->customer('carol', readable: true);
        $this->order('a-first', 'alice', 10, readable: true, customer: 'alice');
        $this->order('a-second', 'alice', 20, readable: true, customer: 'alice');
        $this->order('b-first', 'bob', 30, readable: true, customer: 'bob');
        $this->order('b-second', 'bob', 40, readable: true, customer: 'bob');
        $this->order('c-hidden', 'carol', 5, readable: false, customer: 'carol');
        $this->order('c-only', 'carol', 50, readable: true, customer: 'carol');

        $queries = [Query::join(self::ORDERS, 'ord', [Query::on('orders', '$id')]), Query::orderAsc('ord.amount')];

        $this->assertSame(20, $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, $queries)->getAttribute('ord.amount'));
        $this->assertSame(50, $this->database->resolveCursor(self::CUSTOMERS, 'carol', CursorDirection::Before, $queries)->getAttribute('ord.amount'), 'the hidden order never pairs with carol');
        $this->assertWalksEveryRowOnce($queries, ['alice', 'alice', 'bob', 'bob', 'carol'], 2);
        $this->assertCursorSplitsTheList($queries);
    }

    public function testCursorCarriesOnlyTheValuesTheListPagesBy(): void
    {
        $this->customer('alice', readable: true);
        $this->order('a-first', 'alice', 10, readable: true, status: 'open');

        $cursor = $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, [$this->ordersJoin(), Query::orderDesc('name'), Query::orderAsc('ord.status')]);
        $sequence = $this->database->getAuthorization()->skip(fn (): Document => $this->database->getDocument(self::CUSTOMERS, 'alice'))->getSequence();

        $expected = [
            Document::ID => 'alice',
            Document::SEQUENCE => $sequence,
            Document::COLLECTION => self::CUSTOMERS,
            'name' => 'Alice',
            'ord.status' => 'open',
            'ord.$id' => 'a-first',
        ];
        $actual = $cursor->getArrayCopy();
        \ksort($expected);
        \ksort($actual);

        $this->assertSame($expected, $actual);
    }

    public function testCursorIgnoresTheCursorQueriesOfTheList(): void
    {
        $this->customer('alice', readable: true);
        $this->customer('bob', readable: true);
        $alice = $this->database->getDocument(self::CUSTOMERS, 'alice');

        $cursor = $this->database->resolveCursor(self::CUSTOMERS, 'bob', CursorDirection::Before, [Query::orderAsc('name'), Query::cursorAfter($alice)]);

        $this->assertSame(['alice'], $this->ids($this->database->find(self::CUSTOMERS, [Query::orderAsc('name'), Query::cursorBefore($cursor)])));
    }

    public function testMissingDocumentResolvesToAnEmptyCursor(): void
    {
        $this->assertTrue($this->database->resolveCursor(self::CUSTOMERS, 'nobody', CursorDirection::After)->isEmpty());
        $this->assertTrue($this->database->resolveCursor(self::CUSTOMERS, 'nobody', CursorDirection::Before, [$this->ordersJoin(), Query::orderAsc('ord.amount')])->isEmpty());
    }

    public function testMissingCollectionIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database->resolveCursor('missing', 'alice', CursorDirection::After);
    }

    public function testInvalidJoinIsRefused(): void
    {
        $this->customer('alice', readable: true);

        $this->expectException(QueryException::class);

        $this->database->resolveCursor(self::CUSTOMERS, 'alice', CursorDirection::After, [Query::join('missing', 'gone', [Query::on('$id', 'customerId')])]);
    }

    /**
     * After each listed document the list resumes behind the last row it pairs with that document, before it the list
     * ends ahead of its first row.
     *
     * @param  list<Query>  $queries
     */
    private function assertCursorSplitsTheList(array $queries): void
    {
        $all = $this->rows($this->database->find(self::CUSTOMERS, [...$queries, Query::limit(100)]));
        $ids = \array_column($all, 0);
        $this->assertNotSame([], $all, 'the list has rows to page through');

        foreach (\array_unique($ids) as $id) {
            $first = (int) \array_search($id, $ids, true);
            $last = (int) \array_key_last(\array_filter($ids, static fn (string $listed): bool => $listed === $id));

            $this->assertSame(\array_slice($all, $last + 1), $this->rows($this->page($queries, $id, CursorDirection::After)), "after {$id}");
            $this->assertSame(\array_slice($all, 0, $first), $this->rows($this->page($queries, $id, CursorDirection::Before)), "before {$id}");
        }
    }

    /**
     * Pages of $size forward from the start and backward from the end, each cursor resolved from the ID of the page's
     * edge row, return every row of the list once, in list order.
     *
     * @param  list<Query>  $queries
     * @param  list<string>  $expected
     */
    private function assertWalksEveryRowOnce(array $queries, array $expected, int $size = 1): void
    {
        $all = $this->rows($this->database->find(self::CUSTOMERS, [...$queries, Query::limit(100)]));
        $this->assertSame($expected, \array_column($all, 0));

        $forward = [];
        $cursor = null;
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = $this->rows($this->database->find(self::CUSTOMERS, [
                ...$queries,
                Query::limit($size),
                ...($cursor === null ? [] : [Query::cursorAfter($this->database->resolveCursor(self::CUSTOMERS, $cursor, CursorDirection::After, $queries))]),
            ]));
            if ($rows === []) {
                break;
            }
            \array_push($forward, ...$rows);
            $cursor = $rows[\count($rows) - 1][0];
        }
        $this->assertSame($all, $forward, 'paging forward returns every row once');

        $cursor = $all[\count($all) - 1][0];
        $backward = \array_values(\array_filter($all, static fn (array $row): bool => $row[0] === $cursor));
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = $this->rows($this->database->find(self::CUSTOMERS, [
                ...$queries,
                Query::limit($size),
                Query::cursorBefore($this->database->resolveCursor(self::CUSTOMERS, $cursor, CursorDirection::Before, $queries)),
            ]));
            if ($rows === []) {
                break;
            }
            $backward = [...$rows, ...$backward];
            $cursor = $rows[0][0];
        }
        $this->assertSame($all, $backward, 'paging backward returns every row once');
    }

    /**
     * @param  list<Query>  $queries
     * @return list<Document>
     */
    private function page(array $queries, string $id, CursorDirection $direction): array
    {
        $cursor = $this->database->resolveCursor(self::CUSTOMERS, $id, $direction, $queries);

        return \array_values($this->database->find(self::CUSTOMERS, [
            ...$queries,
            $direction === CursorDirection::After ? Query::cursorAfter($cursor) : Query::cursorBefore($cursor),
        ]));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array{0: string, 1: mixed}>
     */
    private function rows(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): array => [$document->getId(), $document->getAttribute('ord.$id')], $documents));
    }

    private function ordersJoin(): Query
    {
        return Query::join(self::ORDERS, 'ord', [Query::on('$id', 'customerId')]);
    }

    private function customer(string $id, bool $readable): void
    {
        $this->create(self::CUSTOMERS, $id, ['name' => \ucfirst($id)], $readable);
    }

    private function order(
        string $id,
        string $customerId,
        int $amount,
        bool $readable,
        string $status = 'paid',
        ?string $placedAt = null,
        ?string $productId = null,
        ?string $customer = null,
    ): void {
        $this->create(self::ORDERS, $id, [
            'customerId' => $customerId,
            'productId' => $productId,
            'status' => $status,
            'amount' => $amount,
            'placedAt' => $placedAt,
            ...($customer === null ? [] : ['customer' => $customer]),
        ], $readable);
    }

    private function product(string $id, string $name, ?int $price = null): void
    {
        $this->create(self::PRODUCTS, $id, ['name' => $name, 'price' => $price], readable: true);
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function create(string $collection, string $id, array $attributes, bool $readable): void
    {
        $this->database->getAuthorization()->skip(fn (): Document => $this->database->createDocument($collection, new Document([
            Document::ID => $id,
            Document::PERMISSIONS => [Permission::read(Role::user($readable ? 'reader' : 'other'))],
            ...$attributes,
        ])));
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(Collection::create(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
    }
}
