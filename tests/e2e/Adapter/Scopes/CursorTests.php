<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;
use Utopia\Query\CursorDirection;

/**
 * resolveCursor() turns the ID of a document into the cursor a list pages from. The document is read whether or not
 * the caller can read it; after it a page starts behind the last row the list pairs with it, before it a page ends
 * ahead of the first, and a joined order takes its value from the joined rows the caller can read.
 */
trait CursorTests
{
    private const string CURSOR_CUSTOMERS = 'cursor_customers';

    private const string CURSOR_ORDERS = 'cursor_orders';

    public function testResolveCursorPagesPastADocumentTheCallerCannotRead(): void
    {
        $database = $this->getDatabase();
        $this->seedCursorCollections($database);

        try {
            $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, 'alice', ['name' => 'Alice'], readable: true);
            $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, 'bob', ['name' => 'Bob'], readable: false);
            $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, 'carol', ['name' => 'Carol'], readable: true);

            $queries = [Query::orderAsc('name')];
            $after = $database->resolveCursor(self::CURSOR_CUSTOMERS, 'bob', CursorDirection::After, $queries);
            $before = $database->resolveCursor(self::CURSOR_CUSTOMERS, 'bob', CursorDirection::Before, $queries);

            $this->assertSame('bob', $after->getId());
            $this->assertSame('Bob', $after->getAttribute('name'));
            $this->assertSame(['carol'], $this->cursorIds($database->find(self::CURSOR_CUSTOMERS, [...$queries, Query::cursorAfter($after)])), 'a caller who cannot read the cursor document still pages past it');
            $this->assertSame(['alice'], $this->cursorIds($database->find(self::CURSOR_CUSTOMERS, [...$queries, Query::cursorBefore($before)])));
            $this->assertTrue($database->resolveCursor(self::CURSOR_CUSTOMERS, 'nobody', CursorDirection::After, $queries)->isEmpty());
        } finally {
            $this->deleteCursorCollections($database);
        }
    }

    public function testResolveCursorTakesAJoinedOrderFromTheRowsTheCallerCanRead(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedCursorCollections($database);

        try {
            $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, 'alice', ['name' => 'Alice'], readable: true);
            $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, 'bob', ['name' => 'Bob'], readable: false);
            $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, 'carol', ['name' => 'Carol'], readable: true);
            foreach ([
                ['a-first', 'alice', 10, true, 'paid'],
                ['a-hidden', 'alice', 35, false, 'paid'],
                ['a-open', 'alice', 60, true, 'open'],
                ['a-second', 'alice', 25, true, 'paid'],
                ['b-hidden', 'bob', 30, false, 'paid'],
                ['b-only', 'bob', 20, true, 'paid'],
                ['c-only', 'carol', 15, true, 'paid'],
            ] as [$id, $customer, $amount, $readable, $status]) {
                $this->createCursorDocument($database, self::CURSOR_ORDERS, $id, ['customerId' => $customer, 'amount' => $amount, 'status' => $status], $readable);
            }

            $queries = [
                Query::join(self::CURSOR_ORDERS, 'ord', [Query::on('$id', 'customerId')]),
                Query::equal('ord.status', ['paid']),
                Query::orderAsc('ord.amount'),
            ];

            $this->assertSame(
                [['alice', 'a-first'], ['carol', 'c-only'], ['alice', 'a-second']],
                $this->cursorRows($database->find(self::CURSOR_CUSTOMERS, [...$queries, Query::limit(100)])),
            );
            $this->assertSame(25, self::cursorInteger($database->resolveCursor(self::CURSOR_CUSTOMERS, 'alice', CursorDirection::After, $queries)->getAttribute('ord.amount')), 'after alice the list resumes behind its last paid order the caller can read');
            $this->assertSame(10, self::cursorInteger($database->resolveCursor(self::CURSOR_CUSTOMERS, 'alice', CursorDirection::Before, $queries)->getAttribute('ord.amount')));
            $this->assertSame(20, self::cursorInteger($database->resolveCursor(self::CURSOR_CUSTOMERS, 'bob', CursorDirection::After, $queries)->getAttribute('ord.amount')), 'the joined values of a document the caller cannot read come from the rows it can read');

            $this->assertSame([], $this->cursorPage($database, $queries, 'alice', CursorDirection::After));
            $this->assertSame([], $this->cursorPage($database, $queries, 'alice', CursorDirection::Before));
            $this->assertSame([['alice', 'a-second']], $this->cursorPage($database, $queries, 'carol', CursorDirection::After));
            $this->assertSame([['alice', 'a-first']], $this->cursorPage($database, $queries, 'carol', CursorDirection::Before));
            $this->assertSame([['alice', 'a-second']], $this->cursorPage($database, $queries, 'bob', CursorDirection::After), 'bob is not listed, and the list resumes behind the order of 20 the caller can read');
            $this->assertSame([['alice', 'a-first'], ['carol', 'c-only']], $this->cursorPage($database, $queries, 'bob', CursorDirection::Before));
        } finally {
            $this->deleteCursorCollections($database);
        }
    }

    public function testResolveCursorOverAJoinOnAOneToManyRelationshipPagesEveryRowOnce(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedCursorCollections($database);

        try {
            $database->createRelationship(self::CURSOR_CUSTOMERS, Relationship::oneToMany(relatedCollection: self::CURSOR_ORDERS, key: 'orders', twoWayKey: 'customer'));

            foreach (['alice' => 'Alice', 'bob' => 'Bob', 'carol' => 'Carol'] as $id => $name) {
                $this->createCursorDocument($database, self::CURSOR_CUSTOMERS, $id, ['name' => $name], readable: true);
            }
            foreach ([
                ['a-first', 'alice', 10, true],
                ['a-second', 'alice', 20, true],
                ['b-first', 'bob', 30, true],
                ['b-second', 'bob', 40, true],
                ['c-hidden', 'carol', 5, false],
                ['c-only', 'carol', 50, true],
            ] as [$id, $customer, $amount, $readable]) {
                $this->createCursorDocument($database, self::CURSOR_ORDERS, $id, ['customer' => $customer, 'amount' => $amount, 'status' => 'paid'], $readable);
            }

            $queries = [Query::join(self::CURSOR_ORDERS, 'ord', [Query::on('orders', '$id')]), Query::orderAsc('ord.amount')];
            $all = $this->cursorRows($database->find(self::CURSOR_CUSTOMERS, [...$queries, Query::limit(100)]));
            $this->assertSame([['alice', 'a-first'], ['alice', 'a-second'], ['bob', 'b-first'], ['bob', 'b-second'], ['carol', 'c-only']], $all);

            $forward = [];
            $cursor = null;
            for ($page = 0; $page <= \count($all); $page++) {
                $rows = $this->cursorRows($database->find(self::CURSOR_CUSTOMERS, [
                    ...$queries,
                    Query::limit(2),
                    ...($cursor === null ? [] : [Query::cursorAfter($database->resolveCursor(self::CURSOR_CUSTOMERS, $cursor, CursorDirection::After, $queries))]),
                ]));
                if ($rows === []) {
                    break;
                }
                \array_push($forward, ...$rows);
                $cursor = $rows[\count($rows) - 1][0];
            }
            $this->assertSame($all, $forward, 'paging forward by the ID of each page\'s last row returns every row once');

            $backward = [['carol', 'c-only']];
            $cursor = 'carol';
            for ($page = 0; $page <= \count($all); $page++) {
                $rows = $this->cursorRows($database->find(self::CURSOR_CUSTOMERS, [
                    ...$queries,
                    Query::limit(2),
                    Query::cursorBefore($database->resolveCursor(self::CURSOR_CUSTOMERS, $cursor, CursorDirection::Before, $queries)),
                ]));
                if ($rows === []) {
                    break;
                }
                $backward = [...$rows, ...$backward];
                $cursor = $rows[0][0];
            }
            $this->assertSame($all, $backward, 'paging backward by the ID of each page\'s first row returns every row once');
        } finally {
            $this->deleteCursorCollections($database);
        }
    }

    private function seedCursorCollections(Database $database): void
    {
        $this->deleteCursorCollections($database);

        $permissions = [Permission::create(Role::any())];
        $database->createCollection(Collection::create(
            id: self::CURSOR_CUSTOMERS,
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: $permissions,
            documentSecurity: true,
        ));
        $database->createCollection(Collection::create(
            id: self::CURSOR_ORDERS,
            attributes: [
                Attribute::string(key: 'customerId', size: 16),
                Attribute::string(key: 'status', size: 16),
                Attribute::integer(key: 'amount'),
            ],
            permissions: $permissions,
            documentSecurity: true,
        ));
    }

    private function deleteCursorCollections(Database $database): void
    {
        foreach ([self::CURSOR_CUSTOMERS, self::CURSOR_ORDERS] as $collection) {
            if ($database->collectionExists($collection)) {
                $database->deleteCollection($collection);
            }
        }
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createCursorDocument(Database $database, string $collection, string $id, array $attributes, bool $readable): void
    {
        $database->getAuthorization()->skip(fn (): Document => $database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read($readable ? Role::any() : Role::user('cursor-other'))],
            ...$attributes,
        ])));
    }

    /**
     * @param  list<Query>  $queries
     * @return list<array{0: string, 1: mixed}>
     */
    private function cursorPage(Database $database, array $queries, string $id, CursorDirection $direction): array
    {
        $cursor = $database->resolveCursor(self::CURSOR_CUSTOMERS, $id, $direction, $queries);

        return $this->cursorRows($database->find(self::CURSOR_CUSTOMERS, [
            ...$queries,
            $direction === CursorDirection::After ? Query::cursorAfter($cursor) : Query::cursorBefore($cursor),
        ]));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function cursorIds(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array{0: string, 1: mixed}>
     */
    private function cursorRows(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): array => [$document->getId(), $document->getAttribute('ord.$id')], $documents));
    }

    private static function cursorInteger(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
