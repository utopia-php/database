<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;
use Utopia\Query\CursorDirection;
use Utopia\Query\Schema\ColumnType;

/**
 * A join condition whose left column is the parent side of a one-to-many relationship compares the main `$id` with
 * the two-way key each child stores, on every read that takes joins. The joined collection is read as a direct list of
 * it would be: under its permissions and the current tenant. Every other relationship attribute joins as in 8.0.3.
 */
trait JoinRelationshipTests
{
    private const string JOIN_RELATIONSHIP_CUSTOMERS = 'jrel_customers';

    private const string JOIN_RELATIONSHIP_ORDERS = 'jrel_orders';

    private const string JOIN_RELATIONSHIP_REGIONS = 'jrel_regions';

    private const array JOIN_RELATIONSHIP_COLLECTIONS = [
        self::JOIN_RELATIONSHIP_CUSTOMERS,
        self::JOIN_RELATIONSHIP_ORDERS,
        self::JOIN_RELATIONSHIP_REGIONS,
    ];

    public function testJoinRelationshipOnTheParentSideOfAOneToManyMatchesTheTwoWayKey(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $this->assertSame(
                ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4', 'c3/-'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, [Query::leftJoin(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')])], 'purchase'),
                'a left join keeps the customer without orders',
            );
            $this->assertSame(
                ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, [Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', 'purchase.$id')])], 'purchase'),
            );
            $this->assertSame(
                ['c1/o3', 'c1/o4', 'c2/o1', 'c2/o2', 'c3/o1', 'c3/o2', 'c3/o3', 'c3/o4'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, [Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id', '!=')])], 'purchase'),
                'the operator of the condition is kept',
            );
            $this->assertSame(
                ['c1/o1', 'c2/o4'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, [Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [
                    Query::on('orders', '$id'),
                    Query::on('region', 'region'),
                    Query::equal('purchase.status', ['paid']),
                ])], 'purchase'),
                'the other conditions of the ON list compare what they named',
            );

            $rows = $database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, [
                Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]),
                Query::select(['name', 'purchase.amount']),
                Query::orderAsc('purchase.amount'),
            ]);
            $this->assertSame([10, 20, 30, 40], \array_map(static fn (Document $row): ?int => self::joinRelationshipInteger($row->getAttribute('purchase.amount')), $rows));
            $this->assertSame(['Ada', 'Ada', 'Bob', 'Bob'], \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows));
        } finally {
            $this->deleteJoinRelationshipCollections($database);
        }
    }

    public function testJoinRelationshipKeepsEveryOtherRelationshipAsBefore(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $this->assertSame(
                ['o1/c1', 'o2/c1', 'o3/c2', 'o4/c2'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_ORDERS, [Query::join(self::JOIN_RELATIONSHIP_CUSTOMERS, 'buyer', [Query::on('customer', '$id')])], 'buyer'),
                'the child side of a one-to-many holds the parent ID in its own column',
            );
            $this->assertSame(
                ['c1/r1', 'c2/r2', 'c3/r1'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, [Query::join(self::JOIN_RELATIONSHIP_REGIONS, 'area', [Query::on('region', '$id')])], 'area'),
                'the parent side of a many-to-one holds the related ID in its own column',
            );

            try {
                $database->find(self::JOIN_RELATIONSHIP_REGIONS, [Query::join(self::JOIN_RELATIONSHIP_CUSTOMERS, 'resident', [Query::on('customers', '$id')])]);
                $this->fail('the child side of a many-to-one has no column to join on');
            } catch (QueryException $exception) {
                $this->assertStringContainsString('Cannot join on virtual relationship attribute: customers', $exception->getMessage());
            }
        } finally {
            $this->deleteJoinRelationshipCollections($database);
        }
    }

    public function testJoinRelationshipIsMatchedByEveryRead(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $join = Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]);
            $leftJoin = Query::leftJoin(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]);

            $this->assertSame(4, $database->count(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join]));
            $this->assertSame(5, $database->count(self::JOIN_RELATIONSHIP_CUSTOMERS, [$leftJoin]));
            $this->assertSame(100, self::joinRelationshipInteger($database->sum(self::JOIN_RELATIONSHIP_CUSTOMERS, 'purchase.amount', [$join])));
            $this->assertSame(
                [['Ada', 30], ['Bob', 70]],
                \array_map(
                    static fn (array $row): array => [$row['name'], self::joinRelationshipInteger($row['total'])],
                    $database->aggregate(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join, Query::sum('purchase.amount', 'total'), Query::groupBy(['name']), Query::orderAsc('name')]),
                ),
            );
            $this->assertSame(
                [4],
                \array_map(
                    static fn (array $row): ?int => self::joinRelationshipInteger($row['rows']),
                    $database->aggregate(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join, Query::count('*', 'rows')]),
                ),
            );
            $this->assertSame(
                20,
                self::joinRelationshipInteger($database->getDocument(self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', [
                    Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id'), Query::equal('purchase.status', ['open'])]),
                    Query::select(['name', 'purchase.amount']),
                ])->getAttribute('purchase.amount')),
            );
            $this->assertSame(['orders', '=', '$id'], $join->getJoinOnQueries()[0]->getValues(), 'the caller\'s join is left as given');
        } finally {
            $this->deleteJoinRelationshipCollections($database);
        }
    }

    public function testJoinRelationshipReadsOnlyTheJoinedRowsTheCallerCanRead(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $authorization = $database->getAuthorization();
            $database->updateCollection(self::JOIN_RELATIONSHIP_ORDERS, new CollectionUpdate(permissions: [Permission::create(Role::any())], documentSecurity: true));
            $authorization->skip(fn (): Document => $database->updateDocument(self::JOIN_RELATIONSHIP_ORDERS, 'o2', new Document(['$permissions' => [Permission::read(Role::user('bob'))]])));
            $queries = [Query::leftJoin(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')])];

            $this->withJoinRelationshipRoles($database, [Role::any()->toString()], function () use ($database, $queries): void {
                $this->assertSame(['c1/o1', 'c2/o3', 'c2/o4', 'c3/-'], $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, $queries, 'purchase'));
                $this->assertSame(80, self::joinRelationshipInteger($database->sum(self::JOIN_RELATIONSHIP_CUSTOMERS, 'purchase.amount', $queries)));
            });
            $this->withJoinRelationshipRoles($database, [Role::any()->toString(), Role::user('bob')->toString()], function () use ($database, $queries): void {
                $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4', 'c3/-'], $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, $queries, 'purchase'));
            });

            $database->updateCollection(self::JOIN_RELATIONSHIP_ORDERS, new CollectionUpdate(permissions: [Permission::create(Role::any())], documentSecurity: false));
            $this->withJoinRelationshipRoles($database, [Role::any()->toString()], function () use ($database, $queries): void {
                try {
                    $database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, $queries);
                    $this->fail('a joined collection readable neither at collection nor at document level is refused');
                } catch (AuthorizationException $exception) {
                    $this->assertStringContainsString(self::JOIN_RELATIONSHIP_ORDERS, $exception->getMessage());
                }
            });
        } finally {
            $this->deleteJoinRelationshipCollections($database);
        }
    }

    public function testJoinRelationshipCursorPagesEveryJoinedRowOnce(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            foreach ([
                'left join' => [Query::leftJoin(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]), ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4', 'c3/-']],
                'inner join' => [Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]), ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4']],
            ] as $case => [$join, $expected]) {
                $queries = [$join, Query::orderAsc('purchase.amount')];
                $all = \array_values($database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, [...$queries, Query::limit(100)]));
                $keys = \array_map(static fn (Document $row): string => self::joinRelationshipPair($row, 'purchase'), $all);
                $sorted = $keys;
                \sort($sorted);
                $this->assertSame($expected, $sorted, "{$case}: the unpaged read returns each joined row once");

                $forward = [];
                $cursor = null;
                for ($page = 0; $page <= \count($all); $page++) {
                    $rows = $database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
                    \array_push($forward, ...\array_map(static fn (Document $row): string => self::joinRelationshipPair($row, 'purchase'), $rows));
                    if (\count($rows) < 2) {
                        break;
                    }
                    $cursor = $rows[1];
                }
                $this->assertSame($keys, $forward, "{$case}: paging forward in pages of two");

                $backward = [];
                $cursor = $all[\count($all) - 1];
                for ($page = 0; $page <= \count($all); $page++) {
                    $rows = $database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, [...$queries, Query::limit(2), Query::cursorBefore($cursor)]);
                    $backward = [...\array_map(static fn (Document $row): string => self::joinRelationshipPair($row, 'purchase'), $rows), ...$backward];
                    if (\count($rows) < 2) {
                        break;
                    }
                    $cursor = $rows[0];
                }
                $this->assertSame(\array_slice($keys, 0, -1), $backward, "{$case}: paging backward in pages of two from the last row");
            }

            $queries = [Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]), Query::orderAsc('purchase.amount')];
            $this->assertSame(20, self::joinRelationshipInteger($database->resolveCursor(self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', CursorDirection::After, $queries)->getAttribute('purchase.amount')));
            $this->assertSame(30, self::joinRelationshipInteger($database->resolveCursor(self::JOIN_RELATIONSHIP_CUSTOMERS, 'c2', CursorDirection::Before, $queries)->getAttribute('purchase.amount')));

            $forward = [];
            $cursor = null;
            for ($page = 0; $page <= 4; $page++) {
                $rows = $database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, [
                    ...$queries,
                    Query::limit(2),
                    ...($cursor === null ? [] : [Query::cursorAfter($database->resolveCursor(self::JOIN_RELATIONSHIP_CUSTOMERS, $cursor, CursorDirection::After, $queries))]),
                ]);
                if ($rows === []) {
                    break;
                }
                \array_push($forward, ...\array_map(static fn (Document $row): string => self::joinRelationshipPair($row, 'purchase'), $rows));
                $cursor = $rows[\count($rows) - 1]->getId();
            }
            $this->assertSame(['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'], $forward, 'paging by the ID of each page\'s last row returns every row once');
        } finally {
            $this->deleteJoinRelationshipCollections($database);
        }
    }

    public function testJoinRelationshipIteratesEveryJoinedRowOnceOrFailsBeforeYieldingARow(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $join = Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]);

            $rows = $database->cursor(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join, Query::select(['name', 'purchase.amount']), Query::orderAsc('purchase.amount')], 2);
            /** @var int $yielded */
            $yielded = 0;
            try {
                foreach ($rows as $row) {
                    $yielded++;
                }
                $this->fail('a one-to-many join pairs a customer with several orders, so its next page orders by the joined $id the rows lack');
            } catch (OrderException $exception) {
                $this->assertSame('purchase.$id', $exception->getAttribute());
            }
            $this->assertSame(0, $yielded, 'the read is refused before the caller acts on any of its rows');

            $rows = $database->cursor(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join, Query::select(['name', 'purchase.$id', 'purchase.amount']), Query::orderAsc('purchase.amount')], 2);
            $this->assertSame(
                ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
                \array_map(static fn (Document $row): string => self::joinRelationshipPair($row, 'purchase'), \iterator_to_array($rows, false)),
            );
        } finally {
            $this->deleteJoinRelationshipCollections($database);
        }
    }

    public function testJoinRelationshipReadsOnlyTheCurrentTenantsRows(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database) || ! $database->hasSharedTables()) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $tenant = $database->getTenant();
        $integerTenants = $database->getIdAttributeType() === ColumnType::Integer;
        $first = $integerTenants ? 801 : 'tenant_801';
        $second = $integerTenants ? 802 : 'tenant_802';

        try {
            $database->setTenant(null);
            $this->deleteJoinRelationshipCollections($database);
            $this->createJoinRelationshipSchema($database);

            $database->setTenant($first);
            $this->createJoinRelationshipDocuments($database, [
                [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', ['name' => 'Ada']],
                [self::JOIN_RELATIONSHIP_ORDERS, 'o1', ['amount' => 10, 'status' => 'paid', 'customer' => 'c1']],
                [self::JOIN_RELATIONSHIP_ORDERS, 'o2', ['amount' => 20, 'status' => 'open', 'customer' => 'c1']],
            ]);

            $database->setTenant($second);
            $this->createJoinRelationshipDocuments($database, [
                [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', ['name' => 'Ada']],
                [self::JOIN_RELATIONSHIP_ORDERS, 'o9', ['amount' => 90, 'status' => 'paid', 'customer' => 'c1']],
            ]);

            $queries = [Query::leftJoin(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')])];

            $database->setTenant($first);
            $this->assertSame(['c1/o1', 'c1/o2'], $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, $queries, 'purchase'));
            $this->assertSame(30, self::joinRelationshipInteger($database->sum(self::JOIN_RELATIONSHIP_CUSTOMERS, 'purchase.amount', $queries)));

            $database->setTenant($second);
            $this->assertSame(['c1/o9'], $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, $queries, 'purchase'));
            $this->assertSame(1, $database->count(self::JOIN_RELATIONSHIP_CUSTOMERS, $queries));
        } finally {
            $database->setTenant(null);
            $this->deleteJoinRelationshipCollections($database);
            $database->setTenant($tenant);
        }
    }

    private function supportsJoinRelationships(Database $database): bool
    {
        $adapter = $database->getAdapter();

        return $adapter->supports(Capability::Joins)
            && $adapter->supports(Capability::Aggregations)
            && $adapter->hasFeature(Feature\Relationships::class);
    }

    /**
     * Customers c1 (region r1, orders o1 and o2), c2 (region r2, orders o3 and o4) and c3 (region r1, no orders).
     */
    private function seedJoinRelationships(Database $database): void
    {
        $this->deleteJoinRelationshipCollections($database);
        $this->createJoinRelationshipSchema($database);
        $this->createJoinRelationshipDocuments($database, [
            [self::JOIN_RELATIONSHIP_REGIONS, 'r1', ['name' => 'north']],
            [self::JOIN_RELATIONSHIP_REGIONS, 'r2', ['name' => 'south']],
            [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', ['name' => 'Ada', 'region' => 'r1']],
            [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c2', ['name' => 'Bob', 'region' => 'r2']],
            [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c3', ['name' => 'Cy', 'region' => 'r1']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o1', ['amount' => 10, 'status' => 'paid', 'region' => 'r1', 'customer' => 'c1']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o2', ['amount' => 20, 'status' => 'open', 'region' => 'r1', 'customer' => 'c1']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o3', ['amount' => 30, 'status' => 'paid', 'region' => 'r1', 'customer' => 'c2']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o4', ['amount' => 40, 'status' => 'paid', 'region' => 'r2', 'customer' => 'c2']],
        ]);
    }

    private function createJoinRelationshipSchema(Database $database): void
    {
        $permissions = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())];
        $database->createCollection(Collection::create(
            id: self::JOIN_RELATIONSHIP_CUSTOMERS,
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: self::JOIN_RELATIONSHIP_ORDERS,
            attributes: [
                Attribute::integer(key: 'amount'),
                Attribute::string(key: 'status', size: 16),
                Attribute::string(key: 'region', size: 16),
            ],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: self::JOIN_RELATIONSHIP_REGIONS,
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: $permissions,
        ));

        $database->createRelationship(self::JOIN_RELATIONSHIP_CUSTOMERS, Relationship::oneToMany(relatedCollection: self::JOIN_RELATIONSHIP_ORDERS, key: 'orders', twoWayKey: 'customer'));
        $database->createRelationship(self::JOIN_RELATIONSHIP_CUSTOMERS, Relationship::manyToOne(relatedCollection: self::JOIN_RELATIONSHIP_REGIONS, key: 'region', twoWay: true, twoWayKey: 'customers'));
    }

    /**
     * @param  list<array{string, string, array<string, mixed>}>  $documents
     */
    private function createJoinRelationshipDocuments(Database $database, array $documents): void
    {
        foreach ($documents as [$collection, $id, $attributes]) {
            $database->createDocument($collection, new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any())],
                ...$attributes,
            ]));
        }
    }

    private function deleteJoinRelationshipCollections(Database $database): void
    {
        foreach (self::JOIN_RELATIONSHIP_COLLECTIONS as $collection) {
            if ($database->collectionExists($collection)) {
                $database->deleteCollection($collection);
            }
        }
    }

    /**
     * Each row as `<main $id>/<joined $id>`, sorted, with `-` for a row the outer join did not match.
     *
     * @param  list<Query>  $queries
     * @return list<string>
     */
    private function joinRelationshipPairs(Database $database, string $collection, array $queries, string $alias): array
    {
        $pairs = \array_map(
            static fn (Document $row): string => self::joinRelationshipPair($row, $alias),
            $database->find($collection, [...$queries, Query::limit(100)]),
        );
        \sort($pairs);

        return $pairs;
    }

    private static function joinRelationshipPair(Document $row, string $alias): string
    {
        $joined = $row->getAttribute($alias.'.$id');

        return $row->getId().'/'.(\is_string($joined) ? $joined : '-');
    }

    /**
     * @param  list<string>  $roles
     * @param  callable(): void  $callback
     */
    private function withJoinRelationshipRoles(Database $database, array $roles, callable $callback): void
    {
        $authorization = $database->getAuthorization();
        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }

        try {
            $callback();
        } finally {
            $authorization->cleanRoles();
            foreach ($previous as $role) {
                $authorization->addRole($role);
            }
        }
    }

    private static function joinRelationshipInteger(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
