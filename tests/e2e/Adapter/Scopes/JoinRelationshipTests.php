<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;

/**
 * A join on a one-to-many relationship attribute, or on the child side of a two-way many-to-one, matches this
 * document's `$id` against the column the related collection stores it in. The joined collection is read as a direct
 * list of it would be, by aggregate() as by every other read.
 */
trait JoinRelationshipTests
{
    private const string JOIN_RELATIONSHIP_CUSTOMERS = 'jrel_customers';

    private const string JOIN_RELATIONSHIP_ORDERS = 'jrel_orders';

    private const string JOIN_RELATIONSHIP_REGIONS = 'jrel_regions';

    private const string JOIN_RELATIONSHIP_WAREHOUSES = 'jrel_warehouses';

    private const array JOIN_RELATIONSHIP_COLLECTIONS = [
        self::JOIN_RELATIONSHIP_CUSTOMERS,
        self::JOIN_RELATIONSHIP_ORDERS,
        self::JOIN_RELATIONSHIP_REGIONS,
        self::JOIN_RELATIONSHIP_WAREHOUSES,
    ];

    public function testJoinRelationshipOnAOneToManyAttributeMatchesTheRelatedKey(): void
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
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, Query::leftJoin(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]), 'purchase'),
                'a left join keeps the customer without orders',
            );
            $this->assertSame(
                ['c1/o1', 'c1/o2', 'c2/o3', 'c2/o4'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', 'purchase.$id')]), 'purchase'),
            );
            $this->assertSame(
                ['c1/o3', 'c1/o4', 'c2/o1', 'c2/o2', 'c3/o1', 'c3/o2', 'c3/o3', 'c3/o4'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id', '!=')]), 'purchase'),
                'the operator of the condition is kept',
            );
            $this->assertSame(
                ['c1/o1', 'c2/o4'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [
                    Query::on('orders', '$id'),
                    Query::on('region', 'region'),
                    Query::equal('purchase.status', ['paid']),
                ]), 'purchase'),
                'the other conditions of the ON list compare what they named',
            );

            $rows = $database->find(self::JOIN_RELATIONSHIP_CUSTOMERS, [
                Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id')]),
                Query::select(['name', 'purchase.amount']),
                Query::orderAsc('purchase.amount'),
            ]);
            $this->assertSame([10, 20, 30, 40], \array_map(static fn (Document $row): int => (int) $row->getAttribute('purchase.amount'), $rows));
            $this->assertSame(['Ada', 'Ada', 'Bob', 'Bob'], \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows));
        } finally {
            $this->deleteJoinRelationshipCollections($database, ...self::JOIN_RELATIONSHIP_COLLECTIONS);
        }
    }

    public function testJoinRelationshipOnTheChildSideOfATwoWayManyToOneMatchesTheParentKey(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $this->assertSame(
                ['r1/c1', 'r1/c3', 'r2/c2'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_REGIONS, Query::join(self::JOIN_RELATIONSHIP_CUSTOMERS, 'resident', [Query::on('customers', '$id')]), 'resident'),
            );
            $this->assertSame(
                ['c1/r1', 'c2/r2', 'c3/r1'],
                $this->joinRelationshipPairs($database, self::JOIN_RELATIONSHIP_CUSTOMERS, Query::join(self::JOIN_RELATIONSHIP_REGIONS, 'area', [Query::on('region', '$id')]), 'area'),
                'the parent side holds the related ID in its own column',
            );
        } finally {
            $this->deleteJoinRelationshipCollections($database, ...self::JOIN_RELATIONSHIP_COLLECTIONS);
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

            $this->assertSame(4, $database->count(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join]));
            $this->assertSame(100, (int) $database->sum(self::JOIN_RELATIONSHIP_CUSTOMERS, 'purchase.amount', [$join]));
            $this->assertSame(
                [['Ada', 30], ['Bob', 70]],
                \array_map(
                    static fn (array $row): array => [$row['name'], (int) $row['total']],
                    $database->aggregate(self::JOIN_RELATIONSHIP_CUSTOMERS, [$join, Query::sum('purchase.amount', 'total'), Query::groupBy(['name']), Query::orderAsc('name')]),
                ),
            );
            $this->assertSame(
                20,
                (int) $database->getDocument(self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', [
                    Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', '$id'), Query::equal('purchase.status', ['open'])]),
                    Query::select(['name', 'purchase.amount']),
                ])->getAttribute('purchase.amount'),
            );
        } finally {
            $this->deleteJoinRelationshipCollections($database, ...self::JOIN_RELATIONSHIP_COLLECTIONS);
        }
    }

    public function testJoinRelationshipKeepsRefusingAttributesWithoutAColumnToMatch(): void
    {
        $database = $this->getDatabase();
        if (! $this->supportsJoinRelationships($database)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedJoinRelationships($database);

        try {
            $database->createCollection(Collection::create(
                id: self::JOIN_RELATIONSHIP_WAREHOUSES,
                attributes: [Attribute::string(key: 'name', size: 16)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
            $database->createRelationship(self::JOIN_RELATIONSHIP_ORDERS, Relationship::manyToOne(relatedCollection: self::JOIN_RELATIONSHIP_WAREHOUSES, key: 'warehouse', twoWayKey: 'stock'));

            $refusals = [
                'the child side of a one-way many-to-one' => [self::JOIN_RELATIONSHIP_WAREHOUSES, Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('stock', '$id')]), 'stock'],
                'a join of another collection than the related one' => [self::JOIN_RELATIONSHIP_CUSTOMERS, Query::join(self::JOIN_RELATIONSHIP_REGIONS, 'area', [Query::on('orders', '$id')]), 'orders'],
                'a comparison with another column than the related $id' => [self::JOIN_RELATIONSHIP_CUSTOMERS, Query::join(self::JOIN_RELATIONSHIP_ORDERS, 'purchase', [Query::on('orders', 'status')]), 'orders'],
            ];

            foreach ($refusals as $case => [$collection, $join, $attribute]) {
                try {
                    $database->find($collection, [$join]);
                    $this->fail("{$case} must be refused");
                } catch (QueryException $exception) {
                    $this->assertStringContainsString('Cannot join on virtual relationship attribute: '.$attribute, $exception->getMessage(), $case);
                }
            }
        } finally {
            $this->deleteJoinRelationshipCollections($database, ...self::JOIN_RELATIONSHIP_COLLECTIONS);
        }
    }

    public function testJoinRelationshipAggregateRefusesAJoinedCollectionWithoutGrantOrDocumentSecurity(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->seedJoinRelationshipReadRule($database, 'jrel_deny', joinedPermissions: [Permission::create(Role::any())], joinedDocumentSecurity: false);

        try {
            $queries = [Query::join($joined, 'entry', [Query::on('$id', 'mainId')]), Query::count('*', 'rows')];

            $this->withJoinRelationshipRoles($database, [Role::any()->toString()], function () use ($database, $main, $queries): void {
                try {
                    $database->aggregate($main, $queries);
                    $this->fail('aggregate() must refuse a joined collection readable neither at collection nor at document level');
                } catch (AuthorizationException $exception) {
                    $this->assertStringContainsString('jrel_deny_j', $exception->getMessage());
                }
            });

            $rows = $database->getAuthorization()->skip(fn (): array => $database->aggregate($main, $queries));
            $this->assertSame(2, (int) $rows[0]['rows'], 'with authorization disabled every joined row is read');
        } finally {
            $this->deleteJoinRelationshipCollections($database, $main, $joined);
        }
    }

    public function testJoinRelationshipAggregateFiltersAJoinedCollectionPerDocument(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->seedJoinRelationshipReadRule($database, 'jrel_owned', joinedPermissions: [Permission::create(Role::any())], joinedDocumentSecurity: true);

        try {
            $this->withJoinRelationshipRoles($database, [Role::any()->toString(), Role::user('alice')->toString()], function () use ($database, $main, $joined): void {
                $joinedRows = $database->aggregate($joined, [Query::count('*', 'rows'), Query::sum('amount', 'total')]);
                $rows = $database->aggregate($main, [
                    Query::join($joined, 'entry', [Query::on('$id', 'mainId')]),
                    Query::count('*', 'rows'),
                    Query::sum('entry.amount', 'total'),
                ]);

                $this->assertSame([1, 5], [(int) $joinedRows[0]['rows'], (int) $joinedRows[0]['total']], 'a direct aggregate reads only the rows the caller holds read on');
                $this->assertSame([1, 5], [(int) $rows[0]['rows'], (int) $rows[0]['total']], 'an aggregate over the join reads the same rows');
            });
        } finally {
            $this->deleteJoinRelationshipCollections($database, $main, $joined);
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
        $this->deleteJoinRelationshipCollections($database, ...self::JOIN_RELATIONSHIP_COLLECTIONS);

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

        $documents = [
            [self::JOIN_RELATIONSHIP_REGIONS, 'r1', ['name' => 'north']],
            [self::JOIN_RELATIONSHIP_REGIONS, 'r2', ['name' => 'south']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o1', ['amount' => 10, 'status' => 'paid', 'region' => 'r1']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o2', ['amount' => 20, 'status' => 'open', 'region' => 'r1']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o3', ['amount' => 30, 'status' => 'paid', 'region' => 'r1']],
            [self::JOIN_RELATIONSHIP_ORDERS, 'o4', ['amount' => 40, 'status' => 'paid', 'region' => 'r2']],
            [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c1', ['name' => 'Ada', 'region' => 'r1', 'orders' => ['o1', 'o2']]],
            [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c2', ['name' => 'Bob', 'region' => 'r2', 'orders' => ['o3', 'o4']]],
            [self::JOIN_RELATIONSHIP_CUSTOMERS, 'c3', ['name' => 'Cy', 'region' => 'r1']],
        ];

        foreach ($documents as [$collection, $id, $attributes]) {
            $database->createDocument($collection, new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any())],
                ...$attributes,
            ]));
        }
    }

    private function deleteJoinRelationshipCollections(Database $database, string ...$collections): void
    {
        foreach ($collections as $collection) {
            if ($database->collectionExists($collection)) {
                $database->deleteCollection($collection);
            }
        }
    }

    /**
     * Each row as `<main $id>/<joined $id>`, sorted, with `-` for a row the outer join did not match.
     *
     * @return list<string>
     */
    private function joinRelationshipPairs(Database $database, string $collection, Query $join, string $alias): array
    {
        $pairs = \array_map(
            static function (Document $row) use ($alias): string {
                $joined = $row->getAttribute($alias.'.$id');

                return $row->getId().'/'.(\is_string($joined) ? $joined : '-');
            },
            $database->find($collection, [$join, Query::limit(100)]),
        );
        \sort($pairs);

        return $pairs;
    }

    /**
     * A main collection every caller reads, with one document, and a joined collection holding two entries for it:
     * one readable by user alice and one by user bob.
     *
     * @param  list<string>  $joinedPermissions
     * @return array{string, string}
     */
    private function seedJoinRelationshipReadRule(Database $database, string $prefix, array $joinedPermissions, bool $joinedDocumentSecurity): array
    {
        $main = $prefix.'_m';
        $joined = $prefix.'_j';
        $this->deleteJoinRelationshipCollections($database, $main, $joined);

        $database->createCollection(Collection::create(
            id: $main,
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createCollection(Collection::create(
            id: $joined,
            attributes: [Attribute::string(key: 'mainId', size: 36), Attribute::integer(key: 'amount')],
            permissions: $joinedPermissions,
            documentSecurity: $joinedDocumentSecurity,
        ));

        $database->createDocument($main, new Document(['$id' => 'customer', 'name' => 'Customer', '$permissions' => [Permission::read(Role::any())]]));
        $database->getAuthorization()->skip(function () use ($database, $joined): void {
            $database->createDocument($joined, new Document(['$id' => 'alice-entry', 'mainId' => 'customer', 'amount' => 5, '$permissions' => [Permission::read(Role::user('alice'))]]));
            $database->createDocument($joined, new Document(['$id' => 'bob-entry', 'mainId' => 'customer', 'amount' => 7, '$permissions' => [Permission::read(Role::user('bob'))]]));
        });

        return [$main, $joined];
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
}
