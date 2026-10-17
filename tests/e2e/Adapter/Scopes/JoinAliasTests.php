<?php

namespace Tests\E2E\Adapter\Scopes;

use Closure;
use Tests\E2E\Adapter\Support\JoinTables;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;

/**
 * A join given no alias is read under the ID of the table it joins, and refused with the reason when
 * that ID cannot serve as an alias.
 */
trait JoinAliasTests
{
    private const string ALIAS_ORDERS = 'jda_orders';

    private const string ALIAS_CUSTOMERS = 'jda_customers';

    private const string ALIAS_COLORS = 'jda_colors';

    private const string ALIAS_STAFF = 'jda_staff';

    private const string ALIAS_HYPHEN = 'jda-items';

    private const string ALIAS_DOT = 'jda.items';

    public function testJoinWithoutAliasIsReadUnderItsTableId(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);

        try {
            $rows = $database->find(self::ALIAS_ORDERS, [
                Query::join(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')]),
                Query::select(['total', self::ALIAS_CUSTOMERS.'.name']),
                Query::orderAsc('total'),
            ]);

            $this->assertSame(
                [[10, 'Ann'], [20, 'Bob']],
                \array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute(self::ALIAS_CUSTOMERS.'.name')], $rows),
            );
            $this->assertArrayHasKey(self::ALIAS_CUSTOMERS.'.name', $rows[0]->getArrayCopy());

            $this->assertSame(2, $database->count(self::ALIAS_ORDERS, [
                Query::join(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')]),
                Query::equal(self::ALIAS_CUSTOMERS.'.name', ['Ann', 'Bob']),
            ]));

            $parsed = Query::parse(Query::join(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')])->toString());
            $rows = $database->find(self::ALIAS_ORDERS, [
                $parsed,
                Query::select(['total', self::ALIAS_CUSTOMERS.'.name']),
                Query::equal(self::ALIAS_CUSTOMERS.'.name', ['Bob']),
            ]);
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]->getAttribute(self::ALIAS_CUSTOMERS.'.name'), 'a parsed join without an alias reads the same');
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    public function testLeftJoinWithoutAliasIsReadUnderItsTableId(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);

        try {
            $rows = $database->find(self::ALIAS_ORDERS, [
                Query::leftJoin(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')]),
                Query::select(['total', self::ALIAS_CUSTOMERS.'.name']),
                Query::orderAsc('total'),
            ]);

            $this->assertSame(
                [[10, 'Ann'], [20, 'Bob'], [30, null]],
                \array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute(self::ALIAS_CUSTOMERS.'.name')], $rows),
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    public function testCrossJoinWithoutAliasIsReadUnderItsTableId(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);

        try {
            $rows = $database->find(self::ALIAS_ORDERS, [
                Query::crossJoin(self::ALIAS_COLORS, ''),
                Query::select(['total', self::ALIAS_COLORS.'.name']),
                Query::lessThan('total', 30),
                Query::orderAsc('total'),
                Query::orderAsc(self::ALIAS_COLORS.'.name'),
            ]);

            $this->assertSame(
                [[10, 'blue'], [10, 'red'], [20, 'blue'], [20, 'red']],
                \array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute(self::ALIAS_COLORS.'.name')], $rows),
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    /**
     * The main collection is never referenced by its own ID, so a self-join without an alias reads the
     * joined copy under the table ID and the main copy bare.
     */
    public function testSelfJoinWithoutAliasIsReadUnderItsTableId(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);

        try {
            $rows = $database->find(self::ALIAS_STAFF, [
                Query::join(self::ALIAS_STAFF, '', [Query::on('managerId', '$id')]),
                Query::select(['name', self::ALIAS_STAFF.'.name']),
                Query::orderAsc('name'),
            ]);

            $this->assertSame(
                [['Cal', 'Ann'], ['Dee', 'Cal']],
                \array_map(static fn (Document $row): array => [$row->getAttribute('name'), $row->getAttribute(self::ALIAS_STAFF.'.name')], $rows),
            );

            $this->assertRejectedJoin(
                'The left column of a join condition must belong to the main collection or to a join declared before it: '.self::ALIAS_STAFF.'.managerId',
                fn (): mixed => $database->find(self::ALIAS_STAFF, [Query::join(self::ALIAS_STAFF, '', [Query::on(self::ALIAS_STAFF.'.managerId', '$id')])]),
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    public function testJoinWithoutAliasRefusesATableIdThatIsNotAnAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);

        try {
            foreach ([self::ALIAS_DOT] as $table) {
                $this->assertRejectedJoin(
                    "Table ID \"{$table}\" cannot be used as the join alias: an alias must start with a letter, a digit or an underscore and contain only letters, digits, underscores and hyphens. Pass an alias for the join",
                    fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join($table, '', [Query::on('customerId', '$id')])]),
                    $table,
                );
                $this->assertRejectedJoin(
                    "Table ID \"{$table}\" cannot be used as the join alias: an alias must start with a letter, a digit or an underscore and contain only letters, digits, underscores and hyphens. Pass an alias for the join",
                    fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::crossJoin($table, '')]),
                    $table.' cross join',
                );

                $rows = $database->find(self::ALIAS_ORDERS, [
                    Query::join($table, 'item', [Query::on('customerId', 'customerId')]),
                    Query::select(['total', 'item.label']),
                ]);
                $this->assertSame(['first'], \array_map(static fn (Document $row): mixed => $row->getAttribute('item.label'), $rows), $table.' with an alias');
            }

            $rows = $database->find(self::ALIAS_ORDERS, [
                Query::join(self::ALIAS_HYPHEN, '', [Query::on('customerId', 'customerId')]),
                Query::select(['total', self::ALIAS_HYPHEN.'.label']),
            ]);
            $this->assertSame(['first'], \array_map(static fn (Document $row): mixed => $row->getAttribute(self::ALIAS_HYPHEN.'.label'), $rows), 'a hyphenated table ID is an alias');

            $this->assertRejectedJoin(
                'Join alias must start with a letter, a digit or an underscore and contain only letters, digits, underscores and hyphens',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join(self::ALIAS_CUSTOMERS, 'my.alias', [Query::on('customerId', '$id')])]),
                'an explicit alias keeps its message',
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    public function testJoinWithoutAliasRefusesTheReservedMainAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);
        $this->createAliasCollection($database, Query::DEFAULT_ALIAS, [Attribute::string(key: 'customerId', size: 32)]);

        try {
            $this->assertRejectedJoin(
                'Table ID "'.Query::DEFAULT_ALIAS.'" cannot be used as the join alias: it is reserved for the main collection. Pass an alias for the join',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join(Query::DEFAULT_ALIAS, '', [Query::on('customerId', 'customerId')])]),
            );
            $this->assertRejectedJoin(
                'Join alias "'.Query::DEFAULT_ALIAS.'" is reserved for the main collection',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join(self::ALIAS_CUSTOMERS, Query::DEFAULT_ALIAS, [Query::on('customerId', '$id')])]),
                'an explicit alias keeps its message',
            );
        } finally {
            $this->cleanupAggCollections($database, [...$this->aliasFixtureCollections(), Query::DEFAULT_ALIAS]);
        }
    }

    public function testJoinWithoutAliasRefusesARelationshipKey(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);
        $database->createRelationship(self::ALIAS_ORDERS, Relationship::manyToOne(relatedCollection: self::ALIAS_CUSTOMERS, key: self::ALIAS_CUSTOMERS, twoWayKey: 'orders'));

        try {
            $this->assertRejectedJoin(
                'Table ID "'.self::ALIAS_CUSTOMERS.'" cannot be used as the join alias: it is the key of the relationship attribute "'.self::ALIAS_CUSTOMERS.'". Pass an alias for the join',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')])]),
            );
            $this->assertRejectedJoin(
                'Join alias "'.self::ALIAS_CUSTOMERS.'" is the key of the relationship attribute "'.self::ALIAS_CUSTOMERS.'": give the join another alias',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join(self::ALIAS_COLORS, self::ALIAS_CUSTOMERS, [Query::on('customerId', '$id')])]),
                'an explicit alias keeps its message',
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    public function testJoiningATableTwiceWithoutAliasesIsRefused(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);

        try {
            $twice = [
                Query::join(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')]),
                Query::leftJoin(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')]),
            ];
            $message = 'Table ID "'.self::ALIAS_CUSTOMERS.'" cannot be used as the join alias: another join of the query already uses it as its alias. Pass an alias for the join';

            $this->assertRejectedJoin($message, fn (): mixed => $database->find(self::ALIAS_ORDERS, $twice), 'find()');
            $this->assertRejectedJoin($message, fn (): mixed => $database->count(self::ALIAS_ORDERS, $twice), 'count()');
            $this->assertRejectedJoin($message, fn (): mixed => $database->find(self::ALIAS_ORDERS, [$twice[0], Query::crossJoin(self::ALIAS_CUSTOMERS, '')]), 'a cross join');

            $this->assertRejectedJoin(
                'Join alias "c" is declared more than once',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [
                    Query::join(self::ALIAS_CUSTOMERS, 'c', [Query::on('customerId', '$id')]),
                    Query::join(self::ALIAS_COLORS, 'c', [Query::on('customerId', '$id')]),
                ]),
                'an explicit alias keeps its message',
            );

            $rows = $database->find(self::ALIAS_ORDERS, [
                Query::join(self::ALIAS_CUSTOMERS, '', [Query::on('customerId', '$id')]),
                Query::leftJoin(self::ALIAS_CUSTOMERS, 'again', [Query::on('customerId', '$id')]),
                Query::select(['total', self::ALIAS_CUSTOMERS.'.name', 'again.name']),
                Query::orderAsc('total'),
            ]);
            $this->assertSame(
                [['Ann', 'Ann'], ['Bob', 'Bob']],
                \array_map(static fn (Document $row): array => [$row->getAttribute(self::ALIAS_CUSTOMERS.'.name'), $row->getAttribute('again.name')], $rows),
                'aliasing the second join reads both',
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    /**
     * A query hook may point a join at another table, as Appwrite points a public table ID at its
     * internal collection. The alias is fixed when the join is built, so rows stay keyed by the ID
     * the caller gave and a refusal names that ID, never the internal one.
     */
    public function testJoinWithoutAliasKeepsTheTableIdAHookRewrites(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasFixtures($database);
        $hook = new JoinTables(['customers' => self::ALIAS_CUSTOMERS, 'public.customers' => self::ALIAS_CUSTOMERS]);
        $database->addHook($hook);

        try {
            $rows = $database->find(self::ALIAS_ORDERS, [
                Query::parseQuery(['method' => 'join', 'attribute' => 'customers', 'values' => [Query::on('customerId', '$id')->toArray()]]),
                Query::select(['total', 'customers.name']),
                Query::orderAsc('total'),
            ]);

            $this->assertSame(['Ann', 'Bob'], \array_map(static fn (Document $row): mixed => $row->getAttribute('customers.name'), $rows));
            $this->assertArrayNotHasKey(self::ALIAS_CUSTOMERS.'.name', $rows[0]->getArrayCopy(), 'rows are not keyed by the internal table');

            $this->assertRejectedJoin(
                'Table ID "public.customers" cannot be used as the join alias: an alias must start with a letter, a digit or an underscore and contain only letters, digits, underscores and hyphens. Pass an alias for the join',
                fn (): mixed => $database->find(self::ALIAS_ORDERS, [Query::join('public.customers', '', [Query::on('customerId', '$id')])]),
            );
        } finally {
            $database->removeHook($hook);
            $this->cleanupAggCollections($database, $this->aliasFixtureCollections());
        }
    }

    /**
     * @param  non-empty-string  $message
     */
    private function assertRejectedJoin(string $message, Closure $read, string $case = ''): void
    {
        try {
            $read();
            $this->fail(($case === '' ? '' : $case.': ').'the join ran instead of being refused with "'.$message.'"');
        } catch (QueryException $error) {
            $this->assertStringEndsWith($message, $error->getMessage(), $case);
        }
    }

    /**
     * @return list<string>
     */
    private function aliasFixtureCollections(): array
    {
        return [self::ALIAS_ORDERS, self::ALIAS_CUSTOMERS, self::ALIAS_COLORS, self::ALIAS_STAFF, self::ALIAS_HYPHEN, self::ALIAS_DOT];
    }

    private function createAliasFixtures(Database $database): void
    {
        $this->cleanupAggCollections($database, [...$this->aliasFixtureCollections(), Query::DEFAULT_ALIAS]);

        $this->createAliasCollection($database, self::ALIAS_ORDERS, [Attribute::string(key: 'customerId', size: 32), Attribute::integer(key: 'total')]);
        $this->createAliasCollection($database, self::ALIAS_CUSTOMERS, [Attribute::string(key: 'name', size: 32)]);
        $this->createAliasCollection($database, self::ALIAS_COLORS, [Attribute::string(key: 'name', size: 32)]);
        $this->createAliasCollection($database, self::ALIAS_STAFF, [Attribute::string(key: 'name', size: 32), Attribute::string(key: 'managerId', size: 32)]);
        foreach ([self::ALIAS_HYPHEN, self::ALIAS_DOT] as $table) {
            $this->createAliasCollection($database, $table, [Attribute::string(key: 'customerId', size: 32), Attribute::string(key: 'label', size: 32)]);
        }

        foreach ([
            [self::ALIAS_ORDERS, 'o1', ['customerId' => 'ann', 'total' => 10]],
            [self::ALIAS_ORDERS, 'o2', ['customerId' => 'bob', 'total' => 20]],
            [self::ALIAS_ORDERS, 'o3', ['customerId' => 'nobody', 'total' => 30]],
            [self::ALIAS_CUSTOMERS, 'ann', ['name' => 'Ann']],
            [self::ALIAS_CUSTOMERS, 'bob', ['name' => 'Bob']],
            [self::ALIAS_COLORS, 'red', ['name' => 'red']],
            [self::ALIAS_COLORS, 'blue', ['name' => 'blue']],
            [self::ALIAS_STAFF, 'ann', ['name' => 'Ann', 'managerId' => '']],
            [self::ALIAS_STAFF, 'cal', ['name' => 'Cal', 'managerId' => 'ann']],
            [self::ALIAS_STAFF, 'dee', ['name' => 'Dee', 'managerId' => 'cal']],
            [self::ALIAS_HYPHEN, 'i1', ['customerId' => 'ann', 'label' => 'first']],
            [self::ALIAS_DOT, 'i1', ['customerId' => 'ann', 'label' => 'first']],
        ] as [$collection, $id, $attributes]) {
            $database->createDocument($collection, new Document(['$id' => $id, '$permissions' => [Permission::read(Role::any())], ...$attributes]));
        }
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function createAliasCollection(Database $database, string $id, array $attributes): void
    {
        $database->createCollection(Collection::create(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
    }
}
