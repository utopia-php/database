<?php

namespace Tests\E2E\Adapter\Scopes;

use Closure;
use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

/**
 * A join is read under an alias shaped like an Appwrite ID: a unique ID that starts with a digit, or
 * an ID with a hyphen. Every read is compared with the same read under a plain alias, so the alias
 * reaches each clause it can appear in quoted.
 */
trait JoinAliasIdTests
{
    private const string ID_ORDERS = 'jai_orders';

    private const string ID_ITEMS = 'jai_items';

    private const string ID_UNIQUE = '6ad1e9ed691819ab52ee';

    private const string ID_HYPHEN = 'order-items';

    private const string ID_LONG = 'a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8';

    private const string KEY_LONG = 'label_of_the_item_kept_for_ordering_';

    private const string KEY_LONG_NUMBER = 'quantity_of_the_item_kept_for_totals';

    private const string KEY_SIXTY = 'a_sixty_two_character_attribute_key_whose_name_nearly_fills_63';

    private const string BASELINE = 'plain';

    public function testJoinWithoutAliasReadsAnAppwriteTableId(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasIdFixtures($database);

        try {
            foreach ([self::ID_UNIQUE, self::ID_HYPHEN] as $table) {
                $this->assertSame(
                    [[10, 'first'], [20, 'second']],
                    $this->readAliasIdRows($database, Query::join($table, '', [Query::on('customerId', 'customerId')]), $table),
                    $table.': the visible items under the table ID',
                );

                foreach ($this->aliasIdJoins() as $method => $join) {
                    $this->assertSame(
                        $this->readAliasIdShapes($database, $join($table, self::BASELINE), self::BASELINE),
                        $this->readAliasIdShapes($database, $join($table, ''), $table),
                        "{$table} {$method} without an alias reads as under a plain alias",
                    );
                }
            }
        } finally {
            $this->cleanupAggCollections($database, $this->aliasIdCollections());
        }
    }

    public function testJoinAliasShapedLikeAnAppwriteIdIsRead(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasIdFixtures($database);

        try {
            foreach (['6ad1e9ed691819ab52ef', 'order-lines', '_6ad1', '0'] as $alias) {
                $this->assertSame(
                    [[10, 'first'], [20, 'second']],
                    $this->readAliasIdRows($database, Query::join(self::ID_ITEMS, $alias, [Query::on('customerId', 'customerId')]), $alias),
                    $alias.': the visible items under the alias',
                );

                foreach ($this->aliasIdJoins() as $method => $join) {
                    $this->assertSame(
                        $this->readAliasIdShapes($database, $join(self::ID_ITEMS, self::BASELINE), self::BASELINE),
                        $this->readAliasIdShapes($database, $join(self::ID_ITEMS, $alias), $alias),
                        "{$method} aliased {$alias} reads as under a plain alias",
                    );
                }
            }

            $rows = $database->find(self::ID_ORDERS, [
                Query::join(self::ID_UNIQUE, '', [Query::on('customerId', 'customerId')]),
                Query::join(self::ID_ITEMS, 'order-lines', [Query::on('customerId', 'customerId')]),
                Query::select(['total', self::ID_UNIQUE.'.label', 'order-lines.label']),
                Query::equal('order-lines.label', ['first']),
            ]);
            $this->assertSame([[10, 'first', 'first']], \array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute(self::ID_UNIQUE.'.label'), $row->getAttribute('order-lines.label')], $rows), 'two such aliases in one read');
        } finally {
            $this->cleanupAggCollections($database, $this->aliasIdCollections());
        }
    }

    /**
     * Postgres folds identifiers past 63 bytes, so a long alias and a long attribute must still come
     * back under their full `alias.attribute` key.
     */
    public function testJoinWithoutAliasReadsALongTableIdAndAttribute(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasIdFixtures($database);

        try {
            $key = self::ID_LONG.'.'.self::KEY_LONG;
            $rows = $database->find(self::ID_ORDERS, [
                Query::join(self::ID_LONG, '', [Query::on('customerId', 'customerId')]),
                Query::select(['total', $key]),
                Query::orderAsc('total'),
                Query::orderDesc($key),
            ]);

            $this->assertSame([[10, 'first'], [20, 'second']], \array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute($key)], $rows));

            $rows = $database->find(self::ID_ORDERS, [
                Query::fullOuterJoin(self::ID_LONG, '', [Query::on('customerId', 'customerId')]),
                Query::select(['total', $key]),
                Query::orderAsc('total'),
            ]);
            $this->assertSame([[10, 'first'], [20, 'second'], [30, null]], \array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute($key)], $rows), 'a full outer join');

            $document = $database->getDocument(self::ID_ORDERS, 'o2', [
                Query::join(self::ID_LONG, '', [Query::on('customerId', 'customerId')]),
                Query::select(['total', $key]),
            ]);
            $this->assertSame('second', $document->getAttribute($key), 'getDocument()');

            $quantity = self::ID_LONG.'.'.self::KEY_LONG_NUMBER;
            $this->assertSame(5, $database->sum(self::ID_ORDERS, $quantity, [
                Query::join(self::ID_LONG, '', [Query::on('customerId', 'customerId')]),
                Query::distinct(),
                Query::select([$quantity]),
            ]), 'a distinct sum of a joined column');

            $sixty = $database->find(self::ID_ORDERS, [
                Query::join(self::ID_ITEMS, 'i', [Query::on('customerId', 'customerId')]),
                Query::select(['total', 'i.'.self::KEY_SIXTY]),
                Query::orderAsc('total'),
            ]);
            $this->assertSame(['first', 'second'], \array_map(static fn (Document $row): mixed => $row->getAttribute('i.'.self::KEY_SIXTY), $sixty), 'a column whose own name nearly fills the label');

            $groups = $database->aggregate(self::ID_ORDERS, [
                Query::join(self::ID_LONG, '', [Query::on('customerId', 'customerId')]),
                Query::count('*', 'cnt'),
                Query::groupBy([$key]),
            ]);
            $labels = \array_map(static fn (array $row): mixed => $row[$key] ?? null, $groups);
            \sort($labels);
            $this->assertSame(['first', 'second'], $labels, 'a group of the joined column');
            $this->assertSame(1, $database->count(self::ID_ORDERS, [
                Query::join(self::ID_LONG, '', [Query::on('customerId', 'customerId')]),
                Query::equal($key, ['second']),
            ]));
        } finally {
            $this->cleanupAggCollections($database, $this->aliasIdCollections());
        }
    }

    public function testJoinAliasesOutsideTheIdCharsetAreStillRefused(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->createAliasIdFixtures($database);
        $invalid = 'Join alias must start with a letter, a digit or an underscore and contain only letters, digits, underscores and hyphens';

        try {
            foreach (['-items', 'order.items', 'order items', 'x`y', 'x"y'] as $alias) {
                $this->assertAliasIdRefused($invalid, fn (): mixed => $database->find(self::ID_ORDERS, [Query::join(self::ID_ITEMS, $alias, [Query::on('customerId', 'customerId')])]), $alias);
            }

            $this->assertAliasIdRefused(
                'Join alias must be at most 63 characters',
                fn (): mixed => $database->find(self::ID_ORDERS, [Query::join(self::ID_ITEMS, \str_repeat('a', 64), [Query::on('customerId', 'customerId')])]),
                'a 64 character alias',
            );
            $this->assertAliasIdRefused(
                'Join alias "'.\strtoupper(Query::DEFAULT_ALIAS).'" is reserved for the main collection',
                fn (): mixed => $database->find(self::ID_ORDERS, [Query::join(self::ID_ITEMS, \strtoupper(Query::DEFAULT_ALIAS), [Query::on('customerId', 'customerId')])]),
                'the reserved alias',
            );
            $this->assertAliasIdRefused(
                'Join alias "ORDER-ITEMS" is declared more than once',
                fn (): mixed => $database->find(self::ID_ORDERS, [
                    Query::join(self::ID_HYPHEN, '', [Query::on('customerId', 'customerId')]),
                    Query::join(self::ID_ITEMS, 'ORDER-ITEMS', [Query::on('customerId', 'customerId')]),
                ]),
                'a hyphenated alias repeated in another case',
            );
        } finally {
            $this->cleanupAggCollections($database, $this->aliasIdCollections());
        }
    }

    /**
     * @return array<string, Closure(string, string): Query>
     */
    private function aliasIdJoins(): array
    {
        $on = static fn (): array => [Query::on('customerId', 'customerId')];

        return [
            'join' => static fn (string $table, string $alias): Query => Query::join($table, $alias, $on()),
            'leftJoin' => static fn (string $table, string $alias): Query => Query::leftJoin($table, $alias, $on()),
            'rightJoin' => static fn (string $table, string $alias): Query => Query::rightJoin($table, $alias, $on()),
            'fullOuterJoin' => static fn (string $table, string $alias): Query => Query::fullOuterJoin($table, $alias, $on()),
            'crossJoin' => static fn (string $table, string $alias): Query => Query::crossJoin($table, $alias),
        ];
    }

    /**
     * @return list<array{mixed, mixed}>
     */
    private function readAliasIdRows(Database $database, Query $join, string $alias): array
    {
        $rows = $database->find(self::ID_ORDERS, [
            $join,
            Query::select(['total', $alias.'.label']),
            Query::orderAsc('total'),
            Query::orderAsc($alias.'.label'),
        ]);

        foreach ($rows as $row) {
            $this->assertArrayHasKey($alias.'.label', $row->getArrayCopy(), 'rows are keyed alias.column');
        }

        return \array_values(\array_map(static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute($alias.'.label')], $rows));
    }

    /**
     * Every read a join alias reaches, each as plain data with the alias written as `@`, or the refusal
     * it gives.
     *
     * @return array<string, mixed>
     */
    private function readAliasIdShapes(Database $database, Query $join, string $alias): array
    {
        $label = $alias.'.label';
        $pair = static fn (Document $row): array => [$row->getAttribute('total'), $row->getAttribute($label)];

        $reads = [
            'find' => fn (): array => $this->readAliasIdRows($database, $join, $alias),
            'filter' => fn (): array => \array_map($pair, $database->find(self::ID_ORDERS, [$join, Query::select(['total', $label]), Query::equal($label, ['second']), Query::orderAsc('total')])),
            'filter by an ON condition' => fn (): array => \array_map($pair, $database->find(self::ID_ORDERS, [$join, Query::select(['total', $label]), Query::notEqual($label, 'zzz'), Query::orderDesc($label), Query::orderAsc('total')])),
            'order' => fn (): array => \array_map($pair, $database->find(self::ID_ORDERS, [$join, Query::select(['total', $label]), Query::orderDesc($label), Query::orderAsc('total')])),
            'wildcard' => fn (): array => \array_map(static fn (Document $row): mixed => $row->getAttribute($label), $database->find(self::ID_ORDERS, [$join, Query::select(['total', $alias.'.*']), Query::orderAsc('total'), Query::orderAsc($label)])),
            'count' => fn (): int => $database->count(self::ID_ORDERS, [$join]),
            'count filtered' => fn (): int => $database->count(self::ID_ORDERS, [$join, Query::isNotNull($label)]),
            'sum' => fn (): mixed => $database->sum(self::ID_ORDERS, 'total', [$join]),
            'page' => function () use ($database, $join, $alias, $label, $pair): array {
                $queries = [$join, Query::select(['total', $label, $alias.'.$id']), Query::orderAsc('total'), Query::orderAsc($label)];
                $first = $database->find(self::ID_ORDERS, [...$queries, Query::limit(1)]);

                return \array_map($pair, $first === [] ? [] : $database->find(self::ID_ORDERS, [...$queries, Query::cursorAfter($first[0]), Query::limit(2)]));
            },
            'group' => function () use ($database, $join, $label): array {
                $groups = \array_map(
                    static fn (array $row): array => [$row[$label] ?? null, $row['cnt'] ?? null, $row['spent'] ?? null],
                    $database->aggregate(self::ID_ORDERS, [$join, Query::count('*', 'cnt'), Query::sum('total', 'spent'), Query::groupBy([$label]), Query::having([Query::greaterThan('cnt', 0)])]),
                );
                \sort($groups);

                return $groups;
            },
            'aggregate a joined column' => fn (): array => \array_map(
                static fn (array $row): array => [$row['labels'] ?? null, $row['rows'] ?? null],
                $database->aggregate(self::ID_ORDERS, [$join, Query::countDistinct($label, 'labels'), Query::count('*', 'rows')]),
            ),
        ];

        $shapes = [];
        foreach ($reads as $name => $read) {
            try {
                $shapes[$name] = $read();
            } catch (QueryException $error) {
                $shapes[$name] = 'refused: '.\str_replace($alias, '@', $error->getMessage());
            } catch (Throwable $error) {
                $this->fail("{$name} under {$alias} failed: ".$error::class.': '.$error->getMessage());
            }
        }

        return $shapes;
    }

    private function assertAliasIdRefused(string $message, Closure $read, string $case): void
    {
        try {
            $read();
            $this->fail($case.': the join ran instead of being refused with "'.$message.'"');
        } catch (QueryException $error) {
            $this->assertStringContainsString($message, $error->getMessage(), $case);
        }
    }

    /**
     * @return list<string>
     */
    private function aliasIdCollections(): array
    {
        return [self::ID_ORDERS, self::ID_ITEMS, self::ID_UNIQUE, self::ID_HYPHEN, self::ID_LONG];
    }

    private function createAliasIdFixtures(Database $database): void
    {
        $this->cleanupAggCollections($database, $this->aliasIdCollections());

        $database->createCollection(Collection::create(
            id: self::ID_ORDERS,
            attributes: [Attribute::string(key: 'customerId', size: 32), Attribute::integer(key: 'total')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        foreach ([['o1', 'ann', 10], ['o2', 'bob', 20], ['o3', 'nobody', 30]] as [$id, $customer, $total]) {
            $database->createDocument(self::ID_ORDERS, new Document(['$id' => $id, 'customerId' => $customer, 'total' => $total]));
        }

        foreach ([self::ID_ITEMS => 'label', self::ID_UNIQUE => 'label', self::ID_HYPHEN => 'label', self::ID_LONG => self::KEY_LONG] as $table => $key) {
            $database->createCollection(Collection::create(
                id: $table,
                attributes: [
                    Attribute::string(key: 'customerId', size: 32),
                    Attribute::string(key: $key, size: 32),
                    Attribute::integer(key: self::KEY_LONG_NUMBER, required: false),
                    Attribute::string(key: self::KEY_SIXTY, size: 32, required: false),
                ],
                permissions: [Permission::create(Role::any())],
                documentSecurity: true,
            ));
            foreach ([['i1', 'ann', 'first', Role::any()], ['i2', 'bob', 'second', Role::any()], ['i3', 'ann', 'hidden', Role::user('someone-else')]] as [$id, $customer, $value, $reader]) {
                $database->createDocument($table, new Document([
                    '$id' => $id,
                    'customerId' => $customer,
                    $key => $value,
                    self::KEY_LONG_NUMBER => $value === 'second' ? 3 : 2,
                    self::KEY_SIXTY => $value,
                    '$permissions' => [Permission::read($reader)],
                ]));
            }
        }
    }
}
