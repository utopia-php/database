<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature\RawQuery;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Storage;

trait MySQLJoinPlanTests
{
    private const int MAX_PARTIAL_PLANS = 10_000;

    private const array READABLE = ['alice', 'bob', 'carol', 'dave', 'frank'];

    private const string HIDDEN = 'eve';

    private const int OUTER_JOIN_LINKS = 4;

    private const string SEMI_JOIN_PLAN = '/<subquery\d+>|semijoin|weedout|Remove duplicates from input/i';

    public function testEightCheckedSelfJoinsKeepTheJoinOrderSearchSmall(): void
    {
        $database = $this->getDatabase();
        $customers = 'checked_self_joins';
        $database->createCollection(Collection::create(id: $customers, permissions: [Permission::create(Role::any())], documentSecurity: true));

        try {
            $this->seed($database, $customers, [Role::any(), Role::user(self::HIDDEN)]);

            $this->assertJoinOrderSearchStaysSmall($database, $customers, \array_map(
                static fn (int $peer): Query => Query::join($customers, 'peer'.$peer, [Query::on('$id', '$id')]),
                \range(1, 8),
            ));
        } finally {
            $database->deleteCollection($customers);
        }
    }

    public function testEightJoinsOfWhichFourAreCheckedKeepTheJoinOrderSearchSmall(): void
    {
        $database = $this->getDatabase();
        $customers = 'partly_checked_joins';
        $labels = 'partly_checked_labels';
        $database->createCollection(Collection::create(id: $customers, permissions: [Permission::create(Role::any())], documentSecurity: true));
        $database->createCollection(Collection::create(id: $labels, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));

        try {
            $this->seed($database, $customers, [Role::any(), Role::user(self::HIDDEN)]);
            $this->seed($database, $labels, [Role::any(), Role::any()]);

            $this->assertJoinOrderSearchStaysSmall($database, $customers, [
                ...\array_map(
                    static fn (int $peer): Query => Query::join($customers, 'peer'.$peer, [Query::on('$id', '$id')]),
                    \range(1, 4),
                ),
                ...\array_map(
                    static fn (int $label): Query => Query::join($labels, 'label'.$label, [Query::on('name', 'name')]),
                    \range(1, 4),
                ),
            ]);
        } finally {
            $database->deleteCollection($labels);
            $database->deleteCollection($customers);
        }
    }

    /**
     * A semi-joined check in an outer join's ON clause is run by scanning its materialised rows once
     * per outer row (seconds with one checked link, a timeout with two), so every outer-joined check
     * carries NO_SEMIJOIN below LARGE_JOIN too, and the plan runs none of them as a semi-join.
     */
    public function testLeftJoinedChecksStaySubqueries(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();
        $this->assertInstanceOf(RawQuery::class, $adapter);

        $customers = 'left_joined_customers';
        $database->createCollection(Collection::create(id: $customers, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $links = \array_map(static fn (int $link): string => 'left_joined_link'.$link, \range(1, self::OUTER_JOIN_LINKS));
        foreach ($links as $link) {
            $database->createCollection(Collection::create(id: $link, permissions: [Permission::create(Role::any())], documentSecurity: true));
        }

        $authorization = $database->getAuthorization();
        $roles = $authorization->getRoles();

        try {
            $this->seed($database, $customers, [Role::any(), Role::any()]);
            foreach ($links as $link) {
                $this->seed($database, $link, [Role::any(), Role::user(self::HIDDEN)]);
            }

            $authorization->cleanRoles();
            $authorization->addRole(Role::any()->toString());
            $authorization->addRole(Role::user('caller')->toString());

            for ($count = 1; $count <= self::OUTER_JOIN_LINKS; $count++) {
                $joins = \array_map(
                    static fn (int $link): Query => Query::leftJoin($links[$link - 1], 'c'.$link, [Query::on('name', 'name')]),
                    \range(1, $count),
                );

                [$found, $statement] = $this->tracing($adapter, fn (): array => $database->find($customers, [
                    ...$joins,
                    Query::orderAsc('name'),
                    Query::limit(100),
                ]));

                $expected = [];
                foreach ([...self::READABLE, self::HIDDEN] as $id) {
                    $expected[] = [$id, $id === self::HIDDEN ? null : $id];
                }
                \usort($expected, static fn (array $left, array $right): int => \strcmp(\ucfirst($left[0]), \ucfirst($right[0])));
                $this->assertSame($expected, \array_map(
                    static fn (Document $document): array => [$document->getId(), $document->getAttribute('c'.$count.'.$id')],
                    $found,
                ), 'A left join keeps every customer and pairs only the rows the caller may read');

                $this->assertSame($count, \substr_count($statement, '/*+ NO_SEMIJOIN() */'), 'Every left-joined check carries NO_SEMIJOIN: '.$statement);

                $plan = $this->treePlan($adapter, $statement);
                $this->assertDoesNotMatchRegularExpression(self::SEMI_JOIN_PLAN, $plan, 'A left-joined check ran as a semi-join with '.$count.' links: '.$plan);
            }
        } finally {
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }
            foreach ($links as $link) {
                $database->deleteCollection($link);
            }
            $database->deleteCollection($customers);
        }
    }

    /**
     * Under shared tables every index but `_uid`'s and the primary key leads with `_tenant`, and MySQL looks a
     * joined table's rows up by an equality on the tenant alone, once per row the join pairs, when no index
     * serves the join: the tenant's whole table every time. The join reads like the one over a table of its
     * own: no lookup is considered for the table no index serves, the lookup its index serves still is.
     */
    public function testJoinNoIndexServesIsNotLookedUpByTheTenant(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();
        $this->assertInstanceOf(RawQuery::class, $adapter);

        $customers = 'tenant_lookup_customers';
        $labels = 'tenant_lookup_labels';
        $tags = 'tenant_lookup_tags';
        $open = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $customers, permissions: [Permission::create(Role::any())], documentSecurity: true));
        $database->createCollection(Collection::create(id: $labels, permissions: $open, documentSecurity: false));
        $database->createCollection(Collection::create(id: $tags, permissions: $open, documentSecurity: false));

        $authorization = $database->getAuthorization();
        $roles = $authorization->getRoles();

        try {
            $this->seed($database, $customers, [Role::any(), Role::user(self::HIDDEN)]);
            $this->seed($database, $labels, [Role::any(), Role::any()]);
            $this->seed($database, $tags, [Role::any(), Role::any()]);
            $database->createIndex($tags, Index::key('name_key', ['name']));

            $authorization->cleanRoles();
            $authorization->addRole(Role::any()->toString());
            $authorization->addRole(Role::user('caller')->toString());

            [$found, $trace] = $this->traced($adapter, '`named`', fn (): array => $database->find($customers, [
                Query::join($labels, 'named', [Query::on('name', 'name')]),
                Query::join($tags, 'tagged', [Query::on('name', 'name')]),
                Query::select(['name']),
                Query::limit(100),
            ]));

            $ids = \array_map(static fn (Document $document): string => $document->getId(), $found);
            \sort($ids);
            $this->assertSame(self::READABLE, $ids);

            $this->assertSame([], $this->lookups($trace, 'named'), 'A join no index serves is not looked up');
            $this->assertContains('name_key', $this->lookups($trace, 'tagged'), 'A join its index serves is looked up through it');
        } finally {
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }
            foreach ([$tags, $labels, $customers] as $collection) {
                $database->deleteCollection($collection);
            }
        }
    }

    /**
     * Runs $read with the optimizer trace on and returns its result and the trace of the statement naming $marker.
     *
     * @param  callable(): array<Document>  $read
     * @return array{array<Document>, array<mixed>}
     */
    private function traced(RawQuery $adapter, string $marker, callable $read): array
    {
        $adapter->rawMutation("SET SESSION optimizer_trace = 'enabled=on', optimizer_trace_offset = -5, optimizer_trace_limit = 5, optimizer_trace_max_mem_size = 67108864");

        try {
            $found = $read();
            $traces = $adapter->rawQuery('SELECT QUERY, TRACE FROM information_schema.OPTIMIZER_TRACE');
        } finally {
            $adapter->rawMutation("SET SESSION optimizer_trace = 'enabled=off'");
        }

        $matching = [];
        foreach ($traces as $trace) {
            $query = $trace->getAttribute('QUERY');
            $text = $trace->getAttribute('TRACE');
            if (\is_string($query) && \is_string($text) && \str_contains($query, $marker)) {
                $matching[] = $text;
            }
        }
        $this->assertNotSame([], $matching, 'The optimizer trace must hold the read');

        $trace = \json_decode($matching[\array_key_last($matching)], true);
        $this->assertIsArray($trace, 'The optimizer trace must fit its memory');

        return [$found, $trace];
    }

    /**
     * The indexes the optimizer considered looking the rows of $alias up through.
     *
     * @param  array<mixed>  $trace
     * @return list<string>
     */
    private function lookups(array $trace, string $alias): array
    {
        $indexes = [];
        $walk = static function (mixed $node) use (&$walk, &$indexes, $alias): void {
            if (! \is_array($node)) {
                return;
            }

            $table = $node['table'] ?? null;
            $best = $node['best_access_path'] ?? null;
            $paths = \is_array($best) ? ($best['considered_access_paths'] ?? null) : null;
            if (\is_string($table) && \str_ends_with($table, '`'.$alias.'`') && \is_array($paths)) {
                foreach ($paths as $path) {
                    if (\is_array($path) && \in_array($path['access_type'] ?? null, ['ref', 'eq_ref'], true) && \is_string($path['index'] ?? null)) {
                        $indexes[$path['index']] = true;
                    }
                }
            }

            foreach ($node as $child) {
                $walk($child);
            }
        };
        $walk($trace);

        return \array_keys($indexes);
    }

    /**
     * Runs $read with the optimizer trace on and returns its result and the SELECT the server received,
     * with its values in place (the adapter emulates prepared statements).
     *
     * @param  callable(): array<Document>  $read
     * @return array{array<Document>, string}
     */
    private function tracing(RawQuery $adapter, callable $read): array
    {
        $adapter->rawMutation("SET SESSION optimizer_trace = 'enabled=on', optimizer_trace_offset = -5, optimizer_trace_limit = 5");

        try {
            $found = $read();
            $traces = $adapter->rawQuery('SELECT QUERY FROM information_schema.OPTIMIZER_TRACE');
        } finally {
            $adapter->rawMutation("SET SESSION optimizer_trace = 'enabled=off'");
        }

        $statements = \array_values(\array_filter(
            \array_map(static function (Document $trace): string {
                $query = $trace->getAttribute('QUERY');
                self::assertIsString($query);

                return $query;
            }, $traces),
            static fn (string $query): bool => \str_contains($query, 'LEFT JOIN'),
        ));
        $this->assertNotSame([], $statements, 'The optimizer trace must hold the read');

        return [$found, $statements[\array_key_last($statements)]];
    }

    private function treePlan(RawQuery $adapter, string $statement): string
    {
        $rows = $adapter->rawQuery('EXPLAIN FORMAT=TREE '.$statement);
        $this->assertCount(1, $rows);
        $plan = $rows[0]->getAttribute('EXPLAIN');
        $this->assertIsString($plan);

        return $plan;
    }

    /**
     * @param  array{Role, Role}  $readers  Who may read the readable documents, and who the hidden one
     */
    private function seed(Database $database, string $collection, array $readers): void
    {
        $adapter = $database->getAdapter();
        $this->assertInstanceOf(RawQuery::class, $adapter);

        $database->createAttribute($collection, Attribute::string(key: 'name', size: 64, required: true));

        // InnoDB recalculates index statistics in the background, at most every ten seconds, so a
        // read right after these writes plans with the empty table's. Held there, every run does.
        foreach ([$collection, Storage::permissionsTable($collection)] as $table) {
            $adapter->rawMutation('ALTER TABLE `'.$database->getDatabase().'`.`'.$database->getNamespace().'_'.$table.'` STATS_AUTO_RECALC = 0');
        }

        [$readable, $hidden] = $readers;
        foreach ([...self::READABLE, self::HIDDEN] as $id) {
            $database->createDocument($collection, new Document([
                '$id' => $id,
                'name' => \ucfirst($id),
                '$permissions' => [Permission::read($id === self::HIDDEN ? $hidden : $readable)],
            ]));
        }
    }

    /**
     * @param  list<Query>  $joins
     */
    private function assertJoinOrderSearchStaysSmall(Database $database, string $collection, array $joins): void
    {
        $adapter = $database->getAdapter();
        $this->assertInstanceOf(RawQuery::class, $adapter);

        $queries = [...$joins, Query::select(['name']), Query::limit(100)];

        $authorization = $database->getAuthorization();
        $roles = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());
        $authorization->addRole(Role::user('caller')->toString());

        try {
            $found = \array_map(static fn (Document $document): string => $document->getId(), $database->find($collection, $queries));
            $findPlans = $this->partialPlans($adapter);
            $total = $database->count($collection, $queries);
            $countPlans = $this->partialPlans($adapter);
        } finally {
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }
        }

        \sort($found);
        $this->assertSame(self::READABLE, $found);
        $this->assertSame(\count(self::READABLE), $total);
        $this->assertLessThan(self::MAX_PARTIAL_PLANS, $findPlans, 'Partial plans the optimizer built for find()');
        $this->assertLessThan(self::MAX_PARTIAL_PLANS, $countPlans, 'Partial plans the optimizer built for count()');
    }

    private function partialPlans(RawQuery $adapter): int
    {
        $status = $adapter->rawQuery("SHOW SESSION STATUS LIKE 'Last_query_partial_plans'");
        $this->assertCount(1, $status);

        $plans = $status[0]->getAttribute('Value');
        $this->assertIsNumeric($plans);
        $this->assertGreaterThan(0, (int) $plans, 'The status must describe the read on this session');

        return (int) $plans;
    }
}
