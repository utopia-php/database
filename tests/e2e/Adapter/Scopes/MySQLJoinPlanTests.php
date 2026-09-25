<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature\RawQuery;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Storage;

trait MySQLJoinPlanTests
{
    private const int MAX_PARTIAL_PLANS = 10_000;

    private const array READABLE = ['alice', 'bob', 'carol', 'dave', 'frank'];

    private const string HIDDEN = 'eve';

    public function testEightCheckedSelfJoinsKeepTheJoinOrderSearchSmall(): void
    {
        $database = $this->getDatabase();
        $customers = 'checked_self_joins';
        $database->createCollection(new Collection(id: $customers, permissions: [Permission::create(Role::any())], documentSecurity: true));

        try {
            $this->seed($database, $customers, [Role::any(), Role::user(self::HIDDEN)]);

            $this->assertJoinOrderSearchStaysSmall($database, $customers, \array_map(
                static fn (int $peer): Query => Query::join($customers, '$id', '$id', '=', 'peer'.$peer),
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
        $database->createCollection(new Collection(id: $customers, permissions: [Permission::create(Role::any())], documentSecurity: true));
        $database->createCollection(new Collection(id: $labels, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));

        try {
            $this->seed($database, $customers, [Role::any(), Role::user(self::HIDDEN)]);
            $this->seed($database, $labels, [Role::any(), Role::any()]);

            $this->assertJoinOrderSearchStaysSmall($database, $customers, [
                ...\array_map(
                    static fn (int $peer): Query => Query::join($customers, '$id', '$id', '=', 'peer'.$peer),
                    \range(1, 4),
                ),
                ...\array_map(
                    static fn (int $label): Query => Query::join($labels, 'name', 'name', '=', 'label'.$label),
                    \range(1, 4),
                ),
            ]);
        } finally {
            $database->deleteCollection($labels);
            $database->deleteCollection($customers);
        }
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
