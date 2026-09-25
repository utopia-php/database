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

    public function testEightCheckedSelfJoinsKeepTheJoinOrderSearchSmall(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();
        $this->assertInstanceOf(RawQuery::class, $adapter);

        $collection = 'checked_self_joins';
        $database->createCollection(new Collection(id: $collection, permissions: [Permission::create(Role::any())], documentSecurity: true));

        $authorization = $database->getAuthorization();
        $roles = $authorization->getRoles();

        try {
            $database->createAttribute($collection, Attribute::string(key: 'name', size: 64, required: true));

            // InnoDB recalculates index statistics in the background, at most every ten seconds, so a
            // read right after these writes plans with the empty table's. Held there, every run does.
            foreach ([$collection, Storage::permissionsTable($collection)] as $table) {
                $adapter->rawMutation('ALTER TABLE '.$this->sqlTable($database, $table).' STATS_AUTO_RECALC = 0');
            }

            $readable = ['alice', 'bob', 'carol', 'dave', 'frank'];
            foreach ([...$readable, 'eve'] as $id) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    'name' => \ucfirst($id),
                    '$permissions' => [Permission::read($id === 'eve' ? Role::user('hidden') : Role::any())],
                ]));
            }

            $queries = [
                ...\array_map(
                    static fn (int $peer): Query => Query::join($collection, '$id', '$id', '=', 'peer'.$peer),
                    \range(1, 8),
                ),
                Query::select(['name']),
                Query::limit(100),
            ];

            $authorization->cleanRoles();
            $authorization->addRole(Role::any()->toString());
            $authorization->addRole(Role::user('caller')->toString());

            $found = \array_map(static fn (Document $document): string => $document->getId(), $database->find($collection, $queries));
            $findPlans = $this->partialPlans($adapter);
            $total = $database->count($collection, $queries);
            $countPlans = $this->partialPlans($adapter);
        } finally {
            $authorization->cleanRoles();
            foreach ($roles as $role) {
                $authorization->addRole($role);
            }
            $database->deleteCollection($collection);
        }

        \sort($found);
        $this->assertSame($readable, $found);
        $this->assertSame(\count($readable), $total);
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

    private function sqlTable(Database $database, string $table): string
    {
        return '`'.$database->getDatabase().'`.`'.$database->getNamespace().'_'.$table.'`';
    }
}
