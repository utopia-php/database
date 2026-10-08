<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;
use Utopia\Database\Query;

class PostgresTest extends Base
{
    public static ?Database $database = null;
    protected static ?PDO $pdo = null;
    protected static string $namespace;

    /**
     * @reture Adapter
     */
    public function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'postgres';
        $dbPort = '5432';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};", $dbUser, $dbPass, Postgres::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new Postgres($pdo), $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;
        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '"' . $this->getDatabase()->getDatabase(). '"."' . $this->getDatabase()->getNamespace() . '_' . $collection . '"';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN \"{$column}\"";

        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $key = "\"".$this->getDatabase()->getNamespace()."_".$this->getDatabase()->getTenant()."_{$collection}_{$index}\"";

        $sql = "DROP INDEX \"".$this->getDatabase()->getDatabase()."\".{$key}";

        self::$pdo->exec($sql);

        return true;
    }

    /**
     * Reading must be answerable from the row alone. The permissions table holds the same fact,
     * but reaching it needs a join, and a join has to be resolved before anything can be ordered,
     * which costs a full read of the collection whenever the ordering could have come from an
     * index instead.
     */
    public function testReadDoesNotTouchThePermissionsTable(): void
    {
        $database = $this->getDatabase();

        // no collection level read, so the permission is enforced per document
        $database->createCollection('permsPlan', permissions: [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $database->createAttribute('permsPlan', 'title', Database::VAR_STRING, 64, true);

        foreach (['visible' => Role::any(), 'hidden' => Role::user('nobody')] as $title => $role) {
            $database->createDocument('permsPlan', new Document([
                '$permissions' => [Permission::read($role)],
                'title' => $title,
            ]));
        }

        $table = $database->getNamespace() . '_permsPlan_perms';

        $scans = function () use ($table): int {
            self::$pdo->query('SELECT pg_stat_force_next_flush()');
            self::$pdo->query('SELECT pg_stat_clear_snapshot()');

            $statement = self::$pdo->prepare('SELECT COALESCE(SUM(seq_scan + COALESCE(idx_scan, 0)), 0) FROM pg_stat_user_tables WHERE relname = :table');
            $statement->execute([':table' => $table]);

            return (int)$statement->fetchColumn();
        };

        $before = $scans();

        $results = $database->find('permsPlan');

        $this->assertCount(1, $results, 'Only the readable document may come back');
        $this->assertSame('visible', $results[0]->getAttribute('title'));

        $this->assertSame(
            $before,
            $scans(),
            'A read must be satisfied from the row, without reaching the permissions table'
        );

        $database->deleteCollection('permsPlan');
    }

    /**
     * A reader holding many roles must still be answered from the permissions index when few
     * documents are readable. Matching each role as its own clause made the planner add up a
     * minimum estimate per role, so enough roles convinced it most of the collection was
     * readable, and it walked the whole collection in order instead.
     *
     * Sequential scans are priced out of the session, as in the vector plan test, so the
     * assertion measures the estimate rather than the size of the collection.
     */
    public function testManyRolesUseThePermissionsIndex(): void
    {
        $database = $this->getDatabase();
        $authorization = $database->getAuthorization();

        $database->createCollection('rolesPlan', permissions: [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $database->createAttribute('rolesPlan', 'owner', Database::VAR_INTEGER, 0, true);

        $documents = [];
        for ($i = 0; $i < 50000; $i++) {
            $documents[] = new Document([
                '$permissions' => [Permission::read(Role::user("owner{$i}"))],
                'owner' => $i,
            ]);
        }
        $database->createDocuments('rolesPlan', $documents, 1000);

        $table = $database->getNamespace() . '_rolesPlan';
        self::$pdo->exec("VACUUM ANALYZE \"{$database->getDatabase()}\".\"{$table}\"");

        $scans = function () use ($table): int {
            self::$pdo->query('SELECT pg_stat_force_next_flush()');
            self::$pdo->query('SELECT pg_stat_clear_snapshot()');

            $statement = self::$pdo->prepare('
                SELECT COALESCE(SUM(statistics.idx_scan), 0)
                FROM pg_stat_user_indexes AS statistics
                JOIN pg_class AS index ON index.oid = statistics.indexrelid
                JOIN pg_am AS method ON method.oid = index.relam
                WHERE statistics.relname = :table AND method.amname = \'gin\'
            ');
            $statement->execute([':table' => $table]);

            return (int)$statement->fetchColumn();
        };

        $before = $scans();

        self::$pdo->exec('SET enable_seqscan = off');

        try {
            for ($i = 0; $i < 60; $i++) {
                $authorization->addRole(Role::user("stranger{$i}")->toString());
            }
            $authorization->addRole(Role::user('owner7')->toString());

            $results = $database->find('rolesPlan', [Query::limit(25)]);
        } finally {
            self::$pdo->exec('RESET enable_seqscan');
            $authorization->cleanRoles();
            $authorization->addRole(Role::any()->toString());
        }

        $this->assertCount(1, $results, 'Only the document owned by the reader may come back');
        $this->assertSame(7, $results[0]->getAttribute('owner'));

        $this->assertGreaterThan(
            $before,
            $scans(),
            'A selective read must be answered from the permissions index however many roles the reader holds'
        );

        $database->deleteCollection('rolesPlan');
    }

    /**
     * A vector search must order by distance alone, because a vector index can answer exactly
     * one sort key. Adding a second one does not merely make the index look expensive, it makes
     * it unusable, and the collection is read in full instead.
     *
     * Sequential scans are priced out of the session so that the planner falls back to one only
     * when the index genuinely cannot answer the ordering. That separates a hard block from a
     * costing preference, and keeps the assertion independent of how many rows are present.
     */
    public function testVectorSearchUsesTheIndex(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('vectorPlan', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ], documentSecurity: false);

        $database->createAttribute('vectorPlan', 'embedding', Database::VAR_VECTOR, 3, true);
        $database->createIndex('vectorPlan', 'idx_cosine', Database::INDEX_HNSW_COSINE, ['embedding']);

        for ($i = 0; $i < 50; $i++) {
            $database->createDocument('vectorPlan', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [$i / 50, 1 - ($i / 50), 0.0],
            ]));
        }

        $index = $database->getNamespace() . '_' . $database->getTenant() . '_vectorPlan_idx_cosine';

        $scans = function () use ($index): int {
            self::$pdo->query('SELECT pg_stat_force_next_flush()');
            self::$pdo->query('SELECT pg_stat_clear_snapshot()');

            $statement = self::$pdo->prepare('SELECT COALESCE(SUM(idx_scan), 0) FROM pg_stat_user_indexes WHERE indexrelname = :index');
            $statement->execute([':index' => $index]);

            return (int)$statement->fetchColumn();
        };

        $before = $scans();

        self::$pdo->exec('SET enable_seqscan = off');

        try {
            $results = $database->find('vectorPlan', [
                Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
                Query::limit(10),
            ]);
        } finally {
            self::$pdo->exec('RESET enable_seqscan');
        }

        $this->assertCount(10, $results);
        $this->assertEqualsWithDelta(0.0, $results[0]->getAttribute(Database::VECTOR_DISTANCE), 0.001);

        $this->assertGreaterThan(
            $before,
            $scans(),
            'A vector search must be answerable from the vector index, not by reading the collection'
        );

        $database->deleteCollection('vectorPlan');
    }
}
