<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
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
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'postgres';
        $dbPort = '5432';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};", $dbUser, $dbPass, Postgres::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(2);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $database = new Database(new Postgres($pdo), $cache);
        assert(self::$authorization !== null);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($this->testDatabase)
            ->setNamespace(static::$namespace = 'myapp_'.uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;

        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '"'.$this->getDatabase()->getDatabase().'"."'.$this->getDatabase()->getNamespace().'_'.$collection.'"';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN \"{$column}\"";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $key = '"'.$this->getDatabase()->getNamespace().'_'.$this->getDatabase()->getTenant()."_{$collection}_{$index}\"";

        $sql = 'DROP INDEX "'.$this->getDatabase()->getDatabase()."\".{$key}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    public function testCreateCollectionWithMongoSequenceShapedId(): void
    {
        $database = $this->getDatabase();
        $collection = 'database_507f1f77bcf86cd799439012_collection_507f1f77bcf86cd799439013';

        $this->assertGreaterThan(
            Postgres::MAX_IDENTIFIER_NAME,
            \strlen($database->getNamespace().'_'.$collection),
            'The fixture must exceed the Postgres identifier limit, or this test no longer covers long table names'
        );

        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'name', size: 128, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));

        $document = $database->createDocument($collection, new Document([
            '$id' => 'vector-doc',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'embeddings',
        ]));

        $this->assertSame('vector-doc', $document->getId());
        $this->assertSame('embeddings', $database->getDocument($collection, 'vector-doc')->getAttribute('name'));
        $this->assertTrue($database->exists($database->getDatabase(), $collection));
        $this->assertTrue($database->deleteCollection($collection));
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
        $pdo = self::$pdo;
        $this->assertNotNull($pdo);

        $database->createCollection(new Collection(id: 'permsPlan', attributes: [
            Attribute::string(key: 'title', size: 64, required: true),
        ], permissions: [
            Permission::create(Role::any()),
        ], documentSecurity: true));

        foreach (['visible' => Role::any(), 'hidden' => Role::user('nobody')] as $title => $role) {
            $database->createDocument('permsPlan', new Document([
                '$permissions' => [Permission::read($role)],
                'title' => $title,
            ]));
        }

        $table = $database->getNamespace().'_permsPlan_perms';

        $scans = function () use ($pdo, $table): int {
            $pdo->query('SELECT pg_stat_force_next_flush()');
            $pdo->query('SELECT pg_stat_clear_snapshot()');

            $statement = $pdo->prepare('SELECT COALESCE(SUM(seq_scan + COALESCE(idx_scan, 0)), 0) FROM pg_stat_user_tables WHERE relname = :table');
            $statement->execute([':table' => $table]);

            return (int) $statement->fetchColumn();
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
        $pdo = self::$pdo;
        $this->assertNotNull($pdo);

        $database->createCollection(new Collection(id: 'vectorPlan', attributes: [
            Attribute::vector(key: 'embedding', size: 3, required: true),
        ], indexes: [
            Index::hnswCosine(key: 'idx_cosine', attributes: ['embedding']),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ], documentSecurity: false));

        for ($i = 0; $i < 50; $i++) {
            $database->createDocument('vectorPlan', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'embedding' => [$i / 50, 1 - ($i / 50), 0.0],
            ]));
        }

        $index = $database->getNamespace().'_'.$database->getTenant().'_vectorPlan_idx_cosine';

        $scans = function () use ($pdo, $index): int {
            $pdo->query('SELECT pg_stat_force_next_flush()');
            $pdo->query('SELECT pg_stat_clear_snapshot()');

            $statement = $pdo->prepare('SELECT COALESCE(SUM(idx_scan), 0) FROM pg_stat_user_indexes WHERE indexrelname = :index');
            $statement->execute([':index' => $index]);

            return (int) $statement->fetchColumn();
        };

        $before = $scans();

        $pdo->exec('SET enable_seqscan = off');

        try {
            $results = $database->find('vectorPlan', [
                Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
                Query::limit(10),
            ]);
        } finally {
            $pdo->exec('RESET enable_seqscan');
        }

        $this->assertCount(10, $results);
        $this->assertEqualsWithDelta(0.0, $results[0]->getAttribute(Document::DISTANCE), 0.001);

        $this->assertGreaterThan(
            $before,
            $scans(),
            'A vector search must be answerable from the vector index, not by reading the collection'
        );

        $database->deleteCollection('vectorPlan');
    }
}
