<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;
use Utopia\Database\Query;

class SQLiteTest extends Base
{
    public static ?Database $database = null;

    protected static ?PDO $pdo = null;

    protected static string $namespace;

    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $db = __DIR__.'/database_'.static::getTestToken().'.sql';

        if (file_exists($db)) {
            unlink($db);
        }

        $dsn = $db;
        // $dsn = 'memory'; // Overwrite for fast tests
        $pdo = new PDO('sqlite:'.$dsn, null, null, SQLite::getPDOAttributes());

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(3);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $adapter = new SQLite($pdo);
        $adapter->setEmulateMySQL(true);

        $database = new Database($adapter, $cache);
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

    public function testPatternQueriesMatchWildcardCharactersLiterally(): void
    {
        $database = $this->getDatabase();
        $collection = 'likeEscape';

        $database->createCollection(new Collection(id: $collection, attributes: [
            Attribute::string(key: 'name', size: 64, required: true),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        foreach (['a_b', 'axb', 'c%d', 'cxxd', 'e\\f', 'e\\\\f'] as $name) {
            $database->createDocument($collection, new Document(['name' => $name]));
        }

        $cases = [
            [Query::containsString('name', ['a_b']), ['a_b']],
            [Query::containsAny('name', ['c%d', 'e\\f']), ['c%d', 'e\\f']],
            [Query::containsAll('name', ['c%', '%d']), ['c%d']],
            [Query::notContains('name', ['_', '\\']), ['axb', 'c%d', 'cxxd']],
            [Query::startsWith('name', 'e\\f'), ['e\\f']],
            [Query::endsWith('name', '_b'), ['a_b']],
            [Query::notStartsWith('name', 'c%'), ['a_b', 'axb', 'cxxd', 'e\\f', 'e\\\\f']],
            [Query::notEndsWith('name', '\\\\f'), ['a_b', 'axb', 'c%d', 'cxxd', 'e\\f']],
        ];

        foreach ($cases as [$query, $expected]) {
            $names = \array_map(
                fn (Document $document): mixed => $document->getAttribute('name'),
                $database->find($collection, [$query]),
            );
            \sort($names);
            \sort($expected);

            $this->assertSame($expected, $names, $query->toString());
        }
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '`'.$this->getDatabase()->getNamespace().'_'.$collection.'`';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        $index = '`'.$this->getDatabase()->getNamespace().'_'.$this->getDatabase()->getTenant()."_{$collection}_{$index}`";
        $sql = "DROP INDEX {$index}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }
}
