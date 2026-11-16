<?php

namespace Tests\E2E\Adapter;

use Redis;
use Tests\E2E\Adapter\Scopes\MySQLJoinPlanTests;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Operator;
use Utopia\Database\PDO;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;

class MySQLTest extends Base
{
    use MySQLJoinPlanTests;

    public static ?Database $database = null;

    protected static ?PDO $pdo = null;

    #[\Override]
    protected static string $namespace;

    /**
     * @throws Duplicate
     * @throws Exception
     * @throws Limit
     */
    #[\Override]
    public function getDatabase(): Database
    {
        if (! is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'mysql';
        $dbPort = '3307';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, self::PDO_ATTRIBUTES);

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->select(1);
        $cache = new Cache((new RedisAdapter($redis))->setMaxRetries(3));

        $database = new Database(new MySQL($pdo), $cache);
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

    #[\Override]
    protected function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '`'.$this->getDatabase()->getDatabase().'`.`'.$this->getDatabase()->getNamespace().'_'.$collection.'`';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN `{$column}`";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }

    /**
     * A concurrent write to another column, landing after the update read the row and before it wrote, is
     * neither blocked by the update nor undone by it: an update of operators only holds no lock while reading
     * and writes back only its own columns.
     */
    public function testOperatorOnlyUpdateLeavesConcurrentWriteToAnotherColumn(): void
    {
        $database = $this->getDatabase();
        $collection = 'hot_row_'.\uniqid();
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::integer(key: 'opens'), Attribute::string(key: 'status', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument($collection, new Document([Document::ID => 'row', 'opens' => 0, 'status' => 'before']));

        $other = new PDO('mysql:host=mysql;port=3307;charset=utf8mb4', 'root', 'password', [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]);
        $other->exec('SET SESSION innodb_lock_wait_timeout = 1');
        $table = '`'.$database->getDatabase().'`.`'.$database->getNamespace().'_'.$collection.'`';

        $between = new class () extends Authorization {
            public ?\Closure $write = null;

            #[\Override]
            public function isValid(mixed $value): bool
            {
                if ($this->write !== null && $value instanceof Input && $value->getAction() === PermissionType::Update->value) {
                    ($this->write)();
                    $this->write = null;
                }

                return parent::isValid($value);
            }
        };
        $between->write = fn () => $other->exec("UPDATE {$table} SET `status` = 'concurrent' WHERE `_uid` = 'row'");

        $authorization = $database->getAuthorization();
        $database->setAuthorization($between);
        try {
            $updated = $database->updateDocument($collection, 'row', new Document([
                Document::ID => 'row',
                Document::PERMISSIONS => [],
                'opens' => Operator::increment(1),
            ]));
        } finally {
            $database->setAuthorization($authorization);
        }

        $this->assertNull($between->write, 'The concurrent write never ran');
        $this->assertSame(1, $updated->getAttribute('opens'));
        $statement = $other->query("SELECT `opens`, `status` FROM {$table} WHERE `_uid` = 'row'");
        $this->assertNotFalse($statement);
        $this->assertSame([['opens' => 1, 'status' => 'concurrent']], $statement->fetchAll(\PDO::FETCH_ASSOC));

        $database->deleteCollection($collection);
    }

    #[\Override]
    protected function deleteIndex(string $collection, string $index): bool
    {
        $sqlTable = '`'.$this->getDatabase()->getDatabase().'`.`'.$this->getDatabase()->getNamespace().'_'.$collection.'`';
        $sql = "DROP INDEX `{$index}` ON {$sqlTable}";

        assert(self::$pdo !== null);
        self::$pdo->exec($sql);

        return true;
    }
}
