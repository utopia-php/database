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
        [$updated, $row, $blocked] = $this->incrementWhileAnotherColumnIsWritten(Operator::increment(1));

        $this->assertFalse($blocked, 'The concurrent write waited for the row lock');
        $this->assertSame(1, $updated->getAttribute('opens'));
        $this->assertSame(['opens' => 1, 'status' => 'concurrent'], $row);
    }

    /**
     * A bounded increment is validated against the stored value, so it still reads the row under the lock:
     * a concurrent write cannot land between that check and the write.
     */
    public function testBoundedIncrementKeepsTheRowLockedFromReadToWrite(): void
    {
        [$updated, $row, $blocked] = $this->incrementWhileAnotherColumnIsWritten(Operator::increment(1, 10));

        $this->assertTrue($blocked, 'The concurrent write landed between the bound check and the write');
        $this->assertSame(1, $updated->getAttribute('opens'));
        $this->assertSame(['opens' => 1, 'status' => 'before'], $row);
    }

    /**
     * Increment a row's `opens` while a second connection writes its `status` between the update's read and
     * its write.
     *
     * @return array{Document, array{opens: int, status: string}, bool} The updated document, the stored row,
     *                                                                and whether the other write hit a lock wait timeout
     */
    private function incrementWhileAnotherColumnIsWritten(Operator $increment): array
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
        $blocked = false;
        $between->write = function () use ($other, $table, &$blocked): void {
            try {
                $other->exec("UPDATE {$table} SET `status` = 'concurrent' WHERE `_uid` = 'row'");
            } catch (\PDOException $error) {
                $blocked = ($error->errorInfo[1] ?? null) === 1205;
                if (! $blocked) {
                    throw $error;
                }
            }
        };

        $authorization = $database->getAuthorization();
        $database->setAuthorization($between);
        try {
            $updated = $database->updateDocument($collection, 'row', new Document([
                Document::ID => 'row',
                Document::PERMISSIONS => [],
                'opens' => $increment,
            ]));
        } finally {
            $database->setAuthorization($authorization);
        }

        $this->assertNull($between->write, 'The concurrent write never ran');
        $statement = $other->query("SELECT `opens`, `status` FROM {$table} WHERE `_uid` = 'row'");
        $this->assertNotFalse($statement);
        $row = $statement->fetch(\PDO::FETCH_ASSOC);
        $this->assertIsArray($row);
        $this->assertIsInt($row['opens']);
        $this->assertIsString($row['status']);
        $database->deleteCollection($collection);

        return [$updated, ['opens' => $row['opens'], 'status' => $row['status']], $blocked];
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
