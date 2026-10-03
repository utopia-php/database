<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolAlterLockTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    public function testEnableLocksReachesTheBorrowedAdapter(): void
    {
        [$database, $pool] = $this->database();

        $database->enableLocks(true);
        $pool->createAttribute('posts', Attribute::string(key: 'title', size: 64));

        $this->assertCount(1, $this->statements);
        $this->assertStringStartsWith('ALTER TABLE', $this->statements[0]);
        $this->assertStringEndsWith(',LOCK=SHARED', $this->statements[0]);
    }

    public function testDisablingLocksReachesTheBorrowedAdapter(): void
    {
        [$database, $pool] = $this->database();

        $database->enableLocks(true);
        $database->enableLocks(false);
        $pool->createAttribute('posts', Attribute::string(key: 'title', size: 64));

        $this->assertCount(1, $this->statements);
        $this->assertStringNotContainsString('LOCK=SHARED', $this->statements[0]);
    }

    /**
     * @return array{Database, Pool}
     */
    private function database(): array
    {
        $statement = self::createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = self::createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $sql) use ($statement): PDOStatement {
            $this->statements[] = $sql;

            return $statement;
        });

        $connection = new MariaDB($pdo);

        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($connection),
        );

        $pool = new Pool($connections);
        $database = new Database($pool, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('locks')
            ->setNamespace('locks');

        return [$database, $pool];
    }
}
