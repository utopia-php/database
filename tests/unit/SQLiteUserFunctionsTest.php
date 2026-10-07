<?php

namespace Tests\Unit;

use ErrorException;
use PDO;
use Pdo\Sqlite as PdoSqlite;
use PDOException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\PDO as DatabasePDO;

final class SQLiteUserFunctionsTest extends TestCase
{
    public function testRegistersRegexpWithoutPhpDeprecations(): void
    {
        $deprecations = [];
        set_error_handler(
            static function (int $severity, string $message, string $file, int $line) use (&$deprecations): bool {
                if ($severity !== E_DEPRECATED) {
                    return false;
                }

                $deprecations[] = $message;
                throw new ErrorException($message, 0, $severity, $file, $line);
            }
        );

        try {
            $connection = new DatabasePDO('sqlite::memory:', null, null);
            $adapter = new SQLite($connection);
        } finally {
            restore_error_handler();
        }

        $this->assertSame([], $deprecations);
        $this->assertRegexp($connection);
    }

    public function testRegistersRegexpOnNativeSqliteSubclass(): void
    {
        $connection = PdoSqlite::connect('sqlite::memory:');

        $this->assertInstanceOf(PdoSqlite::class, $connection);

        new SQLite($connection);

        $this->assertRegexp($connection);
    }

    public function testReconnectRegistersRegexpOnReplacementConnection(): void
    {
        $connection = new DatabasePDO('sqlite::memory:', null, null);
        $adapter = new SQLite($connection);

        $this->assertRegexp($connection);

        $adapter->reconnect();

        $this->assertRegexp($connection);
    }

    public function testDoesNotUseDeprecatedFallbackForGenericPdo(): void
    {
        $deprecations = [];
        set_error_handler(
            static function (int $severity, string $message, string $file, int $line) use (&$deprecations): bool {
                if ($severity !== E_DEPRECATED) {
                    return false;
                }

                $deprecations[] = $message;
                throw new ErrorException($message, 0, $severity, $file, $line);
            }
        );

        $connection = new PDO('sqlite::memory:');
        try {
            new SQLite($connection);
        } finally {
            restore_error_handler();
        }

        $this->assertSame([], $deprecations);
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('no such function: REGEXP');
        $connection->query("SELECT 'appwrite' REGEXP '^app'");
    }

    public function testDispatchesToTheModernWrapperMethod(): void
    {
        $connection = new class () extends DatabasePDO {
            /** @var array<int, string> */
            public array $calls = [];

            public function __construct()
            {
            }

            public function __call(string $method, array $args): mixed
            {
                $this->calls[] = $method;

                return true;
            }
        };

        new SQLite($connection);

        $this->assertSame(['createFunction'], $connection->calls);
    }

    public function testARefusedRegistrationLeavesTheAdapterUsable(): void
    {
        $connection = new class () extends PDO {
            public function __construct()
            {
            }

            public function createFunction(
                string $name,
                callable $callback,
                int $arguments = -1,
                int $flags = 0,
            ): bool {
                return false;
            }
        };

        $this->expectNotToPerformAssertions();

        new SQLite($connection);
    }

    public function testAFailedRegistrationLeavesTheAdapterUsable(): void
    {
        $connection = new class () extends PDO {
            public function __construct()
            {
            }

            public function createFunction(
                string $name,
                callable $callback,
                int $arguments = -1,
                int $flags = 0,
            ): bool {
                throw new RuntimeException('Registration failed');
            }
        };

        $this->expectNotToPerformAssertions();

        new SQLite($connection);
    }

    private function assertRegexp(DatabasePDO|PDO $connection): void
    {
        $statement = $connection->query(<<<'SQL'
            SELECT
                'appwrite' REGEXP '^app' AS matches_pattern,
                'utopia' REGEXP '^app' AS misses_pattern
            SQL);

        $this->assertNotFalse($statement);
        $this->assertSame([
            'matches_pattern' => 1,
            'misses_pattern' => 0,
        ], $statement->fetch(\PDO::FETCH_ASSOC));
    }
}
