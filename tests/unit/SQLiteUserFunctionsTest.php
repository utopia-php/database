<?php

namespace Tests\Unit;

use ErrorException;
use PDO;
use Pdo\Sqlite as PdoSqlite;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;
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
        $this->assertTrue($adapter->supports(Capability::PCRE));
        $this->assertRegexp($connection);
    }

    public function testRegistersRegexpOnNativeSqliteSubclass(): void
    {
        $connection = PdoSqlite::connect('sqlite::memory:');

        $this->assertInstanceOf(PdoSqlite::class, $connection);

        $adapter = new SQLite($connection);

        $this->assertTrue($adapter->supports(Capability::PCRE));
        $this->assertRegexp($connection);
    }

    public function testReconnectRegistersRegexpOnReplacementConnection(): void
    {
        $connection = new DatabasePDO('sqlite::memory:', null, null);
        $adapter = new SQLite($connection);

        $this->assertRegexp($connection);

        $adapter->reconnect();

        $this->assertTrue($adapter->supports(Capability::PCRE));
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

        try {
            $adapter = new SQLite(new PDO('sqlite::memory:'));
        } finally {
            restore_error_handler();
        }

        $this->assertSame([], $deprecations);
        $this->assertFalse($adapter->supports(Capability::PCRE));
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

        $adapter = new SQLite($connection);

        $this->assertSame(['createFunction'], $connection->calls);
        $this->assertTrue($adapter->supports(Capability::PCRE));
    }

    public function testDoesNotAdvertisePcreWhenRegistrationReturnsFalse(): void
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

        $adapter = new SQLite($connection);

        $this->assertFalse($adapter->supports(Capability::PCRE));
    }

    public function testDoesNotAdvertisePcreWhenRegistrationThrows(): void
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

        $adapter = new SQLite($connection);

        $this->assertFalse($adapter->supports(Capability::PCRE));
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
