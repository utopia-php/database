<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;

final class IdentifierQuotingTest extends TestCase
{
    /**
     * @return array<string, array{string}>
     */
    public static function names(): array
    {
        return [
            'plain' => ['total'],
            'backtick' => ['a`b'],
            'trailing backtick' => ['total`'],
            'double quote' => ['a"b'],
            'trailing double quote' => ['total"'],
            'both quote chars' => ['a`"b'],
            'statement terminator' => ['a`; SELECT 2 AS `b'],
        ];
    }

    #[DataProvider('names')]
    public function testABacktickQuotedNameIsReadAsExactlyThatName(string $name): void
    {
        $pdo = new PDO('sqlite::memory:');
        $adapter = new class ($pdo) extends SQLite {
            public function identifier(string $name): string
            {
                return $this->quote($name);
            }
        };

        $this->assertSame([$name], $this->columnNames($pdo, $adapter->identifier($name)));
    }

    #[DataProvider('names')]
    public function testADoubleQuotedNameIsReadAsExactlyThatName(string $name): void
    {
        $pdo = new PDO('sqlite::memory:');
        $adapter = new class ($pdo) extends Postgres {
            public function identifier(string $name): string
            {
                return $this->quote($name);
            }
        };

        $this->assertSame([$name], $this->columnNames($pdo, $adapter->identifier($name)));
    }

    /**
     * SQLite reads both backtick and double quoted identifiers, each escaping its quote char by
     * doubling it, so it answers which column name a quoted identifier denotes for either dialect.
     *
     * @return list<string>
     */
    private function columnNames(PDO $pdo, string $identifier): array
    {
        $statement = $pdo->query('SELECT 1 AS '.$identifier);
        $this->assertNotFalse($statement);

        /** @var array<string, mixed>|false $row */
        $row = $statement->fetch(PDO::FETCH_ASSOC);
        $this->assertIsArray($row);

        return \array_keys($row);
    }
}
