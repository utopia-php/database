<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Exception as DatabaseException;

final class AdapterDatabaseRenameTest extends TestCase
{
    /**
     * Databases of a fake MariaDB server and the tables each holds.
     *
     * @var array<string, list<string>>
     */
    private array $databases = [];

    /**
     * A table another connection creates in the old database while the rename runs.
     */
    private ?string $created = null;

    /**
     * @return iterable<string, array{Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [new MariaDB(new stdClass())];
        yield 'MySQL' => [new MySQL(new stdClass())];
        yield 'Postgres' => [new Postgres(new stdClass())];
        yield 'Memory' => [new Memory()];
    }

    #[DataProvider('adapters')]
    public function testSharedTablesRefuseTheRename(Adapter $adapter): void
    {
        $adapter->setSharedTables(true);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cannot rename a database while shared tables are enabled');

        $adapter->update('library', 'archive');
    }

    public function testMariaDBMovesEveryTableAndDropsTheOldDatabase(): void
    {
        $this->databases = ['library' => ['ns_authors', 'ns_books']];

        $this->assertTrue($this->mariadb()->update('library', 'archive'));

        $this->assertSame(['archive' => ['ns_authors', 'ns_books']], $this->databases);
    }

    public function testMariaDBKeepsAnOldDatabaseThatGainedATableDuringTheRename(): void
    {
        $this->databases = ['library' => ['ns_authors', 'ns_books']];
        $this->created = 'ns_loans';

        try {
            $this->mariadb()->update('library', 'archive');
            $this->fail('An old database that is not empty must not be dropped');
        } catch (DatabaseException $error) {
            $this->assertSame('Database library was renamed to archive but holds tables created during the rename, so it was not dropped', $error->getMessage());
        }

        $this->assertSame(['library' => ['ns_loans'], 'archive' => ['ns_authors', 'ns_books']], $this->databases);
    }

    private function mariadb(): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback($this->statement(...));

        return new MariaDB($pdo);
    }

    private function statement(string $query): PDOStatement
    {
        $bound = new stdClass();
        $bound->values = [];
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('bindValue')->willReturnCallback(static function (int|string $parameter, mixed $value) use ($bound): bool {
            $bound->values[] = $value;

            return true;
        });
        $statement->method('execute')->willReturnCallback(function () use ($query): bool {
            $this->apply($query);

            return true;
        });
        $statement->method('fetchAll')->willReturnCallback(fn (): array => $this->rows($query, $bound->values));
        $statement->method('closeCursor')->willReturn(true);

        return $statement;
    }

    private function apply(string $query): void
    {
        \preg_match_all('/`([^`]+)`/', $query, $identifiers);
        $names = $identifiers[1];

        match (true) {
            \str_contains($query, 'CREATE DATABASE') => $this->databases[$names[0]] = [],
            \str_contains($query, 'DROP DATABASE') => $this->drop($names[0]),
            \str_contains($query, 'RENAME TABLE') => $this->move($names),
            default => null,
        };
    }

    /**
     * @param  list<string>  $names  database, table, database, table for each move
     */
    private function move(array $names): void
    {
        foreach (\array_chunk($names, 4) as [$from, $table, $to]) {
            $this->databases[$from] = \array_values(\array_diff($this->databases[$from], [$table]));
            $this->databases[$to][] = $table;
        }

        if ($this->created !== null) {
            $this->databases[$names[0]][] = $this->created;
        }
    }

    private function drop(string $database): void
    {
        unset($this->databases[$database]);
    }

    /**
     * @param  list<mixed>  $values
     * @return list<array<string, string>>
     */
    private function rows(string $query, array $values): array
    {
        $database = \is_string($values[0] ?? null) ? $values[0] : '';

        if (\str_contains($query, 'SCHEMATA')) {
            return isset($this->databases[$database]) ? [['SCHEMA_NAME' => $database]] : [];
        }

        return \array_map(static fn (string $table): array => ['TABLE_NAME' => $table], $this->databases[$database] ?? []);
    }
}
