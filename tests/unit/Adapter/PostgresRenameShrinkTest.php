<?php

namespace Tests\Unit\Adapter;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Exception\Truncate as TruncateException;

/**
 * updateAttribute() with a new key and a type the stored values refuse must leave the column
 * under its old key, as MariaDB's single CHANGE COLUMN does: the metadata keeps the old key.
 */
final class PostgresRenameShrinkTest extends TestCase
{
    private const string CATALOG = 'SELECT a.attname FROM pg_attribute a';

    /** @var list<string> */
    private array $statements = [];

    public function testARefusedTypeChangeRunsNoRename(): void
    {
        $adapter = $this->createAdapter();

        try {
            $adapter->updateAttribute('notes', Attribute::string(key: 'title', size: 4), 'heading');
            $this->fail('The truncation must be reported');
        } catch (TruncateException) {
            $this->addToAssertionCount(1);
        }

        $this->assertSame([], \array_values(\array_filter($this->statements, static fn (string $statement): bool => \str_contains($statement, 'RENAME COLUMN'))));
    }

    private function createAdapter(): Postgres
    {
        $truncation = new PDOException('SQLSTATE[22001]: String data, right truncated: 7 ERROR:  value too long for type character varying(4)');
        (new ReflectionProperty(Exception::class, 'code'))->setValue($truncation, '22001');
        $truncation->errorInfo = ['22001', 7, 'value too long for type character varying(4)'];

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($truncation): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            if (\str_contains($query, ' TYPE ')) {
                $statement->method('execute')->willThrowException($truncation);
            } else {
                $statement->method('execute')->willReturn(true);
            }
            $statement->method('fetchAll')->willReturn(\str_starts_with($query, self::CATALOG) ? ['_id', 'title'] : []);

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
