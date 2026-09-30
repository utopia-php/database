<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;

final class PostgresColumnRewriteTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    public function testDatetimeRewriteCastsTheColumnItself(): void
    {
        $this->createAdapter()->updateAttribute('events', Attribute::datetime(key: 'at'));

        $this->assertContains(
            'ALTER TABLE "database"."namespace_events" ALTER COLUMN "at" TYPE TIMESTAMP(3) USING "at"::TIMESTAMP(3)',
            $this->statements,
        );
    }

    public function testRenamedDatetimeRewriteCastsTheRenamedColumn(): void
    {
        $this->createAdapter()->updateAttribute('events', Attribute::datetime(key: 'at'), 'happenedAt');

        $this->assertSame('ALTER TABLE "database"."namespace_events" RENAME COLUMN "at" TO "happenedAt"', $this->statements[0]);
        $this->assertSame(
            'ALTER TABLE "database"."namespace_events" ALTER COLUMN "happenedAt" TYPE TIMESTAMP(3) USING "happenedAt"::TIMESTAMP(3)',
            $this->statements[1],
        );
    }

    private function createAdapter(): Postgres
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
