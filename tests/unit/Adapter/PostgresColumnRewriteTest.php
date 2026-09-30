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

    public function testRenamedDatetimeRewriteCastsTheColumnBeforeRenamingIt(): void
    {
        $this->createAdapter()->updateAttribute('events', Attribute::datetime(key: 'at'), 'happenedAt');

        $this->assertStringStartsWith('SELECT a.attname FROM pg_attribute a', $this->statements[0]);
        $this->assertSame(
            'ALTER TABLE "database"."namespace_events" ALTER COLUMN "at" TYPE TIMESTAMP(3) USING "at"::TIMESTAMP(3)',
            $this->statements[1],
        );
        $this->assertSame('ALTER TABLE "database"."namespace_events" ALTER COLUMN "at" DROP NOT NULL', $this->statements[2]);
        $this->assertSame('ALTER TABLE "database"."namespace_events" RENAME COLUMN "at" TO "happenedAt"', $this->statements[3]);
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
