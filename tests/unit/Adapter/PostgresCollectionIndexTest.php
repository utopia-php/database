<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Index;
use Utopia\Query\Schema\Order;

final class PostgresCollectionIndexTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return array<string, array{0: bool, 1: string}>
     */
    public static function objectPathIndexes(): array
    {
        return [
            'dedicated tables' => [false, 'CREATE INDEX "namespace__places_countryfirst" ON "database"."namespace_places" ((("data"->>\'country\')::text) DESC, "status")'],
            'shared tables' => [true, 'CREATE INDEX IF NOT EXISTS "namespace__places_countryfirst" ON "database"."namespace_places" ("_tenant", (("data"->>\'country\')::text) DESC, "status")'],
        ];
    }

    #[DataProvider('objectPathIndexes')]
    public function testCollectionIndexOnAnObjectPathIndexesTheJsonPath(bool $sharedTables, string $statement): void
    {
        $adapter = $this->createAdapter($sharedTables);

        $adapter->createCollection(
            'places',
            [Attribute::object(key: 'data'), Attribute::string(key: 'status', size: 32)],
            [Index::key(key: 'countryfirst', attributes: ['data.country', 'status'], orders: [Order::Desc, null])],
        );

        $this->assertSame($statement, $this->statements[\array_key_last($this->statements)]);
    }

    private function createAdapter(bool $sharedTables): Postgres
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
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant($sharedTables ? 7 : null);

        return $adapter;
    }
}
