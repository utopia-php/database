<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Attribute;
use Utopia\Database\Index;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\Order;

final class IndexColumnOrderTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return array<string, array{0: class-string<MariaDB>, 1: bool, 2: string}>
     */
    public static function collectionIndexes(): array
    {
        return [
            'MariaDB' => [MariaDB::class, false, 'INDEX `tagsfirst` (`tags`(255), `status`, `name`(16) DESC)'],
            'MariaDB with shared tables' => [MariaDB::class, true, 'INDEX `tagsfirst` (`_tenant`, `tags`(255), `status`, `name`(16) DESC)'],
            'MySQL' => [MySQL::class, false, 'INDEX `tagsfirst` ((CAST(`tags` AS char(255) ARRAY)), `status`, `name`(16) DESC)'],
            'MySQL with shared tables' => [MySQL::class, true, 'INDEX `tagsfirst` (`_tenant`, (CAST(`tags` AS char(255) ARRAY)), `status`, `name`(16) DESC)'],
        ];
    }

    /**
     * @param  class-string<MariaDB>  $adapterClass
     */
    #[DataProvider('collectionIndexes')]
    public function testCollectionIndexKeepsAttributeOrder(string $adapterClass, bool $sharedTables, string $index): void
    {
        $adapter = $this->createAdapter($adapterClass, $sharedTables);

        $adapter->createCollection(
            'places',
            [
                Attribute::string(key: 'tags', size: 64, array: true),
                Attribute::string(key: 'status', size: 32),
                Attribute::string(key: 'name', size: 128),
            ],
            [Index::key(key: 'tagsfirst', attributes: ['tags', 'status', 'name'], lengths: [255, null, 16], orders: [null, null, Order::Desc])],
        );

        $this->assertStringContainsString($index, $this->statements[0]);
    }

    /**
     * @return array<string, array{0: bool, 1: string}>
     */
    public static function objectPathIndexes(): array
    {
        return [
            'dedicated tables' => [false, 'CREATE INDEX "namespace__places_countryfirst" ON "database"."namespace_places" ((("data"->>\'country\')::text) DESC, "status")'],
            'shared tables' => [true, 'CREATE INDEX "namespace_7_places_countryfirst" ON "database"."namespace_places" ("_tenant", (("data"->>\'country\')::text) DESC, "status")'],
        ];
    }

    #[DataProvider('objectPathIndexes')]
    public function testPostgresObjectPathKeepsItsPosition(bool $sharedTables, string $statement): void
    {
        $adapter = $this->createAdapter(Postgres::class, $sharedTables);

        $adapter->createIndex(
            'places',
            Index::key(key: 'countryfirst', attributes: ['data.country', 'status'], orders: [Order::Desc, null]),
            ['data.country' => ColumnType::Object->value, 'status' => ColumnType::String->value],
        );

        $this->assertSame([$statement], $this->statements);
    }

    public function testRepeatedColumnKeepsItsOwnLengthAndOrder(): void
    {
        $mariadb = $this->createAdapter(MariaDB::class, false);
        $mariadb->createCollection(
            'places',
            [Attribute::string(key: 'name', size: 128)],
            [Index::key(key: 'twice', attributes: ['name', 'name'], lengths: [8, 16], orders: [Order::Asc, Order::Desc])],
        );
        $this->assertStringContainsString('INDEX `twice` (`name`(8) ASC, `name`(16) DESC)', $this->statements[0]);

        $this->statements = [];
        $postgres = $this->createAdapter(Postgres::class, false);
        $postgres->createIndex('places', Index::key(key: 'twice', attributes: ['name', 'name'], orders: [Order::Asc, Order::Desc]));
        $this->assertSame(['CREATE INDEX "namespace__places_twice" ON "database"."namespace_places" ("name" ASC, "name" DESC)'], $this->statements);
    }

    public function testPostgresOperatorClassFollowsEveryColumn(): void
    {
        $adapter = $this->createAdapter(Postgres::class, false);

        $adapter->createIndex('places', Index::trigram(key: 'names', attributes: ['name', 'status']));
        $adapter->createIndex('places', Index::hnswCosine(key: 'embeddings', attributes: ['embedding']));

        $this->assertSame([
            'CREATE INDEX "namespace__places_names" ON "database"."namespace_places" USING GIN ("name" gin_trgm_ops, "status" gin_trgm_ops)',
            'CREATE INDEX "namespace__places_embeddings" ON "database"."namespace_places" USING HNSW ("embedding" vector_cosine_ops)',
        ], $this->statements);
    }

    /**
     * @param  class-string<SQL>  $adapterClass
     */
    private function createAdapter(string $adapterClass, bool $sharedTables): SQL
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });

        $adapter = new $adapterClass($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant($sharedTables ? 7 : null);

        return $adapter;
    }
}
