<?php

namespace Tests\Unit\Collections;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class AnalyzeCollectionTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    public function testSQLiteRecordsStatisticsForTheTableAndItsPermissions(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $database = new Database(new SQLite($pdo), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('analyze')
            ->setNamespace('analyze');
        $database->create();
        $database->createCollection(Collection::create(
            id: 'places',
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any())],
        ));

        foreach (\range(1, 20) as $number) {
            $document = $database->createDocument('places', new Document(['name' => 'place'.$number]));
            $pdo->exec("INSERT INTO `analyze_places_perms` (`_type`, `_permission`, `_document`) VALUES ('read', 'any', '{$document->getId()}')");
        }

        $this->assertTrue($database->analyzeCollection('places'));

        $statement = $pdo->query('SELECT DISTINCT tbl FROM sqlite_stat1 ORDER BY tbl');
        $this->assertInstanceOf(PDOStatement::class, $statement);
        $this->assertSame(['analyze_places', 'analyze_places_perms'], $statement->fetchAll(PDO::FETCH_COLUMN));
    }

    public function testPostgresAnalyzesTheTableAndItsPermissions(): void
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

        $this->assertTrue($adapter->analyzeCollection('places'));
        $this->assertSame(['ANALYZE "database"."namespace_places"; ANALYZE "database"."namespace_places_perms"'], $this->statements);
    }
}
