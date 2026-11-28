<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Two connections racing to create the same table can see the loser's CREATE TABLE fail as SQLITE_SCHEMA (17)
 * rather than SQLITE_ERROR (1). Both mean the table exists, so both must reach callers as a duplicate.
 */
final class SQLiteSchemaChangeRaceTest extends TestCase
{
    private const string NAMESPACE = 'schema_race';

    private const string COLLECTION = 'tombstones';

    private PDO $pdo;

    #[\Override]
    protected function setUp(): void
    {
        $this->pdo = new SchemaChangeRacingPDO();
    }

    public function testACreateThatLosesToASchemaChangeIsADuplicateAndKeepsTheWinnersTables(): void
    {
        $winner = $this->adapter(new SQLite($this->pdo));
        $winner->createCollection(self::COLLECTION, [Attribute::string('name', size: 64)]);

        $loser = $this->adapter(new SQLite($this->pdo));

        try {
            $loser->createCollection(self::COLLECTION, [Attribute::string('name', size: 64)]);
            $this->fail('A create that lost the race must be reported');
        } catch (DuplicateException $error) {
            $this->assertSame('Collection already exists', $error->getMessage());
            $previous = $error->getPrevious();
            $this->assertInstanceOf(PDOException::class, $previous);
            $this->assertSame(17, $previous->errorInfo[1] ?? null);
        }

        $this->assertSame([self::NAMESPACE.'_'.self::COLLECTION, self::NAMESPACE.'_'.self::COLLECTION.'_perms'], $this->tables());
    }

    public function testASharedTableCreateThatLosesToASchemaChangeStillRegistersTheLosingTenant(): void
    {
        $winner = $this->database(new SQLite($this->pdo), 1);
        $winner->createCollection($this->collection());

        $loser = $this->database(new SQLite($this->pdo), 2);
        $loser->createCollection($this->collection());

        $this->assertSame(self::COLLECTION, $loser->getCollection(self::COLLECTION)->getId());
        $loser->createDocument(self::COLLECTION, new Document(['$id' => 'loser', 'name' => 'loser']));
        $this->assertSame(1, $loser->count(self::COLLECTION));
        $this->assertSame(0, $winner->count(self::COLLECTION));
    }

    private function adapter(SQLite $adapter): SQLite
    {
        $adapter->setDatabase('main');
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter;
    }

    private function database(SQLite $adapter, int $tenant): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('main')
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables(true)
            ->setTenant($tenant)
            ->setAuthorization(new Authorization());
        $database->create();

        return $database;
    }

    private function collection(): Collection
    {
        return Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        );
    }

    /**
     * @return list<string>
     */
    private function tables(): array
    {
        $statement = $this->pdo->query("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '".self::NAMESPACE."\\_%' ESCAPE '\\' ORDER BY name");
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        $names = [];
        foreach ($statement->fetchAll(PDO::FETCH_COLUMN) as $name) {
            if (\is_string($name)) {
                $names[] = $name;
            }
        }

        return $names;
    }
}
