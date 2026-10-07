<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class SQLiteSharedFulltextTest extends TestCase
{
    private const string NAMESPACE = 'fulltext';

    private const array TENANTS = ['tenant-a', "tenant'b"];

    private PDO $pdo;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->database = new Database(new SQLite($this->pdo), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables(true)
            ->setTenant(self::TENANTS[0])
            ->setAuthorization(new Authorization());
        $this->database->create();

        foreach (self::TENANTS as $tenant) {
            $this->database->setTenant($tenant);
            $this->database->createCollection(Collection::create(
                id: 'notes',
                attributes: [Attribute::string('body', size: 128)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                documentSecurity: false,
            ));
            $this->database->createDocument('notes', new Document(['$id' => 'early', 'body' => 'shared word written before the index']));
            $this->database->createIndex('notes', Index::fulltext(key: 'body_search', attributes: ['body']));
            $this->database->createDocument('notes', new Document(['$id' => 'late', 'body' => 'shared word written after the index']));
        }
    }

    public function testAStringTenantsFulltextIndexSeesOnlyItsOwnRows(): void
    {
        foreach (self::TENANTS as $tenant) {
            $this->assertSame(['early', 'late'], $this->search($tenant, 'shared'), $tenant . ' finds its own rows');
            $this->assertSame($this->sequences($tenant), $this->indexedRows($tenant), $tenant . ' indexes only its own rows');
        }
    }

    public function testAnotherTenantsWritesLeaveTheIndexAlone(): void
    {
        $this->database->setTenant(self::TENANTS[1]);
        $this->database->updateDocument('notes', 'late', new Document(['body' => 'rewritten']));
        $this->database->deleteDocument('notes', 'early');

        $this->assertSame(['early', 'late'], $this->search(self::TENANTS[0], 'shared'));
        $this->assertSame($this->sequences(self::TENANTS[0]), $this->indexedRows(self::TENANTS[0]));

        $this->assertSame([], $this->search(self::TENANTS[1], 'shared'));
        $this->assertSame(['late'], $this->search(self::TENANTS[1], 'rewritten'));
        $this->assertSame($this->sequences(self::TENANTS[1]), $this->indexedRows(self::TENANTS[1]));
    }

    /**
     * @return list<string>
     */
    private function search(string $tenant, string $term): array
    {
        $this->database->setTenant($tenant);
        $ids = \array_map(
            static fn (Document $document): string => $document->getId(),
            $this->database->find('notes', [Query::search('body', $term)]),
        );
        \sort($ids);

        return $ids;
    }

    /**
     * @return list<int>
     */
    private function sequences(string $tenant): array
    {
        $statement = $this->pdo->prepare('SELECT _id FROM `' . self::NAMESPACE . '_notes` WHERE _tenant = ? ORDER BY _id');
        $this->assertInstanceOf(\PDOStatement::class, $statement);
        $statement->execute([$tenant]);

        return \array_values(\array_map(static function (mixed $value): int {
            self::assertIsNumeric($value);

            return (int) $value;
        }, $statement->fetchAll(PDO::FETCH_COLUMN)));
    }

    public function testAFulltextIndexForATenantTheDriverCannotQuoteIsRefused(): void
    {
        $pdo = new class ('sqlite::memory:') extends PDO {
            public function quote(string $string, int $type = PDO::PARAM_STR): string|false
            {
                return false;
            }
        };
        $adapter = new SQLite($pdo);
        $adapter->setDatabase(self::NAMESPACE);
        $adapter->setNamespace(self::NAMESPACE);
        $adapter->setSharedTables(true);
        $adapter->setTenant('tenant-c');
        $adapter->createCollection('notes', [Attribute::string('body', size: 128)]);

        try {
            $adapter->createIndex('notes', Index::fulltext(key: 'body_search', attributes: ['body']));
            $this->fail('a tenant that cannot be written into the index triggers must refuse the index');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to quote SQLite tenant', $error->getMessage());
        }
    }

    /**
     * @return list<int>
     */
    private function indexedRows(string $tenant): array
    {
        $tables = $this->pdo->prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ? AND name LIKE '%\\_fts' ESCAPE '\\'");
        $this->assertInstanceOf(\PDOStatement::class, $tables);
        $tables->execute([self::NAMESPACE . '_' . \str_replace("'", '', $tenant) . '_notes_%']);
        $names = $tables->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $names, 'one fulltext table for ' . $tenant);
        $name = $names[0];
        $this->assertIsString($name);

        $statement = $this->pdo->query('SELECT rowid FROM `' . $name . '` WHERE `' . $name . "` MATCH 'word OR rewritten' ORDER BY rowid");
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        return \array_values(\array_map(static function (mixed $value): int {
            self::assertIsNumeric($value);

            return (int) $value;
        }, $statement->fetchAll(PDO::FETCH_COLUMN)));
    }
}
