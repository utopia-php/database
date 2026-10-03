<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

/**
 * SQLite index names embed the tenant. The existence probe in createIndex()
 * and the lookup in deleteIndex() use the filtered tenant, so the index has to
 * be created under that same filtered name.
 */
final class SQLiteTenantIndexTest extends TestCase
{
    private const string NAMESPACE = 'tenant_index';

    private const string COLLECTION = 'profiles';

    private PDO $pdo;

    private SQLite $adapter;

    public function testDeletingAnIndexDropsItForATenantOutsideTheIdentifierAlphabet(): void
    {
        $database = $this->database('acme.1');
        $database->createIndex(self::COLLECTION, Index::unique(key: 'email', attributes: ['email']));

        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'email'));
        $this->assertSame([], $this->indexes(), 'The index deleteIndex() reported as dropped must be gone');

        $database->createDocument(self::COLLECTION, new Document(['email' => 'user@example.com']));
        $database->createDocument(self::COLLECTION, new Document(['email' => 'user@example.com']));

        $this->assertSame(2, $database->count(self::COLLECTION), 'A deleted unique index must stop rejecting duplicates');
    }

    public function testCreatingAnIndexThatAlreadyExistsIsANoOp(): void
    {
        $database = $this->database('acme.1');
        $index = Index::key(key: 'email', attributes: ['email']);
        $database->createIndex(self::COLLECTION, $index);

        $this->assertTrue($this->adapter->createIndex(self::COLLECTION, $index));
        $this->assertSame([self::NAMESPACE.'_acme1_'.self::COLLECTION.'_email'], $this->indexes());
    }

    public function testATenantCannotBreakOutOfTheIndexIdentifier(): void
    {
        $database = $this->database('a`b');
        $database->createIndex(self::COLLECTION, Index::key(key: 'email', attributes: ['email']));

        $this->assertSame([self::NAMESPACE.'_ab_'.self::COLLECTION.'_email'], $this->indexes());
    }

    private function database(string $tenant): Database
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->adapter = new SQLite($this->pdo);

        $database = new Database($this->adapter, new Cache(new None()));
        $database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables(true)
            ->setTenant($tenant)
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string('email', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        ));

        return $database;
    }

    /**
     * @return list<string>
     */
    private function indexes(): array
    {
        $statement = $this->pdo->query("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '".self::NAMESPACE.'_'.self::COLLECTION."'");
        $this->assertInstanceOf(PDOStatement::class, $statement);

        $names = [];
        foreach ($statement->fetchAll(PDO::FETCH_COLUMN) as $name) {
            if (\is_string($name) && \str_ends_with($name, '_email')) {
                $names[] = $name;
            }
        }

        return $names;
    }
}
