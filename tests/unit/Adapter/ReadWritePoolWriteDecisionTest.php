<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class ReadWritePoolWriteDecisionTest extends TestCase
{
    private const string DATABASE = 'replication';

    private const string NAMESPACE = 'replication';

    private const string COLLECTION = 'sessions';

    private SQLite $primary;

    private Database $database;

    protected function setUp(): void
    {
        $this->primary = new SQLite(new PDO('sqlite::memory:'));
        $replica = new SQLite(new PDO('sqlite::memory:'));

        foreach ([$this->primary, $replica] as $adapter) {
            $server = $this->createDatabase($adapter);
            $server->create();
            $server->createCollection(new Collection(
                id: self::COLLECTION,
                attributes: [Attribute::string(key: 'expiry', size: 32)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                documentSecurity: false,
            ));
            $server->createDocument(self::COLLECTION, new Document(['$id' => 'session', 'expiry' => '2025']));
        }

        $pool = new ReadWritePool($this->createConnections($this->primary), $this->createConnections($replica));
        $pool->setSticky(false);
        $this->database = $this->createDatabase($pool);
    }

    public function testDeleteDocumentsSelectsItsBatchOnThePrimary(): void
    {
        $this->createDatabase($this->primary)->updateDocument(self::COLLECTION, 'session', new Document(['expiry' => '2099']));

        $deleted = $this->database->deleteDocuments(self::COLLECTION, [Query::lessThan('expiry', '2026')]);

        $this->assertSame(0, $deleted, 'The batch was selected on a replica that has not seen the renewal');
        $this->assertSame('2099', $this->readFromThePrimary('session')->getAttribute('expiry'));
    }

    public function testUpdateDocumentsSelectsItsBatchOnThePrimary(): void
    {
        $this->createDatabase($this->primary)->updateDocument(self::COLLECTION, 'session', new Document(['expiry' => '2099']));

        $updated = $this->database->updateDocuments(
            self::COLLECTION,
            new Document(['expiry' => '2030']),
            [Query::lessThan('expiry', '2026')],
        );

        $this->assertSame(0, $updated, 'The batch was selected on a replica that has not seen the renewal');
        $this->assertSame('2099', $this->readFromThePrimary('session')->getAttribute('expiry'));
    }

    public function testUpsertComparesAgainstTheDocumentOnThePrimary(): void
    {
        $this->createDatabase($this->primary)->updateDocument(self::COLLECTION, 'session', new Document(['expiry' => '2099']));

        $upserted = $this->database->upsertDocuments(self::COLLECTION, [new Document(['$id' => 'session', 'expiry' => '2025'])]);

        $this->assertSame(1, $upserted, 'The existing document was read from a replica that has not seen the renewal');
        $this->assertSame('2025', $this->readFromThePrimary('session')->getAttribute('expiry'));
    }

    private function readFromThePrimary(string $id): Document
    {
        return $this->createDatabase($this->primary)->getDocument(self::COLLECTION, $id);
    }

    private function createDatabase(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase(self::DATABASE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());

        return $database;
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function createConnections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return $connections;
    }
}
