<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

/**
 * Under shared tables a table holds every tenant's collection of that id, so a create whose definition is not stored
 * must never drop it: another tenant may have adopted it, even when this create made it.
 */
final class SharedCollectionCleanupTest extends TestCase
{
    public function testAMongoCreateWhoseDefinitionIsNotStoredKeepsTheSharedCollection(): void
    {
        $client = new class () extends Client {
            /** @var list<string> */
            public array $dropped = [];

            public function __construct()
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            #[\Override]
            public function getHost(): string
            {
                return 'mongo';
            }

            #[\Override]
            public function isReplicaSet(): bool
            {
                return false;
            }

            /**
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass
            {
                return (object) ['cursor' => (object) ['firstBatch' => [['name' => 'adopted']], 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $indexes
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function createIndexes(string $collection, array $indexes, array $options = []): bool
            {
                return true;
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function dropCollection(string $name, array $options = []): bool
            {
                $this->dropped[] = $name;

                return true;
            }
        };
        $adapter = new class ($client) extends Mongo {
            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                return new Document();
            }

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                throw new RuntimeException('not writable primary');
            }
        };
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        $this->attemptCreate($this->database($adapter));

        $this->assertSame([], $client->dropped, 'A shared collection other tenants use must not be dropped');
    }

    public function testASQLCreateWhoseDefinitionIsNotStoredKeepsATableAnotherTenantAdopted(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $peer = $this->database($this->sqlite($pdo, 1));
        $peer->create();

        $adapter = new class ($pdo) extends SQLite {
            /** @var (\Closure(): void)|null */
            public ?\Closure $beforeDefinition = null;

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                if ($collection->getId() === Database::METADATA && $this->beforeDefinition !== null) {
                    ($this->beforeDefinition)();

                    throw new RuntimeException('write conflict');
                }

                return parent::createDocument($collection, $document);
            }
        };
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);
        $adapter->beforeDefinition = static function () use ($peer): void {
            $peer->createCollection(self::definition());
            $peer->createDocument('logs', new Document(['$id' => 'kept', 'message' => 'peer']));
        };

        $this->attemptCreate($this->database($adapter));

        $this->assertSame('peer', $peer->getDocument('logs', 'kept')->getAttribute('message'), 'The table another tenant adopted must not be dropped');
    }

    private function attemptCreate(Database $database): void
    {
        try {
            $database->createCollection(self::definition());
        } catch (Throwable) {
            return;
        }

        $this->fail('A create whose definition is not stored must fail');
    }

    private static function definition(): Collection
    {
        return Collection::create(
            id: 'logs',
            attributes: [Attribute::string(key: 'message', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        );
    }

    private function sqlite(PDO $pdo, int $tenant): SQLite
    {
        $adapter = new SQLite($pdo);
        $adapter->setSharedTables(true);
        $adapter->setTenant($tenant);

        return $adapter;
    }

    private function database(Mongo|SQLite $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        return (new Database($adapter, new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('shared')
            ->setNamespace('shared');
    }
}
