<?php

namespace Tests\Unit\Collections;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\StderrCapture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class CollectionGuardsTest extends TestCase
{
    private const string COLLECTION = 'shared';

    private const int TENANT = 5;

    public function testDeletingTheMetadataCollectionDropsItsTable(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter);

        $this->assertTrue($adapter->collectionExists('guards', Database::METADATA));
        $database->deleteCollection(Database::METADATA);
        $this->assertFalse($adapter->collectionExists('guards', Database::METADATA));
    }

    public function testDeletingTheMetadataCollectionDropsItsTableOnSQLite(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->database($adapter);

        $database->deleteCollection(Database::METADATA);
        $this->assertFalse($adapter->collectionExists('guards', Database::METADATA));
    }

    public function testAMetadataFailureWhoseCleanupAlsoFailsKeepsTheMetadataFailure(): void
    {
        $cause = new RuntimeException('the definition could not be written');
        $adapter = new class () extends Memory {
            public bool $failDrops = false;

            #[\Override]
            public function deleteCollection(string $id): bool
            {
                if ($this->failDrops) {
                    throw new RuntimeException('the table could not be dropped');
                }

                return parent::deleteCollection($id);
            }
        };
        $database = new class ($adapter, new Cache(new None()), $cause) extends Database {
            public function __construct(Adapter $adapter, Cache $cache, private readonly RuntimeException $cause)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function createDocument(string $collection, Document $document): Document
            {
                if ($collection === self::METADATA && $document->getId() === 'failing') {
                    throw $this->cause;
                }

                return parent::createDocument($collection, $document);
            }
        };
        $database->setDatabase('guards')->setNamespace('guards_'.\uniqid());
        $database->create();
        $adapter->failDrops = true;

        $error = null;
        $log = StderrCapture::during(function () use ($database, &$error): void {
            try {
                $database->createCollection(Collection::create(id: 'failing'));
            } catch (DatabaseException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $error, 'a collection whose definition is not written must not be created');
        $this->assertSame("Failed to create collection metadata for 'failing': the definition could not be written", $error->getMessage());
        $this->assertSame($cause, $error->getPrevious());
        $this->assertStringContainsString("Failed to rollback collection 'failing': the table could not be dropped", $log, 'the failed cleanup is logged');
    }

    public function testATenantCannotChangeATenantlessCollection(): void
    {
        $database = $this->sharedDatabase();
        $permissions = $database->getCollection(self::COLLECTION)->getPermissions();

        $database->setTenant(self::TENANT);
        try {
            $database->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::user('intruder'))], documentSecurity: true));
            $this->fail('a tenant must not change a collection it does not own');
        } catch (NotFoundException $error) {
            $this->assertSame('Collection not found', $error->getMessage());
        }

        $database->setTenant(null);
        $this->assertSame($permissions, $database->getCollection(self::COLLECTION)->getPermissions());
    }

    public function testATenantCannotDeleteATenantlessCollection(): void
    {
        $database = $this->sharedDatabase();

        $database->setTenant(self::TENANT);
        try {
            $database->deleteCollection(self::COLLECTION);
            $this->fail('a tenant must not delete a collection it does not own');
        } catch (NotFoundException $error) {
            $this->assertSame('Collection not found', $error->getMessage());
        }

        $database->setTenant(null);
        $this->assertNotNull($database->findCollection(self::COLLECTION));
    }

    public function testADefinitionThatCannotBeDeletedRestoresTheTable(): void
    {
        $adapter = new Memory();
        $cause = new RuntimeException('the definition could not be deleted');
        $database = new class ($adapter, new Cache(new None()), $cause) extends Database {
            public function __construct(Adapter $adapter, Cache $cache, private readonly RuntimeException $cause)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function deleteDocument(string $collection, string $id): bool
            {
                if ($collection === self::METADATA) {
                    throw $this->cause;
                }

                return parent::deleteDocument($collection, $id);
            }
        };
        $this->prepare($database);
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'row', 'name' => 'row']));

        try {
            $database->deleteCollection(self::COLLECTION);
            $this->fail('a collection whose definition stays must not lose its table');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to persist metadata for collection deletion '".self::COLLECTION."': the definition could not be deleted", $error->getMessage());
            $this->assertSame($cause, $error->getPrevious());
        }

        $this->assertTrue($adapter->collectionExists('guards', self::COLLECTION), 'the table is created again');
        $this->assertNotNull($database->findCollection(self::COLLECTION));
        $this->assertSame([], $database->find(self::COLLECTION), 'the restored table is empty: only its definition survives');
    }

    public function testTheSizeOfAMissingCollectionIsNotFound(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));

        foreach ([
            fn (): int => $database->getSizeOfCollection('missing'),
            fn (): int => $database->getSizeOfCollectionOnDisk('missing'),
        ] as $size) {
            try {
                $size();
                $this->fail('a missing collection has no size');
            } catch (NotFoundException $error) {
                $this->assertSame('Collection not found', $error->getMessage());
            }
        }
    }

    public function testATenantCannotReadTheSizeOfATenantlessCollection(): void
    {
        $database = $this->sharedDatabase();
        $this->assertGreaterThanOrEqual(0, $database->getSizeOfCollection(self::COLLECTION));

        $database->setTenant(self::TENANT);
        foreach ([
            fn (): int => $database->getSizeOfCollection(self::COLLECTION),
            fn (): int => $database->getSizeOfCollectionOnDisk(self::COLLECTION),
        ] as $size) {
            try {
                $size();
                $this->fail('a tenant must not read the size of a collection it does not own');
            } catch (NotFoundException $error) {
                $this->assertSame('Collection not found', $error->getMessage());
            }
        }
    }

    public function testTheSizeOnDiskNeedsATenantUnderSharedTables(): void
    {
        $database = $this->sharedDatabase();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Missing tenant. Tenant must be set when table sharing is enabled.');

        $database->getSizeOfCollectionOnDisk(self::COLLECTION);
    }

    public function testALostCreationRaceKeepsTheTableAndLogsAFailedCachePurge(): void
    {
        $adapter = new Memory();
        $winner = new DuplicateException('Document already exists');
        $database = new class ($adapter, new Cache(new None()), $winner) extends Database {
            public function __construct(Adapter $adapter, Cache $cache, private readonly DuplicateException $winner)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function createDocument(string $collection, Document $document): Document
            {
                if ($collection === self::METADATA && $document->getId() === 'raced') {
                    throw $this->winner;
                }

                return parent::createDocument($collection, $document);
            }

            #[\Override]
            public function purgeCachedDocument(string $collection, string $id): void
            {
                if ($id === 'raced') {
                    throw new RuntimeException('the cache is down');
                }

                parent::purgeCachedDocument($collection, $id);
            }
        };
        $database->setDatabase('guards')->setNamespace('guards_'.\uniqid());
        $database->create();

        $error = null;
        $log = StderrCapture::during(function () use ($database, &$error): void {
            try {
                $database->createCollection(Collection::create(id: 'raced'));
            } catch (DuplicateException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DuplicateException::class, $error);
        $this->assertSame('Collection raced already exists', $error->getMessage());
        $this->assertSame($winner, $error->getPrevious());
        $this->assertStringContainsString('Warning: Failed to purge stale collection cache: the cache is down', $log);
        $this->assertTrue($adapter->collectionExists('guards', 'raced'), 'the table the winner described is kept');
    }

    public function testADefinitionThatCannotBeDeletedKeepsItsFailureWhenTheTableCannotBeRestored(): void
    {
        $adapter = new class () extends Memory {
            public bool $failCreates = false;

            #[\Override]
            public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
            {
                if ($this->failCreates) {
                    throw new RuntimeException('the table could not be created again');
                }

                return parent::createCollection($name, $attributes, $indexes);
            }
        };
        $cause = new RuntimeException('the definition could not be deleted');
        $database = new class ($adapter, new Cache(new None()), $cause) extends Database {
            public function __construct(Adapter $adapter, Cache $cache, private readonly RuntimeException $cause)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function deleteDocument(string $collection, string $id): bool
            {
                if ($collection === self::METADATA) {
                    throw $this->cause;
                }

                return parent::deleteDocument($collection, $id);
            }
        };
        $this->prepare($database);
        $adapter->failCreates = true;

        try {
            $database->deleteCollection(self::COLLECTION);
            $this->fail('a collection whose definition stays must not be reported deleted');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to persist metadata for collection deletion '".self::COLLECTION."': the definition could not be deleted", $error->getMessage());
            $this->assertSame($cause, $error->getPrevious());
        }

        $this->assertFalse($adapter->collectionExists('guards', self::COLLECTION), 'the table stays dropped');
        $this->assertNotNull($database->findCollection(self::COLLECTION), 'the definition stays');
    }

    private function database(Adapter $adapter): Database
    {
        return $this->prepare(new Database($adapter, new Cache(new None())));
    }

    private function prepare(Database $database): Database
    {
        $database->setDatabase('guards')->setNamespace('guards_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }

    private function sharedDatabase(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('guards')
            ->setNamespace('guards_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(null);
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
