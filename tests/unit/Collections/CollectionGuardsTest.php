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
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class CollectionGuardsTest extends TestCase
{
    private const string COLLECTION = 'shared';

    public function testDeletingTheMetadataCollectionDropsItsTable(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter);

        $this->assertTrue($adapter->exists('guards', Database::METADATA));
        $this->assertTrue($database->deleteCollection(Database::METADATA));
        $this->assertFalse($adapter->exists('guards', Database::METADATA));
    }

    public function testDeletingTheMetadataCollectionDropsItsTableOnSQLite(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->database($adapter);

        $this->assertTrue($database->deleteCollection(Database::METADATA));
        $this->assertFalse($adapter->exists('guards', Database::METADATA));
    }

    public function testAMetadataFailureWhoseCleanupAlsoFailsKeepsTheMetadataFailure(): void
    {
        $cause = new RuntimeException('the definition could not be written');
        $adapter = new class () extends Memory {
            public bool $failDrops = false;

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
                $database->createCollection(new Collection(id: 'failing'));
            } catch (DatabaseException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $error, 'a collection whose definition is not written must not be created');
        $this->assertSame("Failed to create collection metadata for 'failing': the definition could not be written", $error->getMessage());
        $this->assertSame($cause, $error->getPrevious());
        $this->assertStringContainsString("Failed to rollback collection 'failing': the table could not be dropped", $log, 'the failed cleanup is logged');
    }

    private function database(Adapter $adapter): Database
    {
        return $this->prepare(new Database($adapter, new Cache(new None())));
    }

    private function prepare(Database $database): Database
    {
        $database->setDatabase('guards')->setNamespace('guards_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
