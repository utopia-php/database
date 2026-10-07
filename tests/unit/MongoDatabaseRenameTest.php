<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Mongo\Exception as MongoException;

final class MongoDatabaseRenameTest extends TestCase
{
    public function testRenameMovesEveryCollectionAndDropsTheOldDatabase(): void
    {
        $client = $this->client(['library' => ['authors', 'books', 'system.views']]);

        $this->assertTrue(new Mongo($client)->update('library', 'archive'));

        $this->assertSame(['archive' => ['authors', 'books']], $client->databases);
    }

    public function testAFailurePartWayMovesTheMovedCollectionsBack(): void
    {
        $client = $this->client(['library' => ['authors', 'books', 'loans']]);
        $client->failOn = 'library.loans';

        try {
            new Mongo($client)->update('library', 'archive');
            $this->fail('A failed collection move must fail the rename');
        } catch (DatabaseException|MongoException $error) {
            $this->assertStringContainsString('rename refused', $error->getMessage());
        }

        $this->assertSame(['library' => ['authors', 'books', 'loans']], $client->databases);
    }

    public function testAPagedListingIsRefusedBeforeAnythingMoves(): void
    {
        $client = $this->client(['library' => ['authors', 'books']]);
        $client->cursor = 42;

        try {
            new Mongo($client)->update('library', 'archive');
            $this->fail('A listing the server pages must refuse the rename');
        } catch (DatabaseException $error) {
            $this->assertSame('Database has more collections than one listing returns, so it cannot be renamed', $error->getMessage());
        }

        $this->assertSame(['library' => ['authors', 'books']], $client->databases);
    }

    public function testSharedTablesRefuseTheRenameBeforeAnythingMoves(): void
    {
        $client = $this->client(['library' => ['books']]);
        $adapter = new Mongo($client);
        $adapter->setSharedTables(true);

        try {
            $adapter->update('library', 'archive');
            $this->fail('A rename under shared tables must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot rename a database while shared tables are enabled', $error->getMessage());
        }

        $this->assertSame(['library' => ['books']], $client->databases);
    }

    public function testCollectionExistsAsksTheNamedDatabase(): void
    {
        $adapter = new Mongo($this->client(['library' => ['ns_books'], 'archive' => []]));
        $adapter->setNamespace('ns');

        $this->assertTrue($adapter->collectionExists('library', 'books'));
        $this->assertFalse($adapter->collectionExists('archive', 'books'));
    }

    public function testASharedCollectionAlreadyInTheAdaptersDatabaseIsNotCreatedAgain(): void
    {
        $client = $this->client(['library' => ['ns_books'], 'archive' => []]);
        $adapter = new Mongo($client);
        $adapter->setNamespace('ns');
        $adapter->setDatabase('library');
        $adapter->setSharedTables(true);

        $this->assertTrue($adapter->createCollection('books'));

        $this->assertSame(['library' => ['ns_books'], 'archive' => []], $client->databases);
    }

    /**
     * @param  array<string, list<string>>  $databases
     */
    private function client(array $databases): MongoDatabaseRenameClient
    {
        return new MongoDatabaseRenameClient($databases);
    }
}
