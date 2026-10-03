<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class TransactionDefinitionReadTest extends TestCase
{
    private const string COLLECTION = 'accounts';

    public function testATransactionReadsAnUnchangedDefinitionOnce(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $reads = $database->withTransaction(function () use ($database, $adapter): int {
            $adapter->reset();
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $database->getDocument(self::COLLECTION, 'ada');
            $database->getCollection(self::COLLECTION);

            return $adapter->metadataReads;
        });

        $this->assertSame(1, $reads);
        $this->assertSame(2, $database->getDocument(self::COLLECTION, 'ada')->getAttribute('balance'));
    }

    public function testASchemaChangeInsideTheTransactionIsRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);

        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        [$before, $after] = $database->withTransaction(function () use ($database): array {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $before = $database->getCollection(self::COLLECTION);
            $database->updateCollection(self::COLLECTION, [Permission::read(Role::users())], true);

            return [$before, $database->getCollection(self::COLLECTION)];
        });

        $this->assertFalse($before->getAttribute('documentSecurity'));
        $this->assertTrue($after->getAttribute('documentSecurity'));
        $this->assertSame(['read("users")'], $after->getPermissions());
    }

    public function testADefinitionReadInOneTransactionIsNotReusedByTheNext(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);
        $database->withTransaction(function () use ($database): void {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $database->getCollection(self::COLLECTION);
        });

        $database->updateCollection(self::COLLECTION, [Permission::read(Role::any()), Permission::update(Role::any())], true);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $collection = $database->withTransaction(function () use ($database): Collection {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 3]));

            return $database->getCollection(self::COLLECTION);
        });

        $this->assertTrue($collection->getAttribute('documentSecurity'));
    }

    public function testChangingADefinitionReadInATransactionDoesNotChangeTheNextRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);

        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $second = $database->withTransaction(function () use ($database): Collection {
            $database->updateDocument(self::COLLECTION, 'ada', new Document(['balance' => 2]));
            $first = $database->getCollection(self::COLLECTION);
            $first->setAttribute('name', 'changed');
            $first->attributes[0]->setAttribute('size', 1);

            return $database->getCollection(self::COLLECTION);
        });

        $this->assertSame(self::COLLECTION, $second->getAttribute('name'));
        $this->assertSame(0, $second->attributes[0]->size);
    }

    private function database(CountingMemory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new RedisLeasableCache()));
        $database->setDatabase('transactions')->setNamespace('transactions_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: 'balance')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ada', 'balance' => 1]));
        $database->getDocument(self::COLLECTION, 'ada');

        return $database;
    }
}
