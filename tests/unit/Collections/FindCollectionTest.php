<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Validator\Authorization;

final class FindCollectionTest extends TestCase
{
    public function testReturnsTheStoredCollection(): void
    {
        $database = $this->database();
        $database->createCollection(Collection::create('books', 'Books', [Attribute::string('title', 64)]));

        $found = $database->findCollection('books');

        $this->assertInstanceOf(Collection::class, $found);
        $this->assertSame('books', $found->getId());
        $this->assertSame('Books', $found->name());
        $this->assertSame(['title'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $found->attributes()));
    }

    public function testReturnsNullForAMissingCollection(): void
    {
        $this->assertNull($this->database()->findCollection('missing'));
    }

    public function testReturnsTheMetadataDefinition(): void
    {
        $found = $this->database()->findCollection(Database::METADATA);

        $this->assertInstanceOf(Collection::class, $found);
        $this->assertSame(Database::METADATA, $found->getId());
    }

    public function testReturnsNullAfterTheCollectionIsDeleted(): void
    {
        $database = $this->database();
        $database->createCollection(Collection::create('books'));

        $database->deleteCollection('books');

        $this->assertNull($database->findCollection('books'));
    }

    public function testAnotherTenantsCollectionIsNotFound(): void
    {
        $database = $this->database(sharedTables: true);
        $database->setTenant(1);
        $database->createCollection(Collection::create('books'));

        $this->assertSame('books', $database->findCollection('books')?->getId());

        $database->setTenant(2);

        $this->assertNull($database->findCollection('books'));

        try {
            $database->getCollection('books');
            $this->fail('Expected another tenant\'s collection to be not found');
        } catch (NotFoundException $error) {
            $this->assertSame('Collection not found', $error->getMessage());
        }
    }

    private function database(bool $sharedTables = false): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('find_collection')
            ->setNamespace('find_collection_'.\uniqid())
            ->setSharedTables($sharedTables);
        $database->create();

        return $database;
    }
}
