<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class GetCollectionThrowsTest extends TestCase
{
    public function testGetCollectionThrowsForAMissingCollection(): void
    {
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $this->database()->getCollection('missing');
    }

    public function testGetCollectionReturnsAnExistingCollection(): void
    {
        $database = $this->database();
        $database->createCollection(Collection::create('books'));

        $this->assertSame('books', $database->getCollection('books')->getId());
    }

    public function testUpdateCollectionThrowsForAMissingCollection(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->updateCollection('missing', new CollectionUpdate(documentSecurity: false));
    }

    public function testDeleteCollectionThrowsForAMissingCollection(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->deleteCollection('missing');
    }

    public function testSizeOfAMissingCollectionThrows(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->getSizeOfCollection('missing');
    }

    public function testUpdateCollectionChangesOnlyTheGivenFields(): void
    {
        $database = $this->database();
        $permissions = [Permission::read(Role::any())];
        $database->createCollection(Collection::create('books', permissions: $permissions, documentSecurity: true));

        $updated = $database->updateCollection('books', new CollectionUpdate(documentSecurity: false));

        $this->assertFalse($updated->documentSecurity());
        $this->assertSame($permissions, $updated->getPermissions());
        $this->assertFalse($database->getCollection('books')->documentSecurity());
        $this->assertSame($permissions, $database->getCollection('books')->getPermissions());
    }

    public function testUpdateCollectionReplacesPermissions(): void
    {
        $database = $this->database();
        $database->createCollection(Collection::create('books', documentSecurity: false));
        $permissions = [Permission::read(Role::users())];

        $database->updateCollection('books', new CollectionUpdate(permissions: $permissions));

        $stored = $database->getCollection('books');
        $this->assertSame($permissions, $stored->getPermissions());
        $this->assertFalse($stored->documentSecurity());
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('get_collection')
            ->setNamespace('get_collection_'.\uniqid());
        $database->create();

        return $database;
    }
}
