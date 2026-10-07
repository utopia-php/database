<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\Validator\Authorization;

final class ManyToManyDeleteTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Adapter, RelationshipSide}>
     */
    public static function deletions(): array
    {
        $memory = static fn (): Adapter => new Memory();
        $sqlite = static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'));

        return [
            'memory from the parent side' => [$memory, RelationshipSide::Parent],
            'memory from the child side' => [$memory, RelationshipSide::Child],
            'sqlite from the parent side' => [$sqlite, RelationshipSide::Parent],
            'sqlite from the child side' => [$sqlite, RelationshipSide::Child],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('deletions')]
    public function testDeletingAManyToManyDropsItsJunction(Closure $adapter, RelationshipSide $side): void
    {
        $database = $this->database($adapter());
        $junction = '_'.$database->getCollection('books')->getSequence().'_'.$database->getCollection('authors')->getSequence();

        match ($side) {
            RelationshipSide::Parent => $database->deleteRelationship('books', 'authors'),
            RelationshipSide::Child => $database->deleteRelationship('authors', 'books'),
        };

        $this->assertNull($database->findCollection($junction));

        $database->createRelationship('books', $this->relationship());

        $this->assertNotNull($database->findCollection($junction));
        $this->assertSame([], $database->getDocument('books', 'b1')->getAttribute('authors'));
        $this->assertSame([], $database->getDocument('authors', 'a1')->getAttribute('books'));
    }

    private function relationship(): Relationship
    {
        return Relationship::manyToMany('authors', key: 'authors', twoWay: true, twoWayKey: 'books');
    }

    private function database(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('many_to_many_delete')
            ->setNamespace('many_to_many_delete_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
        $database->createCollection(Collection::create(id: 'books', attributes: [Attribute::string(key: 'title', size: 64)], permissions: $permissions, documentSecurity: false));
        $database->createCollection(Collection::create(id: 'authors', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $permissions, documentSecurity: false));
        $database->createRelationship('books', $this->relationship());

        $database->createDocument('authors', new Document(['$id' => 'a1', 'name' => 'Ada']));
        $database->createDocument('books', new Document(['$id' => 'b1', 'title' => 'Notes', 'authors' => ['a1']]));

        return $database;
    }
}
