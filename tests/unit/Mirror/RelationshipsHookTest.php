<?php

namespace Tests\Unit\Mirror;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Mirror;
use Utopia\Database\Relationship;

final class RelationshipsHookTest extends TestCase
{
    /**
     * @return iterable<string, array{bool}>
     */
    public static function preparing(): iterable
    {
        yield 'preparing' => [true];
        yield 'not preparing' => [false];
    }

    public function testTheMirrorKeepsTheGivenHook(): void
    {
        $mirror = new Mirror($this->database(new Memory()), $this->database(new Memory()));
        $hook = new Relationships();

        $mirror->addHook($hook);

        $this->assertSame($hook, $mirror->getRelationshipHook());
    }

    public function testBothSidesRelateDocumentsWithACopyOfTheGivenHook(): void
    {
        $source = $this->database(new Memory());
        $destination = $this->database(new Memory());
        $mirror = new Mirror($source, $destination);
        $hook = new class (prepare: false) extends Relationships {
        };

        $mirror->addHook($hook);

        $this->assertInstanceOf($hook::class, $source->getRelationshipHook());
        $this->assertInstanceOf($hook::class, $destination->getRelationshipHook());
        $this->assertNotSame($hook, $source->getRelationshipHook());
        $this->assertNotSame($source->getRelationshipHook(), $destination->getRelationshipHook());
    }

    #[DataProvider('preparing')]
    public function testBothSidesHonourPrepare(bool $prepare): void
    {
        $source = $this->family(new CountingMemory());
        $destination = $this->family(new CountingMemory());
        $mirror = new Mirror($source, $destination);

        $mirror->addHook(new Relationships(prepare: $prepare));

        foreach (['source' => $source, 'destination' => $destination] as $side => $database) {
            $adapter = $database->getAdapter();
            $this->assertInstanceOf(CountingMemory::class, $adapter);
            $adapter->reset();

            $database->createDocument('parents', new Document([
                '$id' => 'p1',
                'name' => 'p1',
                'children' => [new Document(['$id' => 'c1', 'name' => 'c1'])],
            ]));

            if ($prepare) {
                $this->assertSame(0, $adapter->documentReads, 'The '.$side.' read a related document it prepares');
            } else {
                $this->assertGreaterThan(0, $adapter->documentReads, 'The '.$side.' prepared a related document it relates one by one');
            }
        }
    }

    public function testTheSourceRelatesDocumentsWithoutADestination(): void
    {
        $source = $this->family(new Memory());
        $mirror = new Mirror($source);

        $mirror->addHook(new Relationships(prepare: false));
        $mirror->createDocument('parents', new Document([
            '$id' => 'p1',
            'name' => 'p1',
            'children' => [new Document(['$id' => 'c1', 'name' => 'c1'])],
        ]));

        $child = $source->skipRelationships(static fn (): Document => $source->getDocument('children', 'c1'));
        $this->assertSame('p1', $child->getAttribute('parent'));
    }

    private function database(Memory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('mirror_relationships')->setNamespace('mirror');
        $database->create();

        return $database;
    }

    private function family(Memory $adapter): Database
    {
        $database = $this->database($adapter);
        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ];
        foreach (['parents', 'children'] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }
        $database->createRelationship('parents', Relationship::oneToMany(relatedCollection: 'children', twoWay: true, key: 'children', twoWayKey: 'parent'));

        return $database;
    }
}
