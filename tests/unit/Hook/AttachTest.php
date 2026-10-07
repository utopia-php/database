<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Event\HookFixture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Attachable;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Mirror;
use Utopia\Database\Relationship;
use Utopia\Query\Hook;

final class AttachTest extends TestCase
{
    public function testAddHookAttachesTheDatabaseItIsAddedTo(): void
    {
        $database = HookFixture::sqlite();
        $hook = new AttachTestHook();

        $database->addHook($hook);
        $database->createDocument(HookFixture::COLLECTION, new Document(['$id' => 'first', 'title' => 'first', 'views' => 1]));

        $this->assertSame([$database], $hook->attached);
        $this->assertSame(['first'], $hook->created);
    }

    public function testAHookThatFailsToAttachIsNotRegistered(): void
    {
        $database = HookFixture::sqlite();
        $hook = new AttachTestHook(new RuntimeException('Not this database'));

        try {
            $database->addHook($hook);
            $this->fail('A hook that failed to attach was added');
        } catch (RuntimeException $error) {
            $this->assertSame('Not this database', $error->getMessage());
        }
        $database->createDocument(HookFixture::COLLECTION, new Document(['$id' => 'first', 'title' => 'first', 'views' => 1]));

        $this->assertSame([], $hook->created);
    }

    public function testAnUnknownHookIsRefusedBeforeItIsAttached(): void
    {
        $database = HookFixture::memory();
        $hook = new class () implements Attachable, Hook {
            /** @var list<Database> */
            public array $attached = [];

            public function attach(Database $database): void
            {
                $this->attached[] = $database;
            }
        };

        try {
            $database->addHook($hook);
            $this->fail('An unknown hook was accepted');
        } catch (DatabaseException $error) {
            $this->assertSame('Unknown hook: '.$hook::class, $error->getMessage());
        }

        $this->assertSame([], $hook->attached);
    }

    public function testAMirrorAttachesTheHookToItself(): void
    {
        $mirror = new Mirror(HookFixture::sqlite(), new Database(new Memory(), new Cache(new None())));
        $hook = new AttachTestHook();

        $mirror->addHook($hook);

        $this->assertSame([$mirror], $hook->attached);
    }

    public function testTheRelationshipsHookRelatesThroughTheDatabaseItIsAddedTo(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setDatabase('attach')->setNamespace('attach');
        $database->create();
        foreach (['parents', 'children'] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            ));
        }
        $database->createRelationship('parents', Relationship::oneToMany(relatedCollection: 'children', twoWay: true, key: 'children', twoWayKey: 'parent'));

        $database->addHook(new Relationships());
        $database->createDocument('parents', new Document([
            '$id' => 'p1',
            'name' => 'p1',
            'children' => [new Document(['$id' => 'c1', 'name' => 'c1'])],
        ]));

        $children = $database->getDocument('parents', 'p1')->getAttribute('children');
        $this->assertIsArray($children);
        $this->assertCount(1, $children);
        $this->assertInstanceOf(Document::class, $children[0]);
        $this->assertSame('c1', $children[0]->getId());
    }
}
