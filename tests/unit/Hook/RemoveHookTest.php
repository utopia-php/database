<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Tests\Unit\Event\RecordingLifecycle;
use Tests\Unit\Relationships\RecordingWrite;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Decorator;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Hook\Write;
use Utopia\Database\Mirror;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\Role;

final class RemoveHookTest extends TestCase
{
    public function testRemovingALifecycleHookStopsItsEventsAndKeepsTheOthers(): void
    {
        $database = HookFixture::memory();
        $removed = new RecordingLifecycle();
        $kept = new RecordingLifecycle();
        $database->addHook($removed)->addHook($kept);

        $this->assertSame($database, $database->removeHook($removed));
        $database->getCollection(HookFixture::COLLECTION);

        $this->assertSame([], $removed->getEvents());
        $this->assertSame([Event::CollectionRead], $kept->getEvents());
    }

    public function testRemovingAClassRemovesEveryHookOfIt(): void
    {
        $database = HookFixture::memory();
        $first = new RecordingLifecycle();
        $second = new RecordingLifecycle();
        $database->addHook($first)->addHook($second);

        $database->removeHook(RecordingLifecycle::class);
        $database->getCollection(HookFixture::COLLECTION);

        $this->assertSame([], $first->getEvents());
        $this->assertSame([], $second->getEvents());
    }

    public function testRemovingADecoratorLeavesDocumentsUndecorated(): void
    {
        $database = HookFixture::memory();
        $decorator = new class () implements Decorator {
            public function decorate(Event $event, Document $collection, Document $document): Document
            {
                return $document->setAttribute('decorated', true);
            }
        };
        $database->addHook($decorator);
        HookFixture::seed($database, ['first']);

        $database->removeHook($decorator);

        $this->assertNull($database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('decorated'));
    }

    public function testRemovingAWriteHookStopsItInterceptingWrites(): void
    {
        $database = HookFixture::sqlite();
        $hook = new RecordingWrite();
        $database->addHook($hook);
        HookFixture::seed($database, ['first']);

        $database->removeHook($hook);
        HookFixture::seed($database, ['second']);

        $this->assertSame([['create', HookFixture::COLLECTION, ['first']]], $hook->writes);
    }

    public function testRemovingAWriteHookByAnInterfaceRemovesEveryWriteHook(): void
    {
        $database = HookFixture::sqlite();
        $hook = new RecordingWrite();
        $database->addHook($hook);

        $database->removeHook(Write::class);
        HookFixture::seed($database, ['first']);

        $this->assertSame([], $hook->writes);
    }

    public function testRemovingATransformStopsItRewritingStatements(): void
    {
        $database = HookFixture::sqlite();
        $transform = new class () implements Transform {
            public int $transformed = 0;

            public function transform(Event $event, string $query): string
            {
                $this->transformed++;

                return $query;
            }
        };
        $database->addHook($transform);
        HookFixture::seed($database, ['first']);
        $this->assertGreaterThan(0, $transform->transformed);

        $database->removeHook($transform);
        $transformed = $transform->transformed;
        HookFixture::seed($database, ['second']);

        $this->assertSame($transformed, $transform->transformed);
    }

    public function testRemovingTheRelationshipsHookStopsRelatingDocuments(): void
    {
        $database = $this->family();
        $database->addHook(new Relationships());

        $database->removeHook(Relationships::class);
        $database->createDocument('parents', $this->parent());

        $this->assertTrue($database->getDocument('children', 'c1')->isEmpty());
    }

    public function testRemovingTheRelationshipsHookFromAMirrorRemovesItFromBothSides(): void
    {
        $source = $this->family();
        $destination = $this->family();
        $mirror = new Mirror($source, $destination);
        $hook = new Relationships();
        $mirror->addHook($hook);

        $mirror->removeHook($hook);
        $source->createDocument('parents', $this->parent());
        $destination->createDocument('parents', $this->parent());

        $this->assertTrue($source->getDocument('children', 'c1')->isEmpty());
        $this->assertTrue($destination->getDocument('children', 'c1')->isEmpty());
    }

    public function testRemovingALifecycleHookFromAMirrorStopsItsEvents(): void
    {
        $mirror = new Mirror(HookFixture::memory());
        $recorder = new RecordingLifecycle();
        $mirror->addHook($recorder);

        $mirror->removeHook($recorder);
        $mirror->getCollection(HookFixture::COLLECTION);

        $this->assertSame([], $recorder->getEvents());
    }

    private function family(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setDatabase('remove_hook')->setNamespace('remove_hook');
        $database->create();
        foreach (['parents', 'children'] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            ));
        }
        $database->createRelationship('parents', Relationship::oneToMany(relatedCollection: 'children', twoWay: true, key: 'children', twoWayKey: 'parent'));

        return $database;
    }

    private function parent(): Document
    {
        return new Document([
            '$id' => 'p1',
            'name' => 'p1',
            'children' => [new Document(['$id' => 'c1', 'name' => 'c1'])],
        ]);
    }
}
