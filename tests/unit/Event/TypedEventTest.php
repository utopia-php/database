<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Hook\Selective;
use Utopia\Database\Index;
use Utopia\Database\Query;

/**
 * Every lifecycle hook receives one typed event per database event, carrying that event's payload as typed
 * properties: one test per event family.
 */
final class TypedEventTest extends TestCase
{
    private Database $database;

    private RecordingLifecycle $recorder;

    protected function setUp(): void
    {
        $this->database = HookFixture::sqlite();
        $this->recorder = new RecordingLifecycle();
        $this->database->addHook($this->recorder);
    }

    public function testEveryCaseButAllNamesAFinalEventClassThatCarriesIt(): void
    {
        foreach (Event::cases() as $case) {
            $class = $case->domain();
            if ($case === Event::All) {
                $this->assertNull($class);

                continue;
            }

            $this->assertNotNull($class, $case->value.' has no event class');
            $this->assertTrue(\is_subclass_of($class, Domain::class), $class.' is not a Domain event');
        }
    }

    public function testDatabaseEvents(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $database->list();
        $database->update('hooks', 'renamed');
        $database->delete('renamed');

        $this->assertSame([Event::DatabaseList, Event::DatabaseUpdate, Event::DatabaseDelete], $recorder->getEvents());
        [$listed, $updated, $deleted] = [
            $recorder->received(Event::DatabaseList)[0],
            $recorder->received(Event::DatabaseUpdate)[0],
            $recorder->received(Event::DatabaseDelete)[0],
        ];
        $this->assertInstanceOf(Event\Database\Listed::class, $listed);
        $this->assertContainsOnlyInstancesOf(Document::class, $listed->databases);
        $this->assertInstanceOf(Event\Database\Updated::class, $updated);
        $this->assertSame(['hooks', 'renamed'], [$updated->database, $updated->new]);
        $this->assertInstanceOf(Event\Database\Deleted::class, $deleted);
        $this->assertSame(['renamed', true], [$deleted->database, $deleted->deleted]);
    }

    public function testCollectionEvents(): void
    {
        $this->database->createCollection(Collection::create(id: 'comments'));
        $this->database->getCollection('comments');
        $this->database->updateCollection('comments', new CollectionUpdate(documentSecurity: true));
        $collections = $this->database->listCollections();
        $this->database->deleteCollection('comments');

        $created = $this->only(Event::CollectionCreate, Event\Collection\Created::class);
        $this->assertSame('comments', $created->collection);
        $this->assertSame('comments', $created->definition->getId());

        $read = $this->only(Event::CollectionRead, Event\Collection\Read::class);
        $this->assertSame('comments', $read->definition->getId());

        $updated = $this->only(Event::CollectionUpdate, Event\Collection\Updated::class);
        $this->assertTrue($updated->definition->documentSecurity());

        $this->assertSame($collections, $this->only(Event::CollectionList, Event\Collection\Listed::class)->collections);

        $deleted = $this->only(Event::CollectionDelete, Event\Collection\Deleted::class);
        $this->assertSame('comments', $deleted->collection);
    }

    public function testAttributeEvents(): void
    {
        $this->database->createAttribute(HookFixture::COLLECTION, Attribute::string(key: 'summary', size: 64));
        $this->database->createAttributes(HookFixture::COLLECTION, [Attribute::integer(key: 'likes')]);
        $this->database->updateAttribute(HookFixture::COLLECTION, 'likes', new AttributeUpdate(required: true));
        $this->database->renameAttribute(HookFixture::COLLECTION, 'summary', 'abstract');
        $this->database->deleteAttribute(HookFixture::COLLECTION, 'abstract');

        $this->assertSame(['summary', 'likes'], \array_map(
            static fn (Domain $event): string => $event instanceof Event\Attribute\Created ? $event->attribute->key : '',
            $this->recorder->received(Event::AttributeCreate),
        ));

        $batch = $this->only(Event::AttributesCreate, Event\Attribute\BatchCreated::class);
        $this->assertSame(HookFixture::COLLECTION, $batch->collection);
        $this->assertSame(['likes'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $batch->attributes));

        $updated = $this->only(Event::AttributeUpdate, Event\Attribute\Updated::class);
        $this->assertSame(['likes', true], [$updated->attribute->key, $updated->attribute->required]);

        $renamed = $this->only(Event::AttributeRename, Event\Attribute\Renamed::class);
        $this->assertSame([HookFixture::COLLECTION, 'summary', 'abstract'], [$renamed->collection, $renamed->old, $renamed->attribute->key]);

        $deleted = $this->only(Event::AttributeDelete, Event\Attribute\Deleted::class);
        $this->assertSame('abstract', $deleted->attribute->key);
    }

    public function testIndexEvents(): void
    {
        $this->database->createIndex(HookFixture::COLLECTION, Index::key(key: 'byTitle', attributes: ['title']));
        $this->database->createIndexes(HookFixture::COLLECTION, [
            Index::key(key: 'byViews', attributes: ['views']),
            Index::key(key: 'byBoth', attributes: ['title', 'views']),
        ]);
        $this->database->renameIndex(HookFixture::COLLECTION, 'byTitle', 'titles');
        $this->database->deleteIndex(HookFixture::COLLECTION, 'titles');

        $this->assertSame(['byTitle', 'byViews', 'byBoth'], \array_map(
            static fn (Domain $event): string => $event instanceof Event\Index\Created ? $event->index->key : '',
            $this->recorder->received(Event::IndexCreate),
        ));

        $batch = $this->only(Event::IndexesCreate, Event\Index\BatchCreated::class);
        $this->assertSame(['byViews', 'byBoth'], \array_map(static fn (Index $index): string => $index->key, $batch->indexes));

        $renamed = $this->only(Event::IndexRename, Event\Index\Renamed::class);
        $this->assertSame(['byTitle', 'titles'], [$renamed->old, $renamed->index->key]);

        $deleted = $this->only(Event::IndexDelete, Event\Index\Deleted::class);
        $this->assertSame([HookFixture::COLLECTION, 'titles'], [$deleted->collection, $deleted->index->key]);
    }

    public function testDocumentEvents(): void
    {
        $collection = HookFixture::COLLECTION;
        $this->database->createDocument($collection, new Document([Document::ID => 'first', 'title' => 'first', 'views' => 1]));
        $this->database->getDocument($collection, 'first');
        $this->database->updateDocument($collection, 'first', new Document(['title' => 'renamed']));
        $this->database->upsertDocument($collection, new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2]));
        $this->database->increaseDocumentAttribute($collection, 'first', 'views', 2);
        $this->database->decreaseDocumentAttribute($collection, 'first', 'views');
        $this->database->find($collection, [Query::orderAsc('title')]);
        $this->database->count($collection);
        $this->database->sum($collection, 'views');
        $this->database->deleteDocument($collection, 'second');

        $this->assertSame('first', $this->only(Event::DocumentCreate, Event\Document\Created::class)->document->getId());
        $this->assertSame('first', $this->only(Event::DocumentRead, Event\Document\Read::class)->document->getId());
        $this->assertSame('renamed', $this->only(Event::DocumentUpdate, Event\Document\Updated::class)->document->getAttribute('title'));

        $upserted = $this->only(Event::DocumentUpsert, Event\Document\Upserted::class);
        $this->assertSame(['second', true], [$upserted->document->getId(), $upserted->created]);
        $this->assertSame([], $this->recorder->received(Event::DocumentsUpsert), 'A single upsert fires no batch event');

        $increased = $this->only(Event::DocumentIncrease, Event\Document\Increased::class);
        $this->assertSame(['views', 3], [$increased->attribute, $increased->document->getAttribute('views')]);
        $decreased = $this->only(Event::DocumentDecrease, Event\Document\Decreased::class);
        $this->assertSame(['views', 2], [$decreased->attribute, $decreased->document->getAttribute('views')]);

        $found = $this->only(Event::DocumentFind, Event\Document\Found::class);
        $this->assertSame(['first', 'second'], \array_map(static fn (Document $document): string => $document->getId(), $found->documents));
        $counted = $this->only(Event::DocumentCount, Event\Document\Counted::class);
        $this->assertSame([$collection, 2], [$counted->collection, $counted->count]);

        $summed = $this->only(Event::DocumentSum, Event\Document\Summed::class);
        $this->assertSame(['views', 4], [$summed->attribute, $summed->sum]);

        $deleted = $this->only(Event::DocumentDelete, Event\Document\Deleted::class);
        $this->assertSame([$collection, 'second', 2], [$deleted->collection, $deleted->document->getId(), $deleted->document->getAttribute('views')]);

        $this->assertContains($collection.'/second', \array_map(
            static fn (Domain $event): string => $event instanceof Event\Document\Purged ? $event->collection.'/'.$event->id : '',
            $this->recorder->received(Event::DocumentPurge),
        ));
    }

    public function testBatchUpsertCountsCreatedAndUpdatedDocuments(): void
    {
        HookFixture::seed($this->database, ['first']);

        $this->database->upsertDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'first', 'title' => 'renamed', 'views' => 1]),
            new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2]),
            new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3]),
        ]);

        $batch = $this->only(Event::DocumentsUpsert, Event\Document\BatchUpserted::class);
        $this->assertSame([HookFixture::COLLECTION, 2, 1, 3], [$batch->collection, $batch->created, $batch->updated, $batch->count]);
        $this->assertSame([], $this->recorder->received(Event::DocumentUpsert));
    }

    public function testPermissionEventsCarryTheirCase(): void
    {
        $created = new Event\Permission\Created('posts', 'first', ['read("any")']);
        $read = new Event\Permission\Read('posts', 'first', ['read("any")']);
        $deleted = new Event\Permission\Deleted('posts', 'first', []);

        $this->assertSame(
            [Event::PermissionsCreate, Event::PermissionsRead, Event::PermissionsDelete],
            [$created->event, $read->event, $deleted->event],
        );
        $this->assertSame(['posts', 'first', ['read("any")']], [$created->collection, $created->document, $created->permissions]);
    }

    public function testNoEventIsBuiltForAnEventNoHookHandles(): void
    {
        $database = HookFixture::sqlite();
        $selective = new class () extends RecordingLifecycle implements Selective {
            public function handles(Event $event): bool
            {
                return $event === Event::DocumentCreate;
            }
        };
        $database->addHook($selective);

        HookFixture::seed($database, ['first']);
        $database->getDocument(HookFixture::COLLECTION, 'first');
        $database->find(HookFixture::COLLECTION);

        $this->assertSame([Event::DocumentCreate], $selective->getEvents());
    }

    /**
     * @template T of Domain
     *
     * @param  class-string<T>  $class
     * @return T
     */
    private function only(Event $event, string $class): Domain
    {
        $received = $this->recorder->received($event);
        $this->assertCount(1, $received, $event->value.' fired '.\count($received).' times');
        $this->assertInstanceOf($class, $received[0]);

        return $received[0];
    }
}
