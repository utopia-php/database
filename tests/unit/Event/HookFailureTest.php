<?php

namespace Tests\Unit\Event;

use Closure;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use TypeError;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;

final class HookFailureTest extends TestCase
{
    /**
     * Events 7.x dispatched inside a try/catch, so a listener failure never failed the call.
     *
     * @return iterable<string, array{Event, Closure(Database): mixed}>
     */
    public static function isolatedOperations(): iterable
    {
        yield 'database list' => [Event::DatabaseList, static fn (Database $database): mixed => $database->list()];
        yield 'collection create' => [Event::CollectionCreate, static fn (Database $database): mixed => $database->createCollection(new Collection(id: 'comments'))];
        yield 'collection update' => [Event::CollectionUpdate, static fn (Database $database): mixed => $database->updateCollection(HookFixture::COLLECTION, [Permission::read(Role::any())], true)];
        yield 'collection read' => [Event::CollectionRead, static fn (Database $database): mixed => $database->getCollection(HookFixture::COLLECTION)];
        yield 'collection list' => [Event::CollectionList, static fn (Database $database): mixed => $database->listCollections()];
        yield 'document purge from createAttribute' => [Event::DocumentPurge, static fn (Database $database): mixed => $database->createAttribute(HookFixture::COLLECTION, Attribute::string(key: 'summary', size: 64))];
        yield 'attribute create' => [Event::AttributeCreate, static fn (Database $database): mixed => $database->createAttribute(HookFixture::COLLECTION, Attribute::string(key: 'summary', size: 64))];
        yield 'attributes create' => [Event::AttributesCreate, static fn (Database $database): mixed => $database->createAttributes(HookFixture::COLLECTION, [Attribute::string(key: 'summary', size: 64)])];
        yield 'attribute update' => [Event::AttributeUpdate, static fn (Database $database): mixed => $database->updateAttributeRequired(HookFixture::COLLECTION, 'title', true)];
        yield 'attribute delete' => [Event::AttributeDelete, static fn (Database $database): mixed => $database->deleteAttribute(HookFixture::COLLECTION, 'views')];
        yield 'index rename' => [Event::IndexRename, static function (Database $database): mixed {
            $database->createIndex(HookFixture::COLLECTION, Index::key(key: 'by_title', attributes: ['title']));

            return $database->renameIndex(HookFixture::COLLECTION, 'by_title', 'by_heading');
        }];
        yield 'index delete' => [Event::IndexDelete, static function (Database $database): mixed {
            $database->createIndex(HookFixture::COLLECTION, Index::key(key: 'by_title', attributes: ['title']));

            return $database->deleteIndex(HookFixture::COLLECTION, 'by_title');
        }];
        yield 'collection delete' => [Event::CollectionDelete, static fn (Database $database): mixed => $database->deleteCollection(HookFixture::COLLECTION)];
    }

    /**
     * Events 7.x dispatched without a try/catch, so a listener failure failed the call.
     *
     * @return iterable<string, array{Event, Closure(Database): mixed}>
     */
    public static function propagatingOperations(): iterable
    {
        yield 'index create' => [Event::IndexCreate, static fn (Database $database): mixed => $database->createIndex(HookFixture::COLLECTION, Index::key(key: 'by_title', attributes: ['title']))];
        yield 'document read' => [Event::DocumentRead, static fn (Database $database): mixed => $database->getDocument(HookFixture::COLLECTION, 'first')];
        yield 'document create' => [Event::DocumentCreate, static fn (Database $database): mixed => $database->createDocument(HookFixture::COLLECTION, new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2]))];
        yield 'documents create' => [Event::DocumentsCreate, static fn (Database $database): mixed => $database->createDocuments(HookFixture::COLLECTION, [new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2])])];
        yield 'document update' => [Event::DocumentUpdate, static fn (Database $database): mixed => $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']))];
        yield 'documents update' => [Event::DocumentsUpdate, static fn (Database $database): mixed => $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]))];
        yield 'documents upsert' => [Event::DocumentsUpsert, static fn (Database $database): mixed => $database->upsertDocuments(HookFixture::COLLECTION, [new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 5])])];
        yield 'document increase' => [Event::DocumentIncrease, static fn (Database $database): mixed => $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views')];
        yield 'document decrease' => [Event::DocumentDecrease, static fn (Database $database): mixed => $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views')];
        yield 'document delete' => [Event::DocumentDelete, static fn (Database $database): mixed => $database->deleteDocument(HookFixture::COLLECTION, 'first')];
        yield 'documents delete' => [Event::DocumentsDelete, static fn (Database $database): mixed => $database->deleteDocuments(HookFixture::COLLECTION)];
        yield 'document find' => [Event::DocumentFind, static fn (Database $database): mixed => $database->find(HookFixture::COLLECTION)];
        yield 'document find one' => [Event::DocumentFind, static fn (Database $database): mixed => $database->findOne(HookFixture::COLLECTION)];
        yield 'document count' => [Event::DocumentCount, static fn (Database $database): mixed => $database->count(HookFixture::COLLECTION)];
        yield 'document sum' => [Event::DocumentSum, static fn (Database $database): mixed => $database->sum(HookFixture::COLLECTION, 'views')];
    }

    /**
     * @param  Closure(Database): mixed  $operation
     */
    #[DataProvider('isolatedOperations')]
    public function testIsolatedEventSwallowsHookExceptionAndRunsLaterHooks(Event $event, Closure $operation): void
    {
        $database = $this->database();
        $later = new RecordingLifecycle();
        $database
            ->addHook(new FailingLifecycle($event, new RuntimeException('isolated')))
            ->addHook($later);

        $operation($database);

        $this->assertContains($event, $later->getEvents());
    }

    /**
     * @param  Closure(Database): mixed  $operation
     */
    #[DataProvider('propagatingOperations')]
    public function testPropagatingEventSurfacesFirstHookException(Event $event, Closure $operation): void
    {
        $database = $this->database();
        $failure = new RuntimeException('propagated');
        $later = new RecordingLifecycle();
        $database
            ->addHook(new FailingLifecycle($event, $failure))
            ->addHook($later);

        $this->assertSame($failure, $this->failureOf(static fn () => $operation($database)));
        $this->assertNotContains($event, $later->getEvents());
    }

    /**
     * @param  Closure(Database): mixed  $operation
     */
    #[DataProvider('isolatedOperations')]
    #[DataProvider('propagatingOperations')]
    public function testErrorAlwaysSurfaces(Event $event, Closure $operation): void
    {
        $database = $this->database();
        $error = new TypeError('broken hook');
        $database->addHook(new FailingLifecycle($event, $error));

        $this->assertSame($error, $this->failureOf(static fn () => $operation($database)));
    }

    public function testAssertionFailureRaisedInsideHookReachesCaller(): void
    {
        $database = $this->database();
        $failure = new AssertionFailedError('expected another event');
        $database->addHook(new FailingLifecycle(Event::DocumentCreate, $failure));

        $this->assertSame($failure, $this->failureOf(static fn () => $database->createDocument(
            HookFixture::COLLECTION,
            new Document([Document::ID => 'second', 'title' => 'second', 'views' => 2]),
        )));
    }

    private function database(): Database
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, ['first']);

        return $database;
    }

    /**
     * @param  callable(): mixed  $operation
     */
    private function failureOf(callable $operation): ?Throwable
    {
        try {
            $operation();
        } catch (Throwable $failure) {
            return $failure;
        }

        return null;
    }
}
