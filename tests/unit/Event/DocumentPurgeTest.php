<?php

namespace Tests\Unit\Event;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use TypeError;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Mirror;
use Utopia\Database\Query;

final class DocumentPurgeTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(): Database}>
     */
    public static function databases(): iterable
    {
        yield 'memory' => [HookFixture::memory(...)];
        yield 'sqlite' => [HookFixture::sqlite(...)];
    }

    /**
     * @return iterable<string, array{Closure(Database): mixed}>
     */
    public static function purgingCalls(): iterable
    {
        yield 'updateDocument' => [static fn (Database $database): mixed => $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']))];
        yield 'updateDocuments' => [static fn (Database $database): mixed => $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]))];
        yield 'upsertDocuments' => [static fn (Database $database): mixed => $database->upsertDocuments(HookFixture::COLLECTION, [new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 5])])];
        yield 'increaseDocumentAttribute' => [static fn (Database $database): mixed => $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views')];
        yield 'decreaseDocumentAttribute' => [static fn (Database $database): mixed => $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views')];
        yield 'deleteDocument' => [static fn (Database $database): mixed => $database->deleteDocument(HookFixture::COLLECTION, 'first')];
        yield 'deleteDocuments' => [static fn (Database $database): mixed => $database->deleteDocuments(HookFixture::COLLECTION)];
        yield 'purgeCachedDocument' => [static fn (Database $database): mixed => $database->purgeCachedDocument(HookFixture::COLLECTION, 'first')];
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentPurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));

        $this->assertSame([Event::DocumentPurge, Event::DocumentUpdate], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentPurgesBothIdentifiersOfARenamedDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first']);

        $database->updateDocument(HookFixture::COLLECTION, 'first', new Document([Document::ID => 'renamed']));

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentUpdate], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/renamed'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentsPurgesEveryUpdatedDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second', 'third']);

        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), [Query::notEqual('title', 'third')]);

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentsUpdate], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/second'], $this->purged($recorder));
    }

    public function testUpsertDocumentsPurgesEveryUpsertedDocument(): void
    {
        [$database, $recorder] = $this->seeded(HookFixture::sqlite(), ['first', 'second']);

        $database->upsertDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'first', 'title' => 'upserted', 'views' => 5]),
            new Document([Document::ID => 'fourth', 'title' => 'fourth', 'views' => 4]),
        ]);

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentsUpsert], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/fourth'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testIncreaseDocumentAttributePurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', 2);

        $this->assertSame([Event::DocumentPurge, Event::DocumentIncrease], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testDecreaseDocumentAttributePurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'second', 'views');

        $this->assertSame([Event::DocumentPurge, Event::DocumentDecrease], $recorder->getEvents());
        $this->assertSame(['posts/second'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testDeleteDocumentPurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second']);

        $database->deleteDocument(HookFixture::COLLECTION, 'first');

        $this->assertSame([Event::DocumentPurge, Event::DocumentDelete], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testDeleteDocumentsPurgesEveryDeletedDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second', 'third']);

        $database->deleteDocuments(HookFixture::COLLECTION, [Query::notEqual('title', 'second')]);

        $this->assertSame([Event::DocumentPurge, Event::DocumentPurge, Event::DocumentsDelete], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/third'], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testBatchWritesPurgeEveryBatch(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first', 'second', 'third']);

        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), batchSize: 2);
        $database->deleteDocuments(HookFixture::COLLECTION, batchSize: 2);

        $this->assertSame([
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentsUpdate,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentsDelete,
        ], $recorder->getEvents());
        $this->assertSame([
            'posts/first',
            'posts/second',
            'posts/third',
            'posts/first',
            'posts/second',
            'posts/third',
        ], $this->purged($recorder));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testWritesThatChangeNothingPurgeNothing(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first']);

        $database->updateDocument(HookFixture::COLLECTION, 'missing', new Document(['title' => 'renamed']));
        $database->deleteDocument(HookFixture::COLLECTION, 'missing');
        $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 10]), [Query::equal('title', ['missing'])]);
        $database->deleteDocuments(HookFixture::COLLECTION, [Query::equal('title', ['missing'])]);

        $this->assertSame([Event::DocumentsUpdate, Event::DocumentsDelete], $recorder->getEvents());
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testPurgeCachedDocumentPurgesTheDocument(Closure $database): void
    {
        [$database, $recorder] = $this->seeded($database(), ['first']);

        $this->assertTrue($database->purgeCachedDocument(HookFixture::COLLECTION, 'first'));

        $this->assertSame([Event::DocumentPurge], $recorder->getEvents());
        $this->assertSame(['posts/first'], $this->purged($recorder));
    }

    public function testDocumentPurgeFiresOnceWhenTheTransactionIsRetried(): void
    {
        $adapter = new class () extends Memory {
            public int $commitFailures = 0;

            public function commitTransaction(): bool
            {
                if ($this->commitFailures > 0) {
                    $this->commitFailures--;

                    throw new RuntimeException('commit lost');
                }

                return parent::commitTransaction();
            }
        };
        [$database, $recorder] = $this->seeded(HookFixture::database($adapter), ['first', 'second']);

        $adapter->commitFailures = 1;
        $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['title' => 'renamed']));
        $adapter->commitFailures = 1;
        $database->deleteDocuments(HookFixture::COLLECTION);

        $this->assertSame([
            Event::DocumentPurge,
            Event::DocumentUpdate,
            Event::DocumentPurge,
            Event::DocumentPurge,
            Event::DocumentsDelete,
        ], $recorder->getEvents());
        $this->assertSame(['posts/first', 'posts/first', 'posts/second'], $this->purged($recorder));
    }

    /**
     * @param  Closure(Database): mixed  $call
     */
    #[DataProvider('purgingCalls')]
    public function testDocumentPurgeHookFailureReachesTheCaller(Closure $call): void
    {
        foreach ([new RuntimeException('region broadcast failed'), new TypeError('broken hook')] as $failure) {
            $database = HookFixture::sqlite();
            HookFixture::seed($database, ['first']);
            $later = new RecordingLifecycle();
            $database
                ->addHook(new FailingLifecycle(Event::DocumentPurge, $failure))
                ->addHook($later);

            $this->assertSame($failure, $this->failureOf(static fn () => $call($database)));
            $this->assertNotContains(Event::DocumentPurge, $later->getEvents());
        }
    }

    public function testDocumentPurgeThroughMirrorReachesTheCaller(): void
    {
        $source = HookFixture::sqlite();
        HookFixture::seed($source, ['first']);
        $mirror = new Mirror($source);
        $failure = new RuntimeException('region broadcast failed');
        $mirror->addHook(new FailingLifecycle(Event::DocumentPurge, $failure));

        $this->assertSame($failure, $this->failureOf(static fn () => $mirror->purgeCachedDocument(HookFixture::COLLECTION, 'first')));
        $this->assertSame($failure, $this->failureOf(static fn () => $mirror->updateDocument(
            HookFixture::COLLECTION,
            'first',
            new Document(['title' => 'renamed']),
        )));
    }

    /**
     * @param  list<string>  $ids
     * @return array{Database, RecordingLifecycle}
     */
    private function seeded(Database $database, array $ids): array
    {
        HookFixture::seed($database, $ids);

        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        return [$database, $recorder];
    }

    /**
     * @return list<string>
     */
    private function purged(RecordingLifecycle $recorder): array
    {
        $purged = [];
        foreach ($recorder->getPayloads(Event::DocumentPurge) as $payload) {
            $this->assertInstanceOf(Document::class, $payload);
            $purged[] = $payload->getCollection().'/'.$payload->getId();
        }

        return $purged;
    }

    /**
     * @param  callable(): mixed  $call
     */
    private function failureOf(callable $call): ?Throwable
    {
        try {
            $call();
        } catch (Throwable $failure) {
            return $failure;
        }

        return null;
    }
}
