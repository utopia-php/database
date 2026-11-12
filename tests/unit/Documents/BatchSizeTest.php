<?php

namespace Tests\Unit\Documents;

use Closure;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;

/**
 * Every bulk write and the cursor take a batch size of at most BATCH_SIZE: a larger one is refused before anything
 * is read or written, instead of being clamped.
 */
final class BatchSizeTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(Database, int): mixed}>
     */
    public static function batchedCalls(): iterable
    {
        yield 'createDocuments' => [static fn (Database $database, int $batchSize): int => $database->createDocuments(
            HookFixture::COLLECTION,
            [new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3])],
            $batchSize,
        )];
        yield 'updateDocuments' => [static fn (Database $database, int $batchSize): int => $database->updateDocuments(
            HookFixture::COLLECTION,
            new Document(['views' => 10]),
            batchSize: $batchSize,
        )];
        yield 'upsertDocuments' => [static fn (Database $database, int $batchSize): int => $database->upsertDocuments(
            HookFixture::COLLECTION,
            [new Document([Document::ID => 'first', 'title' => 'first', 'views' => 10])],
            $batchSize,
        )];
        yield 'deleteDocuments' => [static fn (Database $database, int $batchSize): int => $database->deleteDocuments(
            HookFixture::COLLECTION,
            batchSize: $batchSize,
        )];
        yield 'cursor' => [static fn (Database $database, int $batchSize): mixed => $database->cursor(
            HookFixture::COLLECTION,
            batchSize: $batchSize,
        )];
    }

    /**
     * @param  Closure(Database, int): mixed  $call
     */
    #[DataProvider('batchedCalls')]
    public function testABatchSizeAboveTheMaximumIsRefusedBeforeAnyWrite(Closure $call): void
    {
        $database = $this->database();

        try {
            $call($database, Database::BATCH_SIZE + 1);
            $this->fail('A batch size above '.Database::BATCH_SIZE.' was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Batch size must be at most 1000, got 1001', $error->getMessage());
        }

        $this->assertSame([['first', 1], ['second', 2]], $this->stored($database));
    }

    /**
     * @param  Closure(Database, int): mixed  $call
     */
    #[DataProvider('batchedCalls')]
    public function testTheMaximumBatchSizeIsAccepted(Closure $call): void
    {
        $database = $this->database();

        $result = $call($database, Database::BATCH_SIZE);

        if ($result instanceof Generator) {
            $this->assertCount(2, \iterator_to_array($result, false));
        } else {
            $this->assertIsInt($result);
            $this->assertGreaterThan(0, $result);
        }
    }

    public function testBulkWritesWorkThroughBatchesSmallerThanTheInput(): void
    {
        $database = $this->database();
        $created = [];

        $count = $database->createDocuments(HookFixture::COLLECTION, \array_map(
            static fn (int $views): Document => new Document([Document::ID => 'more'.$views, 'title' => 'more', 'views' => $views]),
            \range(3, 7),
        ), 2, function (Document $document) use (&$created): void {
            $created[] = $document->getId();
        });

        $this->assertSame(5, $count);
        $this->assertSame(['more3', 'more4', 'more5', 'more6', 'more7'], $created);
        $this->assertSame(7, $database->updateDocuments(HookFixture::COLLECTION, new Document(['title' => 'same']), batchSize: 2));
        $this->assertSame(7, $database->deleteDocuments(HookFixture::COLLECTION, batchSize: 2));
        $this->assertSame([], $this->stored($database));
    }

    private function database(): Database
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, ['first', 'second']);

        return $database;
    }

    /**
     * @return list<array{string, mixed}>
     */
    private function stored(Database $database): array
    {
        return \array_values(\array_map(
            static fn (Document $document): array => [$document->getId(), $document->getAttribute('views')],
            $database->find(HookFixture::COLLECTION),
        ));
    }
}
