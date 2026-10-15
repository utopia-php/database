<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Decorator;
use Utopia\Database\Query;

final class DecoratorBatchTest extends TestCase
{
    public const string MARK = 'decoratedBy';

    public function testFindDecoratesEveryDocument(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['a', 'b', 'c']);
        $database->addHook($this->decorator());

        $documents = $database->find(HookFixture::COLLECTION, [Query::orderAsc('views')]);

        $this->assertSame(['a', 'b', 'c'], \array_map(static fn (Document $document): string => $document->getId(), $documents));
        foreach ($documents as $document) {
            $this->assertSame(Event::DocumentFind->value, $document->getAttribute(self::MARK), "{$document->getId()} must be decorated by find()");
        }
    }

    public function testSilencedFindIsNotDecorated(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['a', 'b']);
        $database->addHook($this->decorator());

        $documents = $database->silent(fn (): array => $database->find(HookFixture::COLLECTION));

        $this->assertCount(2, $documents);
        foreach ($documents as $document) {
            $this->assertNull($document->getAttribute(self::MARK));
        }
    }

    public function testCreateDocumentsHandsDecoratedDocumentsToItsCallback(): void
    {
        $database = HookFixture::memory();
        $database->addHook($this->decorator());

        $marks = $this->collect(fn (callable $onNext): int => $database->createDocuments(
            HookFixture::COLLECTION,
            [
                new Document([Document::ID => 'a', 'title' => 'a', 'views' => 1]),
                new Document([Document::ID => 'b', 'title' => 'b', 'views' => 2]),
            ],
            onNext: $onNext,
        ));

        $this->assertSame(['a' => Event::DocumentsCreate->value, 'b' => Event::DocumentsCreate->value], $marks);
    }

    public function testUpdateDocumentsHandsDecoratedDocumentsToItsCallback(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['a', 'b']);
        $database->addHook($this->decorator());

        $marks = $this->collect(fn (callable $onNext): int => $database->updateDocuments(
            HookFixture::COLLECTION,
            new Document(['views' => 10]),
            onNext: $onNext,
        ));

        $this->assertSame(['a' => Event::DocumentsUpdate->value, 'b' => Event::DocumentsUpdate->value], $marks);
    }

    public function testUpsertDocumentsHandsDecoratedDocumentsToItsCallback(): void
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, ['a']);
        $database->addHook($this->decorator());

        $marks = $this->collect(fn (callable $onNext): int => $database->upsertDocuments(
            HookFixture::COLLECTION,
            [
                new Document([Document::ID => 'a', 'title' => 'a', 'views' => 5]),
                new Document([Document::ID => 'b', 'title' => 'b', 'views' => 6]),
            ],
            onNext: $onNext,
        ));

        $this->assertSame(['a' => Event::DocumentsUpsert->value, 'b' => Event::DocumentsUpsert->value], $marks);
    }

    /**
     * @param  callable(callable(Document): void): int  $write
     * @return array<string, mixed>
     */
    private function collect(callable $write): array
    {
        $marks = [];
        $count = $write(static function (Document $document) use (&$marks): void {
            $marks[$document->getId()] = $document->getAttribute(self::MARK);
        });

        $this->assertSame(\count($marks), $count);
        \ksort($marks);

        return $marks;
    }

    private function decorator(): Decorator
    {
        return new class () implements Decorator {
            #[\Override]
            public function decorate(Event $event, Document $collection, Document $document): Document
            {
                return new Document([...$document->getArrayCopy(), DecoratorBatchTest::MARK => $event->value]);
            }
        };
    }
}
