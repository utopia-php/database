<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Every bulk write hands $onNext the written document and the stored document it replaced: null on create, the
 * document as read before the write on update and upsert, and the stored document itself on delete.
 */
final class OnNextTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = HookFixture::sqlite();
        HookFixture::seed($this->database, ['first', 'second']);
    }

    public function testCreatePassesNoPreviousDocument(): void
    {
        $calls = $this->record(fn (callable $onNext): int => $this->database->createDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3]),
        ], onNext: $onNext));

        $this->assertSame([['third', null]], $calls);
    }

    public function testUpdatePassesTheDocumentAsStoredBeforeTheUpdate(): void
    {
        $calls = $this->record(fn (callable $onNext): int => $this->database->updateDocuments(
            HookFixture::COLLECTION,
            new Document(['title' => 'renamed']),
            [Query::equal(Document::ID, ['first'])],
            onNext: $onNext,
        ), 'title');

        $this->assertSame([['renamed', 'first']], $calls);
    }

    public function testUpsertPassesNullForACreateAndTheStoredDocumentForAnUpdate(): void
    {
        $calls = $this->record(fn (callable $onNext): int => $this->database->upsertDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'first', 'title' => 'renamed', 'views' => 1]),
            new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3]),
        ], onNext: $onNext), 'title');

        $this->assertSame([['renamed', 'first'], ['third', null]], $calls);
    }

    public function testDeletePassesTheStoredDocumentAsThePreviousOne(): void
    {
        $pairs = [];
        $deleted = $this->database->deleteDocuments(
            HookFixture::COLLECTION,
            [Query::equal(Document::ID, ['second'])],
            onNext: function (Document $document, ?Document $previous) use (&$pairs): void {
                $pairs[] = [$document, $previous];
            },
        );

        $this->assertSame(1, $deleted);
        $this->assertCount(1, $pairs);
        [$document, $previous] = $pairs[0];
        $this->assertSame('second', $document->getId());
        $this->assertSame($document, $previous, 'The previous document is the stored one, not a copy of it');
        $this->assertSame(2, $previous->getAttribute('views'));
    }

    public function testAThrowingCallbackAbortsTheWrite(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('stop');

        $this->database->createDocuments(HookFixture::COLLECTION, [
            new Document([Document::ID => 'third', 'title' => 'third', 'views' => 3]),
        ], onNext: static function (): void {
            throw new RuntimeException('stop');
        });
    }

    /**
     * @param  callable(callable(Document, ?Document): void): int  $write
     * @return list<array{mixed, mixed}> Each call's document and previous document, by id or by $attribute
     */
    private function record(callable $write, string $attribute = Document::ID): array
    {
        $calls = [];
        $write(static function (Document $document, ?Document $previous = null) use (&$calls, $attribute): void {
            $calls[] = [$document->getAttribute($attribute), $previous?->getAttribute($attribute)];
        });

        return $calls;
    }
}
