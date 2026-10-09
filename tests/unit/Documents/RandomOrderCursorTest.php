<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Document;
use Utopia\Database\Query;

final class RandomOrderCursorTest extends TestCase
{
    public function testARandomOrderReadsEveryRowWhateverTheCursor(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second', 'third']);
        $cursor = $database->getDocument(HookFixture::COLLECTION, 'second');

        foreach ([Query::cursorAfter($cursor), Query::cursorBefore($cursor)] as $page) {
            $ids = \array_map(
                static fn (Document $document): string => $document->getId(),
                $database->find(HookFixture::COLLECTION, [Query::orderRandom(), $page]),
            );
            \sort($ids);

            $this->assertSame(['first', 'second', 'third'], $ids, $page->getMethod()->value);
        }
    }
}
