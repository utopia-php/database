<?php

namespace Tests\Unit\Relationship;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Relationship\Page;
use Utopia\Query\Method;

class PageTest extends TestCase
{
    /**
     * @return list<Document>
     */
    private function documents(): array
    {
        return \array_map(static fn (string $id): Document => new Document(['$id' => $id]), ['a', 'b', 'c', 'd', 'e']);
    }

    /**
     * @param  list<Document>  $documents
     * @return list<string>
     */
    private static function ids(array $documents): array
    {
        return \array_map(static fn (Document $document): string => $document->getId(), $documents);
    }

    public function testNoPaginationIsNoPage(): void
    {
        $filter = Query::equal('approved', [true]);

        [$page, $rest] = Page::from([$filter, Query::orderAsc('name')]);

        $this->assertNull($page);
        $this->assertCount(2, $rest);
        $this->assertSame($filter, $rest[0]);
    }

    public function testSplitsPaginationFromTheRest(): void
    {
        [$page, $rest] = Page::from([Query::limit(2), Query::equal('approved', [true]), Query::offset(1)]);

        $this->assertNotNull($page);
        $this->assertSame(2, $page->limit);
        $this->assertSame(1, $page->offset);
        $this->assertNull($page->cursor);
        $this->assertSame([Method::Equal], \array_map(static fn (Query $query): Method => $query->getMethod(), $rest));
    }

    public function testLimitAndOffset(): void
    {
        [$page] = Page::from([Query::limit(2), Query::offset(1)]);

        $this->assertNotNull($page);
        $this->assertSame(['b', 'c'], self::ids($page->slice($this->documents())));
    }

    public function testCursorAfterDocument(): void
    {
        [$page] = Page::from([Query::cursorAfter(new Document(['$id' => 'b'])), Query::limit(2)]);

        $this->assertNotNull($page);
        $this->assertSame(['c', 'd'], self::ids($page->slice($this->documents())));
    }

    public function testCursorAfterParsedId(): void
    {
        [$page] = Page::from([Query::parse(Query::cursorAfter(new Document(['$id' => 'c']))->toString())]);

        $this->assertNotNull($page);
        $this->assertSame('c', $page->cursor);
        $this->assertSame(['d', 'e'], self::ids($page->slice($this->documents())));
    }

    public function testCursorAfterWithOffset(): void
    {
        [$page] = Page::from([Query::cursorAfter(new Document(['$id' => 'a'])), Query::offset(1), Query::limit(2)]);

        $this->assertNotNull($page);
        $this->assertSame(['c', 'd'], self::ids($page->slice($this->documents())));
    }

    public function testCursorBefore(): void
    {
        [$page] = Page::from([Query::cursorBefore(new Document(['$id' => 'd'])), Query::limit(2)]);

        $this->assertNotNull($page);
        $this->assertSame(['b', 'c'], self::ids($page->slice($this->documents())));
    }

    public function testCursorBeforeWithOffsetCountsBackFromTheCursor(): void
    {
        [$page] = Page::from([Query::cursorBefore(new Document(['$id' => 'd'])), Query::offset(1), Query::limit(2)]);

        $this->assertNotNull($page);
        $this->assertSame(['a', 'b'], self::ids($page->slice($this->documents())));
    }

    public function testCursorBeforeWithOffsetPastTheWindowIsEmpty(): void
    {
        [$page] = Page::from([Query::cursorBefore(new Document(['$id' => 'b'])), Query::offset(1)]);

        $this->assertNotNull($page);
        $this->assertSame([], $page->slice($this->documents()));
    }

    public function testCursorTheParentDoesNotHoldIsEmpty(): void
    {
        [$page] = Page::from([Query::cursorAfter(new Document(['$id' => 'missing']))]);

        $this->assertNotNull($page);
        $this->assertSame([], $page->slice($this->documents()));
    }

    public function testFirstOfEachPaginationQueryWins(): void
    {
        [$page] = Page::from([Query::limit(1), Query::limit(4), Query::offset(2), Query::offset(0)]);

        $this->assertNotNull($page);
        $this->assertSame(['c'], self::ids($page->slice($this->documents())));
    }
}
