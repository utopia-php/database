<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Profiler\Log;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;

/**
 * A cursor over a joined read names the row the read returned: its order values, the main `$sequence` and each
 * joined `$id`. Paging walks every joined row exactly once, in both directions, through rows an outer join did not
 * match on either side; a cursor missing one of those values is refused by name.
 */
final class JoinCursorTest extends TestCase
{
    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')));
    }

    public function testCursorWithoutItsJoinedOrderValueIsRefusedByName(): void
    {
        $queries = [Query::join('notes', 'n', [Query::on('$id', 'author')]), Query::orderAsc('n.rank')];
        $cursor = $this->database->find('authors', [...$queries, Query::limit(1)])[0];
        $cursor->removeAttribute('n.rank');
        $this->assertNotNull($cursor->getAttribute('rank'), 'the main document\'s attribute of the same name is there to fall back to');

        $this->expectException(OrderException::class);
        $this->expectExceptionMessage("Cursor has no value for order attribute 'n.rank'");

        $this->database->find('authors', [...$queries, Query::cursorAfter($cursor)]);
    }

    /**
     * @return iterable<string, array{Method, list<Query>, list<string>}>
     */
    public static function matchedJoins(): iterable
    {
        $inner = ['a1/n1', 'a1/n2', 'a1/n3', 'a2/n4', 'a2/n6'];
        $orders = [
            'joined ascending' => [Query::orderAsc('n.rank')],
            'joined descending' => [Query::orderDesc('n.rank')],
            'main attribute' => [Query::orderAsc('rank')],
            'main $id' => [Query::orderDesc('$id')],
            'default order' => [],
        ];

        foreach ([
            'inner join' => [Method::Join, $inner],
            'left join' => [Method::LeftJoin, [...$inner, 'a3/-']],
        ] as $joinName => [$join, $rows]) {
            foreach ($orders as $orderName => $order) {
                yield "{$joinName}, {$orderName}" => [$join, $order, $rows];
            }
        }
    }

    /**
     * @param  list<Query>  $order
     * @param  list<string>  $rows
     */
    #[DataProvider('matchedJoins')]
    public function testCursorPagingOverAOneToManyJoinReturnsEveryJoinedRowOnce(Method $join, array $order, array $rows): void
    {
        $this->assertPagesEveryRowOnce([new Query($join, 'notes', [Query::on('$id', 'author')], 'n'), ...$order], $rows);
    }

    public function testCursorFromAnotherJoinShapeIsRefusedByTheJoinedIdItLacks(): void
    {
        $read = [Query::join('notes', 'n', [Query::on('$id', 'author')]), Query::orderAsc('rank')];
        $foreign = $this->database->find('authors', [Query::join('notes', 'other', [Query::on('$id', 'author')]), Query::orderAsc('rank'), Query::limit(1)])[0];

        foreach ([$foreign, $this->database->getDocument('authors', 'a1')] as $cursor) {
            try {
                $this->database->find('authors', [...$read, Query::cursorAfter($cursor)]);
                $this->fail('A cursor that does not name a row of this join is refused');
            } catch (OrderException $exception) {
                $this->assertStringContainsString("Cursor has no value for order attribute 'n.\$id'", $exception->getMessage());
                $this->assertSame('n.$id', $exception->getAttribute());
            }
        }
    }

    public function testJoinedCursorOnTheLastRowReturnsNothing(): void
    {
        $queries = [Query::join('notes', 'n', [Query::on('$id', 'author')]), Query::orderAsc('n.rank')];
        $rows = $this->database->find('authors', $queries);

        $this->assertSame([], $this->database->find('authors', [...$queries, Query::cursorAfter($rows[\count($rows) - 1])]));
        $this->assertSame([], $this->database->find('authors', [...$queries, Query::cursorBefore($rows[0])]));
    }

    /**
     * Every row of the unpaged read, then: after and before each row the rest of the read in order, and a walk in
     * pages of two in both directions that returns each row exactly once.
     *
     * @param  list<Query>  $queries
     * @param  list<string>  $expected
     */
    private function assertPagesEveryRowOnce(array $queries, array $expected): void
    {
        $all = \array_values($this->database->find('authors', [...$queries, Query::limit(100)]));
        $keys = \array_map($this->key(...), $all);
        $sorted = $keys;
        \sort($sorted);
        \sort($expected);
        $this->assertSame($expected, $sorted, 'the unpaged read returns each joined row once');

        foreach ($all as $index => $row) {
            $this->assertSame(\array_slice($keys, $index + 1), $this->keys([...$queries, Query::cursorAfter($row)]), "after {$keys[$index]}");
            $this->assertSame(\array_slice($keys, 0, $index), $this->keys([...$queries, Query::cursorBefore($row)]), "before {$keys[$index]}");
        }

        $forward = [];
        $cursor = null;
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = $this->database->find('authors', [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
            \array_push($forward, ...\array_map($this->key(...), $rows));
            if (\count($rows) < 2) {
                break;
            }
            $cursor = $rows[1];
        }
        $this->assertSame($keys, $forward, 'paging forward in pages of two');

        $backward = [];
        $cursor = $all[\count($all) - 1];
        for ($page = 0; $page <= \count($all); $page++) {
            $rows = $this->database->find('authors', [...$queries, Query::limit(2), Query::cursorBefore($cursor)]);
            $backward = [...\array_map($this->key(...), $rows), ...$backward];
            if (\count($rows) < 2) {
                break;
            }
            $cursor = $rows[0];
        }
        $this->assertSame(\array_slice($keys, 0, -1), $backward, 'paging backward in pages of two from the last row');
    }

    /**
     * @param  list<Query>  $queries
     * @return list<string>
     */
    private function keys(array $queries): array
    {
        return \array_values(\array_map($this->key(...), $this->database->find('authors', [...$queries, Query::limit(100)])));
    }

    private function key(Document $row, string $alias = 'n'): string
    {
        $joined = $row->getAttribute($alias.'.$id');

        return ($row->getId() === '' ? '-' : $row->getId()).'/'.(\is_string($joined) ? $joined : '-');
    }

    /**
     * @return iterable<string, array{Method, bool, list<Query>, list<string>}>
     */
    public static function outerJoins(): iterable
    {
        $inner = ['a1/n1', 'a1/n2', 'a1/n3', 'a2/n4', 'a2/n6'];
        $orders = [
            'joined ascending' => [Query::orderAsc('n.rank')],
            'joined descending' => [Query::orderDesc('n.rank')],
            'main attribute' => [Query::orderAsc('rank')],
            'default order' => [],
        ];

        foreach ([
            'right join' => [Method::RightJoin, false, [...$inner, '-/n5']],
            'emulated full outer join' => [Method::FullOuterJoin, false, [...$inner, 'a3/-', '-/n5']],
            'native full outer join' => [Method::FullOuterJoin, true, [...$inner, 'a3/-', '-/n5']],
        ] as $joinName => [$join, $native, $rows]) {
            foreach ($orders as $orderName => $order) {
                yield "{$joinName}, {$orderName}" => [$join, $native, $order, $rows];
            }
        }
    }

    /**
     * @param  list<Query>  $order
     * @param  list<string>  $rows
     */
    #[DataProvider('outerJoins')]
    public function testCursorPagingOverAnOuterJoinPassesRowsWithoutAMainDocument(Method $join, bool $native, array $order, array $rows): void
    {
        if ($native) {
            $this->useDatabase(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $this->assertPagesEveryRowOnce([new Query($join, 'notes', [Query::on('$id', 'author')], 'n'), ...$order], $rows);
    }

    public function testPlainReadRefusesACursorWithoutAnIdAsBefore(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Invalid cursor: UID must contain at most');

        $this->database->find('authors', [Query::cursorAfter(new Document(['$collection' => 'authors', 'name' => 'a1', 'rank' => 1]))]);
    }

    /**
     * @return iterable<string, array{string, list<Query>, string, list<mixed>}>
     */
    public static function distinctReads(): iterable
    {
        yield 'distinct read' => ['notes', [Query::distinct(), Query::select(['label']), Query::orderAsc('label')], 'label', ['x', 'y', 'z']];
        yield 'distinct read, descending' => ['notes', [Query::distinct(), Query::select(['label']), Query::orderDesc('label')], 'label', ['z', 'y', 'x']];
        yield 'distinct read over a join' => [
            'authors',
            [Query::join('notes', 'n', [Query::on('$id', 'author')]), Query::distinct(), Query::select(['n.label']), Query::orderAsc('n.label')],
            'n.label',
            ['x', 'y'],
        ];
        yield 'distinct read over a left join, nulls included' => [
            'authors',
            [Query::leftJoin('notes', 'n', [Query::on('$id', 'author')]), Query::distinct(), Query::select(['n.rank']), Query::orderAsc('n.rank')],
            'n.rank',
            [null, 1, 2],
        ];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<mixed>  $values
     */
    #[DataProvider('distinctReads')]
    public function testCursorPagingOverADistinctReadReachesTheEnd(string $collection, array $queries, string $attribute, array $values): void
    {
        $paged = [];
        $cursor = null;
        for ($page = 0; $page <= \count($values); $page++) {
            $rows = $this->database->find($collection, [...$queries, Query::limit(1), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
            if ($rows === []) {
                break;
            }
            $paged[] = $rows[0]->getAttribute($attribute);
            $cursor = $rows[0];
        }

        $this->assertSame($values, $paged);
        $this->assertNotNull($cursor);
        $this->assertSame(\array_slice($values, 0, -1), \array_map(
            static fn (Document $row): mixed => $row->getAttribute($attribute),
            $this->database->find($collection, [...$queries, Query::cursorBefore($cursor)]),
        ));
    }

    public function testIterateOverADistinctReadReachesTheEnd(): void
    {
        $labels = [];
        foreach ($this->database->cursor('notes', [Query::distinct(), Query::select(['label']), Query::orderAsc('label')], batchSize: 1) as $row) {
            $labels[] = $row->getAttribute('label');
            if (\count($labels) > 3) {
                break;
            }
        }

        $this->assertSame(['x', 'y', 'z'], $labels);
    }

    public function testDistinctCursorNeedsAnOrderOnEverySelectedAttribute(): void
    {
        $queries = [Query::distinct(), Query::select(['label', 'rank']), Query::orderAsc('label')];
        $cursor = $this->database->find('notes', [...$queries, Query::limit(1)])[0];

        try {
            $this->database->find('notes', [...$queries, Query::cursorAfter($cursor)]);
            $this->fail('A distinct read whose order does not name every selected attribute cannot be paged');
        } catch (QueryException $exception) {
            $this->assertStringContainsString("'rank'", $exception->getMessage());
        }

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('A cursor on a distinct() read pages along its orders');

        $this->database->find('notes', [Query::distinct(), Query::select(['label']), Query::cursorAfter($this->database->getDocument('notes', 'n1'))]);
    }

    /**
     * @return iterable<string, array{list<Query>, ?string, list<string>}>
     */
    public static function cursorBatches(): iterable
    {
        yield 'no caller page' => [[], null, ['i01', 'i02', 'i03', 'i04', 'i05', 'i06', 'i07', 'i08', 'i09', 'i10']];
        yield 'an offset applies once' => [[Query::offset(2)], null, ['i03', 'i04', 'i05', 'i06', 'i07', 'i08', 'i09', 'i10']];
        yield 'a cursor starts the iteration, which then ends' => [[], 'i04', ['i05', 'i06', 'i07', 'i08', 'i09', 'i10']];
        yield 'a limit caps the iteration' => [[Query::limit(4)], null, ['i01', 'i02', 'i03', 'i04']];
        yield 'a limit and an offset' => [[Query::offset(5), Query::limit(4)], null, ['i06', 'i07', 'i08', 'i09']];
        yield 'a limit beyond the matches' => [[Query::limit(40)], null, ['i01', 'i02', 'i03', 'i04', 'i05', 'i06', 'i07', 'i08', 'i09', 'i10']];
        yield 'a descending order and an offset' => [[Query::orderDesc('$id'), Query::offset(1)], null, ['i09', 'i08', 'i07', 'i06', 'i05', 'i04', 'i03', 'i02', 'i01']];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<string>  $expected
     */
    #[DataProvider('cursorBatches')]
    public function testCursorBuildsEachBatchFromTheCallerQueries(array $queries, ?string $after, array $expected): void
    {
        $this->createItems();
        if ($after !== null) {
            $queries[] = Query::cursorAfter($this->database->getDocument('items', $after));
        }

        foreach ([1, 3, 4, 100] as $batchSize) {
            $ids = [];
            foreach ($this->database->cursor('items', $queries, $batchSize) as $item) {
                $ids[] = $item->getId();
                if (\count($ids) > 20) {
                    break;
                }
            }

            $this->assertSame($expected, $ids, "batches of {$batchSize}");
        }
    }

    /**
     * @return iterable<string, array{Method, list<Query>, string}>
     */
    public static function unpageableJoinedReads(): iterable
    {
        foreach (['inner join' => Method::Join, 'left join' => Method::LeftJoin] as $joinName => $join) {
            yield "{$joinName}, joined id not selected" => [$join, [Query::select(['name', 'n.rank'])], 'n.$id'];
            yield "{$joinName}, joined order not selected" => [$join, [Query::select(['name', 'n.$id']), Query::orderAsc('n.rank')], 'n.rank'];
        }
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('unpageableJoinedReads')]
    public function testPagingAJoinedReadItCannotPageFailsBeforeYieldingARow(Method $join, array $queries, string $missing): void
    {
        $queries = [new Query($join, 'notes', [Query::on('$id', 'author')], 'n'), ...$queries];
        $rows = $this->database->cursor('authors', $queries, 2);
        /** @var int $yielded */
        $yielded = 0;

        try {
            foreach ($rows as $row) {
                $yielded++;
            }
            $this->fail('A read whose rows lack a value its next page orders by cannot be paged');
        } catch (OrderException $exception) {
            $this->assertStringContainsString("Cursor has no value for order attribute '{$missing}'", $exception->getMessage());
        }

        $this->assertSame(0, $yielded, 'The read must be refused before the caller acts on any of its rows');
    }

    public function testPagingAJoinedReadThatFitsOnePageNeedsNoPagingValue(): void
    {
        $rows = \iterator_to_array($this->database->cursor('authors', [Query::join('notes', 'n', [Query::on('$id', 'author')]), Query::select(['name', 'n.rank'])], 10), false);

        $this->assertCount(5, $rows);
    }

    public function testCursorRefusesCursorBefore(): void
    {
        $this->createItems();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cursor before not supported in this method.');

        \iterator_to_array($this->database->cursor('items', [Query::cursorBefore($this->database->getDocument('items', 'i04'))]));
    }

    private function createItems(): void
    {
        $this->database->createCollection(Collection::create(
            id: 'items',
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        for ($number = 1; $number <= 10; $number++) {
            $id = \sprintf('i%02d', $number);
            $this->createDocument('items', $id, ['name' => $id]);
        }
    }

    /**
     * @return iterable<string, array{Method}>
     */
    public static function getDocumentJoins(): iterable
    {
        yield 'inner join' => [Method::Join];
        yield 'left join' => [Method::LeftJoin];
        yield 'right join' => [Method::RightJoin];
        yield 'full outer join' => [Method::FullOuterJoin];
    }

    #[DataProvider('getDocumentJoins')]
    public function testJoinedGetDocumentPairsTheLowestSequenceJoinedRow(Method $join): void
    {
        $this->database->createCollection(Collection::create(
            id: 'drafts',
            attributes: [Attribute::string(key: 'author', size: 16), Attribute::string(key: 'label', size: 16)],
            indexes: [Index::key('author_label', ['author', 'label'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        foreach (['d-first' => 'z', 'd-second' => 'm', 'd-third' => 'a'] as $id => $label) {
            $this->createDocument('drafts', $id, ['author' => 'a1', 'label' => $label]);
        }

        $document = $this->database->getDocument('authors', 'a1', [new Query($join, 'drafts', [Query::on('$id', 'author')], 'd')]);

        $this->assertSame('d-first', $document->getAttribute('d.$id'));
        $this->assertSame('z', $document->getAttribute('d.label'));
    }

    /**
     * @return iterable<string, array{list<Query>, OrderDirection, list<string>}>
     */
    public static function unlimitedOffsets(): iterable
    {
        yield 'fast path' => [[], OrderDirection::Asc, ['i08', 'i09', 'i10']];
        yield 'builder path' => [[Query::notEqual('name', 'none')], OrderDirection::Asc, ['i08', 'i09', 'i10']];
        yield 'builder path, descending' => [[], OrderDirection::Desc, ['i03', 'i02', 'i01']];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<string>  $expected
     */
    #[DataProvider('unlimitedOffsets')]
    public function testAdapterFindWithAnOffsetAndNoLimitReturnsTheRowsAfterIt(array $queries, OrderDirection $direction, array $expected): void
    {
        $this->createItems();
        $collection = $this->database->getCollection('items');

        /** @var list<Document> $rows */
        $rows = $this->database->getAuthorization()->skip(fn (): array => $this->database->getAdapter()->find(
            $collection,
            $queries,
            limit: null,
            offset: 7,
            orderAttributes: ['$sequence'],
            orderTypes: [$direction],
        ));

        $this->assertSame($expected, \array_map(static fn (Document $row): string => $row->getId(), $rows));
    }

    /**
     * @return iterable<string, array{Query, bool}>
     */
    public static function joinTieKeys(): iterable
    {
        yield 'join on the joined $id' => [Query::join('authors', 'a', [Query::on('author', '$id')]), false];
        yield 'left join on the joined $id, qualified' => [Query::leftJoin('authors', 'a', [Query::on('author', 'a.$id')]), false];
        yield 'join on another joined attribute' => [Query::join('authors', 'a', [Query::on('author', 'name')]), true];
        yield 'right join on the joined $id' => [Query::rightJoin('authors', 'a', [Query::on('author', '$id')]), true];
        yield 'full outer join on the joined $id' => [Query::fullOuterJoin('authors', 'a', [Query::on('author', '$id')]), true];
        yield 'join on the joined $id with another operator' => [Query::join('authors', 'a', [Query::on('author', '$id', '!=')]), true];
    }

    #[DataProvider('joinTieKeys')]
    public function testJoinedIdBreaksTiesOnlyWhenAJoinCanPairSeveralRows(Query $join, bool $ordersByJoinedId): void
    {
        $this->database->setProfiling(true);
        $this->database->getProfiler()?->reset();

        $rows = $this->database->find('notes', [$join, Query::orderAsc('label')]);

        $selects = \array_values(\array_filter(
            $this->database->getProfiler()?->getLogs() ?? [],
            static fn (Log $log): bool => \str_starts_with($log->query, 'SELECT') && \str_contains($log->query, 'ORDER BY'),
        ));
        $this->assertNotSame([], $selects);
        $query = $selects[\count($selects) - 1]->query;
        $order = \substr($query, (int) \strrpos($query, 'ORDER BY'));
        $this->assertCount($ordersByJoinedId ? 3 : 2, \explode(',', $order), 'label, the main $sequence and, only when the join can pair several rows, the joined $id: '.$order);

        $keys = \array_map(fn (Document $row): string => $this->key($row, 'a'), $rows);
        $paged = [];
        $cursor = null;
        for ($page = 0; $page <= \count($rows); $page++) {
            $batch = $this->database->find('notes', [$join, Query::orderAsc('label'), Query::limit(1), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
            if ($batch === []) {
                break;
            }
            $paged[] = $this->key($batch[0], 'a');
            $cursor = $batch[0];
        }
        $this->assertSame($keys, $paged);
    }

    /**
     * @return iterable<string, array{Method, list<Query>, list<string>}>
     */
    public static function joinedTieKeysBySelection(): iterable
    {
        foreach (['inner join' => Method::Join, 'left join' => Method::LeftJoin] as $joinName => $join) {
            yield "{$joinName}, no select" => [$join, [], ['_id', 'n._uid']];
            yield "{$joinName}, select of everything" => [$join, [Query::select(['*'])], ['_id', 'n._uid']];
            yield "{$joinName}, select of a joined attribute" => [$join, [Query::select(['name', 'n.label'])], ['_id', 'n._uid']];
            yield "{$joinName}, select of every joined attribute" => [$join, [Query::select(['name', 'n.*'])], ['_id', 'n._uid']];
            yield "{$joinName}, select of main attributes" => [$join, [Query::select(['name', '$id', '$sequence'])], ['_id']];
            yield "{$joinName}, select of main attributes, ordered by a joined one" => [$join, [Query::select(['name']), Query::orderAsc('n.rank')], ['n.rank', '_id']];
        }
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<string>  $columns
     */
    #[DataProvider('joinedTieKeysBySelection')]
    public function testJoinedIdBreaksTiesOnlyWhereTheRowsShowTheJoin(Method $join, array $queries, array $columns): void
    {
        $queries = [new Query($join, 'notes', [Query::on('$id', 'author')], 'n'), ...$queries];

        $this->database->setProfiling(true);
        $this->database->getProfiler()?->reset();
        $rows = $this->database->find('authors', $queries);

        $this->assertSame($columns, $this->orderedColumns());

        $paged = [];
        for ($offset = 0; $offset < \count($rows); $offset++) {
            \array_push($paged, ...$this->database->find('authors', [...$queries, Query::limit(1), Query::offset($offset)]));
        }
        $this->assertSame(
            \array_map(static fn (Document $row): array => $row->getArrayCopy(), $rows),
            \array_map(static fn (Document $row): array => $row->getArrayCopy(), $paged),
            'Paging by offset returns the rows of the read at once, in its order',
        );
    }

    public function testCursorFromARowThatShowsNoJoinedIdIsRefusedAsBefore(): void
    {
        $queries = [Query::leftJoin('notes', 'n', [Query::on('$id', 'author')]), Query::select(['name'])];
        $cursor = $this->database->find('authors', [...$queries, Query::limit(1)])[0];

        $this->database->setProfiling(true);
        $this->database->getProfiler()?->reset();

        try {
            $this->database->find('authors', [...$queries, Query::cursorAfter($cursor)]);
            $this->fail('A row without the joined $id cannot name a joined row');
        } catch (OrderException $exception) {
            $this->assertSame('n.$id', $exception->getAttribute());
        }
    }

    /**
     * The columns of the last read's ORDER BY, unquoted, without their direction.
     *
     * @return list<string>
     */
    private function orderedColumns(): array
    {
        $selects = \array_values(\array_filter(
            $this->database->getProfiler()?->getLogs() ?? [],
            static fn (Log $log): bool => \str_starts_with($log->query, 'SELECT') && \str_contains($log->query, 'ORDER BY'),
        ));
        $this->assertNotSame([], $selects);
        $query = $selects[\count($selects) - 1]->query;
        $order = \substr($query, (int) \strrpos($query, 'ORDER BY') + \strlen('ORDER BY '));
        $order = \explode(' LIMIT ', $order)[0];

        return \array_map(
            static fn (string $column): string => \preg_replace('/^table_main\\.| (ASC|DESC)$/', '', \str_replace(['`', '"'], '', \trim($column))) ?? $column,
            \explode(',', $order),
        );
    }

    private function useDatabase(SQLite $adapter): void
    {
        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase('join_cursor')
            ->setNamespace('join_cursor_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->database->createCollection(Collection::create(
            id: 'authors',
            attributes: [Attribute::string(key: 'name', size: 16), Attribute::integer(key: 'rank', required: false)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $this->database->createCollection(Collection::create(
            id: 'notes',
            attributes: [
                Attribute::string(key: 'author', size: 16),
                Attribute::integer(key: 'rank', required: false),
                Attribute::string(key: 'label', size: 16),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach (['a1' => 1, 'a2' => 2, 'a3' => 3] as $id => $rank) {
            $this->createDocument('authors', $id, ['name' => $id, 'rank' => $rank]);
        }

        foreach ([
            'n1' => ['a1', 1, 'x'],
            'n2' => ['a1', 1, 'x'],
            'n3' => ['a1', 2, 'y'],
            'n4' => ['a2', 1, 'y'],
            'n5' => ['zz', 9, 'z'],
            'n6' => ['a2', null, 'x'],
        ] as $id => [$author, $rank, $label]) {
            $this->createDocument('notes', $id, ['author' => $author, 'rank' => $rank, 'label' => $label]);
        }
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createDocument(string $collection, string $id, array $attributes): void
    {
        $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            ...$attributes,
        ]));
    }
}
