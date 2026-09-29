<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * A cursor over a joined read names the row the read returned: its order values, the main `$sequence` and each
 * joined `$id`. Paging walks every joined row exactly once, in both directions, through rows an outer join did not
 * match on either side; a cursor missing one of those values is refused by name.
 */
final class JoinCursorTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')));
    }

    public function testCursorWithoutItsJoinedOrderValueIsRefusedByName(): void
    {
        $queries = [Query::join('notes', '$id', 'author', '=', 'n'), Query::orderAsc('n.rank')];
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
        $this->assertPagesEveryRowOnce([new Query($join, 'notes', ['$id', '=', 'author', 'n']), ...$order], $rows);
    }

    public function testCursorFromAnotherJoinShapeIsRefusedByTheJoinedIdItLacks(): void
    {
        $read = [Query::join('notes', '$id', 'author', '=', 'n'), Query::orderAsc('rank')];
        $foreign = $this->database->find('authors', [Query::join('notes', '$id', 'author', '=', 'other'), Query::orderAsc('rank'), Query::limit(1)])[0];

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
        $queries = [Query::join('notes', '$id', 'author', '=', 'n'), Query::orderAsc('n.rank')];
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

    private function key(Document $row): string
    {
        $joined = $row->getAttribute('n.$id');

        return ($row->getId() === '' ? '-' : $row->getId()).'/'.(\is_string($joined) ? $joined : '-');
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

        $this->database->createCollection(new Collection(
            id: 'authors',
            attributes: [Attribute::string(key: 'name', size: 16), Attribute::integer(key: 'rank', required: false)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $this->database->createCollection(new Collection(
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
