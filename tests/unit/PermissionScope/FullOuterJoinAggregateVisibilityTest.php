<?php

namespace Tests\Unit\PermissionScope;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Aggregates over a full outer join count and sum exactly the rows the join returns over the
 * documents direct reads return (contract C6): an unreadable author or book adds nothing to a
 * count, a group or a sum, and a book whose author cannot be read counts like a book without an
 * author. Under shared tables another tenant holds a readable copy of every document with larger
 * numbers, so a row that leaks across tenants changes every total.
 */
final class FullOuterJoinAggregateVisibilityTest extends TestCase
{
    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private const string BOOK = 'book';

    private const string TEAM = 'team';

    private const string GENRE = self::BOOK.'.genre';

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    private const int OTHER_TENANT_OFFSET = 1000;

    /**
     * Every author as [team, rank, readable]. "hidden" has a readable book, "absent" has none.
     *
     * @var array<string, array{string, int, bool}>
     */
    private const array AUTHOR_ROWS = [
        'a1' => ['red', 1, true],
        'a2' => ['blue', 2, true],
        'a3' => ['red', 4, true],
        'hidden' => ['blue', 8, false],
        'absent' => ['green', 16, false],
    ];

    /**
     * Every book as [author id, genre, pages, readable]. a1 has a readable and an unreadable book,
     * a2 only an unreadable one, and "ghost" and "phantom" name no author.
     *
     * @var array<string, array{string, ?string, int, bool}>
     */
    private const array BOOK_ROWS = [
        'b1' => ['a1', 'poetry', 10, true],
        'b2' => ['a1', 'prose', 20, false],
        'b3' => ['hidden', 'prose', 40, true],
        'b4' => ['ghost', null, 80, true],
        'b5' => ['a2', 'poetry', 160, false],
        'b6' => ['hidden', 'poetry', 320, false],
        'b7' => ['phantom', 'prose', 640, false],
    ];

    /**
     * Each grant as [authors granted, books granted, the totals direct reads allow]. A granted
     * collection shows every document; otherwise document security shows the readable ones.
     *
     * @return array<string, array{bool, bool, array{rows: int, authors: int, books: int, pages: int, ranks: int}}>
     */
    private static function grants(): array
    {
        return [
            'document security on both collections' => [false, false, ['rows' => 5, 'authors' => 3, 'books' => 3, 'pages' => 130, 'ranks' => 7]],
            'granted authors, document security on books' => [true, false, ['rows' => 6, 'authors' => 5, 'books' => 3, 'pages' => 130, 'ranks' => 31]],
            'document security on authors, granted books' => [false, true, ['rows' => 8, 'authors' => 4, 'books' => 7, 'pages' => 1270, 'ranks' => 8]],
            'collection grants on both collections' => [true, true, ['rows' => 9, 'authors' => 7, 'books' => 7, 'pages' => 1270, 'ranks' => 40]],
        ];
    }

    /**
     * @return iterable<string, array{bool, bool, bool, bool, array{rows: int, authors: int, books: int, pages: int, ranks: int}}>
     */
    public static function configurations(): iterable
    {
        foreach (['emulated' => false, 'native' => true] as $mode => $native) {
            foreach (['dedicated tables' => false, 'shared tables' => true] as $tables => $sharedTables) {
                foreach (self::grants() as $grant => [$grantAuthors, $grantBooks, $totals]) {
                    yield "{$mode} full outer join, {$tables}, {$grant}" => [$native, $sharedTables, $grantAuthors, $grantBooks, $totals];
                }
            }
        }
    }

    /**
     * @param  array{rows: int, authors: int, books: int, pages: int, ranks: int}  $totals
     */
    #[DataProvider('configurations')]
    public function testAggregatesCountOnlyTheRowsDirectReadsReturn(bool $native, bool $sharedTables, bool $grantAuthors, bool $grantBooks, array $totals): void
    {
        $database = $this->database($native, $sharedTables, $grantAuthors, $grantBooks);
        $rows = $this->directlyJoinedRows($database);

        $this->assertSame($totals, $this->totals($rows), 'Direct reads must return the readable documents of the selected tenant');
        $this->assertSame($totals, $this->readTotals($database), 'count() and sum() over the join');
        $this->assertSame($this->groupCounts($rows, 'genre'), $this->readGroupCounts($database, self::GENRE, 'genre'), 'counts grouped by a joined attribute');
        $this->assertSame($this->groupCounts($rows, self::TEAM), $this->readGroupCounts($database, self::TEAM, self::TEAM), 'counts grouped by a main attribute');
    }

    private function database(bool $native, bool $sharedTables, bool $grantAuthors, bool $grantBooks): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(
            $native ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo),
            new Cache(new None()),
        );
        $database
            ->setAuthorization($authorization)
            ->setDatabase('joins')
            ->setNamespace('aggregates')
            ->setSharedTables($sharedTables)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [
                Attribute::string(key: self::TEAM, size: 16, required: true),
                Attribute::integer(key: 'rank', required: true),
            ],
            permissions: $this->collectionPermissions($grantAuthors),
            documentSecurity: ! $grantAuthors,
        ));
        $database->createCollection(new Collection(
            id: self::BOOKS,
            attributes: [
                Attribute::string(key: 'authorId', size: 16, required: true),
                Attribute::string(key: 'genre', size: 16, required: false),
                Attribute::integer(key: 'pages', required: true),
            ],
            permissions: $this->collectionPermissions($grantBooks),
            documentSecurity: ! $grantBooks,
        ));

        if ($sharedTables) {
            $database->setTenant(self::OTHER_TENANT);
            $this->seed($database, self::OTHER_TENANT_OFFSET, everyoneReads: true);
            $database->setTenant(self::TENANT);
        }
        $this->seed($database, 0, everyoneReads: false);

        return $database;
    }

    /**
     * @return list<string>
     */
    private function collectionPermissions(bool $granted): array
    {
        return $granted
            ? [Permission::create(Role::any()), Permission::read(Role::any())]
            : [Permission::create(Role::any())];
    }

    private function seed(Database $database, int $offset, bool $everyoneReads): void
    {
        foreach (self::AUTHOR_ROWS as $id => [$team, $rank, $readable]) {
            $database->createDocument(self::AUTHORS, new Document([
                '$id' => $id,
                '$permissions' => [$this->readPermission($readable || $everyoneReads)],
                self::TEAM => $team,
                'rank' => $rank + $offset,
            ]));
        }
        foreach (self::BOOK_ROWS as $id => [$authorId, $genre, $pages, $readable]) {
            $database->createDocument(self::BOOKS, new Document([
                '$id' => $id,
                '$permissions' => [$this->readPermission($readable || $everyoneReads)],
                'authorId' => $authorId,
                'genre' => $genre,
                'pages' => $pages + $offset,
            ]));
        }
    }

    private function readPermission(bool $readable): string
    {
        return $readable ? Permission::read(Role::any()) : Permission::read(Role::user('someone-else'));
    }

    private function join(): Query
    {
        return Query::fullOuterJoin(self::BOOKS, '$id', 'authorId', '=', self::BOOK);
    }

    /**
     * The rows the full outer join returns over what direct reads of each collection return, with
     * nulls on the side a row has no document on.
     *
     * @return list<array{author: ?string, team: ?string, rank: ?int, book: ?string, genre: ?string, pages: ?int}>
     */
    private function directlyJoinedRows(Database $database): array
    {
        $authors = [];
        foreach ($database->find(self::AUTHORS, [Query::limit(100)]) as $author) {
            $authors[$author->getId()] = $author;
        }

        $rows = [];
        $matched = [];
        foreach ($database->find(self::BOOKS, [Query::limit(100)]) as $book) {
            $authorId = $book->getAttribute('authorId');
            $author = \is_string($authorId) ? $authors[$authorId] ?? null : null;
            if ($author !== null) {
                $matched[$author->getId()] = true;
            }
            $rows[] = $this->row($author, $book);
        }
        foreach ($authors as $id => $author) {
            if (! isset($matched[$id])) {
                $rows[] = $this->row($author, null);
            }
        }

        return $rows;
    }

    /**
     * @return array{author: ?string, team: ?string, rank: ?int, book: ?string, genre: ?string, pages: ?int}
     */
    private function row(?Document $author, ?Document $book): array
    {
        return [
            'author' => $author?->getId(),
            'team' => $this->stringOrNull($author?->getAttribute(self::TEAM)),
            'rank' => $this->integerOrNull($author?->getAttribute('rank')),
            'book' => $book?->getId(),
            'genre' => $this->stringOrNull($book?->getAttribute('genre')),
            'pages' => $this->integerOrNull($book?->getAttribute('pages')),
        ];
    }

    /**
     * @param  list<array{author: ?string, team: ?string, rank: ?int, book: ?string, genre: ?string, pages: ?int}>  $rows
     * @return array{rows: int, authors: int, books: int, pages: int, ranks: int}
     */
    private function totals(array $rows): array
    {
        return [
            'rows' => \count($rows),
            'authors' => \count(\array_filter($rows, static fn (array $row): bool => $row['author'] !== null)),
            'books' => \count(\array_filter($rows, static fn (array $row): bool => $row['book'] !== null)),
            'pages' => \array_sum(\array_map(static fn (array $row): int => $row['pages'] ?? 0, $rows)),
            'ranks' => \array_sum(\array_map(static fn (array $row): int => $row['rank'] ?? 0, $rows)),
        ];
    }

    /**
     * @return array{rows: int, authors: int, books: int, pages: int, ranks: int}
     */
    private function readTotals(Database $database): array
    {
        $documents = $database->find(self::AUTHORS, [
            $this->join(),
            Query::count('*', 'rows'),
            Query::count('$id', 'authors'),
            Query::count(self::BOOK.'.$id', 'books'),
            Query::sum(self::BOOK.'.pages', 'pages'),
            Query::sum('rank', 'ranks'),
        ]);
        $this->assertCount(1, $documents);
        $totals = $documents[0];

        return [
            'rows' => $this->integerOrNull($totals->getAttribute('rows')) ?? -1,
            'authors' => $this->integerOrNull($totals->getAttribute('authors')) ?? -1,
            'books' => $this->integerOrNull($totals->getAttribute('books')) ?? -1,
            'pages' => $this->integerOrNull($totals->getAttribute('pages')) ?? -1,
            'ranks' => $this->integerOrNull($totals->getAttribute('ranks')) ?? -1,
        ];
    }

    /**
     * @param  list<array{author: ?string, team: ?string, rank: ?int, book: ?string, genre: ?string, pages: ?int}>  $rows
     * @param  'genre'|'team'  $key
     * @return list<array{?string, int}>
     */
    private function groupCounts(array $rows, string $key): array
    {
        $counts = [];
        foreach ($rows as $row) {
            $group = \json_encode($row[$key]);
            $counts[$group] = [$row[$key], ($counts[$group][1] ?? 0) + 1];
        }

        return $this->sorted(\array_values($counts));
    }

    /**
     * A group comes back under the grouped attribute's own name, without the join alias.
     *
     * @return list<array{?string, int}>
     */
    private function readGroupCounts(Database $database, string $attribute, string $key): array
    {
        $counts = \array_map(
            fn (Document $group): array => [
                $this->stringOrNull($group->getAttribute($key)),
                $this->integerOrNull($group->getAttribute('rows')) ?? -1,
            ],
            $database->find(self::AUTHORS, [$this->join(), Query::groupBy([$attribute]), Query::count('*', 'rows')]),
        );

        return $this->sorted(\array_values($counts));
    }

    /**
     * @param  list<array{?string, int}>  $counts
     * @return list<array{?string, int}>
     */
    private function sorted(array $counts): array
    {
        \usort($counts, static fn (array $left, array $right): int => \strcmp((string) \json_encode($left), (string) \json_encode($right)));

        return $counts;
    }

    private function stringOrNull(mixed $value): ?string
    {
        return \is_string($value) && $value !== '' ? $value : null;
    }

    private function integerOrNull(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
