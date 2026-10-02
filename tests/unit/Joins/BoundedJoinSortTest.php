<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\BoundedJoinSortSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Index;
use Utopia\Database\Profiler\QueryLog;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * A joined read ordered by main attributes up to a unique one picks the main rows its page can reach before it joins
 * them, so MariaDB and MySQL sort the joined rows of those main rows only: with left joins every main row it matches,
 * with inner joins and conditions on joined attributes those with a matching joined row. The read returns the rows,
 * the order and the pages of the read that sorts the whole join: every read here runs on both and has to agree row
 * for row, through offsets, cursors in both directions, iterate(), hidden documents and a second tenant's rows.
 */
final class BoundedJoinSortTest extends TestCase
{
    private const string PLAIN = 'plain tables, per-document permissions';

    private const string GRANTED = 'plain tables, collection permission';

    private const string SHARED = 'shared tables, per-document permissions';

    /**
     * Each tenant's authors: name, rank, readable. Two hidden authors take main rows a page could otherwise reach.
     */
    private const array AUTHORS = [
        'a01' => ['amber', 2, true],
        'a02' => ['basalt', 1, true],
        'a03' => ['cedar', null, true],
        'a04' => ['delta', 2, false],
        'a05' => ['ember', 3, true],
        'a06' => ['fjord', 1, true],
        'a07' => ['granite', null, false],
        'a08' => ['harbor', 2, true],
        'a09' => ['island', 4, true],
        'a10' => ['juniper', 1, true],
    ];

    /**
     * author, rank, readable: authors with no note, with hidden notes only, with ties on the rank, a note without an
     * author, and a note naming its author in another case, which SQLite matches in the collation of the id index.
     * The second tenant's n15 belongs to a05, whose own notes are hidden: read from the first tenant, it must not
     * count as a05's note.
     */
    private const array NOTES = [
        'n01' => ['a01', 1, true],
        'n02' => ['a01', 2, true],
        'n03' => ['a01', 1, true],
        'n04' => ['a02', 5, true],
        'n05' => ['a03', null, true],
        'n06' => ['a03', 2, false],
        'n07' => ['a04', 1, true],
        'n08' => ['a05', 1, false],
        'n09' => ['a06', 3, true],
        'n10' => ['a06', 3, true],
        'n11' => ['a08', 2, true],
        'n12' => ['a09', 1, true],
        'n13' => ['a09', 1, true],
        'n14' => ['a09', 4, true],
        'n15' => ['zz', 1, true],
        'n16' => ['A10', 2, true],
    ];

    /**
     * author, note, readable.
     */
    private const array TAGS = [
        't01' => ['a01', 'n01', true],
        't02' => ['a01', 'n01', true],
        't03' => ['a01', 'n03', true],
        't04' => ['a06', 'n09', true],
        't05' => ['a09', 'n14', true],
        't06' => ['a02', 'none', true],
        't07' => ['a10', 'none', true],
        't08' => ['a03', 'n05', false],
    ];

    /**
     * @var array<string, array{Database, Database}>
     */
    private array $databases = [];

    /**
     * @return iterable<string, array{string, list<Query>, bool}>
     */
    public static function reads(): iterable
    {
        $notes = Query::leftJoin('notes', '$id', 'author', '=', 'n');
        $innerNotes = Query::join('notes', '$id', 'author', '=', 'n');
        $joins = [
            'notes' => [$notes],
            'notes and their tags' => [$notes, Query::leftJoin('tags', 'n.$id', 'note', '=', 't')],
            'notes and the author\'s tags' => [$notes, Query::leftJoin('tags', '$id', 'author', '=', 't')],
            'inner notes' => [$innerNotes],
            'inner notes and their inner tags' => [$innerNotes, Query::join('tags', 'n.$id', 'note', '=', 't')],
            'notes and their inner tags' => [$notes, Query::join('tags', 'n.$id', 'note', '=', 't')],
            'notes and the author\'s inner tags' => [$notes, Query::join('tags', '$id', 'author', '=', 't')],
        ];
        $orders = [
            'default order' => [],
            'rank' => [Query::orderAsc('rank')],
            'rank descending' => [Query::orderDesc('rank')],
            'name' => [Query::orderAsc('name')],
            '$id descending' => [Query::orderDesc('$id')],
            'rank, $sequence, then the joined rank' => [Query::orderAsc('rank'), Query::orderAsc('$sequence'), Query::orderDesc('n.rank')],
        ];
        $joinedConditions = [
            'a joined attribute that is set' => [Query::isNotNull('n.rank')],
            'a joined attribute equal to a value' => [Query::equal('n.rank', [1])],
            'a joined attribute in a range' => [Query::between('n.rank', 1, 2)],
            'a joined attribute above a value, grouped with one below' => [Query::or([Query::greaterThan('n.rank', 2), Query::lessThan('n.rank', 2)])],
            'a main and a joined condition grouped with and' => [Query::and([Query::lessThan('rank', 4), Query::greaterThanEqual('n.rank', 1)])],
            'a condition on the tags of the notes' => [Query::leftJoin('tags', 'n.$id', 'note', '=', 't'), Query::isNotNull('t.$id')],
            'a joined condition grouped with and inside or' => [Query::or([Query::and([Query::lessThan('n.rank', 2), Query::or([Query::isNull('n.rank'), Query::equal('n.rank', [1])])]), Query::greaterThan('n.rank', 3)])],
            'a condition on the notes and on the author\'s tags' => [Query::leftJoin('tags', '$id', 'author', '=', 't'), Query::lessThanEqual('n.rank', 3), Query::startsWith('t.note', 'n0')],
        ];

        foreach ([self::PLAIN, self::GRANTED, self::SHARED] as $mode) {
            foreach ($joins as $joinName => $join) {
                foreach ($orders as $orderName => $order) {
                    yield "{$mode}: {$joinName}, {$orderName}" => [$mode, [...$join, ...$order], true];
                }
            }

            foreach ($joinedConditions as $conditionName => $conditions) {
                foreach (['default order' => [], 'rank descending' => [Query::orderDesc('rank')]] as $orderName => $order) {
                    yield "{$mode}: notes, {$conditionName}, {$orderName}" => [$mode, [$notes, ...$conditions, ...$order], true];
                    yield "{$mode}: inner notes, {$conditionName}, {$orderName}" => [$mode, [$innerNotes, ...$conditions, ...$order], true];
                }
            }

            yield "{$mode}: notes, joined attributes selected" => [$mode, [$notes, Query::orderAsc('rank'), Query::select(['name', 'rank', 'n.rank', 'n.$id'])], true];
            yield "{$mode}: inner notes, joined attributes selected" => [$mode, [$innerNotes, Query::orderAsc('rank'), Query::select(['name', 'rank', 'n.rank', 'n.$id'])], true];
            yield "{$mode}: notes, every attribute selected" => [$mode, [$notes, Query::orderDesc('rank'), Query::select(['*'])], true];
            yield "{$mode}: notes, a main condition" => [$mode, [$notes, Query::notEqual('name', 'cedar'), Query::orderAsc('rank')], true];
            yield "{$mode}: notes, main conditions grouped" => [$mode, [$notes, Query::or([Query::lessThan('rank', 2), Query::isNull('rank')])], true];
            yield "{$mode}: inner notes, a main and a joined condition" => [$mode, [$innerNotes, Query::notEqual('name', 'cedar'), Query::lessThan('n.rank', 3), Query::orderAsc('rank')], true];
            yield "{$mode}: inner notes, a joined attribute that is not set" => [$mode, [$innerNotes, Query::isNull('n.rank')], true];
            yield "{$mode}: notes and their inner tags, a joined condition that keeps notes without a rank" => [$mode, [$notes, Query::join('tags', 'n.$id', 'note', '=', 't'), Query::or([Query::isNull('n.rank'), Query::lessThan('n.rank', 4)])], true];
            yield "{$mode}: notes, a search on a main attribute" => [$mode, [$notes, Query::search('name', 'amber')], true];
            yield "{$mode}: notes, a search matching every author" => [$mode, [$notes, Query::search('name', 'one'), Query::orderAsc('rank')], true];
            yield "{$mode}: inner notes, a search and a joined condition" => [$mode, [$innerNotes, Query::search('name', 'one'), Query::isNotNull('n.rank'), Query::orderDesc('rank')], true];
            yield "{$mode}: notes, a search that excludes authors" => [$mode, [$notes, Query::notSearch('name', 'amber')], true];
            yield "{$mode}: notes, a joined attribute that is not set" => [$mode, [$notes, Query::isNull('n.rank'), Query::orderAsc('rank')], false];
            yield "{$mode}: notes, a joined attribute other than a value" => [$mode, [$notes, Query::notEqual('n.rank', 1)], true];
            yield "{$mode}: notes, a joined attribute that is not set or below a value" => [$mode, [$notes, Query::or([Query::isNull('n.rank'), Query::lessThan('n.rank', 2)])], false];
            yield "{$mode}: notes, a grouped condition naming a joined and a main attribute" => [$mode, [$notes, Query::or([Query::equal('n.rank', [1]), Query::isNull('rank')])], false];
            yield "{$mode}: inner notes, a grouped condition naming a joined and a main attribute" => [$mode, [$innerNotes, Query::or([Query::equal('n.rank', [1]), Query::isNull('rank')])], false];
            yield "{$mode}: notes and their tags, a grouped condition naming both" => [$mode, [$notes, Query::leftJoin('tags', 'n.$id', 'note', '=', 't'), Query::or([Query::equal('n.rank', [1]), Query::isNotNull('t.$id')])], false];
            yield "{$mode}: notes, ordered by a joined attribute first" => [$mode, [$notes, Query::orderAsc('n.rank')], false];
            yield "{$mode}: inner notes, ordered by a joined attribute first" => [$mode, [$innerNotes, Query::orderAsc('n.rank')], false];
            yield "{$mode}: notes, ordered by a main attribute that is not unique, then a joined one" => [$mode, [$notes, Query::orderAsc('rank'), Query::orderAsc('n.rank')], false];
            yield "{$mode}: notes, right join" => [$mode, [Query::rightJoin('notes', '$id', 'author', '=', 'n'), Query::orderAsc('rank')], false];
            yield "{$mode}: notes, right join behind an inner join" => [$mode, [$innerNotes, Query::rightJoin('tags', '$id', 'author', '=', 't')], false];
            yield "{$mode}: notes, full outer join" => [$mode, [Query::fullOuterJoin('notes', '$id', 'author', '=', 'n')], false];
            yield "{$mode}: notes, main attributes selected" => [$mode, [$notes, Query::orderAsc('rank'), Query::select(['name', 'rank'])], false];
        }
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('reads')]
    public function testEveryWindowOfTheReadMatchesTheReadThatSortsTheWholeJoin(string $mode, array $queries, bool $bounded): void
    {
        [$sorted, $bounding] = $this->databases($mode);
        $all = $this->rows($sorted, $queries, [Query::limit(100)]);
        $this->assertNotSame([], $all);
        $this->assertSame($all, $this->rows($bounding, $queries, [Query::limit(100)]));

        foreach ([1, 2, 3] as $limit) {
            for ($offset = 0; $offset <= \count($all); $offset++) {
                $page = [Query::limit($limit), Query::offset($offset)];
                $expected = \array_slice($all, $offset, $limit);
                $this->assertSame($expected, $this->rows($sorted, $queries, $page), "limit {$limit}, offset {$offset}: the read that sorts the whole join");
                $this->assertSame($expected, $this->rows($bounding, $queries, $page), "limit {$limit}, offset {$offset}");
                $this->assertSame($bounded ? $offset + $limit : null, $this->boundedMainRows($bounding), "limit {$limit}, offset {$offset}: main rows the join sees");
            }
        }
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('reads')]
    public function testCursorPagesMatchTheReadThatSortsTheWholeJoin(string $mode, array $queries, bool $bounded): void
    {
        [$sorted, $bounding] = $this->databases($mode);
        $all = $this->documents($sorted, $queries, [Query::limit(100)]);
        $keys = \array_map($this->key(...), $all);
        if (! $this->pageable($all, $queries)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        foreach ($all as $index => $row) {
            foreach ([1, 2] as $limit) {
                $after = \array_slice($keys, $index + 1, $limit);
                $before = \array_slice($keys, \max(0, $index - $limit), \min($limit, $index));
                foreach ([$sorted, $bounding] as $database) {
                    $this->assertSame($after, $this->keys($database, $queries, [Query::cursorAfter($row), Query::limit($limit)]), "{$limit} after {$keys[$index]}");
                    $this->assertSame($bounded && $database === $bounding ? $limit + 1 : null, $this->boundedMainRows($database), "{$limit} after {$keys[$index]}: main rows the join sees");
                    $this->assertSame($before, $this->keys($database, $queries, [Query::cursorBefore($row), Query::limit($limit)]), "{$limit} before {$keys[$index]}");
                    $this->assertSame($bounded && $database === $bounding ? $limit + 1 : null, $this->boundedMainRows($database), "{$limit} before {$keys[$index]}: main rows the join sees");
                }
            }

            foreach ([$sorted, $bounding] as $database) {
                $this->assertSame(\array_slice($keys, $index + 2, 2), $this->keys($database, $queries, [Query::cursorAfter($row), Query::offset(1), Query::limit(2)]), "offset 1 after {$keys[$index]}");
            }
        }
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('reads')]
    public function testIterationMatchesTheReadThatSortsTheWholeJoin(string $mode, array $queries, bool $bounded): void
    {
        [$sorted, $bounding] = $this->databases($mode);
        $all = $this->documents($sorted, $queries, [Query::limit(100)]);
        if (! $this->pageable($all, $queries)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $keys = \array_map($this->key(...), $all);
        foreach ([[Query::limit(2)], [Query::limit(3), Query::offset(1)]] as $page) {
            $skipped = $page[1] ?? null;
            $expected = $skipped === null ? $keys : \array_slice($keys, 1);
            foreach ([$sorted, $bounding] as $database) {
                $iterated = [];
                foreach ($database->iterate('authors', [...$queries, ...$page]) as $document) {
                    $iterated[] = $this->key($document);
                }
                $this->assertSame($expected, $iterated);

                $batched = [];
                foreach ($database->cursor('authors', [...$queries, ...\array_slice($page, 1)], 2) as $document) {
                    $batched[] = $this->key($document);
                }
                $this->assertSame($expected, $batched);
            }
        }
    }

    public function testAnotherTenantsMainRowsTakeNoPlaceInTheBoundedPage(): void
    {
        [$sorted, $bounding] = $this->databases(self::SHARED);
        $queries = [Query::leftJoin('notes', '$id', 'author', '=', 'n'), Query::limit(4)];

        foreach ([1, 2] as $tenant) {
            $sorted->setTenant($tenant);
            $bounding->setTenant($tenant);
            $expected = $this->rows($sorted, $queries, []);
            $this->assertCount(4, $expected);
            $this->assertSame($expected, $this->rows($bounding, $queries, []));
            foreach ($expected as $row) {
                $this->assertIsString($row['name']);
                $this->assertStringStartsWith($tenant === 1 ? 'one ' : 'two ', $row['name']);
            }
        }
    }

    /**
     * A read whose rows show no joined attribute orders by the main sequence alone (fix 60): there is no joined sort
     * to bound.
     */
    public function testAReadThatShowsNoJoinedAttributeIsNotBounded(): void
    {
        [$sorted, $bounding] = $this->databases(self::PLAIN);
        $queries = [Query::leftJoin('notes', '$id', 'author', '=', 'n'), Query::select(['name'])];

        $this->assertSame($this->rows($sorted, $queries, [Query::limit(5)]), $this->rows($bounding, $queries, [Query::limit(5)]));
        $this->assertNull($this->boundedMainRows($bounding));
    }

    /**
     * @param  list<Document>  $rows
     * @param  list<Query>  $queries
     */
    private function pageable(array $rows, array $queries): bool
    {
        foreach ($queries as $query) {
            if ($query->getMethod() === Method::Select && ! \in_array('*', $query->getValues(), true) && ! \in_array('n.$id', $query->getValues(), true)) {
                return false;
            }
        }

        return $rows !== [];
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<Query>  $page
     * @return list<array<string, mixed>>
     */
    private function rows(Database $database, array $queries, array $page): array
    {
        return \array_map(
            static function (Document $row): array {
                $copy = $row->getArrayCopy();
                unset($copy['$createdAt'], $copy['$updatedAt']);
                foreach (\array_keys($copy) as $key) {
                    if (\str_ends_with($key, '.$createdAt') || \str_ends_with($key, '.$updatedAt')) {
                        unset($copy[$key]);
                    }
                }

                return $copy;
            },
            $this->documents($database, $queries, $page),
        );
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<Query>  $page
     * @return list<Document>
     */
    private function documents(Database $database, array $queries, array $page): array
    {
        $database->getProfiler()?->reset();

        return \array_values($database->find('authors', [...$page, ...$queries]));
    }

    /**
     * @param  list<Query>  $queries
     * @param  list<Query>  $page
     * @return list<string>
     */
    private function keys(Database $database, array $queries, array $page): array
    {
        return \array_map($this->key(...), $this->documents($database, $queries, $page));
    }

    private function key(Document $row): string
    {
        $note = $row->getAttribute('n.$id');
        $tag = $row->getAttribute('t.$id');

        return ($row->getId() ?: '-').'/'.(\is_string($note) ? $note : '-').'/'.(\is_string($tag) ? $tag : '-');
    }

    /**
     * How many main rows the last read let its join see: the limit of the subquery that picks them, or null when the
     * read joins every main row it matches.
     */
    private function boundedMainRows(Database $database): ?int
    {
        $reads = \array_values(\array_filter(
            $database->getProfiler()?->getLogs() ?? [],
            static fn (QueryLog $log): bool => \str_starts_with($log->query, 'SELECT') && \str_contains($log->query, 'JOIN'),
        ));
        $this->assertNotSame([], $reads);
        $read = $reads[\count($reads) - 1];

        if (\preg_match('/^SELECT .+? FROM \(SELECT .+? LIMIT \?\) AS [`"]?table_main[`"]? /', $read->query, $match) !== 1) {
            return null;
        }

        $placeholder = \substr_count($match[0], '?') - 1;
        $bound = $read->bindings[$placeholder] ?? null;
        $this->assertIsInt($bound);

        return $bound;
    }

    /**
     * The same documents in a database that sorts the whole join and one that bounds it.
     *
     * @return array{Database, Database}
     */
    private function databases(string $mode): array
    {
        return $this->databases[$mode] ??= [
            $this->database(new SQLite(new PDO('sqlite::memory:')), $mode),
            $this->database(new BoundedJoinSortSQLite(new PDO('sqlite::memory:')), $mode),
        ];
    }

    private function database(SQLite $adapter, string $mode): Database
    {
        $authorization = new Authorization();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());
        $authorization->addRole(Role::user('reader')->toString());

        $shared = $mode === self::SHARED;
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('bounded_join_sort')
            ->setNamespace('bounded_join_sort')
            ->setSharedTables($shared)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $permissions = $mode === self::GRANTED
            ? [Permission::create(Role::any()), Permission::read(Role::user('reader'))]
            : [Permission::create(Role::any())];
        foreach ([
            'authors' => [Attribute::string(key: 'name', size: 32), Attribute::integer(key: 'rank', required: false)],
            'notes' => [Attribute::string(key: 'author', size: 16), Attribute::integer(key: 'rank', required: false)],
            'tags' => [Attribute::string(key: 'author', size: 16), Attribute::string(key: 'note', size: 16)],
        ] as $id => $attributes) {
            $database->createCollection(new Collection(
                id: $id,
                attributes: $attributes,
                indexes: $id === 'authors' ? [Index::fullText(key: 'name_search', attributes: ['name'])] : [],
                permissions: $permissions,
                documentSecurity: true,
            ));
        }

        $tenants = $shared ? [1 => 'one', 2 => 'two'] : [0 => 'one'];
        foreach ([
            'authors' => self::AUTHORS,
            'notes' => self::NOTES,
            'tags' => self::TAGS,
        ] as $collection => $documents) {
            foreach ($documents as $id => $values) {
                foreach ($tenants as $tenant => $prefix) {
                    if ($shared) {
                        $database->setTenant($tenant);
                    }
                    $attributes = match ($collection) {
                        'authors' => ['name' => $prefix.' '.$values[0], 'rank' => $values[1]],
                        'notes' => ['author' => $tenant === 2 && $id === 'n15' ? 'a05' : $values[0], 'rank' => $values[1]],
                        default => ['author' => $values[0], 'note' => $values[1]],
                    };
                    $readable = $values[2] && ! ($tenant === 2 && $id === 'a06');
                    $database->getAuthorization()->skip(fn () => $database->createDocument($collection, new Document([
                        '$id' => $id,
                        '$permissions' => [Permission::read(Role::user($readable ? 'reader' : 'other'))],
                        ...$attributes,
                    ])));
                }
            }
        }

        if ($shared) {
            $database->setTenant(1);
        }
        $database->enableProfiling();

        return $database;
    }
}
