<?php

namespace Tests\Unit;

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
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * A joined row's internal attributes, read under shared tables where two tenants reuse the same document ids, through
 * both the single-statement full outer join and its LEFT JOIN UNION ALL RIGHT JOIN emulation.
 */
final class JoinedInternalAttributesTest extends TestCase
{
    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private const string BOOK = 'book';

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    private const array INTERNALS = [
        Document::ID,
        Document::SEQUENCE,
        Document::CREATED_AT,
        Document::UPDATED_AT,
        Document::PERMISSIONS,
    ];

    /**
     * @return iterable<string, array{Method, bool, list<string>}>
     */
    public static function joins(): iterable
    {
        $paired = ['a1/b1', 'a1/b2', 'a2/b3'];

        foreach (['emulated' => false, 'native' => true] as $label => $native) {
            yield "inner join, {$label} full outer join" => [Method::Join, $native, $paired];
            yield "left join, {$label} full outer join" => [Method::LeftJoin, $native, [...$paired, 'a3/-']];
            yield "right join, {$label} full outer join" => [Method::RightJoin, $native, [...$paired, '-/b4']];
            yield "full outer join, {$label}" => [Method::FullOuterJoin, $native, [...$paired, 'a3/-', '-/b4']];
        }
    }

    /**
     * @param  list<string>  $rows
     */
    #[DataProvider('joins')]
    public function testAliasWildcardReturnsTheJoinedRowAsADirectReadReturnsIt(Method $join, bool $native, array $rows): void
    {
        $database = $this->database($native);

        $direct = [];
        foreach ($database->find(self::BOOKS) as $book) {
            $direct[$book->getId()] = $book;
        }

        $found = $database->find(self::AUTHORS, [$this->join($join), Query::select(['name', self::BOOK.'.*']), Query::limit(100)]);
        \sort($rows);
        $this->assertSame($rows, $this->sorted($found));

        foreach ($found as $row) {
            $this->assertFalse($row->offsetExists(self::BOOK.'.'.Document::TENANT));

            $id = $row->getAttribute(self::BOOK.'.$id');
            if ($id !== null) {
                $this->assertIsString($id);
            }
            foreach (self::INTERNALS as $internal) {
                $this->assertSame(
                    $id === null ? null : $direct[$id]->getAttribute($internal),
                    $row->getAttribute(self::BOOK.'.'.$internal),
                    self::BOOK.".{$internal} of ".($id ?? 'an unmatched row'),
                );
            }
        }
    }

    /**
     * @param  list<string>  $rows
     */
    #[DataProvider('joins')]
    public function testReadOrderedByAJoinedInternalAttributePagesEveryRowOnce(Method $join, bool $native, array $rows): void
    {
        $database = $this->database($native);
        \sort($rows);

        foreach ([
            Query::orderAsc(self::BOOK.'.$sequence'),
            Query::orderDesc(self::BOOK.'.$sequence'),
            Query::orderAsc(self::BOOK.'.$createdAt'),
            Query::orderDesc(self::BOOK.'.$updatedAt'),
        ] as $order) {
            foreach ([[], [Query::select(['*'])]] as $select) {
                $label = $order->getMethod()->value.' '.$order->getAttribute().($select === [] ? '' : ' with *');
                $queries = [$this->join($join), ...$select, $order];

                $all = $database->find(self::AUTHORS, [...$queries, Query::limit(100)]);
                $keys = \array_map($this->key(...), $all);
                $this->assertSame($rows, $this->sorted($all), $label);

                $forward = [];
                $cursor = null;
                while (\count($forward) <= \count($all)) {
                    $page = $database->find(self::AUTHORS, [...$queries, Query::limit(1), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
                    if ($page === []) {
                        break;
                    }
                    $forward[] = $this->key($page[0]);
                    $cursor = $page[0];
                }
                $this->assertSame($keys, $forward, "{$label}: forward");

                $backward = [];
                $cursor = $all[\count($all) - 1];
                while (\count($backward) <= \count($all)) {
                    $page = $database->find(self::AUTHORS, [...$queries, Query::limit(1), Query::cursorBefore($cursor)]);
                    if ($page === []) {
                        break;
                    }
                    \array_unshift($backward, $this->key($page[0]));
                    $cursor = $page[0];
                }
                $this->assertSame(\array_slice($keys, 0, -1), $backward, "{$label}: backward");
            }
        }
    }

    private function database(bool $native): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($native ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('joins')
            ->setNamespace('internals')
            ->setSharedTables(true)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: self::BOOKS,
            attributes: [Attribute::string(key: 'authorId', size: 64, required: true)],
            permissions: $permissions,
        ));

        $authorsByBook = [
            self::OTHER_TENANT => ['b1' => 'a3', 'b2' => 'a3', 'b3' => 'a3', 'b4' => 'a1'],
            self::TENANT => ['b1' => 'a1', 'b2' => 'a1', 'b3' => 'a2', 'b4' => 'zz'],
        ];
        foreach ($authorsByBook as $tenant => $books) {
            $database->setTenant($tenant);
            foreach (['a1', 'a2', 'a3'] as $author) {
                $database->createDocument(self::AUTHORS, new Document(['$id' => $author, 'name' => $author, '$permissions' => [Permission::read(Role::any())]]));
            }
            foreach ($books as $book => $author) {
                $database->createDocument(self::BOOKS, new Document([
                    '$id' => $book,
                    'authorId' => $author,
                    '$permissions' => [Permission::read(Role::any()), Permission::update(Role::user($book.'-'.$tenant))],
                ]));
            }
        }

        return $database;
    }

    private function join(Method $method): Query
    {
        return new Query($method, self::BOOKS, [Query::on('$id', 'authorId')], self::BOOK);
    }

    private function key(Document $row): string
    {
        $book = $row->getAttribute(self::BOOK.'.$id');

        return ($row->getId() === '' ? '-' : $row->getId()).'/'.(\is_string($book) ? $book : '-');
    }

    /**
     * @param  array<Document>  $rows
     * @return list<string>
     */
    private function sorted(array $rows): array
    {
        $keys = \array_values(\array_map($this->key(...), $rows));
        \sort($keys);

        return $keys;
    }
}
