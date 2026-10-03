<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeJoinChainSQLite;
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
 * MariaDB, MySQL and SQLite order an emulated full outer join by columns the adapter adds to both
 * halves and drops from the rows it reads. An attribute or a join alias named like those columns
 * reads back like any other, whether the full outer join is emulated or native.
 */
final class FullOuterJoinOrderColumnTest extends TestCase
{
    private const string MAIN = 'main';

    private const string JOINED = 'joined';

    private const string LINK = 'link';

    private const string NOTE = 'foj_ord_note';

    private const string SCORE = 'score';

    private const string ALIAS = 'foj_ord_x';

    private const int TENANT = 7;

    /**
     * m1 matches j1 and nothing matches m2 or j2, so an emulated full outer join returns rows from
     * both of its halves.
     *
     * @var array<string, array{string, string}>
     */
    private const array MAIN_ROWS = ['m1' => ['1', 'first'], 'm2' => ['2', 'second']];

    /**
     * @var array<string, array{string, int}>
     */
    private const array JOINED_ROWS = ['j1' => ['1', 1], 'j2' => ['3', 3]];

    /**
     * @return iterable<string, array{bool}>
     */
    public static function tables(): iterable
    {
        yield 'dedicated tables' => [false];
        yield 'shared tables' => [true];
    }

    #[DataProvider('tables')]
    public function testAttributeNamedLikeAnOrderColumnIsRead(bool $sharedTables): void
    {
        $database = $this->database(native: false, sharedTables: $sharedTables);

        $this->assertSame('first', $database->getDocument(self::MAIN, 'm1')->getAttribute(self::NOTE), 'getDocument');
        $this->assertSame('first', $database->getDocument(self::MAIN, 'm1', [Query::select([self::NOTE])])->getAttribute(self::NOTE), 'getDocument with a select');
        $this->assertSame(['first', 'second'], $this->notes($database->find(self::MAIN)), 'find');
        $this->assertSame(['first', 'second'], $this->notes($database->getAuthorization()->skip(fn (): array => $database->find(self::MAIN))), 'find without authorization');
        $this->assertSame(['second'], $this->notes($database->find(self::MAIN, [Query::equal(self::NOTE, ['second'])])), 'find filtered by the attribute');
        $this->assertSame(['second', 'first'], $this->notes($database->find(self::MAIN, [Query::orderDesc(self::NOTE)])), 'find ordered by the attribute');
    }

    #[DataProvider('tables')]
    public function testFullOuterJoinOrderedByAnAttributeNamedLikeAnOrderColumnReturnsIt(bool $sharedTables): void
    {
        $emulated = $this->database(native: false, sharedTables: $sharedTables);
        $native = $this->database(native: true, sharedTables: $sharedTables);
        $join = Query::fullOuterJoin(self::JOINED, self::LINK, self::LINK, '=', 'j');

        $this->assertReadsMatch($emulated, $native, 'j', [
            'ordered by the attribute' => [
                [$join, Query::orderAsc(self::NOTE)],
                [['', null, 'j2', 3], ['m1', 'first', 'j1', 1], ['m2', 'second', null, null]],
            ],
            'selecting and ordered by the attribute' => [
                [$join, Query::select(['$id', self::NOTE, 'j.$id', 'j.score']), Query::orderDesc(self::NOTE)],
                [['m2', 'second', null, null], ['m1', 'first', 'j1', 1], ['', null, 'j2', 3]],
            ],
            'ordered by a joined attribute and then the attribute, paged' => [
                [$join, Query::orderAsc('j.score'), Query::orderAsc(self::NOTE), Query::limit(2), Query::offset(1)],
                [['m1', 'first', 'j1', 1], ['', null, 'j2', 3]],
            ],
        ]);
    }

    #[DataProvider('tables')]
    public function testJoinAliasNamedLikeAnOrderColumnReturnsItsColumns(bool $sharedTables): void
    {
        $emulated = $this->database(native: false, sharedTables: $sharedTables);
        $native = $this->database(native: true, sharedTables: $sharedTables);
        $alias = self::ALIAS;

        $this->assertSame(
            [['m1', 'first', 'j1', 1]],
            $this->summaries([$emulated->getDocument(self::MAIN, 'm1', [Query::leftJoin(self::JOINED, self::LINK, self::LINK, '=', $alias)])], $alias),
            'getDocument',
        );

        $this->assertReadsMatch($emulated, $native, $alias, [
            'joined' => [
                [Query::join(self::JOINED, self::LINK, self::LINK, '=', $alias)],
                [['m1', 'first', 'j1', 1]],
            ],
            'left joined, selecting its columns' => [
                [Query::leftJoin(self::JOINED, self::LINK, self::LINK, '=', $alias), Query::select(['$id', self::NOTE, "{$alias}.\$id", "{$alias}.score"])],
                [['m1', 'first', 'j1', 1], ['m2', 'second', null, null]],
            ],
            'full outer joined, ordered by its column' => [
                [Query::fullOuterJoin(self::JOINED, self::LINK, self::LINK, '=', $alias), Query::orderDesc("{$alias}.score")],
                [['', null, 'j2', 3], ['m1', 'first', 'j1', 1], ['m2', 'second', null, null]],
            ],
        ]);
    }

    /**
     * Each read returns the expected rows, emulated and native, and the emulated rows carry exactly
     * the columns of the native ones: none of the columns the emulation ordered by, and nothing less.
     *
     * @param  array<string, array{list<Query>, list<array{string, mixed, mixed, mixed}>}>  $reads
     */
    private function assertReadsMatch(Database $emulated, Database $native, string $alias, array $reads): void
    {
        foreach ($reads as $label => [$queries, $expected]) {
            $emulatedRows = $emulated->find(self::MAIN, $queries);
            $nativeRows = $native->find(self::MAIN, $queries);

            $this->assertSame($expected, $this->summaries($emulatedRows, $alias), "{$label}, emulated");
            $this->assertSame($expected, $this->summaries($nativeRows, $alias), "{$label}, native");
            $this->assertSame($this->columns($nativeRows), $this->columns($emulatedRows), "{$label}, columns");
        }
    }

    /**
     * @param  array<Document>  $rows
     * @return list<mixed>
     */
    private function notes(array $rows): array
    {
        return \array_values(\array_map(static fn (Document $row): mixed => $row->getAttribute(self::NOTE), $rows));
    }

    /**
     * @param  array<Document>  $rows
     * @return list<array{string, mixed, mixed, mixed}>
     */
    private function summaries(array $rows, string $alias): array
    {
        return \array_values(\array_map(
            static fn (Document $row): array => [
                $row->getId(),
                $row->getAttribute(self::NOTE),
                $row->getAttribute("{$alias}.\$id"),
                $row->getAttribute("{$alias}.".self::SCORE),
            ],
            $rows,
        ));
    }

    /**
     * @param  array<Document>  $rows
     * @return list<list<string>>
     */
    private function columns(array $rows): array
    {
        return \array_values(\array_map(static function (Document $row): array {
            $columns = \array_map(\strval(...), \array_keys($row->getArrayCopy()));
            \sort($columns);

            return $columns;
        }, $rows));
    }

    private function database(bool $native, bool $sharedTables): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(
            $native ? new NativeJoinChainSQLite($pdo) : new SQLite($pdo),
            new Cache(new None()),
        );
        $database
            ->setAuthorization($authorization)
            ->setDatabase('order_columns')
            ->setNamespace('order_columns_'.\uniqid());
        if ($sharedTables) {
            $database->setSharedTables(true)->setTenant(null);
        }
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: self::MAIN,
            attributes: [
                Attribute::string(key: self::LINK, size: 16, required: true),
                Attribute::string(key: self::NOTE, size: 64, required: false),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: self::JOINED,
            attributes: [
                Attribute::string(key: self::LINK, size: 16, required: true),
                Attribute::integer(key: self::SCORE, required: true),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));

        if ($sharedTables) {
            $database->setTenant(self::TENANT);
        }
        foreach (self::MAIN_ROWS as $id => [$link, $note]) {
            $database->createDocument(self::MAIN, new Document(['$id' => $id, self::LINK => $link, self::NOTE => $note]));
        }
        foreach (self::JOINED_ROWS as $id => [$link, $score]) {
            $database->createDocument(self::JOINED, new Document(['$id' => $id, self::LINK => $link, self::SCORE => $score]));
        }

        return $database;
    }
}
