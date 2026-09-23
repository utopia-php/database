<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeJoinChainSQLite;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * MariaDB, MySQL and SQLite have no FULL OUTER JOIN, so the adapter runs one as a LEFT JOIN
 * UNION ALL a RIGHT JOIN that keeps only the joined table's unmatched rows. Every join after it
 * runs in both halves, so each chain is checked row for row against a native FULL OUTER JOIN
 * (SQLite 3.39+, the single statement PostgreSQL runs).
 */
final class FullOuterJoinChainTest extends TestCase
{
    private const string LINK = 'link';

    /**
     * main m1 matches b1, b1 matches c1, nothing matches c3.
     *
     * @var array<string, array<string, array<string, string|int>>>
     */
    private const array REPORTED = [
        'main' => ['m1' => ['name' => 'm1']],
        'b' => ['b1' => ['mainId' => 'm1', 'score' => 1]],
        'c' => [
            'c1' => ['mainId' => 'm1', 'score' => 10],
            'c3' => ['mainId' => 'zz', 'score' => 30],
        ],
    ];

    /**
     * Every table has rows only it holds, and b and c each hold a row that only an unmatched row
     * of an earlier table matches, which is where the two halves of the emulation meet.
     *
     * @var array<string, array<string, string>>
     */
    private const array LINKS = [
        'main' => ['m1' => '1', 'm2' => '2', 'm3' => '5'],
        'a' => ['a1' => '1', 'a2' => '3', 'a3' => '4'],
        'b' => ['b1' => '1', 'b2' => '3', 'b3' => '6', 'b4' => '1'],
        'c' => ['c1' => '1', 'c2' => '4', 'c3' => '6', 'c4' => '7', 'c5' => '3'],
    ];

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('reportedChains')]
    public function testUnmatchedRowOfALaterRightJoinIsReturnedOnce(array $joins): void
    {
        $database = $this->reportedDatabase();

        $rows = $database->find('main', [...$joins, Query::select(['name', 'b.score', 'c.score'])]);
        $values = \array_map(
            static fn (Document $row): string => (string) \json_encode([$row->getAttribute('name'), $row->getAttribute('b.score'), $row->getAttribute('c.score')]),
            $rows,
        );
        \sort($values);

        $this->assertSame(['["m1",1,10]', '[null,null,30]'], $values);
        $this->assertSame(2, $database->count('main', $joins));
        $this->assertSame(40, $database->sum('main', 'c.score', $joins));
    }

    /**
     * @return iterable<string, array{list<Query>}>
     */
    public static function reportedChains(): iterable
    {
        yield 'right join on the main collection' => [[
            Query::fullOuterJoin('b', '$id', 'mainId', '=', 'b'),
            Query::rightJoin('c', '$id', 'mainId', '=', 'c'),
        ]];
        yield 'right join on the full outer joined collection' => [[
            Query::fullOuterJoin('b', '$id', 'mainId', '=', 'b'),
            Query::rightJoin('c', 'b.mainId', 'mainId', '=', 'c'),
        ]];
    }

    /**
     * Chains of up to three joins with at least one full outer join, every join type in every
     * position and every earlier table in every ON. A chain with one full outer join returns exactly
     * the native rows and count unless a table cross joined after the full outer join decides which
     * rows a later right join matches; a chain with two full outer joins is rejected.
     */
    #[DataProvider('mainDocumentSecurity')]
    public function testEveryChainMatchesANativeFullOuterJoin(bool $documentSecurity): void
    {
        $emulated = $this->linkedDatabase(native: false, documentSecurity: $documentSecurity);
        $native = $this->linkedDatabase(native: true, documentSecurity: $documentSecurity);

        $checked = 0;
        $mismatches = [];
        foreach ($this->chains() as $label => [$joins, $fullOuterJoins, $crossJoinAfterFullOuterJoin]) {
            $expected = $this->rows($native, $joins);
            $expectedCount = $native->count('main', $joins);
            $this->assertSame(\count($expected), $expectedCount, $label);

            try {
                $actual = $this->rows($emulated, $joins);
                $actualCount = $emulated->count('main', $joins);
            } catch (QueryException $exception) {
                if ($fullOuterJoins === 1 && ! $crossJoinAfterFullOuterJoin) {
                    $mismatches[] = "{$label}: rejected ({$exception->getMessage()})";
                }

                continue;
            }

            if ($fullOuterJoins > 1) {
                $mismatches[] = "{$label}: two full outer joins were not rejected";

                continue;
            }

            $checked++;
            if ($actual !== $expected || $actualCount !== $expectedCount) {
                $mismatches[] = "{$label}: expected {$expectedCount} ".\json_encode($expected).", got {$actualCount} ".\json_encode($actual);
            }
        }

        $this->assertSame([], $mismatches);
        $this->assertGreaterThan(150, $checked);
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function mainDocumentSecurity(): iterable
    {
        yield 'main rows readable through the collection' => [false];
        yield 'main rows readable through their own permissions' => [true];
    }

    public function testTwoFullOuterJoinsAreRejectedWhenEmulated(): void
    {
        $database = $this->linkedDatabase(native: false, documentSecurity: false);
        $joins = [
            Query::fullOuterJoin('a', self::LINK, self::LINK, '=', 'a'),
            Query::fullOuterJoin('b', 'a.'.self::LINK, self::LINK, '=', 'b'),
        ];

        try {
            $database->find('main', $joins);
            $this->fail('Two emulated full outer joins must be rejected');
        } catch (QueryException $exception) {
            $this->assertSame('A query can hold only one full outer join on this database', $exception->getMessage());
        }

        $this->expectException(QueryException::class);
        $database->count('main', $joins);
    }

    public function testTwoFullOuterJoinsRunNatively(): void
    {
        $database = $this->linkedDatabase(native: true, documentSecurity: false);

        $rows = $this->rows($database, [
            Query::fullOuterJoin('a', self::LINK, self::LINK, '=', 'a'),
            Query::fullOuterJoin('b', 'a.'.self::LINK, self::LINK, '=', 'b'),
        ]);

        $this->assertSame([
            ['m1', 'a1', 'b1', null],
            ['m1', 'a1', 'b4', null],
            ['m2', null, null, null],
            ['m3', null, null, null],
            [null, 'a2', 'b2', null],
            [null, 'a3', null, null],
            [null, null, 'b3', null],
        ], $rows);
    }

    public function testRightJoinOnATableCrossJoinedAfterTheFullOuterJoinIsRejectedWhenEmulated(): void
    {
        $database = $this->linkedDatabase(native: false, documentSecurity: false);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('A right join after a full outer join has to join on a table joined before it, or on the full outer joined table');

        $database->find('main', [
            Query::fullOuterJoin('a', self::LINK, self::LINK, '=', 'a'),
            Query::crossJoin('b', 'b'),
            Query::rightJoin('c', 'b.'.self::LINK, self::LINK, '=', 'c'),
        ]);
    }

    /**
     * @return iterable<string, array{list<Query>, int, bool}>
     */
    private function chains(): iterable
    {
        $collections = ['a', 'b', 'c'];
        $options = [];
        foreach ($collections as $position => $collection) {
            $options[$position] = [['cross', $collection, null]];
            foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin] as $method) {
                foreach (['main', ...\array_slice($collections, 0, $position)] as $reference) {
                    $options[$position][] = [$method->value, $collection, $reference];
                }
            }
        }

        $prefixes = [[]];
        foreach ($options as $position => $choices) {
            $next = [];
            foreach ($prefixes as $prefix) {
                foreach ($choices as $choice) {
                    $chain = [...$prefix, $choice];
                    $next[] = $chain;
                    yield from $this->chain($chain);
                }
            }
            $prefixes = $next;
        }
    }

    /**
     * @param  list<array{string, string, ?string}>  $specification
     * @return iterable<string, array{list<Query>, int, bool}>
     */
    private function chain(array $specification): iterable
    {
        $joins = [];
        $labels = [];
        $fullOuterJoins = 0;
        $crossJoinAfterFullOuterJoin = false;
        foreach ($specification as [$method, $collection, $reference]) {
            if ($method === 'cross') {
                $joins[] = Query::crossJoin($collection, $collection);
                $labels[] = "cross {$collection}";
                $crossJoinAfterFullOuterJoin = $crossJoinAfterFullOuterJoin || $fullOuterJoins > 0;

                continue;
            }

            $left = $reference === 'main' ? self::LINK : $reference.'.'.self::LINK;
            $joins[] = match (Method::from($method)) {
                Method::Join => Query::join($collection, $left, self::LINK, '=', $collection),
                Method::LeftJoin => Query::leftJoin($collection, $left, self::LINK, '=', $collection),
                Method::RightJoin => Query::rightJoin($collection, $left, self::LINK, '=', $collection),
                default => Query::fullOuterJoin($collection, $left, self::LINK, '=', $collection),
            };
            $labels[] = "{$method} {$collection} on {$reference}";
            if ($method === Method::FullOuterJoin->value) {
                $fullOuterJoins++;
            }
        }

        if ($fullOuterJoins === 0) {
            return;
        }

        yield \implode(', ', $labels) => [$joins, $fullOuterJoins, $crossJoinAfterFullOuterJoin];
    }

    /**
     * @param  list<Query>  $joins
     * @return list<list<?string>>
     */
    private function rows(Database $database, array $joins): array
    {
        $aliases = \array_map(static fn (Query $join): string => $join->getJoinAlias(), $joins);
        $documents = $database->find('main', [
            ...$joins,
            Query::select(['$id', ...\array_map(static fn (string $alias): string => $alias.'.$id', $aliases)]),
            Query::limit(5000),
        ]);

        $rows = [];
        foreach ($documents as $document) {
            $row = [];
            foreach (['', 'a.', 'b.', 'c.'] as $prefix) {
                $id = $document->getAttribute($prefix.'$id');
                $row[] = \is_string($id) && $id !== '' ? $id : null;
            }
            $rows[] = $row;
        }
        \usort($rows, static fn (array $left, array $right): int => \strcmp((string) \json_encode($left), (string) \json_encode($right)));

        return $rows;
    }

    private function linkedDatabase(bool $native, bool $documentSecurity): Database
    {
        $database = $this->database($native);

        $collectionPermissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        foreach (\array_keys(self::LINKS) as $collection) {
            $main = $collection === 'main';
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [Attribute::string(key: self::LINK, size: 16, required: true)],
                permissions: $main && $documentSecurity ? [Permission::create(Role::any())] : $collectionPermissions,
                documentSecurity: $main && $documentSecurity,
            ));
        }

        foreach (self::LINKS as $collection => $links) {
            foreach ($links as $id => $link) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    self::LINK => $link,
                    '$permissions' => [Permission::read(Role::any())],
                ]));
            }
        }

        return $database;
    }

    private function reportedDatabase(): Database
    {
        $database = $this->database(native: false);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: 'main',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        foreach (['b', 'c'] as $collection) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'mainId', size: 64, required: true),
                    Attribute::integer(key: 'score', required: true),
                ],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }

        foreach (self::REPORTED as $collection => $documents) {
            foreach ($documents as $id => $attributes) {
                $database->createDocument($collection, new Document(['$id' => $id, ...$attributes]));
            }
        }

        return $database;
    }

    private function database(bool $native): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(
            $native ? new NativeJoinChainSQLite($pdo) : new SQLite($pdo),
            new Cache(new Memory()),
        );
        $database
            ->setAuthorization($authorization)
            ->setDatabase('chains')
            ->setNamespace('chains_'.\uniqid());
        $database->addHook(new Permissions());
        $database->create();

        return $database;
    }
}
