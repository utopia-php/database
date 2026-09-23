<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeJoinChainSQLite;
use Throwable;
use Utopia\Cache\Adapter\Memory;
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
use Utopia\Query\Method;

/**
 * MariaDB, MySQL and SQLite run a full outer join as the two halves of a UNION ALL. Aggregates,
 * groups, having, distinct() and paging have to see the rows of both halves at once, so every shape
 * is checked against a native FULL OUTER JOIN (SQLite 3.39+, the single statement PostgreSQL runs).
 */
final class FullOuterJoinAggregateTest extends TestCase
{
    private const string LINK = 'link';

    private const string CATEGORY = 'category';

    private const string SCORE = 'score';

    private const int TENANT = 7;

    /**
     * Under shared tables another tenant holds a copy of every row, scored apart, so a row that leaks
     * across tenants changes every total.
     */
    private const int OTHER_TENANT = 8;

    private const int OTHER_TENANT_SCORE_OFFSET = 100;

    /**
     * Every collection holds rows only it can match, so each join keeps rows in both halves of the
     * emulation, and equal categories (null among them) come from both halves.
     *
     * @var array<string, array<string, array{string, ?string, int}>>
     */
    private const array ROWS = [
        'main' => [
            'm1' => ['1', 'p', 10],
            'm2' => ['2', 'q', 20],
            'm3' => ['5', 'p', 30],
        ],
        'a' => [
            'a1' => ['1', 'p', 1],
            'a2' => ['3', 'q', 2],
            'a3' => ['4', null, 3],
        ],
        'b' => [
            'b1' => ['1', 'p', 4],
            'b2' => ['3', 'p', 5],
            'b3' => ['6', null, 6],
            'b4' => ['1', 'q', 7],
        ],
        'c' => [
            'c1' => ['1', 'q', 8],
            'c2' => ['4', 'p', 9],
            'c3' => ['6', null, 10],
            'c4' => ['7', 'p', 11],
        ],
    ];

    public function testCountAndSumSeeBothHalvesOfTheJoin(): void
    {
        $database = $this->database(native: false, documentSecurity: false, sharedTables: false);

        $rows = $database->find('main', [
            Query::fullOuterJoin('b', self::LINK, self::LINK, '=', 'b'),
            Query::count('*', 'rows'),
            Query::sum('b.score', 'total'),
        ]);

        $this->assertSame([['rows' => 6, 'total' => 22]], \array_map(static fn (Document $row): array => $row->getArrayCopy(), $rows));
    }

    /**
     * Every chain of one or two joins holding one full outer join, with the main rows readable
     * through the collection and through their own permissions, answers every aggregate, group,
     * having, distinct() and page shape exactly as the native full outer join does.
     */
    #[DataProvider('mainDocumentSecurity')]
    public function testEveryShapeMatchesANativeFullOuterJoin(bool $documentSecurity): void
    {
        $emulated = $this->database(native: false, documentSecurity: $documentSecurity, sharedTables: false);
        $native = $this->database(native: true, documentSecurity: $documentSecurity, sharedTables: false);

        $this->assertShapesMatch($emulated, $native, $this->chains(), 200);
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function mainDocumentSecurity(): iterable
    {
        yield 'main rows readable through the collection' => [false];
        yield 'main rows readable through their own permissions' => [true];
    }

    public function testEveryShapeMatchesANativeFullOuterJoinUnderSharedTables(): void
    {
        $emulated = $this->database(native: false, documentSecurity: true, sharedTables: true);
        $native = $this->database(native: true, documentSecurity: true, sharedTables: true);

        $totals = $emulated->find('main', [
            Query::fullOuterJoin('b', self::LINK, self::LINK, '=', 'b'),
            Query::count('*', 'rows'),
            Query::sum('b.score', 'total'),
        ]);
        $this->assertSame([['rows' => 6, 'total' => 22]], \array_map(static fn (Document $row): array => $row->getArrayCopy(), $totals));

        $chains = [];
        foreach ($this->chains() as $label => $chain) {
            if (! \str_contains($label, Method::RightJoin->value)) {
                $chains[$label] = $chain;
            }
        }

        $this->assertShapesMatch($emulated, $native, $chains, 150);
    }

    public function testUnaliasedAggregatesKeepTheNamesTheEngineGivesThem(): void
    {
        $emulated = $this->database(native: false, documentSecurity: false, sharedTables: false);
        $native = $this->database(native: true, documentSecurity: false, sharedTables: false);
        $queries = [
            Query::fullOuterJoin('b', self::LINK, self::LINK, '=', 'b'),
            Query::count(),
            Query::sum('b.score'),
            Query::max('score'),
            Query::groupBy(['b.category']),
        ];

        $expected = $this->rows($native, $queries, ordered: false);

        $this->assertCount(3, $expected);
        $this->assertSame($expected, $this->rows($emulated, $queries, ordered: false));
    }

    /**
     * @param  iterable<string, array{list<Query>, string}>  $chains
     */
    private function assertShapesMatch(Database $emulated, Database $native, iterable $chains, int $minimum): void
    {
        $checked = 0;
        $mismatches = [];
        foreach ($chains as $chainLabel => [$joins, $fullOuterJoined]) {
            foreach ($this->shapes($fullOuterJoined) as $shapeLabel => [$shape, $ordered]) {
                $label = "{$chainLabel} / {$shapeLabel}";
                $queries = [...$joins, ...$shape];
                $expected = $this->rows($native, $queries, $ordered);

                try {
                    $actual = $this->rows($emulated, $queries, $ordered);
                } catch (Throwable $throwable) {
                    $mismatches[] = "{$label}: ".$throwable::class." ({$throwable->getMessage()})";

                    continue;
                }

                $checked++;
                if ($actual !== $expected) {
                    $mismatches[] = "{$label}: expected ".\json_encode($expected).', got '.\json_encode($actual);
                }
            }
        }

        $this->assertSame([], $mismatches);
        $this->assertGreaterThan($minimum, $checked);
    }

    /**
     * The shapes every chain runs, over the main collection and the full outer joined alias.
     *
     * @return iterable<string, array{list<Query>, bool}>
     */
    private function shapes(string $alias): iterable
    {
        $category = "{$alias}.".self::CATEGORY;
        $score = "{$alias}.".self::SCORE;

        yield 'a row count' => [[
            Query::count('*', 'rows'),
        ], false];
        yield 'every aggregate' => [[
            Query::count('*', 'rows'),
            Query::count("{$alias}.\$id", 'joined'),
            Query::count('$id', 'main'),
            Query::sum($score, 'total'),
            Query::avg($score, 'mean'),
            Query::min($score, 'low'),
            Query::max($score, 'high'),
            Query::sum(self::SCORE, 'mainTotal'),
            Query::min(self::CATEGORY, 'firstCategory'),
        ], false];
        yield 'distinct counts' => [[
            Query::countDistinct($category, 'categories'),
            Query::countDistinct(self::CATEGORY, 'mainCategories'),
        ], false];
        yield 'grouped by the joined collection' => [[
            Query::groupBy([$category]),
            Query::count('*', 'rows'),
            Query::sum($score, 'total'),
            Query::sum(self::SCORE, 'mainTotal'),
        ], false];
        yield 'grouped by the main collection' => [[
            Query::groupBy([self::CATEGORY]),
            Query::count('*', 'rows'),
            Query::max($score, 'high'),
        ], false];
        yield 'grouped by both collections' => [[
            Query::groupBy([self::CATEGORY, $category]),
            Query::count('*', 'rows'),
            Query::avg($score, 'mean'),
        ], false];
        yield 'having on a count' => [[
            Query::groupBy([$category]),
            Query::count('*', 'rows'),
            Query::having([Query::greaterThan('rows', 1)]),
        ], false];
        yield 'having on a sum and a group' => [[
            Query::groupBy([self::CATEGORY]),
            Query::sum($score, 'total'),
            Query::having([Query::lessThan('total', 12), Query::isNotNull(self::CATEGORY)]),
        ], false];
        yield 'ordered and paged groups' => [[
            Query::groupBy([$category]),
            Query::count('*', 'rows'),
            Query::orderDesc('rows'),
            Query::orderAsc($category),
            Query::limit(2),
            Query::offset(1),
        ], true];
        yield 'distinct aggregated rows' => [[
            Query::distinct(),
            Query::groupBy([$category]),
            Query::count('*', 'rows'),
        ], false];
        yield 'aggregates filtered on the main collection' => [[
            Query::equal(self::CATEGORY, ['p']),
            Query::count('*', 'rows'),
            Query::sum($score, 'total'),
        ], false];
        yield 'aggregates filtered on the joined collection' => [[
            Query::isNotNull($category),
            Query::count('*', 'rows'),
            Query::max(self::SCORE, 'high'),
        ], false];
        yield 'aggregates over no rows' => [[
            Query::equal(self::CATEGORY, ['none']),
            Query::count('*', 'rows'),
            Query::sum($score, 'total'),
            Query::avg($score, 'mean'),
            Query::max($score, 'high'),
        ], false];
    }

    /**
     * Every chain of one or two joins holding exactly one full outer join — every join type before
     * and after it, every earlier table in every ON — and three chains of three joins.
     *
     * @return iterable<string, array{list<Query>, string}>
     */
    private function chains(): iterable
    {
        $methods = [Method::CrossJoin, Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin];

        yield 'fullOuterJoin a on main' => [[$this->join(Method::FullOuterJoin, 'a', 'main')], 'a'];

        foreach ($methods as $first) {
            foreach ($methods as $second) {
                if (($first === Method::FullOuterJoin) === ($second === Method::FullOuterJoin)) {
                    continue;
                }

                foreach ($second === Method::CrossJoin ? ['main'] : ['main', 'a'] as $reference) {
                    yield $this->label([[$first, 'a', 'main'], [$second, 'b', $reference]]) => [
                        [$this->join($first, 'a', 'main'), $this->join($second, 'b', $reference)],
                        $first === Method::FullOuterJoin ? 'a' : 'b',
                    ];
                }
            }
        }

        foreach ([
            [[Method::LeftJoin, 'a', 'main'], [Method::FullOuterJoin, 'b', 'a'], [Method::LeftJoin, 'c', 'b']],
            [[Method::FullOuterJoin, 'b', 'main'], [Method::RightJoin, 'c', 'b'], [Method::Join, 'a', 'c']],
            [[Method::Join, 'a', 'main'], [Method::FullOuterJoin, 'b', 'main'], [Method::RightJoin, 'c', 'a']],
        ] as $specification) {
            yield $this->label($specification) => [
                \array_map(fn (array $join): Query => $this->join(...$join), $specification),
                'b',
            ];
        }
    }

    /**
     * @param  list<array{Method, string, string}>  $specification
     */
    private function label(array $specification): string
    {
        return \implode(', ', \array_map(
            static fn (array $join): string => $join[0] === Method::CrossJoin ? "cross {$join[1]}" : "{$join[0]->value} {$join[1]} on {$join[2]}",
            $specification,
        ));
    }

    private function join(Method $method, string $collection, string $reference): Query
    {
        $left = $reference === 'main' ? self::LINK : "{$reference}.".self::LINK;

        return match ($method) {
            Method::CrossJoin => Query::crossJoin($collection, $collection),
            Method::Join => Query::join($collection, $left, self::LINK, '=', $collection),
            Method::LeftJoin => Query::leftJoin($collection, $left, self::LINK, '=', $collection),
            Method::RightJoin => Query::rightJoin($collection, $left, self::LINK, '=', $collection),
            default => Query::fullOuterJoin($collection, $left, self::LINK, '=', $collection),
        };
    }

    /**
     * @param  list<Query>  $queries
     * @return list<array<string, mixed>>
     */
    private function rows(Database $database, array $queries, bool $ordered): array
    {
        $rows = \array_values(\array_map(
            static fn (Document $document): array => $document->getArrayCopy(),
            $database->find('main', $queries),
        ));

        if (! $ordered) {
            \usort($rows, static fn (array $left, array $right): int => \strcmp((string) \json_encode($left), (string) \json_encode($right)));
        }

        return $rows;
    }

    private function database(bool $native, bool $documentSecurity, bool $sharedTables): Database
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
            ->setDatabase('aggregates')
            ->setNamespace('aggregates_'.\uniqid());
        if ($sharedTables) {
            $database->setSharedTables(true)->setTenant(null);
        }
        $database->addHook(new Permissions());
        $database->create();

        $collectionPermissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        foreach (\array_keys(self::ROWS) as $collection) {
            $main = $collection === 'main';
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: self::LINK, size: 16, required: true),
                    Attribute::string(key: self::CATEGORY, size: 16, required: false),
                    Attribute::integer(key: self::SCORE, required: true),
                ],
                permissions: $main && $documentSecurity ? [Permission::create(Role::any())] : $collectionPermissions,
                documentSecurity: $main && $documentSecurity,
            ));
        }

        if ($sharedTables) {
            $database->setTenant(self::OTHER_TENANT);
            $this->createRows($database, self::OTHER_TENANT_SCORE_OFFSET);
            $database->setTenant(self::TENANT);
        }
        $this->createRows($database, 0);

        return $database;
    }

    private function createRows(Database $database, int $scoreOffset): void
    {
        foreach (self::ROWS as $collection => $rows) {
            foreach ($rows as $id => [$link, $category, $score]) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    self::LINK => $link,
                    self::CATEGORY => $category,
                    self::SCORE => $score + $scoreOffset,
                    '$permissions' => [Permission::read(Role::any())],
                ]));
            }
        }
    }
}
