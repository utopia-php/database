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
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * Shared tables keep every tenant's rows in one table, so a join has to pair and return only the
 * selected tenant's rows. Both tenants below reuse the same document ids, and each table also holds
 * a legacy row with no tenant. A join may not return another tenant's row or a tenantless one, and
 * may not lose one of the tenant's own rows because a row outside the tenant happened to match it:
 * what one tenant reads must not depend on what another tenant stores.
 */
final class JoinTenancyTest extends TestCase
{
    private const string NAMESPACE = 'tenancy';

    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private const string REVIEWS = 'reviews';

    private const string BOOK = 'book';

    private const string REVIEW = 'review';

    private const string PAGES = self::BOOK.'.pages';

    private const string STARS = self::REVIEW.'.stars';

    private const int FIRST = 1;

    private const int SECOND = 2;

    /**
     * Rows are written under this tenant and then stripped of it, standing in for legacy rows
     * that carry no tenant at all.
     */
    private const int TENANTLESS = 3;

    private const string EXTRAS = 'extras';

    private const string EXTRA = 'extra';

    private const string REJECTED = 'rejected';

    /**
     * Each joined collection's numeric attribute.
     */
    private const array NUMBERS = [
        self::BOOKS => 'pages',
        self::REVIEWS => 'stars',
        self::EXTRAS => 'weight',
    ];

    /**
     * Only tenant two and the tenantless rows have extras, so tenant one's cross join with them
     * is empty and every review it right-joins afterwards must come back unmatched.
     *
     * @var array<int, array<string, array<string, string|int>>>
     */
    private const array EXTRA_ROWS = [
        self::SECOND => ['x1' => ['authorId' => 'a1', 'weight' => 5]],
        self::TENANTLESS => ['x9' => ['authorId' => 'a2', 'weight' => 9]],
    ];

    /**
     * Tenant one's book "b2" belongs to an author only tenant two has and its "b3" to the
     * tenantless author; its author "a2" has books only in tenant two; its review "r4" names an
     * author only tenant two has. The tenantless book and review name authors both tenants have.
     *
     * @var array<int, array<string, array<string, array<string, string|int>>>>
     */
    private const array ROWS = [
        self::FIRST => [
            self::AUTHORS => [
                'a1' => ['name' => 'one-a1'],
                'a2' => ['name' => 'one-a2'],
            ],
            self::BOOKS => [
                'b1' => ['authorId' => 'a1', 'pages' => 11],
                'b2' => ['authorId' => 'shared', 'pages' => 12],
                'b3' => ['authorId' => 'legacy', 'pages' => 13],
            ],
            self::REVIEWS => [
                'r1' => ['authorId' => 'a1', 'stars' => 5],
                'r2' => ['authorId' => 'a2', 'stars' => 4],
                'r3' => ['authorId' => 'ghost', 'stars' => 3],
                'r4' => ['authorId' => 'shared', 'stars' => 2],
            ],
        ],
        self::SECOND => [
            self::AUTHORS => [
                'a1' => ['name' => 'two-a1'],
                'a2' => ['name' => 'two-a2'],
                'shared' => ['name' => 'two-shared'],
            ],
            self::BOOKS => [
                'b1' => ['authorId' => 'a1', 'pages' => 21],
                'b2' => ['authorId' => 'a2', 'pages' => 22],
            ],
            self::REVIEWS => [
                'r1' => ['authorId' => 'shared', 'stars' => 1],
            ],
        ],
        self::TENANTLESS => [
            self::AUTHORS => [
                'legacy' => ['name' => 'no-tenant'],
            ],
            self::BOOKS => [
                'orphan' => ['authorId' => 'a1', 'pages' => 99],
            ],
            self::REVIEWS => [
                'stale' => ['authorId' => 'a2', 'stars' => 9],
            ],
        ],
    ];

    /**
     * Every row a tenant must read when authors join books, as [author name, book pages], with
     * null where the join found no match.
     *
     * @var array<string, array<int, list<array{?string, ?int}>>>
     */
    private const array EXPECTED = [
        Method::Join->value => [
            self::FIRST => [['one-a1', 11]],
            self::SECOND => [['two-a1', 21], ['two-a2', 22]],
        ],
        Method::LeftJoin->value => [
            self::FIRST => [['one-a1', 11], ['one-a2', null]],
            self::SECOND => [['two-a1', 21], ['two-a2', 22], ['two-shared', null]],
        ],
        Method::RightJoin->value => [
            self::FIRST => [['one-a1', 11], [null, 12], [null, 13]],
            self::SECOND => [['two-a1', 21], ['two-a2', 22]],
        ],
        Method::FullOuterJoin->value => [
            self::FIRST => [['one-a1', 11], ['one-a2', null], [null, 12], [null, 13]],
            self::SECOND => [['two-a1', 21], ['two-a2', 22], ['two-shared', null]],
        ],
        Method::CrossJoin->value => [
            self::FIRST => [
                ['one-a1', 11], ['one-a1', 12], ['one-a1', 13],
                ['one-a2', 11], ['one-a2', 12], ['one-a2', 13],
            ],
            self::SECOND => [
                ['two-a1', 21], ['two-a1', 22],
                ['two-a2', 21], ['two-a2', 22],
                ['two-shared', 21], ['two-shared', 22],
            ],
        ],
    ];

    /**
     * @return iterable<string, array{Method, array{1: list<array{?string, ?int}>, 2: list<array{?string, ?int}>}, bool, bool}>
     */
    public static function joins(): iterable
    {
        foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin, Method::CrossJoin] as $join) {
            foreach (self::configurations() as $configuration => [$nativeFullOuterJoin, $documentSecurity]) {
                yield "{$join->value}, {$configuration}" => [$join, self::EXPECTED[$join->value], $nativeFullOuterJoin, $documentSecurity];
            }
        }
    }

    /**
     * @return iterable<string, array{Method, bool, bool}>
     */
    public static function joinTypes(): iterable
    {
        foreach (self::joins() as $case => [$join, , $nativeFullOuterJoin, $documentSecurity]) {
            yield $case => [$join, $nativeFullOuterJoin, $documentSecurity];
        }
    }

    /**
     * Chains whose later join keeps rows the earlier ones did not match, the shape where a
     * tenant condition placed after every join drops rows a dedicated database returns.
     *
     * @return iterable<string, array{array{Method, Method}, bool, bool}>
     */
    public static function chains(): iterable
    {
        $chains = [
            [Method::Join, Method::RightJoin],
            [Method::Join, Method::FullOuterJoin],
            [Method::LeftJoin, Method::RightJoin],
            [Method::LeftJoin, Method::FullOuterJoin],
            [Method::RightJoin, Method::RightJoin],
            [Method::RightJoin, Method::Join],
            [Method::CrossJoin, Method::RightJoin],
        ];

        foreach ($chains as [$books, $reviews]) {
            foreach (self::configurations() as $configuration => [$nativeFullOuterJoin, $documentSecurity]) {
                yield "{$books->value} books, {$reviews->value} reviews, {$configuration}" => [[$books, $reviews], $nativeFullOuterJoin, $documentSecurity];
            }
        }
    }

    /**
     * @param array{1: list<array{?string, ?int}>, 2: list<array{?string, ?int}>} $expected
     */
    #[DataProvider('joins')]
    public function testFindReturnsExactlyTheTenantsOwnRows(Method $join, array $expected, bool $nativeFullOuterJoin, bool $documentSecurity): void
    {
        $database = $this->shared($nativeFullOuterJoin, $documentSecurity);

        foreach ([self::FIRST, self::SECOND] as $tenant) {
            $database->setTenant($tenant);

            $documents = $database->find(self::AUTHORS, [
                $this->book($join),
                Query::select(['name', self::PAGES]),
            ]);

            $this->assertSame(
                $this->sorted($expected[$tenant]),
                $this->sorted(\array_map($this->pages(...), $documents)),
                "Tenant {$tenant} must read exactly its own rows through a {$join->value}",
            );
        }
    }

    /**
     * @param array{1: list<array{?string, ?int}>, 2: list<array{?string, ?int}>} $expected
     */
    #[DataProvider('joins')]
    public function testCountCountsExactlyTheTenantsOwnRows(Method $join, array $expected, bool $nativeFullOuterJoin, bool $documentSecurity): void
    {
        $database = $this->shared($nativeFullOuterJoin, $documentSecurity);

        foreach ([self::FIRST, self::SECOND] as $tenant) {
            $database->setTenant($tenant);

            $this->assertSame(
                \count($expected[$tenant]),
                $database->count(self::AUTHORS, [$this->book($join)]),
                "Tenant {$tenant} must count exactly its own rows through a {$join->value}",
            );
        }
    }

    /**
     * @param array{1: list<array{?string, ?int}>, 2: list<array{?string, ?int}>} $expected
     */
    #[DataProvider('joins')]
    public function testSumAddsExactlyTheTenantsOwnRows(Method $join, array $expected, bool $nativeFullOuterJoin, bool $documentSecurity): void
    {
        $database = $this->shared($nativeFullOuterJoin, $documentSecurity);

        foreach ([self::FIRST, self::SECOND] as $tenant) {
            $database->setTenant($tenant);

            $pages = \array_sum(\array_map(
                static fn (array $row): int => $row[1] ?? 0,
                $expected[$tenant],
            ));

            $this->assertSame(
                $pages,
                $database->sum(self::AUTHORS, self::PAGES, [$this->book($join)]),
                "Tenant {$tenant} must sum exactly its own rows through a {$join->value}",
            );
        }
    }

    #[DataProvider('joinTypes')]
    public function testGetDocumentReadsOnlyTheTenantsOwnDocument(Method $join, bool $nativeFullOuterJoin, bool $documentSecurity): void
    {
        $database = $this->shared($nativeFullOuterJoin, $documentSecurity);
        $queries = fn (): array => [$this->book($join), Query::select(['name', self::PAGES])];

        $database->setTenant(self::FIRST);

        $this->assertSame(
            'one-a1',
            $database->getDocument(self::AUTHORS, 'a1', $queries())->getAttribute('name'),
            "Tenant one must read its own a1 through a {$join->value}, not tenant two's",
        );
        $this->assertTrue(
            $database->getDocument(self::AUTHORS, 'shared', $queries())->isEmpty(),
            "Tenant one must not read tenant two's document through a {$join->value}",
        );
        $this->assertTrue(
            $database->getDocument(self::AUTHORS, 'legacy', $queries())->isEmpty(),
            "Tenant one must not read a tenantless document through a {$join->value}",
        );

        $database->setTenant(self::SECOND);

        $this->assertSame(
            'two-a1',
            $database->getDocument(self::AUTHORS, 'a1', $queries())->getAttribute('name'),
            "Tenant two must read its own a1 through a {$join->value}, not tenant one's",
        );
        $this->assertTrue(
            $database->getDocument(self::AUTHORS, 'legacy', $queries())->isEmpty(),
            "Tenant two must not read a tenantless document through a {$join->value}",
        );
    }

    /**
     * @param array{Method, Method} $chain
     */
    #[DataProvider('chains')]
    public function testChainedJoinsReadWhatADedicatedDatabaseReads(array $chain, bool $nativeFullOuterJoin, bool $documentSecurity): void
    {
        [$books, $reviews] = $chain;
        $shared = $this->shared($nativeFullOuterJoin, $documentSecurity);
        $joins = fn (): array => [$this->book($books), $this->review($reviews)];
        $queries = fn (): array => [...$joins(), Query::select(['name', self::PAGES, self::STARS])];
        $label = "{$books->value} books then {$reviews->value} reviews";

        foreach ([self::FIRST, self::SECOND] as $tenant) {
            $dedicated = $this->dedicated($nativeFullOuterJoin, $documentSecurity, $tenant);
            $shared->setTenant($tenant);

            $this->assertSame(
                $this->sorted(\array_map($this->pagesAndStars(...), $dedicated->find(self::AUTHORS, $queries()))),
                $this->sorted(\array_map($this->pagesAndStars(...), $shared->find(self::AUTHORS, $queries()))),
                "Tenant {$tenant} must read through {$label} what its own database would return",
            );
            $this->assertSame(
                $dedicated->count(self::AUTHORS, $joins()),
                $shared->count(self::AUTHORS, $joins()),
                "Tenant {$tenant} must count through {$label} what its own database would count",
            );
        }
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function fullOuterJoinModes(): iterable
    {
        yield 'emulated full outer join' => [false];
        yield 'native full outer join' => [true];
    }

    /**
     * Every chain of two joins, the second joined on the main table or on the first join: a right
     * or full outer join that follows a right, full outer or cross join must not pair its rows with
     * another tenant's rows of the earlier table, or they vanish instead of coming back unmatched.
     * Under shared tables a chain combining a full outer join with a right join is rejected.
     */
    #[DataProvider('fullOuterJoinModes')]
    public function testEveryChainOfTwoJoinsReadsWhatADedicatedDatabaseReads(bool $nativeFullOuterJoin): void
    {
        $shared = $this->sharedWithExtras($nativeFullOuterJoin);

        foreach ([self::FIRST, self::SECOND] as $tenant) {
            $dedicated = $this->dedicated($nativeFullOuterJoin, documentSecurity: false, tenant: $tenant);
            $this->extras($dedicated, self::EXTRA_ROWS[$tenant] ?? []);
            $shared->setTenant($tenant);

            $expected = [];
            $actual = [];
            foreach (self::twoJoinChains() as $label => $joins) {
                $expected[$label] = $this->combinesFullOuterAndRightJoins($joins)
                    ? self::REJECTED
                    : $this->joinedChain($dedicated, $joins);
                $actual[$label] = $this->readChain($shared, $joins);
            }

            $this->assertSame($expected, $actual, "Tenant {$tenant} must read through every chain what its own database would return");
        }
    }

    #[DataProvider('fullOuterJoinModes')]
    public function testAFullOuterJoinCombinedWithARightJoinIsRejectedUnderSharedTables(bool $nativeFullOuterJoin): void
    {
        $database = $this->shared($nativeFullOuterJoin, documentSecurity: false);
        $database->setTenant(self::FIRST);

        foreach ([
            'a right join after a full outer join' => [$this->book(Method::FullOuterJoin), $this->review(Method::RightJoin)],
            'a full outer join after a right join' => [$this->book(Method::RightJoin), $this->review(Method::FullOuterJoin)],
        ] as $label => $joins) {
            foreach ([
                'find' => fn () => $database->find(self::AUTHORS, $joins),
                'count' => fn () => $database->count(self::AUTHORS, $joins),
                'sum' => fn () => $database->sum(self::AUTHORS, self::STARS, $joins),
            ] as $read => $call) {
                try {
                    $call();
                    $this->fail("{$read} with {$label} must be rejected under shared tables");
                } catch (QueryException $exception) {
                    $this->assertStringContainsString('full outer join', $exception->getMessage());
                }
            }
        }
    }

    /**
     * @return array<string, array{bool, bool}>
     */
    private static function configurations(): array
    {
        return [
            'emulated full outer join, document security off' => [false, false],
            'emulated full outer join, document security on' => [false, true],
            'native full outer join, document security off' => [true, false],
            'native full outer join, document security on' => [true, true],
        ];
    }

    /**
     * Both tenants' rows and the tenantless ones, in one set of shared tables.
     */
    private function shared(bool $nativeFullOuterJoin, bool $documentSecurity): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = $this->database($pdo, $nativeFullOuterJoin, $documentSecurity, sharedTables: true);

        foreach (self::ROWS as $tenant => $collections) {
            $database->setTenant($tenant);
            $this->write($database, $collections);
        }

        foreach ([self::AUTHORS, self::BOOKS, self::REVIEWS] as $collection) {
            foreach ([$collection, Storage::permissionsTable($collection)] as $table) {
                $pdo->exec('UPDATE '.self::NAMESPACE.'_'.$table.' SET '.Storage::TENANT.' = NULL WHERE '.Storage::TENANT.' = '.self::TENANTLESS);
            }
        }

        return $database;
    }

    /**
     * One tenant's rows alone, in tables of their own: what that tenant must read.
     */
    private function dedicated(bool $nativeFullOuterJoin, bool $documentSecurity, int $tenant): Database
    {
        $database = $this->database(new PDO('sqlite::memory:'), $nativeFullOuterJoin, $documentSecurity, sharedTables: false);
        $this->write($database, self::ROWS[$tenant]);

        return $database;
    }

    private function database(PDO $pdo, bool $nativeFullOuterJoin, bool $documentSecurity, bool $sharedTables): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(
            $nativeFullOuterJoin ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo),
            new Cache(new None()),
        );
        $database
            ->setAuthorization($authorization)
            ->setDatabase('joins')
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables($sharedTables)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: $documentSecurity,
        ));
        $database->createCollection(new Collection(
            id: self::BOOKS,
            attributes: [
                Attribute::string(key: 'authorId', size: 64, required: true),
                Attribute::integer(key: 'pages', required: true),
            ],
            permissions: $permissions,
            documentSecurity: $documentSecurity,
        ));
        $database->createCollection(new Collection(
            id: self::REVIEWS,
            attributes: [
                Attribute::string(key: 'authorId', size: 64, required: true),
                Attribute::integer(key: 'stars', required: true),
            ],
            permissions: $permissions,
            documentSecurity: $documentSecurity,
        ));

        return $database;
    }

    /**
     * @param array<string, array<string, array<string, string|int>>> $collections
     */
    private function write(Database $database, array $collections): void
    {
        foreach ($collections as $collection => $documents) {
            foreach ($documents as $id => $attributes) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    '$permissions' => [Permission::read(Role::any())],
                    ...$attributes,
                ]));
            }
        }
    }

    private function book(Method $method): Query
    {
        return $this->join($method, self::BOOKS, self::BOOK);
    }

    private function review(Method $method): Query
    {
        return $this->join($method, self::REVIEWS, self::REVIEW);
    }

    private function join(Method $method, string $collection, string $alias): Query
    {
        return match ($method) {
            Method::Join => Query::join($collection, '$id', 'authorId', '=', $alias),
            Method::LeftJoin => Query::leftJoin($collection, '$id', 'authorId', '=', $alias),
            Method::RightJoin => Query::rightJoin($collection, '$id', 'authorId', '=', $alias),
            Method::FullOuterJoin => Query::fullOuterJoin($collection, '$id', 'authorId', '=', $alias),
            Method::CrossJoin => Query::crossJoin($collection, $alias),
            default => throw new \InvalidArgumentException("{$method->value} is not a join this test covers"),
        };
    }

    /**
     * @return array{?string, ?int}
     */
    private function pages(Document $document): array
    {
        return [$this->author($document), $this->integer($document, self::PAGES)];
    }

    /**
     * @return array{?string, ?int, ?int}
     */
    private function pagesAndStars(Document $document): array
    {
        return [$this->author($document), $this->integer($document, self::PAGES), $this->integer($document, self::STARS)];
    }

    private function author(Document $document): ?string
    {
        $name = $document->getAttribute('name');

        return \is_string($name) && $name !== '' ? $name : null;
    }

    private function integer(Document $document, string $attribute): ?int
    {
        $value = $document->getAttribute($attribute);

        return \is_numeric($value) ? (int) $value : null;
    }

    /**
     * @template T of array<int, string|int|null>
     * @param array<T> $rows
     * @return list<T>
     */
    private function sorted(array $rows): array
    {
        \usort($rows, static fn (array $left, array $right): int => \json_encode($left) <=> \json_encode($right));

        return $rows;
    }

    /**
     * The shared tables of shared(), plus extras only tenant two and the tenantless rows have.
     */
    private function sharedWithExtras(bool $nativeFullOuterJoin): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = $this->database($pdo, $nativeFullOuterJoin, documentSecurity: false, sharedTables: true);
        $this->extras($database, []);

        foreach (self::ROWS as $tenant => $collections) {
            $database->setTenant($tenant);
            $this->write($database, [...$collections, self::EXTRAS => self::EXTRA_ROWS[$tenant] ?? []]);
        }

        foreach ([self::AUTHORS, self::BOOKS, self::REVIEWS, self::EXTRAS] as $collection) {
            foreach ([$collection, Storage::permissionsTable($collection)] as $table) {
                $pdo->exec('UPDATE '.self::NAMESPACE.'_'.$table.' SET '.Storage::TENANT.' = NULL WHERE '.Storage::TENANT.' = '.self::TENANTLESS);
            }
        }

        return $database;
    }

    /**
     * @param array<string, array<string, string|int>> $rows
     */
    private function extras(Database $database, array $rows): void
    {
        $database->createCollection(new Collection(
            id: self::EXTRAS,
            attributes: [
                Attribute::string(key: 'authorId', size: 64, required: true),
                Attribute::integer(key: 'weight', required: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $this->write($database, [self::EXTRAS => $rows]);
    }

    /**
     * @return array<string, list<Query>>
     */
    private static function twoJoinChains(): array
    {
        $chains = [];
        foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin, Method::CrossJoin] as $first) {
            [$collection, $alias] = $first === Method::CrossJoin ? [self::EXTRAS, self::EXTRA] : [self::BOOKS, self::BOOK];
            foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin] as $second) {
                foreach (['$id' => self::AUTHORS, $alias.'.authorId' => $alias] as $on => $target) {
                    $chains["{$first->value} {$collection}, {$second->value} reviews on {$target}"] = [
                        self::joinOn($first, $collection, $alias, '$id'),
                        self::joinOn($second, self::REVIEWS, self::REVIEW, $on),
                    ];
                }
            }
        }

        return $chains;
    }

    private static function joinOn(Method $method, string $collection, string $alias, string $on): Query
    {
        return match ($method) {
            Method::Join => Query::join($collection, $on, 'authorId', '=', $alias),
            Method::LeftJoin => Query::leftJoin($collection, $on, 'authorId', '=', $alias),
            Method::RightJoin => Query::rightJoin($collection, $on, 'authorId', '=', $alias),
            Method::FullOuterJoin => Query::fullOuterJoin($collection, $on, 'authorId', '=', $alias),
            Method::CrossJoin => Query::crossJoin($collection, $alias),
            default => throw new \InvalidArgumentException("{$method->value} is not a join this test covers"),
        };
    }

    /**
     * @param list<Query> $joins
     */
    private function combinesFullOuterAndRightJoins(array $joins): bool
    {
        $methods = \array_map(static fn (Query $join): Method => $join->getMethod(), $joins);

        return \in_array(Method::FullOuterJoin, $methods, true) && \in_array(Method::RightJoin, $methods, true);
    }

    /**
     * The rows as [author name, then each join's number], their count and the sum of the last
     * join's number, as find(), count() and sum() return them, or REJECTED when refused.
     *
     * @param list<Query> $joins
     * @return array{rows: list<array<int, string|int|null>>, count: int, sum: int|float}|string
     */
    private function readChain(Database $database, array $joins): array|string
    {
        $copies = static fn (): array => \array_map(static fn (Query $join): Query => clone $join, $joins);
        $numbers = $this->numbers($joins);

        try {
            return [
                'rows' => $this->chainRows($database, $copies(), $numbers),
                'count' => $database->count(self::AUTHORS, $copies()),
                'sum' => $database->sum(self::AUTHORS, $numbers[\count($numbers) - 1], $copies()),
            ];
        } catch (QueryException) {
            return self::REJECTED;
        }
    }

    /**
     * What readChain() must return, taken from a dedicated database's rows alone: nothing
     * filters its joins, so its count and sum follow from its rows. A chain the adapter refuses
     * outright is refused here too.
     *
     * @param list<Query> $joins
     * @return array{rows: list<array<int, string|int|null>>, count: int, sum: int}|string
     */
    private function joinedChain(Database $dedicated, array $joins): array|string
    {
        try {
            $rows = $this->chainRows(
                $dedicated,
                \array_map(static fn (Query $join): Query => clone $join, $joins),
                $this->numbers($joins),
            );
        } catch (QueryException) {
            return self::REJECTED;
        }

        return [
            'rows' => $rows,
            'count' => \count($rows),
            'sum' => \array_sum(\array_map(static fn (array $row): int => (int) $row[\count($row) - 1], $rows)),
        ];
    }

    /**
     * @param list<Query> $joins
     * @return list<string>
     */
    private function numbers(array $joins): array
    {
        return \array_map(
            static fn (Query $join): string => $join->getJoinAlias().'.'.self::NUMBERS[$join->getAttribute()],
            $joins,
        );
    }

    /**
     * @param list<Query> $joins
     * @param list<string> $numbers
     * @return list<array<int, string|int|null>>
     */
    private function chainRows(Database $database, array $joins, array $numbers): array
    {
        return $this->sorted(\array_map(function (Document $document) use ($numbers): array {
            $row = [$this->author($document)];
            foreach ($numbers as $number) {
                $row[] = $this->integer($document, $number);
            }

            return $row;
        }, $database->find(self::AUTHORS, [...$joins, Query::select(['name', ...$numbers]), Query::limit(100)])));
    }
}
