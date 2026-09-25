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
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * A join reads each collection exactly as a direct read of it would (contract C6), for every chain
 * of joins: it returns what the same joins return over the documents direct reads return. An
 * unreadable document therefore changes nothing, not even by being there: a review whose author
 * cannot be read comes back exactly like a review whose author does not exist.
 */
final class JoinChainVisibilityTest extends TestCase
{
    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private const string REVIEWS = 'reviews';

    private const string EXTRAS = 'extras';

    private const string BOOK = 'book';

    private const string REVIEW = 'review';

    private const string EXTRA = 'extra';

    private const string REJECTED = 'rejected';

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    /**
     * Each collection's numeric attribute, read back as "alias.attribute".
     */
    private const array NUMBERS = [
        self::BOOKS => 'pages',
        self::REVIEWS => 'stars',
        self::EXTRAS => 'weight',
    ];

    /**
     * Every document and whether the caller holds document-level read on it. The unreadable ones
     * share keys with readable ones: author "hidden" has a readable book and review, "b5" and "x1"
     * belong to a1 like b1 and r1 do, "b2" and "r5" to a2 like r2 does.
     *
     * @var array<string, array<string, array{array<string, string|int>, bool}>>
     */
    private const array DOCUMENTS = [
        self::AUTHORS => [
            'a1' => [['name' => 'a1'], true],
            'a2' => [['name' => 'a2'], true],
            'hidden' => [['name' => 'hidden'], false],
        ],
        self::BOOKS => [
            'b1' => [['authorId' => 'a1', 'pages' => 1], true],
            'b2' => [['authorId' => 'a2', 'pages' => 2], false],
            'b3' => [['authorId' => 'hidden', 'pages' => 3], true],
            'b4' => [['authorId' => 'ghost', 'pages' => 4], true],
            'b5' => [['authorId' => 'a1', 'pages' => 5], false],
        ],
        self::REVIEWS => [
            'r1' => [['authorId' => 'a1', 'stars' => 10], true],
            'r2' => [['authorId' => 'a2', 'stars' => 20], true],
            'r3' => [['authorId' => 'hidden', 'stars' => 30], true],
            'r4' => [['authorId' => 'ghost', 'stars' => 40], true],
            'r5' => [['authorId' => 'a2', 'stars' => 50], false],
        ],
        self::EXTRAS => [
            'x1' => [['authorId' => 'a1', 'weight' => 100], false],
        ],
    ];

    /**
     * How the caller may read each configuration's collections: a collection-level grant shows
     * every document, document security alone only the documents the caller holds read on.
     *
     * @return array<string, array{authors: bool, joined: bool}>
     */
    private static function grants(): array
    {
        return [
            'document security on every collection' => ['authors' => false, 'joined' => false],
            'collection grants on every collection' => ['authors' => true, 'joined' => true],
            'granted authors, document security on the joined collections' => ['authors' => true, 'joined' => false],
            'document security on authors, granted joined collections' => ['authors' => false, 'joined' => true],
        ];
    }

    /**
     * @return iterable<string, array{bool, bool, bool, bool}>
     */
    public static function configurations(): iterable
    {
        foreach (['emulated full outer join' => false, 'native full outer join' => true] as $mode => $native) {
            foreach (self::grants() as $grant => ['authors' => $authors, 'joined' => $joined]) {
                yield "{$mode}, {$grant}" => [$native, $authors, $joined, false];
            }
            yield "{$mode}, document security on every collection, shared tables" => [$native, false, false, true];
        }
    }

    /**
     * @return iterable<string, array{bool, Method}>
     */
    public static function outerJoins(): iterable
    {
        foreach (['emulated full outer join' => false, 'native full outer join' => true] as $mode => $native) {
            foreach ([Method::RightJoin, Method::FullOuterJoin] as $join) {
                yield "{$join->value}, {$mode}" => [$native, $join];
            }
        }
    }

    #[DataProvider('configurations')]
    public function testEveryJoinChainReadsWhatDirectReadsAllow(bool $native, bool $grantAuthors, bool $grantJoined, bool $sharedTables): void
    {
        $database = $this->database($native, $grantAuthors, $grantJoined, $sharedTables);
        $this->seed($database, $sharedTables);
        $direct = $this->directReads($database, $native);

        $expected = [];
        $actual = [];
        foreach (self::chains() as $label => $joins) {
            $expected[$label] = $this->joined($direct, $joins);
            $actual[$label] = $this->read($database, $joins);
        }

        $this->assertSame(
            $expected,
            $actual,
            'Every chain must return what the same joins return over the documents direct reads return',
        );
    }

    #[DataProvider('outerJoins')]
    public function testAReviewOfAnUnreadableAuthorComesBackLikeAReviewOfAMissingAuthor(bool $native, Method $join): void
    {
        $database = $this->database($native, grantAuthors: false, grantJoined: false, sharedTables: false);
        $this->seed($database, sharedTables: false);

        foreach ([
            'alone' => [$this->join($join, self::REVIEWS, self::REVIEW, '$id')],
            'after an inner join' => [$this->join(Method::Join, self::BOOKS, self::BOOK, '$id'), $this->join($join, self::REVIEWS, self::REVIEW, '$id')],
            'after a left join' => [$this->join(Method::LeftJoin, self::BOOKS, self::BOOK, '$id'), $this->join($join, self::REVIEWS, self::REVIEW, '$id')],
        ] as $label => $joins) {
            $stars = [];
            foreach ($database->find(self::AUTHORS, [...$joins, Query::select(['name', self::REVIEW.'.stars'])]) as $document) {
                $value = $document->getAttribute(self::REVIEW.'.stars');
                if (\is_numeric($value) && \in_array((int) $value, [30, 40], true)) {
                    $name = $document->getAttribute('name');
                    $stars[(int) $value] = \is_string($name) && $name !== '' ? $name : null;
                }
            }

            $this->assertSame(
                [30 => null, 40 => null],
                $this->sortedByKey($stars),
                "Through a {$join->value} {$label}, the review of the unreadable author must come back unmatched, like the review of the author that does not exist",
            );
        }
    }

    public function testCombiningAFullOuterJoinWithARightJoinReadsWhatDirectReadsAllow(): void
    {
        foreach ([false, true] as $native) {
            foreach ([false, true] as $sharedTables) {
                $database = $this->database($native, grantAuthors: false, grantJoined: false, sharedTables: $sharedTables);
                $this->seed($database, $sharedTables);

                $joins = [
                    $this->join(Method::FullOuterJoin, self::BOOKS, self::BOOK, '$id'),
                    $this->join(Method::RightJoin, self::REVIEWS, self::REVIEW, '$id'),
                ];
                $expected = $this->joined($this->directReads($database, $native), $joins);

                $this->assertNotSame(self::REJECTED, $expected, 'Direct reads must answer the combination');
                $this->assertSame($expected, $this->read($database, $joins), 'The combination must read what direct reads allow, with and without shared tables');
            }
        }
    }

    /**
     * Two joins of every kind in a row, the second joined on the main table or on the first join,
     * and every kind of join alone.
     *
     * @return array<string, list<Query>>
     */
    private static function chains(): array
    {
        $chains = [];
        foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin, Method::CrossJoin] as $first) {
            [$collection, $alias] = $first === Method::CrossJoin ? [self::EXTRAS, self::EXTRA] : [self::BOOKS, self::BOOK];
            foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin] as $second) {
                foreach (['$id' => self::AUTHORS, $alias.'.authorId' => $alias] as $on => $target) {
                    $chains["{$first->value} {$collection}, {$second->value} reviews on {$target}"] = [
                        self::join($first, $collection, $alias, '$id'),
                        self::join($second, self::REVIEWS, self::REVIEW, $on),
                    ];
                }
            }
        }
        foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin, Method::CrossJoin] as $single) {
            $chains["{$single->value} books alone"] = [self::join($single, self::BOOKS, self::BOOK, '$id')];
        }

        return $chains;
    }

    private static function join(Method $method, string $collection, string $alias, string $on): Query
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

    private function database(bool $native, bool $grantAuthors, bool $grantJoined, bool $sharedTables): Database
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
            ->setNamespace('visibility')
            ->setSharedTables($sharedTables)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $this->collectionPermissions($grantAuthors),
            documentSecurity: true,
        ));
        foreach (self::NUMBERS as $collection => $number) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'authorId', size: 64, required: true),
                    Attribute::integer(key: $number, required: true),
                ],
                permissions: $this->collectionPermissions($grantJoined),
                documentSecurity: ! $grantJoined,
            ));
        }

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

    /**
     * Under shared tables another tenant holds a readable copy of every document, so a join that
     * pairs across tenants would find a match for each one.
     */
    private function seed(Database $database, bool $sharedTables): void
    {
        $tenants = $sharedTables ? [self::OTHER_TENANT, self::TENANT] : [null];

        foreach ($tenants as $tenant) {
            $database->setTenant($tenant);
            foreach (self::DOCUMENTS as $collection => $documents) {
                foreach ($documents as $id => [$attributes, $readable]) {
                    $database->createDocument($collection, new Document([
                        '$id' => $id,
                        '$permissions' => [
                            $readable || $tenant === self::OTHER_TENANT
                                ? Permission::read(Role::any())
                                : Permission::read(Role::user('someone-else')),
                        ],
                        ...$attributes,
                    ]));
                }
            }
        }
    }

    /**
     * A database of the documents direct reads of each collection return, with nothing left to
     * filter: every collection granted, no document security, no other tenant.
     */
    private function directReads(Database $database, bool $native): Database
    {
        $direct = $this->database($native, grantAuthors: true, grantJoined: true, sharedTables: false);
        foreach (\array_keys(self::DOCUMENTS) as $collection) {
            foreach ($database->find($collection, [Query::limit(100)]) as $document) {
                $direct->createDocument($collection, new Document([
                    '$id' => $document->getId(),
                    '$permissions' => [Permission::read(Role::any())],
                    ...\array_intersect_key(
                        $document->getArrayCopy(),
                        \array_flip(['name', 'authorId', ...\array_values(self::NUMBERS)]),
                    ),
                ]));
            }
        }

        return $direct;
    }

    /**
     * The rows as [author name, then each join's number], their count and the sum of the last
     * join's number, as find(), count() and sum() return them, or REJECTED when refused.
     *
     * @param list<Query> $joins
     * @return array{rows: list<list<string|int|null>>, count: int, sum: int|float}|string
     */
    private function read(Database $database, array $joins): array|string
    {
        $copies = static fn (): array => \array_map(static fn (Query $join): Query => clone $join, $joins);
        $numbers = $this->numbers($joins);

        try {
            return [
                'rows' => $this->rows($database, $copies(), $numbers),
                'count' => $database->count(self::AUTHORS, $copies()),
                'sum' => $database->sum(self::AUTHORS, $numbers[\count($numbers) - 1], $copies()),
            ];
        } catch (QueryException) {
            return self::REJECTED;
        }
    }

    /**
     * What read() must return, taken from the rows alone: nothing filters the joins of a
     * database of direct reads, so its count and sum follow from its rows. A chain the adapter
     * refuses outright is refused here too.
     *
     * @param list<Query> $joins
     * @return array{rows: list<list<string|int|null>>, count: int, sum: int}|string
     */
    private function joined(Database $direct, array $joins): array|string
    {
        try {
            $rows = $this->rows(
                $direct,
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
     * @return list<list<string|int|null>>
     */
    private function rows(Database $database, array $joins, array $numbers): array
    {
        $rows = \array_map(static function (Document $document) use ($numbers): array {
            $name = $document->getAttribute('name');
            $row = [\is_string($name) && $name !== '' ? $name : null];
            foreach ($numbers as $number) {
                $value = $document->getAttribute($number);
                $row[] = \is_numeric($value) ? (int) $value : null;
            }

            return $row;
        }, $database->find(self::AUTHORS, [...$joins, Query::select(['name', ...$numbers]), Query::limit(100)]));
        \usort($rows, static fn (array $left, array $right): int => \json_encode($left) <=> \json_encode($right));

        return $rows;
    }

    /**
     * @param array<int, mixed> $values
     * @return array<int, mixed>
     */
    private function sortedByKey(array $values): array
    {
        \ksort($values);

        return $values;
    }
}
