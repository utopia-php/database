<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\SQL as SQLBuilder;

/**
 * Skipping authorization lifts permissions, never tenant isolation: under shared tables a builder
 * from Database::from() reads and writes only the selected tenant's rows, exactly what the same
 * statement reads in a database holding that tenant's rows alone. Both tenants reuse the same
 * document ids, and every table also holds a legacy row with no tenant.
 */
final class QueryBuilderTenancyTest extends TestCase
{
    private const string NAMESPACE = 'builder';

    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private const string REVIEWS = 'reviews';

    private const string EXTRAS = 'extras';

    private const int FIRST = 1;

    private const int SECOND = 2;

    /**
     * Rows are written under this tenant and then stripped of it, standing in for legacy rows
     * that carry no tenant at all.
     */
    private const int TENANTLESS = 3;

    private const string REJECTED = 'rejected';

    private const string NO_TENANT = 'none';

    private const array ALIASES = [
        self::BOOKS => 'book',
        self::REVIEWS => 'review',
        self::EXTRAS => 'extra',
    ];

    private const array NUMBERS = [
        self::BOOKS => 'pages',
        self::REVIEWS => 'stars',
        self::EXTRAS => 'weight',
    ];

    /**
     * Tenant one's book "b2" belongs to an author only tenant two has and its "b3" to the
     * tenantless author; its review "r3" names no author and "r4" one only tenant two has. Only
     * tenant two and the tenantless rows have extras, so tenant one's cross join with them is
     * empty and every row a later outer join keeps must come back unmatched.
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
            self::EXTRAS => [],
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
            self::EXTRAS => [
                'x1' => ['authorId' => 'a1', 'weight' => 5],
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
            self::EXTRAS => [
                'x9' => ['authorId' => 'a2', 'weight' => 9],
            ],
        ],
    ];

    private PDO $pdo;

    /**
     * Every single join and every chain of two joins, the second joined on the main table or on
     * the first join: chains.php's ten chains among them. A right or full outer join that follows a
     * right, full outer or cross join must not pair its rows with another tenant's rows of the
     * earlier table, or they vanish instead of coming back unmatched.
     */
    public function testEveryJoinAndChainOfTwoJoinsReadsWhatADedicatedDatabaseReads(): void
    {
        $this->assertChainsReadWhatADedicatedDatabaseReads(self::chainsOfTwo());
    }

    public function testChainsOfThreeJoinsReadWhatADedicatedDatabaseReads(): void
    {
        $this->assertChainsReadWhatADedicatedDatabaseReads([
            'right, right, right' => [[JoinType::Right, self::BOOKS, null], [JoinType::Right, self::REVIEWS, null], [JoinType::Right, self::EXTRAS, self::REVIEWS]],
            'cross, right, full on it' => [[JoinType::Cross, self::EXTRAS, null], [JoinType::Right, self::BOOKS, null], [JoinType::FullOuter, self::REVIEWS, self::BOOKS]],
            'full, cross, right on the first' => [[JoinType::FullOuter, self::BOOKS, null], [JoinType::Cross, self::EXTRAS, null], [JoinType::Right, self::REVIEWS, self::BOOKS]],
            'left, right on it, full on that' => [[JoinType::Left, self::BOOKS, null], [JoinType::Right, self::REVIEWS, self::BOOKS], [JoinType::FullOuter, self::EXTRAS, self::REVIEWS]],
            'inner, cross, right' => [[JoinType::Inner, self::BOOKS, null], [JoinType::Cross, self::EXTRAS, null], [JoinType::Right, self::REVIEWS, null]],
            'right, left on it, right on the main table' => [[JoinType::Right, self::BOOKS, null], [JoinType::Left, self::REVIEWS, self::BOOKS], [JoinType::Right, self::EXTRAS, null]],
            'full, full on it, full on that' => [[JoinType::FullOuter, self::BOOKS, null], [JoinType::FullOuter, self::REVIEWS, self::BOOKS], [JoinType::FullOuter, self::EXTRAS, self::REVIEWS]],
        ]);
    }

    public function testARenamedMainTableIsScopedAndRefusedOnlyForRightAndFullOuterJoins(): void
    {
        $shared = $this->shared();
        $shared->setTenant(self::FIRST);
        $dedicated = $this->dedicated(self::FIRST);

        foreach ([JoinType::Inner, JoinType::Left, JoinType::Cross] as $type) {
            $joins = [[$type, $type === JoinType::Cross ? self::EXTRAS : self::BOOKS, null]];

            $this->assertSame(
                $this->read($dedicated, $joins, 'author'),
                $this->read($shared, $joins, 'author'),
                "A {$type->value} must read the selected tenant's rows when the main table is aliased",
            );
        }

        foreach ([JoinType::Right, JoinType::FullOuter] as $type) {
            $this->assertSame(
                self::REJECTED,
                $this->read($shared, [[$type, self::BOOKS, null]], 'author'),
                "A {$type->value} pairs rows with the main table named as Database::from() names it, so renaming it is refused",
            );
        }
    }

    public function testABuilderWithoutATableIsRefused(): void
    {
        $database = $this->shared();
        $database->setTenant(self::FIRST);

        $this->expectException(QueryException::class);
        $database->getAuthorization()->skip(fn () => $this->builder($database)->fromNone()->selectRaw('1')->execute());
    }

    public function testARawUpdateChangesOnlyTheSelectedTenantsRows(): void
    {
        $database = $this->shared();
        $database->setTenant(self::FIRST);

        $this->assertSame(1, $this->update($database, self::BOOKS, ['pages' => 100], [Query::equal('$id', ['b1'])]), 'Tenant two also has a book "b1"');
        $this->assertSame(
            [
                'b1' => [self::FIRST => 100, self::SECOND => 21],
                'b2' => [self::FIRST => 12, self::SECOND => 22],
                'b3' => [self::FIRST => 13],
                'orphan' => [self::NO_TENANT => 99],
            ],
            $this->stored(self::BOOKS, 'pages'),
        );

        $this->assertSame(3, $this->update($database, self::BOOKS, ['pages' => 0]), 'An unfiltered update must reach only the selected tenant');
        $this->assertSame(
            [
                'b1' => [self::FIRST => 0, self::SECOND => 21],
                'b2' => [self::FIRST => 0, self::SECOND => 22],
                'b3' => [self::FIRST => 0],
                'orphan' => [self::NO_TENANT => 99],
            ],
            $this->stored(self::BOOKS, 'pages'),
        );
    }

    public function testARawDeleteRemovesOnlyTheSelectedTenantsRows(): void
    {
        $database = $this->shared();
        $database->setTenant(self::FIRST);

        $this->assertSame(1, $this->delete($database, self::REVIEWS, [Query::equal('$id', ['r1'])]), 'Tenant two also has a review "r1"');
        $this->assertSame(
            [
                'r1' => [self::SECOND => 1],
                'r2' => [self::FIRST => 4],
                'r3' => [self::FIRST => 3],
                'r4' => [self::FIRST => 2],
                'stale' => [self::NO_TENANT => 9],
            ],
            $this->stored(self::REVIEWS, 'stars'),
        );

        $this->assertSame(3, $this->delete($database, self::REVIEWS), 'An unfiltered delete must reach only the selected tenant');
        $this->assertSame(
            ['r1' => [self::SECOND => 1], 'stale' => [self::NO_TENANT => 9]],
            $this->stored(self::REVIEWS, 'stars'),
        );
    }

    public function testWithoutATenantTheBuilderReadsAndWritesNothing(): void
    {
        $database = $this->shared();
        $database->setTenant(null);
        $before = $this->stored(self::AUTHORS, 'name');

        $this->assertSame([], $this->read($database, []), 'Not even the rows without a tenant');
        $this->assertSame(0, $this->update($database, self::AUTHORS, ['name' => 'renamed']));
        $this->assertSame(0, $this->delete($database, self::AUTHORS));
        $this->assertSame($before, $this->stored(self::AUTHORS, 'name'));
    }

    public function testAnotherTenantIsReadOnlyBySelectingIt(): void
    {
        $database = $this->shared();
        $database->setTenant(self::FIRST);

        $selected = $this->read($database, []);
        $other = $database->withTenant(self::SECOND, fn () => $this->read($database, []));

        $this->assertSame(['["one-a1"]', '["one-a2"]'], $selected);
        $this->assertSame(['["two-a1"]', '["two-a2"]', '["two-shared"]'], $other);
    }

    /**
     * @return iterable<string, array{SQL, string}>
     */
    public static function dialects(): iterable
    {
        yield 'PostgreSQL' => [new Postgres(new PDO('sqlite::memory:')), '"'];
        yield 'MariaDB' => [new MariaDB(new PDO('sqlite::memory:')), '`'];
    }

    /**
     * PostgreSQL folds an unquoted identifier to lower case, so a condition that names a mixed-case
     * alias unquoted finds no table by that name. Every table a tenant condition names must be named
     * as the builder declares it, and the main table by its own name, since a bare column is
     * ambiguous once a join is added.
     */
    #[DataProvider('dialects')]
    public function testEveryTenantConditionNamesItsTableAsTheBuilderDeclaresIt(SQL $adapter, string $quote): void
    {
        $adapter->setDatabase('builder');
        $adapter->setNamespace('capture');
        $adapter->setSharedTables(true);
        $adapter->setTenant(7);

        $raw = fn (string $collection): string => $this->rawTable($adapter, $collection);
        $quoted = static fn (string $identifier): string => \implode('.', \array_map(
            static fn (string $part): string => $quote.$part.$quote,
            \explode('.', $identifier),
        ));
        $authors = $quoted($raw(self::AUTHORS));

        $builder = $adapter->getBuilder(self::AUTHORS)
            ->crossJoin($raw(self::EXTRAS), 'Extra')
            ->rightJoin($raw(self::REVIEWS), 'Extra.authorId', 'Review.authorId', '=', 'Review')
            ->joinWhere($raw(self::BOOKS), static function (JoinBuilder $join): void {
                $join->on('Review.authorId', 'Book.authorId');
            }, JoinType::FullOuter, 'Book')
            ->select([$raw(self::AUTHORS).'.name', 'Book.pages']);
        $sql = $builder->build()->query;

        $tenant = static fn (string $table): string => "{$quoted($table)}._tenant IN (?)";
        $missing = static fn (string $table): string => "({$tenant($table)} OR {$quoted($table.'.'.Storage::UID)} IS NULL)";
        $this->assertSame(
            "SELECT {$authors}.{$quote}name{$quote}, {$quote}Book{$quote}.{$quote}pages{$quote}"
            ." FROM {$authors}"
            ." CROSS JOIN {$quoted($raw(self::EXTRAS))} AS {$quote}Extra{$quote}"
            ." RIGHT JOIN {$quoted($raw(self::REVIEWS))} AS {$quote}Review{$quote}"
            ." ON {$quote}Extra{$quote}.{$quote}authorId{$quote} = {$quote}Review{$quote}.{$quote}authorId{$quote}"
            ." AND {$missing($raw(self::AUTHORS))} AND {$tenant('Review')} AND {$missing('Extra')}"
            ." FULL OUTER JOIN {$quoted($raw(self::BOOKS))} AS {$quote}Book{$quote}"
            ." ON {$quote}Review{$quote}.{$quote}authorId{$quote} = {$quote}Book{$quote}.{$quote}authorId{$quote}"
            ." AND {$missing($raw(self::AUTHORS))} AND {$tenant('Book')} AND {$missing('Extra')} AND {$missing('Review')}"
            ." WHERE {$missing($raw(self::AUTHORS))} AND {$missing('Extra')} AND {$missing('Review')} AND {$missing('Book')}",
            $sql,
        );
        $this->assertDoesNotMatchRegularExpression('/(?<!'.\preg_quote($quote, '/').')\b(?:Book|Review|Extra)\b/', $sql, 'Every alias must be quoted');
        $this->assertDoesNotMatchRegularExpression('/(?<!\.)\b_tenant\b/', $sql, 'Every tenant column must name its table');

        $this->assertSame(
            "UPDATE {$authors} SET {$quote}name{$quote} = ? WHERE {$tenant($raw(self::AUTHORS))}",
            $adapter->getBuilder(self::AUTHORS)->set(['name' => 'renamed'])->update()->query,
        );
        $this->assertSame(
            "DELETE FROM {$authors} WHERE {$tenant($raw(self::AUTHORS))}",
            $adapter->getBuilder(self::AUTHORS)->delete()->query,
        );
    }

    /**
     * @param array<string, list<array{JoinType, string, ?string}>> $chains
     */
    private function assertChainsReadWhatADedicatedDatabaseReads(array $chains): void
    {
        $shared = $this->shared();

        foreach ([self::FIRST, self::SECOND] as $tenant) {
            $dedicated = $this->dedicated($tenant);
            $shared->setTenant($tenant);

            $expected = [];
            $actual = [];
            foreach ($chains as $label => $joins) {
                $expected[$label] = $this->read($dedicated, $joins);
                $actual[$label] = $this->read($shared, $joins);
            }

            $this->assertSame($expected, $actual, "Tenant {$tenant} must read through every chain what its own database would return");
        }
    }

    /**
     * @return array<string, list<array{JoinType, string, ?string}>>
     */
    private static function chainsOfTwo(): array
    {
        $chains = [];
        foreach ([JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::FullOuter, JoinType::Cross] as $first) {
            $collection = $first === JoinType::Cross ? self::EXTRAS : self::BOOKS;
            $chains["{$first->value} {$collection}"] = [[$first, $collection, null]];
            $chains["{$first->value} ".self::REVIEWS] = [[$first, self::REVIEWS, null]];

            foreach ([JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::FullOuter] as $second) {
                foreach ([null, $collection] as $on) {
                    $target = $on ?? self::AUTHORS;
                    $chains["{$first->value} {$collection}, {$second->value} reviews on {$target}"] = [
                        [$first, $collection, null],
                        [$second, self::REVIEWS, $on],
                    ];
                }
            }
        }

        return $chains;
    }

    /**
     * Both tenants' rows and the tenantless ones, in one set of shared tables.
     */
    private function shared(): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = $this->database($pdo, sharedTables: true);

        foreach (self::ROWS as $tenant => $collections) {
            $database->setTenant($tenant);
            $this->write($database, $collections);
        }

        foreach (\array_keys(self::ROWS[self::FIRST]) as $collection) {
            foreach ([$collection, Storage::permissionsTable($collection)] as $table) {
                $pdo->exec('UPDATE '.self::table($table).' SET '.Storage::TENANT.' = NULL WHERE '.Storage::TENANT.' = '.self::TENANTLESS);
            }
        }

        $this->pdo = $pdo;

        return $database;
    }

    /**
     * One tenant's rows alone, in tables of their own: what that tenant must read.
     */
    private function dedicated(int $tenant): Database
    {
        $database = $this->database(new PDO('sqlite::memory:'), sharedTables: false);
        $this->write($database, self::ROWS[$tenant]);

        return $database;
    }

    private function database(PDO $pdo, bool $sharedTables): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(new SQLite($pdo), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('joins')
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables($sharedTables)
            ->setTenant(null);
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        foreach (self::NUMBERS as $collection => $number) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'authorId', size: 64, required: true),
                    Attribute::integer(key: $number, required: true),
                ],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }

        return $database;
    }

    /**
     * @param array<string, array<string, array<string, string|int>>> $collections
     */
    private function write(Database $database, array $collections): void
    {
        foreach ($collections as $collection => $documents) {
            foreach ($documents as $id => $attributes) {
                $database->createDocument($collection, new Document(['$id' => $id, ...$attributes]));
            }
        }
    }

    private function builder(Database $database, string $collection = self::AUTHORS): SQLBuilder
    {
        $builder = $database->from($collection);
        $this->assertInstanceOf(SQLBuilder::class, $builder);

        return $builder;
    }

    /**
     * The rows as [author name, then each join's number], sorted, or REJECTED when refused.
     *
     * @param list<array{JoinType, string, ?string}> $joins Each join's type, collection, and the
     *                                                     collection its ON names, or null for the main one
     * @param string $alias The main table's alias, or empty for none
     * @return list<string>|string
     */
    private function read(Database $database, array $joins, string $alias = ''): array|string
    {
        $main = $alias !== '' ? $alias : self::table(self::AUTHORS);

        try {
            $documents = $database->getAuthorization()->skip(function () use ($database, $joins, $alias, $main): mixed {
                $builder = $this->builder($database);
                if ($alias !== '') {
                    $builder->from(self::table(self::AUTHORS), $alias);
                }

                $columns = [$main.'.name'];
                foreach ($joins as [$type, $collection, $on]) {
                    $this->join($builder, $type, $collection, $on === null ? $main.'.'.Storage::UID : self::ALIASES[$on].'.authorId');
                    $columns[] = self::ALIASES[$collection].'.'.self::NUMBERS[$collection];
                }

                return $builder->select($columns)->execute();
            });
        } catch (QueryException) {
            return self::REJECTED;
        }

        $this->assertIsArray($documents);
        $rows = [];
        foreach ($documents as $document) {
            $this->assertInstanceOf(Document::class, $document);
            $name = $document->getAttribute('name');
            $row = [\is_string($name) ? $name : null];
            foreach ($joins as [, $collection]) {
                $number = $document->getAttribute(self::NUMBERS[$collection]);
                $row[] = \is_numeric($number) ? (int) $number : null;
            }
            $rows[] = (string) \json_encode($row);
        }
        \sort($rows);

        return $rows;
    }

    /**
     * @param array<string, mixed> $values
     * @param list<Query> $filters
     */
    private function update(Database $database, string $collection, array $values, array $filters = []): mixed
    {
        return $database->getAuthorization()->skip(
            fn () => $this->builder($database, $collection)->set($values)->filter($filters)->update()->execute(),
        );
    }

    /**
     * @param list<Query> $filters
     */
    private function delete(Database $database, string $collection, array $filters = []): mixed
    {
        return $database->getAuthorization()->skip(
            fn () => $this->builder($database, $collection)->filter($filters)->delete()->execute(),
        );
    }

    private function join(SQLBuilder $builder, JoinType $type, string $collection, string $on): void
    {
        $table = self::table($collection);
        $alias = self::ALIASES[$collection];
        $column = $alias.'.authorId';

        match ($type) {
            JoinType::Inner => $builder->join($table, $on, $column, '=', $alias),
            JoinType::Left => $builder->leftJoin($table, $on, $column, '=', $alias),
            JoinType::Right => $builder->rightJoin($table, $on, $column, '=', $alias),
            JoinType::FullOuter => $builder->joinWhere($table, static function (JoinBuilder $join) use ($on, $column): void {
                $join->on($on, $column);
            }, JoinType::FullOuter, $alias),
            JoinType::Cross => $builder->crossJoin($table, $alias),
            JoinType::Natural => $builder->naturalJoin($table, $alias),
        };
    }

    /**
     * Each stored row's value by document id and tenant, the tenantless row under NO_TENANT.
     *
     * @return array<string, array<int|string, string|int|null>>
     */
    private function stored(string $collection, string $column): array
    {
        $statement = $this->pdo->query('SELECT _uid, _tenant, '.$column.' FROM '.self::table($collection).' ORDER BY _uid, _tenant');
        $this->assertNotFalse($statement);

        $stored = [];
        foreach ($statement->fetchAll(PDO::FETCH_NUM) as $row) {
            $this->assertIsArray($row);
            [$id, $tenant, $value] = $row;
            $this->assertIsString($id);
            $stored[$id][\is_numeric($tenant) ? (int) $tenant : self::NO_TENANT] = \is_numeric($value) ? (int) $value : (\is_string($value) ? $value : null);
        }

        return $stored;
    }

    private function rawTable(SQL $adapter, string $collection): string
    {
        $table = (new ReflectionMethod($adapter, 'getSQLTableRaw'))->invoke($adapter, $collection);
        $this->assertIsString($table);

        return $table;
    }

    private static function table(string $collection): string
    {
        return self::NAMESPACE.'_'.$collection;
    }
}
