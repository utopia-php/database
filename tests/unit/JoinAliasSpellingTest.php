<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
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
 * A join alias is spelled however the caller likes: the builder declares it quoted, so the tenant
 * and permission conditions added for it must name it quoted too. Unquoted, PostgreSQL folds a
 * mixed-case alias to lower case and then finds no table by that name, and PostgreSQL and SQLite
 * read a reserved word as the keyword it is.
 */
final class JoinAliasSpellingTest extends TestCase
{
    private const string AUTHORS = 'authors';

    private const string BOOKS = 'books';

    private const string REVIEWS = 'reviews';

    private const string EXTRAS = 'extras';

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    /**
     * Each joined collection's numeric attribute, read back as "alias.attribute".
     */
    private const array NUMBERS = [
        self::BOOKS => 'pages',
        self::REVIEWS => 'stars',
        self::EXTRAS => 'weight',
    ];

    private const array LOWER_CASE = [self::BOOKS => 'book', self::REVIEWS => 'review', self::EXTRAS => 'extra'];

    private const array MIXED_CASE = [self::BOOKS => 'Book', self::REVIEWS => 'Review', self::EXTRAS => 'Extra'];

    private const array RESERVED_WORDS = [self::BOOKS => 'order', self::REVIEWS => 'group', self::EXTRAS => 'select'];

    /**
     * Every join type and the chains whose later right join repeats the conditions of the tables
     * before it, as [method, joined collection, collection whose alias the ON names or null for
     * the main one].
     *
     * @var array<string, list<array{Method, string, ?string}>>
     */
    private const array CHAINS = [
        'an inner join' => [[Method::Join, self::BOOKS, null]],
        'a left join' => [[Method::LeftJoin, self::BOOKS, null]],
        'a right join' => [[Method::RightJoin, self::BOOKS, null]],
        'a full outer join' => [[Method::FullOuterJoin, self::BOOKS, null]],
        'a cross join' => [[Method::CrossJoin, self::BOOKS, null]],
        'an inner join, then a right join' => [[Method::Join, self::BOOKS, null], [Method::RightJoin, self::REVIEWS, null]],
        'a cross join, then a right join' => [[Method::CrossJoin, self::EXTRAS, null], [Method::RightJoin, self::REVIEWS, null]],
        'a right join, then a right join on it' => [[Method::RightJoin, self::BOOKS, null], [Method::RightJoin, self::REVIEWS, self::BOOKS]],
    ];

    /**
     * Every document and whether the caller holds read on it; the unreadable ones share keys with
     * readable ones, so the permission conditions decide what each join pairs.
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
        ],
        self::REVIEWS => [
            'r1' => [['authorId' => 'a1', 'stars' => 10], true],
            'r2' => [['authorId' => 'a2', 'stars' => 20], true],
            'r3' => [['authorId' => 'ghost', 'stars' => 30], true],
            'r4' => [['authorId' => 'a2', 'stars' => 40], false],
        ],
        self::EXTRAS => [
            'x1' => [['authorId' => 'a1', 'weight' => 100], true],
            'x2' => [['authorId' => 'a2', 'weight' => 200], false],
        ],
    ];

    /**
     * @return iterable<string, array{Closure(PDO): SQL, string}>
     */
    public static function adapters(): iterable
    {
        yield 'PostgreSQL' => [static fn (PDO $pdo): SQL => new Postgres($pdo), '"'];
        yield 'MariaDB' => [static fn (PDO $pdo): SQL => new MariaDB($pdo), '`'];
        yield 'MySQL' => [static fn (PDO $pdo): SQL => new MySQL($pdo), '`'];
    }

    /**
     * @return iterable<string, array{array<string, string>, bool, bool}>
     */
    public static function readers(): iterable
    {
        foreach (['mixed-case aliases' => self::MIXED_CASE, 'reserved-word aliases' => self::RESERVED_WORDS] as $spelling => $aliases) {
            foreach (['dedicated tables' => false, 'shared tables' => true] as $tables => $sharedTables) {
                foreach (['emulated full outer join' => false, 'native full outer join' => true] as $mode => $nativeFullOuterJoin) {
                    yield "{$spelling}, {$tables}, {$mode}" => [$aliases, $sharedTables, $nativeFullOuterJoin];
                }
            }
        }
    }

    /**
     * Every statement a join read sends names each mixed-case alias only quoted, as the builder
     * declares it, under shared tables with every collection read per document.
     *
     * @param Closure(PDO): SQL $adapter
     */
    #[DataProvider('adapters')]
    public function testEveryStatementNamesAMixedCaseAliasQuoted(Closure $adapter, string $quote): void
    {
        $statements = [];
        $sql = $adapter($this->capturingPdo($statements));
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $sql->setAuthorization($authorization);
        $sql->setDatabase('database');
        $sql->setNamespace('namespace');
        $sql->setSharedTables(true);
        $sql->setTenant(self::TENANT);
        $authors = new Document(['$id' => self::AUTHORS, 'attributes' => [], 'documentSecurity' => true]);

        foreach (self::CHAINS as $chain) {
            [$joins, $numbers] = $this->joins($chain, self::MIXED_CASE);

            $sql->find($authors, [...$joins, Query::select(['name', ...$numbers])]);
            $sql->count($authors, $joins);
            $sql->sum($authors, $numbers[\count($numbers) - 1], $joins);
            $sql->getDocument($authors, 'a1', $joins);
        }

        $this->assertCount(\count(self::CHAINS) * 4, $statements);
        $sent = \implode("\n", $statements);
        $quoted = \preg_quote($quote, '/');
        foreach (self::MIXED_CASE as $alias) {
            $this->assertStringContainsString($quote.$alias.$quote.'.', $sent, "{$alias} must be named quoted");
            foreach ($statements as $statement) {
                $this->assertDoesNotMatchRegularExpression("/(?<!{$quoted})\\b{$alias}\\b(?!{$quoted})/", $statement, "{$alias} must never be named unquoted");
            }
        }
    }

    /**
     * @param array<string, string> $aliases
     */
    #[DataProvider('readers')]
    public function testJoinsReadWhatTheyReadWithLowerCaseAliases(array $aliases, bool $sharedTables, bool $nativeFullOuterJoin): void
    {
        $database = $this->database($sharedTables, $nativeFullOuterJoin);

        foreach (self::CHAINS as $label => $chain) {
            $expected = $this->read($database, $chain, self::LOWER_CASE);

            $this->assertNotSame([], $expected['rows'], "{$label} must return rows for the comparison to mean anything");
            $this->assertSame(
                $expected,
                $this->read($database, $chain, $aliases),
                "{$label} aliased ".\implode(', ', $aliases).' must read what it reads with lower-case aliases',
            );
        }
    }

    /**
     * @param list<string> $statements
     */
    private function capturingPdo(array &$statements): PDO
    {
        $statement = self::createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('bindValue')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('fetch')->willReturn(false);
        $statement->method('closeCursor')->willReturn(true);

        $pdo = self::createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$statements, $statement): PDOStatement {
            $statements[] = $query;

            return $statement;
        });

        return $pdo;
    }

    /**
     * Every collection read per document; under shared tables another tenant holds the same
     * documents with larger numbers, so a condition that lost its tenant would change every read.
     */
    private function database(bool $sharedTables, bool $nativeFullOuterJoin): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $pdo = new PDO('sqlite::memory:');
        $database = new Database(
            $nativeFullOuterJoin ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo),
            new Cache(new None()),
        );
        $database
            ->setAuthorization($authorization)
            ->setDatabase('joins')
            ->setNamespace('spelling')
            ->setSharedTables($sharedTables)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
        foreach (self::NUMBERS as $collection => $number) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'authorId', size: 64, required: true),
                    Attribute::integer(key: $number, required: true),
                ],
                permissions: [Permission::create(Role::any())],
                documentSecurity: true,
            ));
        }

        $owners = $sharedTables ? [[self::OTHER_TENANT, 1000], [self::TENANT, 1]] : [[null, 1]];
        foreach ($owners as [$tenant, $scale]) {
            $database->setTenant($tenant);
            foreach (self::DOCUMENTS as $collection => $documents) {
                foreach ($documents as $id => [$attributes, $readable]) {
                    $database->createDocument($collection, new Document([
                        '$id' => $id,
                        '$permissions' => [$readable ? Permission::read(Role::any()) : Permission::read(Role::user('someone-else'))],
                        ...\array_map(static fn (string|int $value): string|int => \is_int($value) ? $value * $scale : $value, $attributes),
                    ]));
                }
            }
        }

        return $database;
    }

    /**
     * @param list<array{Method, string, ?string}> $chain
     * @param array<string, string> $aliases
     * @return array{rows: list<list<string|int|null>>, count: int, sum: int|float, document: list<list<string|int|null>>}
     */
    private function read(Database $database, array $chain, array $aliases): array
    {
        [$joins, $numbers] = $this->joins($chain, $aliases);
        $selection = Query::select(['name', ...$numbers]);

        return [
            'rows' => $this->rows($database->find(self::AUTHORS, [...$joins, $selection, Query::limit(100)]), $numbers),
            'count' => $database->count(self::AUTHORS, $joins),
            'sum' => $database->sum(self::AUTHORS, $numbers[\count($numbers) - 1], $joins),
            'document' => $this->rows([$database->getDocument(self::AUTHORS, 'a1', [...$joins, $selection])], $numbers),
        ];
    }

    /**
     * @param list<array{Method, string, ?string}> $chain
     * @param array<string, string> $aliases
     * @return array{list<Query>, list<string>} The joins and each joined collection's number, alias-qualified
     */
    private function joins(array $chain, array $aliases): array
    {
        $joins = [];
        $numbers = [];
        foreach ($chain as [$method, $collection, $on]) {
            $alias = $aliases[$collection];
            $left = $on === null ? '$id' : $aliases[$on].'.authorId';
            $numbers[] = $alias.'.'.self::NUMBERS[$collection];
            $joins[] = match ($method) {
                Method::Join => Query::join($collection, $left, 'authorId', '=', $alias),
                Method::LeftJoin => Query::leftJoin($collection, $left, 'authorId', '=', $alias),
                Method::RightJoin => Query::rightJoin($collection, $left, 'authorId', '=', $alias),
                Method::FullOuterJoin => Query::fullOuterJoin($collection, $left, 'authorId', '=', $alias),
                Method::CrossJoin => Query::crossJoin($collection, $alias),
                default => throw new \InvalidArgumentException("{$method->value} is not a join"),
            };
        }

        return [$joins, $numbers];
    }

    /**
     * @param array<Document> $documents
     * @param list<string> $numbers
     * @return list<list<string|int|null>>
     */
    private function rows(array $documents, array $numbers): array
    {
        $rows = \array_map(static function (Document $document) use ($numbers): array {
            $name = $document->getAttribute('name');
            $row = [\is_string($name) && $name !== '' ? $name : null];
            foreach ($numbers as $number) {
                $value = $document->getAttribute($number);
                $row[] = \is_numeric($value) ? (int) $value : null;
            }

            return $row;
        }, $documents);
        \usort($rows, static fn (array $left, array $right): int => \json_encode($left) <=> \json_encode($right));

        return $rows;
    }
}
