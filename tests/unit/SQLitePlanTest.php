<?php

namespace Tests\Unit;

use ArrayObject;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Swoole\Database\PDOStatementProxy;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\Statement;

/**
 * SQLite declares every unique index COLLATE NOCASE, and SQLite only uses an index for a comparison
 * in the index's collation. Every lookup the library emits by document id therefore has to compare
 * in NOCASE, or it scans the table: with shared tables and ANALYZE statistics a few self-joins then
 * plan as a Cartesian product. The plans are read with and without ANALYZE statistics.
 */
final class SQLitePlanTest extends TestCase
{
    private const string NAMESPACE = 'plan';

    private const string COLLECTION = 'posts';

    private const string TABLE = '`'.self::NAMESPACE.'_'.self::COLLECTION.'`';

    private const string PERMISSIONS_TABLE = self::NAMESPACE.'_'.self::COLLECTION.'_perms';

    private const string SEARCH_PATTERN = '/^SEARCH (\S+) /';

    private const string ID_SEARCH = '_uid=?';

    private const int DOCUMENTS = 300;

    private const int READABLE = 10;

    private const int MAX_JOINS = 8;

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    private const string READER = 'alice';

    private const string OTHER_READER = 'reader-';

    private const string DETAIL_COLUMN = 'detail';

    private string $path = '';

    private PDO $pdo;

    private Authorization $authorization;

    /**
     * @var ArrayObject<int, array{string, list<mixed>}>
     */
    private ArrayObject $statements;

    protected function setUp(): void
    {
        $this->statements = new ArrayObject();
    }

    protected function tearDown(): void
    {
        if ($this->path !== '' && \is_file($this->path)) {
            \unlink($this->path);
        }
    }

    /**
     * @return iterable<string, array{bool, bool}>
     */
    public static function modes(): iterable
    {
        foreach ([false, true] as $shared) {
            foreach ([true, false] as $analyzed) {
                yield self::mode($shared, $analyzed) => [$shared, $analyzed];
            }
        }
    }

    /**
     * @return iterable<string, array{bool, bool, int, bool}>
     */
    public static function selfJoinCounts(): iterable
    {
        foreach (self::modes() as $mode => [$shared, $analyzed]) {
            foreach ([false, true] as $nested) {
                for ($joins = 1; $joins <= self::MAX_JOINS; $joins++) {
                    yield $mode.', '.$joins.($nested ? ' nested' : '').' self-joins' => [$shared, $analyzed, $joins, $nested];
                }
            }
        }
    }

    #[DataProvider('modes')]
    public function testGetDocumentSearchesTheIdIndex(bool $shared, bool $analyzed): void
    {
        $database = $this->database($shared, $analyzed);

        $document = $this->recording(fn (): Document => $database->getDocument(self::COLLECTION, $this->id(27)));

        $this->assertSame($this->id(27), $document->getId());
        $this->assertIndexedPlans($analyzed);
    }

    #[DataProvider('modes')]
    public function testFindByIdSearchesTheIdIndex(bool $shared, bool $analyzed): void
    {
        $database = $this->database($shared, $analyzed);

        $documents = $this->recording(fn (): array => $database->find(self::COLLECTION, [
            Query::equal('$id', [$this->id(3), $this->id(6), $this->id(7)]),
        ]));

        $this->assertSame([$this->id(3), $this->id(6)], $this->ids($documents));
        $this->assertIndexedPlans($analyzed);
    }

    #[DataProvider('modes')]
    public function testRestrictedFindSearchesThroughThePermissionIndex(bool $shared, bool $analyzed): void
    {
        $database = $this->database($shared, $analyzed);

        $documents = $this->recording(fn (): array => $database->find(self::COLLECTION, [Query::limit(self::DOCUMENTS)]));

        $this->assertSame($this->readableIds(), $this->ids($documents));
        $this->assertIndexedPlans($analyzed);
    }

    #[DataProvider('selfJoinCounts')]
    public function testSelfJoinsSearchAnIndexPerAlias(bool $shared, bool $analyzed, int $joins, bool $nested): void
    {
        $database = $this->database($shared, $analyzed);

        $documents = $this->recording(fn (): array => $database->find(self::COLLECTION, [
            ...$this->selfJoins($joins, $nested),
            Query::limit(self::DOCUMENTS),
        ]));

        $this->assertSame($this->readableIds(), $this->ids($documents));
        $this->assertIndexedPlans($analyzed);
    }

    public function testSharedSelfJoinOfFourReturnsTheRightRows(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->authorization = new Authorization();
        $database = $this->handle(true, self::OTHER_TENANT);
        $database->create();
        $this->createCollection($database);
        $this->seed($database, ['a', 'b', 'c'], 'other');

        $database = $this->handle(true, self::TENANT);
        $this->createCollection($database);
        $this->seed($database, ['a', 'b', 'c', 'd'], 'own');
        $this->authorization->addRole(Role::user(self::READER)->toString());

        $documents = $database->find(self::COLLECTION, $this->selfJoins(4));

        $rows = \array_map(
            fn (Document $document): array => [
                $document->getId(),
                ...\array_map(
                    fn (int $join): mixed => $document->getAttribute('p'.$join.'.name'),
                    \range(1, 4),
                ),
            ],
            $documents,
        );

        $this->assertSame([
            ['a', 'own-a', 'own-a', 'own-a', 'own-a'],
            ['d', 'own-d', 'own-d', 'own-d', 'own-d'],
        ], $rows);
    }

    #[DataProvider('modes')]
    public function testIdComparisonsIgnoreCase(bool $shared, bool $analyzed): void
    {
        $database = $this->database($shared, $analyzed);
        $id = \strtoupper($this->id(6));

        $this->assertSame($this->id(6), $database->getDocument(self::COLLECTION, $id)->getId());
        $this->assertSame([$this->id(6)], $this->ids($database->find(self::COLLECTION, [Query::equal('$id', [$id])])));
        $this->assertSame(
            \array_values(\array_diff($this->readableIds(), [$this->id(6)])),
            $this->ids($database->find(self::COLLECTION, [Query::notEqual('$id', $id), Query::limit(self::DOCUMENTS)])),
        );
    }

    private static function mode(bool $shared, bool $analyzed): string
    {
        return ($shared ? 'shared' : 'plain').' tables '.($analyzed ? 'with' : 'without').' statistics';
    }

    private function database(bool $shared, bool $analyzed = true): Database
    {
        $this->path = (string) \tempnam(\sys_get_temp_dir(), 'sqlite-plan-');
        $this->pdo = new PDO('sqlite:'.$this->path);
        $this->authorization = new Authorization();

        $database = $this->handle($shared, self::TENANT);
        $database->create();
        $this->createCollection($database);

        $ids = \array_map($this->id(...), \range(0, self::DOCUMENTS - 1));
        $this->seed($database, $ids, 'name');

        if ($analyzed) {
            $this->pdo->exec('ANALYZE');
        }
        $this->authorization->addRole(Role::user(self::READER)->toString());

        return $database;
    }

    private function handle(bool $shared, int $tenant): Database
    {
        $adapter = new class ($this->pdo, $this->statements) extends SQLite {
            /**
             * @param  ArrayObject<int, array{string, list<mixed>}>  $statements
             */
            public function __construct(object $pdo, private readonly ArrayObject $statements)
            {
                parent::__construct($pdo);
            }

            protected function prepareStatement(string $sql, ?Event $event = null): DatabasePDOStatement|PDOStatementProxy|PDOStatement
            {
                $this->statements->append([$sql, []]);

                return parent::prepareStatement($sql, $event);
            }

            protected function executeResult(Statement $result, ?Event $event = null, string $collection = ''): PDOStatement|DatabasePDOStatement|PDOStatementProxy
            {
                $statement = parent::executeResult($result, $event, $collection);
                $this->statements[$this->statements->count() - 1] = [$result->query, $result->bindings];

                return $statement;
            }
        };

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization($this->authorization);
        $database->addHook(new Permissions());

        if ($shared) {
            $database->setSharedTables(true)->setTenant($tenant);
        }

        return $database;
    }

    private function createCollection(Database $database): void
    {
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 64)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
    }

    /**
     * Every third document, up to READABLE of them, is readable by READER; each of the others by a
     * user of its own, so the statistics see many users with a few documents each.
     *
     * @param  list<string>  $ids
     */
    private function seed(Database $database, array $ids, string $prefix): void
    {
        $readable = 0;
        $documents = [];
        foreach ($ids as $position => $id) {
            $reader = $position % 3 === 0 && $readable++ < self::READABLE ? self::READER : self::OTHER_READER.$id;
            $documents[] = new Document([
                '$id' => $id,
                'name' => $prefix.'-'.$id,
                '$permissions' => [Permission::read(Role::user($reader))],
            ]);
        }

        $database->createDocuments(self::COLLECTION, $documents);
    }

    /**
     * @template T
     *
     * @param  callable(): T  $operation
     * @return T
     */
    private function recording(callable $operation): mixed
    {
        $this->statements->exchangeArray([]);

        return $operation();
    }

    /**
     * Without statistics SQLite may walk the (_tenant, _id) index for the driving table to serve the
     * ORDER BY, so only there the driving table need not be looked up by id.
     */
    private function assertIndexedPlans(bool $analyzed): void
    {
        $plans = [];
        foreach ($this->statements as [$sql, $bindings]) {
            if (! \str_contains($sql, 'SELECT') || ! \str_contains($sql, self::TABLE)) {
                continue;
            }
            $plans[] = [$sql, $this->plan($sql, $bindings)];
        }

        $this->assertNotSame([], $plans, 'The operation must have read the collection');

        foreach ($plans as [$sql, $details]) {
            $report = $sql."\n  ".\implode("\n  ", $details);
            foreach ($details as $detail) {
                $this->assertStringStartsNotWith('SCAN ', $detail, 'A read scanned a table: '.$report);
                $this->assertStringNotContainsString('AUTOMATIC', $detail, 'A read built a throwaway index: '.$report);

                if (
                    \preg_match(self::SEARCH_PATTERN, $detail, $match) === 1
                    && $match[1] !== self::PERMISSIONS_TABLE
                    && ($analyzed || $match[1] !== Query::DEFAULT_ALIAS)
                ) {
                    $this->assertStringContainsString(self::ID_SEARCH, $detail, 'Every alias of the collection must be looked up by id: '.$report);
                }
            }
        }
    }

    /**
     * @param  list<mixed>  $bindings
     * @return list<string>
     */
    private function plan(string $sql, array $bindings): array
    {
        $statement = $this->pdo->prepare('EXPLAIN QUERY PLAN '.$sql);
        $this->assertInstanceOf(PDOStatement::class, $statement);
        if ($bindings === []) {
            $statement->execute();
        } else {
            $statement->execute(\array_map(static fn (mixed $value): mixed => \is_bool($value) ? (int) $value : $value, $bindings));
        }

        /** @var list<array<string, int|string>> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);

        return \array_map(static fn (array $row): string => (string) $row[self::DETAIL_COLUMN], $rows);
    }

    /**
     * @return list<Query>
     */
    private function selfJoins(int $joins, bool $nested = false): array
    {
        return \array_map(
            static fn (int $join): Query => $nested
                ? Query::join(self::COLLECTION, 'p'.$join, [Query::on('$id', '$id')])
                : Query::join(self::COLLECTION, '$id', '$id', '=', 'p'.$join),
            \range(1, $joins),
        );
    }

    private function id(int $position): string
    {
        return 'doc'.$position;
    }

    /**
     * @return list<string>
     */
    private function readableIds(): array
    {
        return \array_map(fn (int $position): string => $this->id($position * 3), \range(0, self::READABLE - 1));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }
}
