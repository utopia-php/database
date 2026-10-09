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
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\Statement;

/**
 * The permission check of a read is `_uid IN (SELECT _document FROM <collection>_perms …)`. IN already
 * compares against a set, so the subquery needs no DISTINCT: SQLite builds a temporary B-tree for one
 * on every restricted read.
 *
 * On MySQL a joined table's check carries NO_SEMIJOIN when the table is outer-joined, whatever the
 * number of joins, and when the read has LARGE_JOIN joins or more.
 */
final class PermissionSubqueryTest extends TestCase
{
    private const string NAMESPACE = 'subquery';

    private const string COLLECTION = 'posts';

    private const string TABLE = '`'.self::NAMESPACE.'_'.self::COLLECTION.'`';

    private const string DISTINCT_STEP = 'USE TEMP B-TREE FOR DISTINCT';

    private const string DETAIL_COLUMN = 'detail';

    private const int DOCUMENTS = 60;

    private const int TENANT = 1;

    private const string READER = 'alice';

    private const string OTHER_READER = 'other-';

    private const string NO_SEMIJOIN = '/*+ NO_SEMIJOIN() */ ';

    private const int LARGE_JOIN = 5;

    private const string MAIN_ALIAS = 'table_main';

    private PDO $pdo;

    private Authorization $authorization;

    /**
     * @var ArrayObject<int, array{string, list<mixed>}>
     */
    private ArrayObject $statements;

    #[\Override]
    protected function setUp(): void
    {
        $this->statements = new ArrayObject();
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function modes(): iterable
    {
        yield 'plain tables' => [false];
        yield 'shared tables' => [true];
    }

    #[DataProvider('modes')]
    public function testRestrictedFindBuildsNoDistinctStep(bool $shared): void
    {
        $database = $this->database($shared);

        $documents = $this->recording(fn (): array => $database->find(self::COLLECTION, [
            Query::greaterThan('score', -1),
            Query::notEqual('name', 'none'),
            Query::limit(self::DOCUMENTS),
        ]));

        $this->assertSame($this->readableIds(), $this->ids($documents));
        $this->assertNoDistinctStep();
    }

    #[DataProvider('modes')]
    public function testRestrictedCountBuildsNoDistinctStep(bool $shared): void
    {
        $database = $this->database($shared);

        $total = $this->recording(fn (): int => $database->count(self::COLLECTION, [Query::greaterThan('score', -1)]));

        $this->assertSame(\count($this->readableIds()), $total);
        $this->assertNoDistinctStep();
    }

    #[DataProvider('modes')]
    public function testJoinedCheckBuildsNoDistinctStep(bool $shared): void
    {
        $database = $this->database($shared);

        $documents = $this->recording(fn (): array => $database->find(self::COLLECTION, [
            Query::join(self::COLLECTION, 'peer', [Query::on('$id', '$id')]),
            Query::limit(self::DOCUMENTS),
        ]));

        $this->assertSame($this->readableIds(), $this->ids($documents));
        $this->assertNoDistinctStep();
    }

    #[DataProvider('modes')]
    public function testDocumentReadableThroughSeveralRolesIsReturnedOnce(bool $shared): void
    {
        $database = $this->database($shared);
        $this->authorization->addRole(Role::any()->toString());

        $documents = $database->find(self::COLLECTION, [Query::limit(self::DOCUMENTS)]);

        $ids = $this->ids($documents);
        $this->assertSame(\array_values(\array_unique($ids)), $ids, 'A document several roles may read must come back once');
        $this->assertSame($this->readableIds(), $ids);
    }

    /**
     * @return iterable<string, array{JoinType, int, bool}>
     */
    public static function mySQLJoinChains(): iterable
    {
        foreach ([JoinType::Left, JoinType::Right] as $joinType) {
            for ($links = 1; $links < self::LARGE_JOIN; $links++) {
                yield $joinType->name.' join chain of '.$links => [$joinType, $links, true];
            }
        }
        yield 'FullOuter join of 1' => [JoinType::FullOuter, 1, true];
        for ($links = 1; $links < self::LARGE_JOIN; $links++) {
            yield 'Inner join chain of '.$links => [JoinType::Inner, $links, false];
        }
        yield 'Inner join chain of '.self::LARGE_JOIN => [JoinType::Inner, self::LARGE_JOIN, true];
        yield 'Left join chain of '.self::LARGE_JOIN => [JoinType::Left, self::LARGE_JOIN, true];
    }

    #[DataProvider('mySQLJoinChains')]
    public function testMySQLJoinedChecksStaySubqueriesUnderOuterJoins(JoinType $joinType, int $links, bool $hinted): void
    {
        $sql = $this->mySQLFindSql(\array_map(
            static fn (int $link): Query => self::join($joinType, 'orders'.$link, 'o'.$link),
            \range(1, $links),
        ));

        for ($link = 1; $link <= $links; $link++) {
            $checks = $this->checks($sql, 'o'.$link);
            $this->assertNotSame([], $checks, 'Every joined table must be checked: '.$sql);
            foreach ($checks as $hint) {
                $this->assertSame($hinted, $hint, 'The check of o'.$link.' in: '.$sql);
            }
        }

        $main = $this->checks($sql, self::MAIN_ALIAS);
        $this->assertNotSame([], $main, 'The main table must be checked: '.$sql);
        $this->assertNotContains(true, $main, 'The main table\'s check stays a semi-join candidate: '.$sql);
    }

    public function testMySQLMixedChainHintsOnlyTheOuterJoinedChecks(): void
    {
        $sql = $this->mySQLFindSql([
            self::join(JoinType::Inner, 'orders1', 'o1'),
            self::join(JoinType::Left, 'orders2', 'o2'),
            self::join(JoinType::Inner, 'orders3', 'o3'),
        ]);

        $this->assertSame([false], $this->checks($sql, 'o1'), $sql);
        $this->assertSame([true], $this->checks($sql, 'o2'), $sql);
        $this->assertSame([false], $this->checks($sql, 'o3'), $sql);
    }

    public function testMySQLUnaliasedOuterJoinIsHinted(): void
    {
        $sql = $this->mySQLFindSql([Query::leftJoin('orders', 'j0', [Query::on('$id', 'customerId')])]);

        $this->assertSame(1, \substr_count($sql, self::NO_SEMIJOIN), $sql);
    }

    /**
     * @return iterable<string, array{JoinType, int, bool}>
     */
    public static function mySQLJoinsOnTheJoinedId(): iterable
    {
        foreach ([JoinType::Left, JoinType::Right] as $joinType) {
            for ($links = 1; $links <= self::LARGE_JOIN; $links++) {
                yield $joinType->name.' join chain of '.$links => [$joinType, $links, true];
            }
        }
        yield 'FullOuter join of 1' => [JoinType::FullOuter, 1, true];
        for ($links = 1; $links < self::LARGE_JOIN; $links++) {
            yield 'Inner join chain of '.$links => [JoinType::Inner, $links, false];
        }
        yield 'Inner join chain of '.self::LARGE_JOIN => [JoinType::Inner, self::LARGE_JOIN, true];
    }

    #[DataProvider('mySQLJoinsOnTheJoinedId')]
    public function testMySQLJoinOnTheJoinedIdIsHintedLikeAnyOuterJoin(JoinType $joinType, int $links, bool $hinted): void
    {
        $sql = $this->mySQLFindSql(\array_map(
            static fn (int $link): Query => self::joinOnId($joinType, 'orders'.$link, 'o'.$link),
            \range(1, $links),
        ));

        for ($link = 1; $link <= $links; $link++) {
            $checks = $this->checks($sql, 'o'.$link);
            $this->assertNotSame([], $checks, 'Every joined table must be checked: '.$sql);
            foreach ($checks as $hint) {
                $this->assertSame($hinted, $hint, 'The check of o'.$link.' in: '.$sql);
            }
        }

        $this->assertNotContains(true, $this->checks($sql, self::MAIN_ALIAS), 'The main table\'s check stays a semi-join candidate: '.$sql);
    }

    public function testMySQLMixedOuterJoinShapesHintOnlyTheOuterJoinedChecks(): void
    {
        $sql = $this->mySQLFindSql([
            self::joinOnId(JoinType::Left, 'orders1', 'o1'),
            self::join(JoinType::Left, 'orders2', 'o2'),
            Query::leftJoin('orders3', 'o3', [Query::on('o1.customerId', '$id')]),
            Query::join('orders4', 'o4', [Query::on('customerId', '$id')]),
        ]);

        $this->assertSame([true], $this->checks($sql, 'o1'), $sql);
        $this->assertSame([true], $this->checks($sql, 'o2'), $sql);
        $this->assertSame([true], $this->checks($sql, 'o3'), $sql);
        $this->assertSame([false], $this->checks($sql, 'o4'), $sql);
    }

    private static function joinOnId(JoinType $joinType, string $collection, string $alias): Query
    {
        return match ($joinType) {
            JoinType::Left => Query::leftJoin($collection, $alias, [Query::on('customerId', '$id')]),
            JoinType::Right => Query::rightJoin($collection, $alias, [Query::on('customerId', '$id')]),
            JoinType::FullOuter => Query::fullOuterJoin($collection, $alias, [Query::on('customerId', '$id')]),
            default => Query::join($collection, $alias, [Query::on('customerId', '$id')]),
        };
    }

    private static function join(JoinType $joinType, string $collection, string $alias): Query
    {
        return match ($joinType) {
            JoinType::Left => Query::leftJoin($collection, $alias, [Query::on('$id', 'customerId')]),
            JoinType::Right => Query::rightJoin($collection, $alias, [Query::on('$id', 'customerId')]),
            JoinType::FullOuter => Query::fullOuterJoin($collection, $alias, [Query::on('$id', 'customerId')]),
            default => Query::join($collection, $alias, [Query::on('$id', 'customerId')]),
        };
    }

    /**
     * @param  list<Query>  $queries
     */
    private function mySQLFindSql(array $queries): string
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('bindValue')->willReturn(true);
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): PDOStatement {
            $sql = $query;

            return $statement;
        });

        $adapter = new MySQL($pdo);
        $adapter->setDatabase(self::NAMESPACE);
        $adapter->setNamespace(self::NAMESPACE);
        $authorization = new Authorization();
        $authorization->addRole(Role::user(self::READER)->toString());
        $adapter->setAuthorization($authorization);

        $adapter->find(new Document(['$id' => self::COLLECTION, 'documentSecurity' => true]), $queries, limit: 25);

        $this->assertNotSame('', $sql);

        return $sql;
    }

    /**
     * Whether each check of $alias in $sql carries the NO_SEMIJOIN hint.
     *
     * @return list<bool>
     */
    private function checks(string $sql, string $alias): array
    {
        \preg_match_all('/`'.\preg_quote($alias, '/').'`\.`_uid` IN \(SELECT (\/\*\+ NO_SEMIJOIN\(\) \*\/ )?_document /', $sql, $matches);

        return \array_map(static fn (string $hint): bool => $hint !== '', $matches[1]);
    }

    private function database(bool $shared): Database
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->authorization = new Authorization();

        $adapter = new class ($this->pdo, $this->statements) extends SQLite {
            /**
             * @param  ArrayObject<int, array{string, list<mixed>}>  $statements
             */
            public function __construct(object $pdo, private readonly ArrayObject $statements)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            protected function prepareStatement(string $sql, ?Event $event = null): DatabasePDOStatement|PDOStatementProxy|PDOStatement
            {
                $this->statements->append([$sql, []]);

                return parent::prepareStatement($sql, $event);
            }

            #[\Override]
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
            $database->setSharedTables(true)->setTenant(self::TENANT);
        }

        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string('name', size: 64),
                Attribute::integer('score'),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        $documents = [];
        for ($position = 0; $position < self::DOCUMENTS; $position++) {
            $readable = $position % 3 === 0;
            $documents[] = new Document([
                '$id' => $this->id($position),
                'name' => 'name-'.$position,
                'score' => $position,
                '$permissions' => $readable
                    ? [Permission::read(Role::user(self::READER)), Permission::read(Role::any())]
                    : [Permission::read(Role::user(self::OTHER_READER.$position))],
            ]);
        }
        $database->createDocuments(self::COLLECTION, $documents);

        $this->pdo->exec('ANALYZE');
        $this->authorization->addRole(Role::user(self::READER)->toString());

        return $database;
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

    private function assertNoDistinctStep(): void
    {
        $checked = 0;
        foreach ($this->statements as [$sql, $bindings]) {
            if (! \str_contains($sql, 'SELECT') || ! \str_contains($sql, self::TABLE) || ! \str_contains($sql, '_perms')) {
                continue;
            }

            $checked++;
            $plan = $this->plan($sql, $bindings);
            $this->assertNotContains(self::DISTINCT_STEP, $plan, 'The permission subquery built a DISTINCT step: '.$sql."\n  ".\implode("\n  ", $plan));
        }

        $this->assertGreaterThan(0, $checked, 'The operation must have read the collection through its permission check');
    }

    /**
     * @param  list<mixed>  $bindings
     * @return list<string>
     */
    private function plan(string $sql, array $bindings): array
    {
        $statement = $this->pdo->prepare('EXPLAIN QUERY PLAN '.$sql);
        $this->assertInstanceOf(PDOStatement::class, $statement);
        $statement->execute(\array_map(static fn (mixed $value): mixed => \is_bool($value) ? (int) $value : $value, $bindings));

        /** @var list<array<string, int|string>> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);

        return \array_map(static fn (array $row): string => (string) $row[self::DETAIL_COLUMN], $rows);
    }

    private function id(int $position): string
    {
        return 'doc'.\str_pad((string) $position, 3, '0', STR_PAD_LEFT);
    }

    /**
     * @return list<string>
     */
    private function readableIds(): array
    {
        return \array_map($this->id(...), \range(0, self::DOCUMENTS - 1, 3));
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
