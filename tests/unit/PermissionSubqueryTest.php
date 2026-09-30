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
 * The permission check of a read is `_uid IN (SELECT _document FROM <collection>_perms …)`. IN already
 * compares against a set, so the subquery needs no DISTINCT: SQLite builds a temporary B-tree for one
 * on every restricted read.
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
            Query::join(self::COLLECTION, '$id', '$id', '=', 'peer'),
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

            protected function prepareStatement(string $sql, ?Event $event = null): DatabasePDOStatement|PDOStatementProxy|PDOStatement
            {
                $this->statements->append([$sql, []]);

                return parent::prepareStatement($sql, $event);
            }

            protected function executeResult(Statement $result, ?Event $event = null): PDOStatement|DatabasePDOStatement|PDOStatementProxy
            {
                $statement = parent::executeResult($result, $event);
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
        $database->createCollection(new Collection(
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
