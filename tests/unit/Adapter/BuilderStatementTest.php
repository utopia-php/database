<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Throwable;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Change;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\Feature\FullOuterJoins;
use Utopia\Query\Builder\SQL as SQLBuilder;
use Utopia\Query\Builder\Statement;
use Utopia\Query\OrderDirection;

/**
 * Data/Builder/statements.json holds the statements 8.0 compiled before builder() lost its collection
 * argument: the reads builder($collection) handed out, and those the adapter runs itself. Reading a
 * collection through builder()->from() and every statement the adapter builds must compile to the same SQL
 * and bindings.
 */
final class BuilderStatementTest extends TestCase
{
    private const string TENANT_CONDITION = '/((?:[`"][^`"]+[`"]\.)*[`"][^`"]+[`"])\._tenant IN \(\?\)/';

    /** @var list<array{string, list<array{int|string, mixed}>}> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<SQL>, bool}>
     */
    public static function adapters(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class, 'Postgres' => Postgres::class, 'SQLite' => SQLite::class] as $name => $class) {
            yield "{$name}/plain" => [$class, false];
            yield "{$name}/shared" => [$class, true];
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('adapters')]
    public function testReadingACollectionCompilesAsBuilderOverTheCollectionDid(string $class, bool $shared): void
    {
        $this->assertSame($this->expected($class, $shared)['reads'], $this->reads($class, $shared));
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('adapters')]
    public function testTheStatementsTheAdapterBuildsItselfAreUnchanged(string $class, bool $shared): void
    {
        $this->assertSame($this->expected($class, $shared)['internal'], $this->internal($class, $shared));
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('adapters')]
    public function testEachTableOfASharedTablesReadIsKeptToTheTenantOnce(string $class, bool $shared): void
    {
        if (! $shared) {
            $this->assertStringNotContainsString(Storage::TENANT, \implode(' ', \array_column($this->reads($class, false), 0)));

            return;
        }

        $reads = $this->reads($class, true);
        $internal = $this->internal($class, true);
        $adapter = $this->adapter($class, true);
        $authors = $this->quoted($adapter, $this->raw($adapter, 'authors'));
        $aliased = $adapter->builder()->from('authors', 'author')->select(['name'])->build()->query;

        $this->assertSame([$authors], $this->tenantConditions($reads['select'][0]));
        $this->assertSame([$authors], $this->tenantConditions($reads['aggregate'][0]));
        $this->assertSame([$authors], $this->tenantConditions($reads['update'][0]));
        $this->assertSame([$authors], $this->tenantConditions($reads['delete'][0]));
        $this->assertSame([$authors], $this->tenantConditions($reads['reused'][0]));
        $this->assertSame([$this->quoted($adapter, $this->raw($adapter, Storage::permissionsTable('authors')))], $this->tenantConditions($reads['permissions'][0]));
        $this->assertSame([$this->quoted($adapter, $this->raw($adapter, Database::METADATA))], $this->tenantConditions($reads['metadata'][0]));
        $this->assertEqualsCanonicalizing([$authors, $this->quoted($adapter, 'book'), $this->quoted($adapter, 'review')], $this->tenantConditions($reads['join'][0]));
        $this->assertSame([$this->quoted($adapter, 'author')], $this->tenantConditions($aliased));
        $this->assertSame([$this->quoted($adapter, 'table_main')], $this->tenantConditions($internal['find']['statements'][0][0]));
        $this->assertSame([], $this->tenantConditions($reads['insert'][0]));
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('adapters')]
    public function testARightJoinPairsWithTheAliasFromNamesTheCollectionUnder(string $class, bool $shared): void
    {
        $adapter = $this->adapter($class, $shared);

        $statement = $adapter->builder()
            ->from('authors', 'author')
            ->rightJoin('reviews', 'author._uid', 'review.authorId', '=', 'review')
            ->select(['author.name', 'review.stars'])
            ->build();

        $this->assertStringContainsString(
            'FROM '.$this->quoted($adapter, $this->raw($adapter, 'authors')).' AS '.$this->quoted($adapter, 'author'),
            $statement->query,
        );
        if (! $shared) {
            $this->assertSame([], $statement->bindings);

            return;
        }
        $this->assertContains($this->quoted($adapter, 'author'), $this->tenantConditions($statement->query));
        $this->assertNotContains($this->quoted($adapter, $this->raw($adapter, 'authors')), $this->tenantConditions($statement->query));
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('adapters')]
    public function testABuilderThatNamesNoCollectionIsKeptToNoTenant(string $class, bool $shared): void
    {
        $adapter = $this->adapter($class, $shared);
        $table = $this->raw($adapter, 'authors');

        $this->assertSame('SELECT 1', $adapter->builder()->fromNone()->selectRaw('1')->build()->query);
        $this->assertSame(
            'SELECT '.$this->quoted($adapter, 'name').' FROM '.$this->quoted($adapter, $table),
            $adapter->builder()->fromTable($table)->select(['name'])->build()->query,
        );
        $this->assertSame(
            'INSERT INTO '.$this->quoted($adapter, $table).' ('.$this->quoted($adapter, 'name').') VALUES (?)',
            $adapter->builder()->into($table)->set(['name' => 'new'])->insert()->query,
        );
    }

    /**
     * @param class-string<SQL> $class
     * @return array<mixed>
     */
    private function expected(string $class, bool $shared): array
    {
        $fixture = \json_decode((string) \file_get_contents(__DIR__.'/Data/Builder/statements.json'), true, flags: JSON_THROW_ON_ERROR);
        $this->assertIsArray($fixture);
        $key = (new \ReflectionClass($class))->getShortName().($shared ? '/shared' : '/plain');
        $this->assertArrayHasKey($key, $fixture);
        $this->assertIsArray($fixture[$key]);

        return $fixture[$key];
    }

    /**
     * @return list<string>
     */
    private function tenantConditions(string $query): array
    {
        \preg_match_all(self::TENANT_CONDITION, $query, $matches);

        return $matches[1];
    }

    private function quoted(SQL $adapter, string $identifier): string
    {
        $quote = (new ReflectionMethod($adapter, 'getIdentifierQuote'))->invoke($adapter);
        $this->assertIsString($quote);

        return \implode('.', \array_map(static fn (string $part): string => $quote.$part.$quote, \explode('.', $identifier)));
    }

    private function raw(SQL $adapter, string $collection): string
    {
        $table = (new ReflectionMethod($adapter, 'getTableRaw'))->invoke($adapter, $collection);
        $this->assertIsString($table);

        return $table;
    }

    /**
     * @param class-string<SQL> $class
     * @return array<string, array{string, mixed}>
     */
    private function reads(string $class, bool $shared): array
    {
        $adapter = $this->adapter($class, $shared);
        $authors = $this->raw($adapter, 'authors');
        $result = [];
        $from = static fn (string $collection): SQLBuilder => $adapter->builder()->from($collection);

        $result['select'] = $this->compile(static fn () => $from('authors')
            ->select(['name', '$id'])
            ->filter([Query::equal('name', ['x']), Query::greaterThan('$sequence', 3)])
            ->sortDesc('$createdAt')
            ->limit(5)
            ->offset(2)
            ->build());
        $result['join'] = $this->compile(static fn () => $from('authors')
            ->join('books', $authors.'._uid', 'book.authorId', '=', 'book')
            ->leftJoin('reviews', $authors.'._uid', 'review.authorId', '=', 'review')
            ->select([$authors.'.name', 'book.pages', 'review.stars'])
            ->filter([Query::greaterThan('book.pages', 10)])
            ->sortAsc($authors.'.name')
            ->limit(3)
            ->build());
        $result['metadata'] = $this->compile(static fn () => $from(Database::METADATA)
            ->select(['name'])
            ->filter([Query::equal('$id', ['authors'])])
            ->build());
        $result['permissions'] = $this->compile(static fn () => $from(Storage::permissionsTable('authors'))
            ->select([Storage::PERMISSIONS_TYPE, Storage::PERMISSIONS_PERMISSION])
            ->filter([Query::equal(Storage::PERMISSIONS_DOCUMENT, ['a1'])])
            ->build());
        $result['metadataPermissions'] = $this->compile(static fn () => $from(Storage::permissionsTable(Database::METADATA))
            ->select([Storage::PERMISSIONS_PERMISSION])
            ->build());
        $result['rightJoin'] = $this->compile(static fn () => $from('authors')
            ->rightJoin('reviews', $authors.'._uid', 'review.authorId', '=', 'review')
            ->select([$authors.'.name', 'review.stars'])
            ->build());
        if ($from('authors') instanceof FullOuterJoins) {
            $result['fullOuterJoin'] = $this->compile(static fn () => $from('authors')
                ->fullOuterJoin('reviews', $authors.'._uid', 'review.authorId', '=', 'review')
                ->select([$authors.'.name', 'review.stars'])
                ->build());
        }
        $result['aggregate'] = $this->compile(static fn () => $from('authors')
            ->count('*', 'total')
            ->groupBy(['name'])
            ->build());
        $result['update'] = $this->compile(static fn () => $from('authors')
            ->set(['name' => 'renamed'])
            ->filter([Query::equal('$id', ['a1'])])
            ->update());
        $result['delete'] = $this->compile(static fn () => $from('authors')
            ->filter([Query::equal('$id', ['a1'])])
            ->delete());
        $result['insert'] = $this->compile(static fn () => $adapter->builder()
            ->into($authors)
            ->set(['_uid' => 'a9', 'name' => 'new'])
            ->insert());
        $result['reused'] = $this->compile(static function () use ($from) {
            $builder = $from('authors')->select(['name']);
            $builder->build();

            return $builder->build();
        });

        return $result;
    }

    /**
     * @param Closure(): Statement $build
     * @return array{string, mixed}
     */
    private function compile(Closure $build): array
    {
        try {
            $statement = $build();

            return [$statement->query, $statement->bindings];
        } catch (Throwable $error) {
            return [$error::class, $error->getMessage()];
        }
    }

    /**
     * @param class-string<SQL> $class
     * @return array<string, array{outcome: string, statements: list<array{string, list<array{int|string, mixed}>}>}>
     */
    private function internal(string $class, bool $shared): array
    {
        $result = [];
        $collection = new Document(['$id' => 'authors', 'attributes' => [], 'indexes' => []]);
        $capture = function (string $name, Closure $run) use (&$result, $class, $shared): void {
            $this->statements = [];
            $adapter = $this->adapter($class, $shared);
            try {
                $run($adapter);
                $outcome = 'ok';
            } catch (Throwable $error) {
                $outcome = $error::class.': '.$error->getMessage();
            }
            $result[$name] = ['outcome' => $outcome, 'statements' => $this->statements];
        };
        $document = static fn (string $id, array $permissions): Document => new Document([
            '$id' => $id,
            '$permissions' => $permissions,
            '$createdAt' => '2026-09-30 00:00:00.000',
            '$updatedAt' => '2026-09-30 00:00:00.000',
            '$tenant' => $shared ? 7 : null,
            'name' => 'one',
        ]);
        $fullOuterJoin = Query::fullOuterJoin('books', 'book', [Query::on('$id', 'authorId')]);

        $capture('ping', static fn (SQL $adapter) => $adapter->ping());
        $capture('id', static fn (SQL $adapter) => $adapter->id());
        $capture('exists', static fn (SQL $adapter) => $adapter->exists('database'));
        $capture('collectionExists', static fn (SQL $adapter) => $adapter->collectionExists('database', 'authors'));
        $capture('size', static fn (SQL $adapter) => $adapter->getSizeOfCollection('authors'));
        $capture('sizeOnDisk', static fn (SQL $adapter) => $adapter->getSizeOfCollectionOnDisk('authors'));
        $capture('create', static fn (SQL $adapter) => $adapter->createDocument($collection, $document('a1', ['read("any")', 'update("user:1")'])));
        $capture('update', static fn (SQL $adapter) => $adapter->updateDocument($collection, 'a1', $document('a1', ['read("user:2")']), false));
        $capture('rename', static fn (SQL $adapter) => $adapter->updateDocument($collection, 'a1', $document('a2', ['read("user:2")']), false));
        $capture('delete', static fn (SQL $adapter) => $adapter->deleteDocument($collection, 'a1'));
        $capture('upsert', static fn (SQL $adapter) => $adapter->upsertDocuments($collection, [new Change(new Document(['$id' => 'a1', '$permissions' => ['read("any")'], '$tenant' => $shared ? 7 : null]), $document('a1', ['read("user:3")']))]));
        $capture('find', static fn (SQL $adapter) => $adapter->find($collection, [Query::equal('name', ['one'])], limit: 10));
        $capture('findAuthorized', static function (SQL $adapter) use ($collection) {
            $authorization = new Authorization();
            $authorization->addRole('any');
            $adapter->setAuthorization($authorization);

            return $adapter->find($collection, [Query::join('books', 'book', [Query::on('$id', 'authorId')])], limit: 10);
        });
        $capture('fullOuterJoin', static fn (SQL $adapter) => $adapter->find($collection, [$fullOuterJoin], limit: 10));
        $capture('fullOuterJoinRandom', static fn (SQL $adapter) => $adapter->find($collection, [$fullOuterJoin], limit: 10, orderAttributes: [''], orderTypes: [OrderDirection::Random]));
        $capture('fullOuterJoinCount', static fn (SQL $adapter) => $adapter->count($collection, [$fullOuterJoin], 5));
        $capture('count', static fn (SQL $adapter) => $adapter->count($collection, [Query::equal('name', ['one'])], 5));
        $capture('sum', static fn (SQL $adapter) => $adapter->sum($collection, 'pages', [Query::equal('name', ['one'])], 5));

        return $result;
    }

    /**
     * @param class-string<SQL> $class
     */
    private function adapter(string $class, bool $shared): SQL
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $index = \count($this->statements);
            $this->statements[] = [$query, []];
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('fetch')->willReturn(false);
            $statement->method('fetchColumn')->willReturn(\str_contains($query, 'dbstat') ? '0' : false);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('closeCursor')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $position, mixed $value) use ($index): bool {
                $this->statements[$index][1][] = [$position, $value];

                return true;
            });
            $statement->method('bindParam')->willReturnCallback(function (int|string $position, mixed &$value) use ($index): bool {
                $this->statements[$index][1][] = [$position, $value];

                return true;
            });

            return $statement;
        });
        $pdo->method('lastInsertId')->willReturn('12');
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('commit')->willReturn(true);

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        if ($shared) {
            $adapter->setSharedTables(true);
            $adapter->setTenant(7);
        }
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->addWriteHook(new Permissions());

        return $adapter;
    }
}
