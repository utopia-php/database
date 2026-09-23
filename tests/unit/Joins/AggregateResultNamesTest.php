<?php

namespace Tests\Unit\Joins;

use Closure;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
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
 * Each column of an aggregation's result has one name: an aggregate over a main attribute named like
 * its own alias reads the main table even when a joined collection has that attribute too, an alias
 * names neither another aggregate nor a group, and the input counts behind the bitwise aggregates
 * have names no engine shortens.
 */
final class AggregateResultNamesTest extends TestCase
{
    /**
     * PostgreSQL keeps the first 63 bytes of a longer identifier.
     */
    private const int POSTGRES_IDENTIFIER_BYTES = 63;

    /**
     * Each join of main to a, which both declare score, with the sums of main's and of a's score over
     * the rows it returns and the number of those rows: m1 matches a1, nothing matches m2 or a3.
     *
     * @return iterable<string, array{Method, bool, int, int, int}>
     */
    public static function joins(): iterable
    {
        yield 'inner join' => [Method::Join, false, 10, 1, 1];
        yield 'left join' => [Method::LeftJoin, false, 30, 1, 2];
        yield 'right join' => [Method::RightJoin, false, 10, 4, 2];
        yield 'native full outer join' => [Method::FullOuterJoin, true, 30, 4, 3];
        yield 'emulated full outer join' => [Method::FullOuterJoin, false, 30, 4, 3];
    }

    #[DataProvider('joins')]
    public function testMainAttributeAggregatedUnderItsOwnNameIsReadFromTheMainTable(Method $join, bool $native, int $total, int $joinedTotal, int $rows): void
    {
        $database = $this->database($native);
        $joined = $this->join($join);

        $this->assertSame([['total' => $total]], $this->rows($database->find('main', [$joined, Query::sum('score', 'total')])));
        $this->assertSame([['score' => $total]], $this->rows($database->find('main', [$joined, Query::sum('score', 'score')])));
        $this->assertSame([['total' => $total, 'score' => $rows]], $this->rows($database->find('main', [$joined, Query::sum('score', 'total'), Query::count('*', 'score')])), 'another aggregate named like the attribute');
        $this->assertSame([['score' => $joinedTotal]], $this->rows($database->find('main', [$joined, Query::sum('a.score', 'score')])), 'a joined attribute keeps its alias');
    }

    #[DataProvider('joins')]
    public function testUnaliasedAggregatesKeepTheNamesTheEngineGivesThem(Method $join, bool $native, int $total, int $joinedTotal, int $rows): void
    {
        $database = $this->database($native);

        $this->assertSame(
            [['COUNT(*)' => $rows, 'SUM(`table_main`.`score`)' => $total, 'SUM(`a`.`score`)' => $joinedTotal]],
            $this->rows($database->find('main', [$this->join($join), Query::count(), Query::sum('score'), Query::sum('a.score')])),
        );
    }

    /**
     * @return iterable<string, array{Closure(): list<Query>, string}>
     */
    public static function collidingAliases(): iterable
    {
        $grouped = static fn (string $alias, string $attribute): string => 'Invalid query: Aggregate alias "'.$alias.'" is the name the groupBy attribute "'.$attribute.'" is returned under';

        yield 'a grouped main attribute' => [static fn (): array => [Query::count('*', 'link'), Query::groupBy(['link'])], $grouped('link', 'link')];
        yield 'a grouped main attribute over a join' => [static fn (): array => [Query::join('a', 'link', 'link', '=', 'a'), Query::count('*', 'label'), Query::groupBy(['label'])], $grouped('label', 'label')];
        yield 'a grouped main attribute over a full outer join' => [static fn (): array => [Query::fullOuterJoin('a', 'link', 'link', '=', 'a'), Query::count('*', 'label'), Query::groupBy(['label'])], $grouped('label', 'label')];
        yield 'a grouped joined attribute' => [static fn (): array => [Query::join('a', 'link', 'link', '=', 'a'), Query::sum('score', 'score'), Query::groupBy(['a.score'])], $grouped('score', 'a.score')];
        yield 'a grouped internal attribute' => [static fn (): array => [Query::count('*', '_uid'), Query::groupBy(['$id'])], $grouped('_uid', '$id')];
        yield 'another aggregate' => [static fn (): array => [Query::count('*', 'rows'), Query::sum('score', 'rows')], 'Invalid query: Aggregate alias "rows" is given to more than one aggregate'];
    }

    /**
     * @param  Closure(): list<Query>  $queries
     */
    #[DataProvider('collidingAliases')]
    public function testAliasNamingAnotherColumnOfTheResultIsAnInvalidQuery(Closure $queries, string $message): void
    {
        foreach ([false, true] as $native) {
            try {
                $rows = $this->database($native)->find('main', $queries());
                $this->fail(($native ? 'native' : 'emulated').': the shape was accepted and returned '.\json_encode($this->rows($rows)));
            } catch (QueryException $error) {
                $this->assertSame($message, $error->getMessage());
            }
        }
    }

    public function testAliasesNamingNoOtherColumnKeepEveryValue(): void
    {
        $database = $this->database(false);

        $this->assertSame(
            [['links' => 1, 'total' => 10, 'link' => '1'], ['links' => 1, 'total' => 20, 'link' => '2']],
            $this->rows($database->find('main', [Query::count('*', 'links'), Query::sum('score', 'total'), Query::groupBy(['link']), Query::orderAsc('link')])),
        );
        $this->assertSame(
            [['score' => 10, 'label' => 'first']],
            $this->rows($database->find('main', [Query::join('a', 'link', 'link', '=', 'a'), Query::sum('score', 'score'), Query::groupBy(['a.label'])])),
        );
    }

    /**
     * A bitwise aggregate over no input values is null because of an input count the adapter adds
     * next to it. The count's name has to survive PostgreSQL's 63-byte identifiers: a truncated name
     * nulled whichever aggregate carried the truncated alias.
     */
    public function testBitwiseInputCountsHaveNamesPostgresDoesNotTruncate(): void
    {
        $alias = \str_repeat('b', 60);
        $prefix = \substr($alias, 0, 55);

        [$rows, $statement] = $this->postgresFind([
            Query::bitAnd('flags', $alias),
            Query::count('*', $prefix),
            Query::bitOr('flags', 'any_bits'),
        ]);

        $this->assertStringContainsString('COUNT("flags") AS "$inputs:0"', $statement);
        $this->assertStringContainsString('COUNT("flags") AS "$inputs:1"', $statement);
        $this->assertSame([[$alias => null, $prefix => '0', 'any_bits' => null]], $rows);
    }

    /**
     * Run a find on PostgreSQL over no input values, answered as PostgreSQL answers: a count is zero,
     * every other aggregate is null, and each name is kept to its first 63 bytes.
     *
     * @param  list<Query>  $queries
     * @return array{list<array<string, mixed>>, string}
     */
    private function postgresFind(array $queries): array
    {
        $sql = '';
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('closeCursor')->willReturn(true);
        $statement->method('fetchAll')->willReturnCallback(function () use (&$sql): array {
            \preg_match_all('/([A-Z_]+)\((?:DISTINCT )?[^()]*\) AS "([^"]+)"/', $sql, $matches, PREG_SET_ORDER);
            $this->assertNotSame([], $matches, 'no aggregate in: '.$sql);

            $row = [];
            foreach ($matches as [, $function, $name]) {
                $row[\substr($name, 0, self::POSTGRES_IDENTIFIER_BYTES)] = $function === 'COUNT' ? '0' : null;
            }

            return [$row];
        });

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): PDOStatement {
            $sql = $query;

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $rows = $this->rows($adapter->find(new Document(['$id' => 'collection']), $queries, limit: 25));

        return [$rows, $sql];
    }

    private function join(Method $method): Query
    {
        return new Query($method, 'a', ['link', '=', 'link', 'a']);
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array<string, mixed>>
     */
    private function rows(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): array => $document->getArrayCopy(), $documents));
    }

    private function database(bool $native): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = new Database($native ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo), new Cache(new NoCache()));
        $database
            ->setDatabase('aggregate_result_names')
            ->setNamespace('aggregate_result_names_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        foreach (['main', 'a'] as $collection) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'link', size: 64),
                    Attribute::string(key: 'label', size: 64),
                    Attribute::integer(key: 'score'),
                ],
                permissions: $permissions,
            ));
        }

        foreach ([
            ['main', 'm1', '1', 'one', 10],
            ['main', 'm2', '2', 'two', 20],
            ['a', 'a1', '1', 'first', 1],
            ['a', 'a3', '3', 'third', 3],
        ] as [$collection, $id, $link, $label, $score]) {
            $database->createDocument($collection, new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any())],
                'link' => $link,
                'label' => $label,
                'score' => $score,
            ]));
        }

        return $database;
    }
}
