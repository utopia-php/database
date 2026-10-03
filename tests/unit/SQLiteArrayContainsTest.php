<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Builder\SQLite as SQLiteBuilder;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

final class SQLiteArrayContainsTest extends TestCase
{
    private const string COLLECTION = 'tags';

    private const string TABLE = 'documents';

    /**
     * @var array<string, array<string, mixed>>
     */
    private const array DOCUMENTS = [
        't1' => ['labels' => ['a', 'b'], 'numbers' => [1, 2], 'scores' => [0.1, 1.5], 'flags' => [true]],
        't2' => ['labels' => ['c'], 'numbers' => [3], 'scores' => [2.5], 'flags' => [false]],
        't3' => ['labels' => [], 'numbers' => [], 'scores' => [], 'flags' => []],
        't4' => ['labels' => ['é', 'q"x', '1'], 'numbers' => [10], 'scores' => [], 'flags' => []],
        't5' => [],
    ];

    /**
     * @return iterable<string, array{Query, list<string>}>
     */
    public static function containsAnyQueries(): iterable
    {
        yield 'strings' => [Query::containsAny('labels', ['a', 'c']), ['t1', 't2']];
        yield 'a non-ASCII string' => [Query::containsAny('labels', ['é']), ['t4']];
        yield 'a string with a double quote' => [Query::containsAny('labels', ['q"x']), ['t4']];
        yield 'a numeric string' => [Query::containsAny('labels', ['1']), ['t4']];
        yield 'integers' => [Query::containsAny('numbers', [2, 3]), ['t1', 't2']];
        yield 'doubles' => [Query::containsAny('scores', [0.1, 2.5]), ['t1', 't2']];
        yield 'true' => [Query::containsAny('flags', [true]), ['t1']];
        yield 'false' => [Query::containsAny('flags', [false]), ['t2']];
        yield 'no element' => [Query::containsAny('labels', ['z']), []];
    }

    /**
     * @return iterable<string, array{Query, list<string>}>
     */
    public static function containsAllQueries(): iterable
    {
        yield 'every string present' => [Query::containsAll('labels', ['a', 'b']), ['t1']];
        yield 'one string missing' => [Query::containsAll('labels', ['a', 'c']), []];
        yield 'non-ASCII and quoted strings' => [Query::containsAll('labels', ['é', 'q"x']), ['t4']];
        yield 'integers' => [Query::containsAll('numbers', [1, 2]), ['t1']];
        yield 'doubles' => [Query::containsAll('scores', [0.1, 1.5]), ['t1']];
        yield 'a boolean' => [Query::containsAll('flags', [false]), ['t2']];
    }

    /**
     * @return iterable<string, array{Query, list<string>}>
     */
    public static function notContainsQueries(): iterable
    {
        yield 'a string' => [Query::notContains('labels', ['a']), ['t2', 't3', 't4']];
        yield 'any of several strings' => [Query::notContains('labels', ['a', 'c']), ['t3', 't4']];
        yield 'a non-ASCII string' => [Query::notContains('labels', ['é']), ['t1', 't2', 't3']];
        yield 'an integer' => [Query::notContains('numbers', [1]), ['t2', 't3', 't4']];
        yield 'a double' => [Query::notContains('scores', [2.5]), ['t1', 't3', 't4']];
        yield 'a boolean' => [Query::notContains('flags', [true]), ['t2', 't3', 't4']];
    }

    /**
     * @return iterable<string, array{Query, list<string>}>
     */
    public static function deprecatedContainsQueries(): iterable
    {
        yield 'a string' => [new Query(Method::Contains, 'labels', ['a']), ['t1']];
        yield 'integers' => [new Query(Method::Contains, 'numbers', [3, 10]), ['t2', 't4']];
        yield 'a boolean' => [new Query(Method::Contains, 'flags', [false]), ['t2']];
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('containsAnyQueries')]
    public function testContainsAnyMatchesAnyElement(Query $query, array $expected): void
    {
        $this->assertMatches($query, $expected);
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('containsAllQueries')]
    public function testContainsAllMatchesEveryElement(Query $query, array $expected): void
    {
        $this->assertMatches($query, $expected);
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('notContainsQueries')]
    public function testNotContainsExcludesMatchingRows(Query $query, array $expected): void
    {
        $this->assertMatches($query, $expected);
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('deprecatedContainsQueries')]
    public function testDeprecatedContainsOnArrays(Query $query, array $expected): void
    {
        $this->assertMatches($query, $expected);
    }

    public function testJsonFiltersCompareElementsByValue(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->exec('CREATE TABLE '.self::TABLE.' (id TEXT, labels TEXT)');
        $insert = $pdo->prepare('INSERT INTO '.self::TABLE.' (id, labels) VALUES (?, ?)');
        foreach (['t1' => '["a","b"]', 't2' => '["c"]', 't3' => '[]', 't4' => '[1,2.5,"1"]', 't5' => null] as $id => $labels) {
            $insert->execute([$id, $labels]);
        }

        $this->assertSame(['t1', 't2'], $this->selectIds($pdo, $this->builder()->filterJsonOverlaps('labels', ['a', 'c'])));
        $this->assertSame(['t4'], $this->selectIds($pdo, $this->builder()->filterJsonOverlaps('labels', [2.5])));
        $this->assertSame(['t1'], $this->selectIds($pdo, $this->builder()->filterJsonContains('labels', ['a', 'b'])));
        $this->assertSame(['t4'], $this->selectIds($pdo, $this->builder()->filterJsonContains('labels', [1, '1'])));
        $this->assertSame([], $this->selectIds($pdo, $this->builder()->filterJsonContains('labels', ['a', 'c'])));
        $this->assertSame(['t2', 't3', 't4'], $this->selectIds($pdo, $this->builder()->filterJsonNotContains('labels', 'a')));
    }

    /**
     * @param  list<string>  $expected
     */
    private function assertMatches(Query $query, array $expected): void
    {
        $database = $this->database();
        $countQuery = clone $query;

        $ids = \array_map(
            fn (Document $document): string => $document->getId(),
            $database->find(self::COLLECTION, [$query]),
        );
        \sort($ids);

        $this->assertSame($expected, $ids);
        $this->assertSame(\count($expected), $database->count(self::COLLECTION, [$countQuery]));
    }

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setDatabase('array_contains')
            ->setNamespace('array_contains')
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string('labels', size: 32, array: true),
                Attribute::integer('numbers', array: true),
                Attribute::double('scores', array: true),
                Attribute::boolean('flags', array: true),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        ));

        foreach (self::DOCUMENTS as $id => $attributes) {
            $database->createDocument(self::COLLECTION, new Document(['$id' => $id, ...$attributes]));
        }

        return $database;
    }

    private function builder(): SQLiteBuilder
    {
        return (new SQLiteBuilder())->from(self::TABLE)->select(['id']);
    }

    /**
     * @return list<string>
     */
    private function selectIds(PDO $pdo, SQLiteBuilder $builder): array
    {
        $statement = $builder->sortAsc('id')->build();
        $prepared = $pdo->prepare($statement->query);
        $prepared->execute($statement->bindings);

        /** @var list<string> */
        return $prepared->fetchAll(PDO::FETCH_COLUMN);
    }
}
