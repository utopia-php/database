<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * The query builder puts a backslash in front of every `%`, `_` and `\` in a
 * pattern value, and SQLite (unlike MariaDB, MySQL and Postgres) only reads
 * that backslash as an escape when the predicate declares `ESCAPE '\'`.
 */
final class SQLiteLikeEscapeTest extends TestCase
{
    private const string COLLECTION = 'terms';

    private const array NAMES = ['a_b', 'axb', 'c%d', 'cxxd', 'e\\f', 'e\\\\f'];

    /**
     * @return iterable<string, array{Query, list<string>}>
     */
    public static function patterns(): iterable
    {
        yield 'contains an underscore' => [Query::containsString('name', ['a_b']), ['a_b']];
        yield 'contains a percent sign' => [Query::containsString('name', ['c%d']), ['c%d']];
        yield 'contains a backslash' => [Query::containsString('name', ['e\\f']), ['e\\f']];
        yield 'contains any of several' => [Query::containsAny('name', ['a_b', 'c%d']), ['a_b', 'c%d']];
        yield 'contains all of several' => [Query::containsAll('name', ['c%', '%d']), ['c%d']];
        yield 'does not contain an underscore' => [Query::notContains('name', ['_']), ['axb', 'c%d', 'cxxd', 'e\\f', 'e\\\\f']];
        yield 'contains none of several' => [Query::notContains('name', ['%', '\\']), ['a_b', 'axb', 'cxxd']];
        yield 'starts with an underscore' => [Query::startsWith('name', 'a_'), ['a_b']];
        yield 'starts with a percent sign' => [Query::startsWith('name', 'c%'), ['c%d']];
        yield 'starts with a backslash' => [Query::startsWith('name', 'e\\f'), ['e\\f']];
        yield 'ends with an underscore' => [Query::endsWith('name', '_b'), ['a_b']];
        yield 'ends with a percent sign' => [Query::endsWith('name', '%d'), ['c%d']];
        yield 'ends with backslashes' => [Query::endsWith('name', '\\\\f'), ['e\\\\f']];
        yield 'does not start with an underscore' => [Query::notStartsWith('name', 'a_'), ['axb', 'c%d', 'cxxd', 'e\\f', 'e\\\\f']];
        yield 'does not start with a backslash' => [Query::notStartsWith('name', 'e\\'), ['a_b', 'axb', 'c%d', 'cxxd']];
        yield 'does not end with a percent sign' => [Query::notEndsWith('name', '%d'), ['a_b', 'axb', 'cxxd', 'e\\f', 'e\\\\f']];
    }

    /**
     * @param  list<string>  $expected
     */
    #[DataProvider('patterns')]
    public function testPatternQueriesMatchWildcardCharactersLiterally(Query $query, array $expected): void
    {
        $database = $this->database();

        $names = \array_map(
            fn (Document $document): mixed => $document->getAttribute('name'),
            $database->find(self::COLLECTION, [$query]),
        );
        \sort($names);
        \sort($expected);

        $this->assertSame($expected, $names);
        $this->assertSame(\count($expected), $database->count(self::COLLECTION, [$query]));
    }

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setDatabase('like_escape')
            ->setNamespace('like_escape')
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        ));

        foreach (self::NAMES as $name) {
            $database->createDocument(self::COLLECTION, new Document(['name' => $name]));
        }

        return $database;
    }
}
