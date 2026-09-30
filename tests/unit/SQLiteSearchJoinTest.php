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
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

final class SQLiteSearchJoinTest extends TestCase
{
    private const string AUTHORS = 'authors';

    private const string POSTS = 'posts';

    private const string ALIAS = 'post';

    private const array BODIES = [
        'brown' => 'the quick brown fox',
        'lazy' => 'a lazy dog sleeps',
        'foxes' => 'foxes run at night',
        'phrase' => 'quick fox',
    ];

    /**
     * @return iterable<string, array{bool, string}>
     */
    public static function searches(): iterable
    {
        foreach (['plain tables' => false, 'shared tables' => true] as $tables => $shared) {
            yield $tables.', two words' => [$shared, 'quick fox'];
            yield $tables.', one word' => [$shared, 'lazy'];
            yield $tables.', exact phrase' => [$shared, '"quick fox"'];
        }
    }

    #[DataProvider('searches')]
    public function testJoinedSearchMatchesTheJoinedCollectionsSearch(bool $shared, string $term): void
    {
        $database = $this->database($shared);

        $expected = $this->authorIds($database->find(self::POSTS, [Query::search('body', $term)]));
        $this->assertNotSame([], $expected);

        $found = $this->ids($database->find(self::AUTHORS, [
            $this->join(),
            Query::search(self::ALIAS.'.body', $term),
        ]));

        $this->assertSame($expected, $found);
        $this->assertSame(\count($expected), $database->count(self::AUTHORS, [
            $this->join(),
            Query::search(self::ALIAS.'.body', $term),
        ]));
    }

    #[DataProvider('searches')]
    public function testJoinedNotSearchIsTheComplement(bool $shared, string $term): void
    {
        $database = $this->database($shared);

        $matching = $this->authorIds($database->find(self::POSTS, [Query::search('body', $term)]));
        $expected = \array_values(\array_diff(\array_keys(self::BODIES), $matching));
        \sort($expected);

        $found = $this->ids($database->find(self::AUTHORS, [
            $this->join(),
            Query::notSearch(self::ALIAS.'.body', $term),
        ]));

        $this->assertSame($expected, $found);
        $this->assertSame(\count($expected), $database->count(self::AUTHORS, [
            $this->join(),
            Query::notSearch(self::ALIAS.'.body', $term),
        ]));
    }

    private function join(): Query
    {
        return Query::join(self::POSTS, '$id', 'authorId', '=', self::ALIAS);
    }

    /**
     * @param  array<Document>  $posts
     * @return list<string>
     */
    private function authorIds(array $posts): array
    {
        $ids = [];
        foreach ($posts as $post) {
            $authorId = $post->getAttribute('authorId');
            $this->assertIsString($authorId);
            $ids[] = $authorId;
        }
        \sort($ids);

        return $ids;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $documents);
        \sort($ids);

        return $ids;
    }

    private function database(bool $shared): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('search_join')
            ->setNamespace('search_join');

        if ($shared) {
            $database->setSharedTables(true)->setTenant(1);
        }

        $database->create();

        $this->seed($database, static fn (string $author): string => self::BODIES[$author]);

        if ($shared) {
            $database->withTenant(2, fn () => $this->seed($database, static fn (string $author): string => 'unrelated words'));
        }

        return $database;
    }

    /**
     * @param  callable(string): string  $body
     */
    private function seed(Database $database, callable $body): void
    {
        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ];

        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: self::POSTS,
            attributes: [
                Attribute::string(key: 'authorId', size: 64),
                Attribute::string(key: 'body', size: 256),
            ],
            indexes: [new Index(key: 'body_search', type: IndexType::Fulltext, attributes: ['body'])],
            permissions: $permissions,
            documentSecurity: false,
        ));

        foreach (\array_keys(self::BODIES) as $author) {
            $database->createDocument(self::AUTHORS, new Document([Document::ID => $author, 'name' => $author]));
            $database->createDocument(self::POSTS, new Document([Document::ID => 'post_'.$author, 'authorId' => $author, 'body' => $body($author)]));
        }
    }
}
