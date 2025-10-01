<?php

global $cli;

use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Swoole\Runtime;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\CLI\Console;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Validator\Boolean;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

// Global pools for faster document generation
$namesPool = ['Alice', 'Bob', 'Carol', 'Dave', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy', 'Mallory', 'Niaj', 'Olivia', 'Peggy', 'Quentin', 'Rupert', 'Sybil', 'Trent', 'Uma', 'Victor'];
$genresPool = ['fashion','food','travel','music','lifestyle','fitness','diy','sports','finance'];
$tagsPool   = ['short','quick','easy','medium','hard'];

/**
 * @Example
 * docker compose exec tests bin/load --adapter=mariadb --limit=1000
 */
$cli
    ->task('relationships')
    ->desc('Load database with mock relationships for testing')
    ->param('adapter', '', new Text(0), 'Database adapter')
    ->param('limit', 0, new Integer(true), 'Total number of records to add to database')
    ->param('name', 'myapp_' . uniqid(), new Text(0), 'Name of created database.', true)
    ->param('sharedTables', false, new Boolean(true), 'Whether to use shared tables', true)
    ->action(function (string $adapter, int $limit, string $name, bool $sharedTables) {
        $start = null;
        $namespace = '_ns';
        $cache = new Cache(new NoCache());

        Console::info("Filling {$adapter} with {$limit} records: {$name}");

        //Runtime::enableCoroutine();

        $dbAdapters = [
            'mariadb' => [
                'host' => 'mariadb',
                'port' => 3306,
                'user' => 'root',
                'pass' => 'password',
                'dsn' => static fn (string $host, int $port) => "mysql:host={$host};port={$port};charset=utf8mb4",
                'adapter' => MariaDB::class,
                'attrs' => MariaDB::getPDOAttributes(),
            ],
            'mysql' => [
                'host' => 'mysql',
                'port' => 3307,
                'user' => 'root',
                'pass' => 'password',
                'dsn' => static fn (string $host, int $port) => "mysql:host={$host};port={$port};charset=utf8mb4",
                'adapter' => MySQL::class,
                'attrs' => MySQL::getPDOAttributes(),
            ],
            'postgres' => [
                'host' => 'postgres',
                'port' => 5432,
                'user' => 'postgres',
                'pass' => 'password',
                'dsn' => static fn (string $host, int $port) => "pgsql:host={$host};port={$port}",
                'adapter' => Postgres::class,
                'attrs' => Postgres::getPDOAttributes(),
            ],
        ];

        if (!isset($dbAdapters[$adapter])) {
            Console::error("Adapter '{$adapter}' not supported");
            return;
        }

        $cfg = $dbAdapters[$adapter];

        //Co\run(function () use (&$start, $limit, $name, $sharedTables, $namespace, $cache, $cfg) {
        $pdo = new PDO(
            ($cfg['dsn'])($cfg['host'], $cfg['port']),
            $cfg['user'],
            $cfg['pass'],
            $cfg['attrs']
        );

        $database = (new Database(new ($cfg['adapter'])($pdo), $cache))
            ->setDatabase($name)
            ->setNamespace($namespace)
            ->setSharedTables($sharedTables);

        createRelationshipSchema($database);

        $pdo = null;

        $pool = new PDOPool(
            (new PDOConfig())
                ->withHost($cfg['host'])
                ->withPort($cfg['port'])
                ->withDbName($name)
                ->withCharset('utf8mb4')
                ->withUsername($cfg['user'])
                ->withPassword($cfg['pass']),
            128
        );

        $start = \microtime(true);

        for ($i = 0; $i < $limit / 1000; $i++) {
            \go(function () use ($cfg, $pool, $name, $namespace, $sharedTables, $cache) {
                try {
                    $pdo = $pool->get();

                    $database = (new Database(new ($cfg['adapter'])($pdo), $cache))
                        ->setDatabase($name)
                        ->setNamespace($namespace)
                        ->setSharedTables($sharedTables);

                    createRelationshipDocuments($database);
                    $pool->put($pdo);
                } catch (\Throwable $error) {
                    Console::error('Coroutine error: ' . $error->getMessage());
                }
            });
        }

        benchmarkSingleQueries($database);
        benchmarkBatchQueries($database);
        benchmarkPagination($database);

        $time = microtime(true) - $start;
        Console::success("Completed in {$time} seconds");
    });

function createRelationshipSchema(Database $database): void
{
    if ($database->exists($database->getDatabase())) {
        $database->delete($database->getDatabase());
    }
    $database->create();

    Authorization::setRole(Role::any()->toString());

    $database->createCollection('authors', permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ]);
    $database->createAttribute('authors', 'name', Database::VAR_STRING, 256, true);
    $database->createAttribute('authors', 'created', Database::VAR_DATETIME, 0, true, filters: ['datetime']);
    $database->createAttribute('authors', 'bio', Database::VAR_STRING, 5000, true);
    $database->createAttribute('authors', 'avatar', Database::VAR_STRING, 256, true);
    $database->createAttribute('authors', 'website', Database::VAR_STRING, 256, true);

    $database->createCollection('articles', permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ]);
    $database->createAttribute('articles', 'title', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'text', Database::VAR_STRING, 5000, true);
    $database->createAttribute('articles', 'genre', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'views', Database::VAR_INTEGER, 0, true);
    $database->createAttribute('articles', 'tags', Database::VAR_STRING, 0, true, array: true);

    $database->createCollection('users', permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ]);
    $database->createAttribute('users', 'username', Database::VAR_STRING, 256, true);
    $database->createAttribute('users', 'email', Database::VAR_STRING, 256, true);
    $database->createAttribute('users', 'password', Database::VAR_STRING, 256, true);

    $database->createCollection('comments', permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ]);
    $database->createAttribute('comments', 'content', Database::VAR_STRING, 256, true);
    $database->createAttribute('comments', 'likes', Database::VAR_INTEGER, 8, true, signed: false);

    $database->createRelationship('authors', 'articles', Database::RELATION_MANY_TO_MANY, true, onDelete: Database::RELATION_MUTATE_SET_NULL);
    $database->createRelationship('articles', 'comments', Database::RELATION_ONE_TO_MANY, true, twoWayKey: 'article', onDelete: Database::RELATION_MUTATE_CASCADE);
    $database->createRelationship('users', 'comments', Database::RELATION_ONE_TO_MANY, true, twoWayKey: 'user', onDelete: Database::RELATION_MUTATE_CASCADE);
}

function createRelationshipDocuments(Database $database): void
{
    global $namesPool, $genresPool, $tagsPool;

    $documents = [];
    $start = \microtime(true);

    // Prepare pools for nested data
    $numAuthors = 10;
    $numUsers = 10;
    $numArticlesPerAuthor = 10;
    $numCommentsPerArticle = 10;

    // Generate users
    $users = [];
    for ($u = 0; $u < $numUsers; $u++) {
        $users[] = new Document([
            'username' => $namesPool[\array_rand($namesPool)],
            'email'    => \strtolower($namesPool[\array_rand($namesPool)]) . '@example.com',
            'password' => \bin2hex(\random_bytes(8)),
        ]);
    }

    // Generate authors with nested articles and comments
    for ($a = 0; $a < $numAuthors; $a++) {
        $author = new Document([
            'name'      => $namesPool[array_rand($namesPool)],
            'created'   => DateTime::now(),
            'bio'       => \substr(\bin2hex(\random_bytes(32)), 0, 100),
            'avatar'    => 'https://example.com/avatar/' . $a,
            'website'   => 'https://example.com/user/' . $a,
        ]);

        // Nested articles
        $authorArticles = [];
        for ($i = 0; $i < $numArticlesPerAuthor; $i++) {
            $article = new Document([
                'title'   => 'Article ' . ($i + 1) . ' by ' . $author->getAttribute('name'),
                'text'    => \substr(\bin2hex(\random_bytes(64)), 0, \mt_rand(100, 200)),
                'genre'   => $genresPool[array_rand($genresPool)],
                'views'   => \mt_rand(0, 1000),
                'tags'    => \array_slice($tagsPool, 0, \mt_rand(1, \count($tagsPool))),
            ]);

            // Nested comments
            $comments = [];
            for ($c = 0; $c < $numCommentsPerArticle; $c++) {
                $comment = new Document([
                    'content' => 'Comment ' . ($c + 1),
                    'likes'   => \mt_rand(0, 10000),
                    'user'    => $users[\array_rand($users)],
                ]);
                $comments[] = $comment;
            }

            $article->setAttribute('comments', $comments);
            $authorArticles[] = $article;
        }

        $author->setAttribute('articles', $authorArticles);
        $documents[] = $author;
    }

    $time = microtime(true) - $start;
    Console::info("Prepared nested documents in {$time} seconds");

    // Insert authors (with nested articles, comments, and users)
    $start = \microtime(true);
    $database->createDocuments('authors', $documents);
    $time = \microtime(true) - $start;
    Console::success("Inserted nested documents in {$time} seconds");
}

/**
 * Benchmark querying a single document from each collection.
 */
function benchmarkSingleQueries(Database $database): void
{
    $collections = ['authors', 'articles', 'users', 'comments'];
    foreach ($collections as $collection) {
        // Fetch one document ID to use
        $docs = $database->find($collection, [Query::limit(1)]);
        if (empty($docs)) {
            Console::warning("No documents in {$collection} for single query benchmark.");
            continue;
        }
        $id = $docs[0]->getId();

        $start = microtime(true);
        $database->getDocument($collection, $id);
        $time = microtime(true) - $start;

        Console::info("Single query ({$collection}) took {$time} seconds");
    }
}

/**
 * Benchmark querying 20 documents from each collection.
 */
function benchmarkBatchQueries(Database $database): void
{
    $collections = ['authors', 'articles', 'users', 'comments'];
    foreach ($collections as $collection) {
        $start = microtime(true);
        $database->find($collection, [Query::limit(20)]);
        $time = microtime(true) - $start;

        Console::info("Batch query 20 ({$collection}) took {$time} seconds");
    }
}

/**
 * Benchmark pagination through entire collection in chunks of 100.
 */
function benchmarkPagination(Database $database): void
{
    $collections = ['authors', 'articles', 'users', 'comments'];
    foreach ($collections as $collection) {
        $offset = 0;
        $limit = 100;
        $start = microtime(true);
        do {
            $docs = $database->find($collection, [
                Query::limit($limit),
                Query::offset($offset),
            ]);
            $count = count($docs);
            $offset += $limit;
        } while ($count === $limit);
        $time = microtime(true) - $start;

        Console::info("Pagination ({$collection}) over all documents took {$time} seconds");
    }
}
