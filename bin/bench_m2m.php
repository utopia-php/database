<?php

/**
 * Standalone M2M Relationship Query Benchmark
 *
 * Seeds data and benchmarks M2M relationship query patterns.
 * Run with: php bin/bench_m2m.php
 *
 * Requires MariaDB on localhost:8703 (docker compose up -d mariadb)
 */

ini_set('memory_limit', '2G');

require_once __DIR__ . '/../vendor/autoload.php';

use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

// --- Config ---
$dbHost = getenv('MARIADB_HOST') ?: 'mariadb';
$dbPort = getenv('MARIADB_PORT') ?: '3306';
$numAuthors          = 500;
$numArticlesPerAuthor = 50; // total articles = 500 * 50 = 25000
$warmup   = 3;
$runs     = 20;
$dbName   = 'bench_m2m';
$namespace = '_bench';

// --- Setup ---
echo "=== M2M Relationship Query Benchmark ===\n\n";

$pdo = new PDO(
    "mysql:host={$dbHost};port={$dbPort};charset=utf8mb4",
    'root',
    'password',
    SQL::getPDOAttributes()
);

$cache = new Cache(new NoCache());
$authorization = new Authorization();
$authorization->addRole(Role::any()->toString());
$authorization->setDefaultStatus(true);

$database = new Database(new MariaDB($pdo), $cache);
$database
    ->setAuthorization($authorization)
    ->setDatabase($dbName)
    ->setNamespace($namespace);

// Fresh database
if ($database->exists($dbName)) {
    $database->delete($dbName);
}
$database->create();

// Schema
$database->createCollection('authors', permissions: [
    Permission::create(Role::any()),
    Permission::read(Role::any()),
]);
$database->createAttribute('authors', 'name', Database::VAR_STRING, 256, true);

$database->createCollection('articles', permissions: [
    Permission::create(Role::any()),
    Permission::read(Role::any()),
]);
$database->createAttribute('articles', 'title', Database::VAR_STRING, 256, true);
$database->createAttribute('articles', 'genre', Database::VAR_STRING, 256, true);
$database->createIndex('articles', 'idx_genre', Database::INDEX_KEY, ['genre']);

$database->createRelationship(
    'authors',
    'articles',
    Database::RELATION_MANY_TO_MANY,
    true,
    onDelete: Database::RELATION_MUTATE_SET_NULL
);

// --- Seed Data ---
echo "Seeding: {$numAuthors} authors x {$numArticlesPerAuthor} articles = "
   . ($numAuthors * $numArticlesPerAuthor) . " total articles\n";

$genres = ['fashion', 'food', 'travel', 'music', 'lifestyle', 'fitness', 'diy', 'sports', 'finance'];
$names  = ['Alice', 'Bob', 'Carol', 'Dave', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy'];

$allArticleIds = [];

for ($a = 0; $a < $numAuthors; $a++) {
    $articles = [];
    for ($i = 0; $i < $numArticlesPerAuthor; $i++) {
        $articleId = 'art_' . str_pad($a, 3, '0', STR_PAD_LEFT) . '_' . str_pad($i, 3, '0', STR_PAD_LEFT);
        $articles[] = new Document([
            '$id' => $articleId,
            'title' => 'Article ' . ($i + 1) . ' by Author ' . $a,
            'genre' => $genres[array_rand($genres)],
        ]);
        $allArticleIds[] = $articleId;
    }

    $database->createDocument('authors', new Document([
        '$id' => 'author_' . str_pad($a, 3, '0', STR_PAD_LEFT),
        'name' => $names[$a % count($names)] . ' ' . $a,
        'articles' => $articles,
        '$permissions' => ['read("any")'],
    ]));

    if (($a + 1) % 10 === 0) {
        echo "  Created {$a}/{$numAuthors} authors...\n";
    }
}

echo "  Seeding complete: " . count($allArticleIds) . " articles, {$numAuthors} authors\n\n";

// --- Benchmark ---
function bench(string $label, callable $fn, int $warmup, int $runs): void
{
    for ($i = 0; $i < $warmup; $i++) {
        $fn();
    }

    $times = [];
    $resultCount = 0;
    for ($i = 0; $i < $runs; $i++) {
        $start = hrtime(true);
        $result = $fn();
        $times[] = (hrtime(true) - $start) / 1e6;
        $resultCount = is_countable($result) ? count($result) : 0;
    }

    sort($times);
    $n = count($times);
    $median = $times[(int)($n / 2)];
    $mean   = array_sum($times) / $n;
    $min    = $times[0];
    $p95    = $times[(int)($n * 0.95)];

    printf(
        "  %-45s  median: %7.1f ms  mean: %7.1f ms  min: %7.1f ms  p95: %7.1f ms  (%d docs)\n",
        $label,
        $median,
        $mean,
        $min,
        $p95,
        $resultCount
    );
}

echo "Benchmarking ({$warmup} warmup + {$runs} measured runs each):\n\n";

// =======================================
// GENERAL QUERIES (no relationship traversal)
// =======================================
echo "--- General Queries (plain find, no relationship traversal) ---\n\n";

// getDocument
bench(
    "getDocument('articles', id)",
    fn () => $database->getDocument('articles', $allArticleIds[0]),
    $warmup,
    $runs
);

// skipRelationships find (raw, no population)
bench(
    "find('articles') skip-rels limit(100)",
    fn () => $database->skipRelationships(fn () => $database->find('articles', [Query::limit(100)])),
    $warmup,
    $runs
);
bench(
    "find('articles') skip-rels limit(1000)",
    fn () => $database->skipRelationships(fn () => $database->find('articles', [Query::limit(1000)])),
    $warmup,
    $runs
);
bench(
    "find('articles') skip-rels limit(5000)",
    fn () => $database->skipRelationships(fn () => $database->find('articles', [Query::limit(5000)])),
    $warmup,
    $runs
);

// find WITH relationship population (authors populated on each article)
bench(
    "find('articles') + rels limit(100)",
    fn () => $database->find('articles', [Query::limit(100)]),
    $warmup,
    $runs
);
bench(
    "find('articles') + rels limit(500)",
    fn () => $database->find('articles', [Query::limit(500)]),
    $warmup,
    $runs
);

// find authors WITH relationship population (articles populated on each author)
bench(
    "find('authors') + rels limit(25)",
    fn () => $database->find('authors', [Query::limit(25)]),
    $warmup,
    $runs
);
bench(
    "find('authors') + rels limit(100)",
    fn () => $database->find('authors', [Query::limit(100)]),
    $warmup,
    $runs
);

// Filter queries (no relationship traversal)
bench(
    "find('articles') genre='fashion' skip-rels",
    fn () => $database->skipRelationships(fn () => $database->find('articles', [
        Query::equal('genre', ['fashion']),
        Query::limit(5000),
    ])),
    $warmup,
    $runs
);

// Pagination
bench(
    "paginate('articles') skip-rels 100/page",
    function () use ($database) {
        $cursor = null;
        $total = 0;
        do {
            $queries = [Query::limit(100)];
            if ($cursor !== null) {
                $queries[] = Query::cursorAfter($cursor);
            }
            $docs = $database->skipRelationships(fn () => $database->find('articles', $queries));
            $count = count($docs);
            $total += $count;
            if ($count > 0) {
                $cursor = $docs[$count - 1];
            }
        } while ($count === 100);
        return range(1, $total);
    },
    $warmup,
    $runs
);

echo "\n--- M2M Relationship Queries ---\n\n";

// Pick IDs
$singleArticleId = $allArticleIds[0];
$threeArticleIds = [$allArticleIds[0], $allArticleIds[1], $allArticleIds[2]];
$twoArticleIds   = [$allArticleIds[0], $allArticleIds[1]];

// 1) equal('articles.$id', [single])
bench(
    "equal('articles.\$id', [1 val])",
    fn () => $database->find('authors', [
        Query::equal('articles.$id', [$singleArticleId]),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 2) equal('articles.$id', [3 values])
bench(
    "equal('articles.\$id', [3 vals])",
    fn () => $database->find('authors', [
        Query::equal('articles.$id', $threeArticleIds),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 3) containsAll('articles.$id', [2 values])
bench(
    "containsAll('articles.\$id', [2 vals])",
    fn () => $database->find('authors', [
        Query::containsAll('articles.$id', $twoArticleIds),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 4) Reverse: find articles by author
bench(
    "equal('authors.\$id', [1 val]) [reverse]",
    fn () => $database->find('articles', [
        Query::equal('authors.$id', ['author_000']),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 5) equal('articles.$id', [1]) + attribute filter
bench(
    "equal('articles.\$id', [1]) + equal('name')",
    fn () => $database->find('authors', [
        Query::equal('articles.$id', [$singleArticleId]),
        Query::equal('name', ['Alice 0']),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 6) Non-$id attribute query (genre) — not optimized by subquery, baseline comparison
bench(
    "equal('articles.genre', ['fashion'])",
    fn () => $database->find('authors', [
        Query::equal('articles.genre', ['fashion']),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 7) Genre query with select (no relationship population)
bench(
    "equal('articles.genre', ['fashion']) + select",
    fn () => $database->find('authors', [
        Query::equal('articles.genre', ['fashion']),
        Query::select(['$id', 'name']),
        Query::limit(5000),
    ]),
    $warmup,
    $runs
);

// 8) Genre query with small limit
bench(
    "equal('articles.genre', ['fashion']) + limit(5)",
    fn () => $database->find('authors', [
        Query::equal('articles.genre', ['fashion']),
        Query::limit(5),
    ]),
    $warmup,
    $runs
);

echo "\n=== Done ===\n";

// Cleanup
$database->delete($dbName);
echo "Database cleaned up.\n";
