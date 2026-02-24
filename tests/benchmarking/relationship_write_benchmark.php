<?php

require_once __DIR__ . "/../../vendor/autoload.php";

use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\CLI\Console;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO as DbPDO;
use Utopia\Database\Validator\Authorization;

// Config levels
$level = strtoupper($argv[1] ?? 'MEDIUM');
$shape = strtolower($argv[2] ?? 'ids'); // 'ids' or 'docs'
$levels = [
    'LIGHT' => [
        'tags' => 100,
        'tags_per_post' => 20,
        'num_posts' => 5,
        'articles' => 80,
        'articles_per_author' => 20,
        'num_authors' => 5,
        'users' => 50,
        'num_profiles' => 10,
        'num_companies' => 5,
        'employees_per_company' => 20,
    ],
    'MEDIUM' => [
        'tags' => 300,
        'tags_per_post' => 60,
        'num_posts' => 12,
        'articles' => 300,
        'articles_per_author' => 40,
        'num_authors' => 8,
        'users' => 150,
        'num_profiles' => 20,
        'num_companies' => 6,
        'employees_per_company' => 40,
    ],
    'HEAVY' => [
        'tags' => 800,
        'tags_per_post' => 120,
        'num_posts' => 24,
        'articles' => 800,
        'articles_per_author' => 60,
        'num_authors' => 12,
        'users' => 400,
        'num_profiles' => 40,
        'num_companies' => 10,
        'employees_per_company' => 70,
    ],
];

if (!isset($levels[$level])) {
    Console::error("Invalid level: {$level}");
    exit(1);
}

/**
 * @param array<string, int> $config
 * @return array<string, mixed>
 */
function bench(array $config, string $shape): array
{
    Authorization::setRole('any');
    Authorization::setDefaultStatus(true);

    // DB + Cache
    $dbFile = sys_get_temp_dir() . '/rel-bench-' . uniqid() . '.db';
    @unlink($dbFile);
    $pdo = new DbPDO('sqlite:' . $dbFile, null, null, SQLite::getPDOAttributes());
    $adapter = new SQLite($pdo);

    $redis = new Redis();
    $redis->connect('redis', 6379);
    $redis->flushAll();
    $cache = new Cache(new RedisAdapter($redis));

    $database = new Database($adapter, $cache);
    $database->setDatabase('bench')->setNamespace('bench_' . uniqid());
    if ($database->exists()) {
        $database->delete();
    }
    $database->create();
    // no internal toggles; the "fast" flag determines whether we provide string IDs (fast) or Document objects (baseline) in relationship arrays

    // Query counter
    $queries = [
        'select' => 0,
        'insert' => 0,
        'update' => 0,
        'delete' => 0,
        'total' => 0,
    ];
    $database->getAdapter()->before(Database::EVENT_ALL, 'counter', function ($sql) use (&$queries) {
        $queries['total']++;
        $q = strtoupper(ltrim($sql));
        if (str_starts_with($q, 'SELECT')) {
            $queries['select']++;
        } elseif (str_starts_with($q, 'INSERT')) {
            $queries['insert']++;
        } elseif (str_starts_with($q, 'UPDATE')) {
            $queries['update']++;
        } elseif (str_starts_with($q, 'DELETE')) {
            $queries['delete']++;
        }
        return $sql;
    });

    // Schema
    $database->createCollection('posts');
    $database->createAttribute('posts', 'title', Database::VAR_STRING, 255, true);
    $database->createCollection('tags');
    $database->createAttribute('tags', 'name', Database::VAR_STRING, 100, true);
    $database->createRelationship('posts', 'tags', Database::RELATION_MANY_TO_MANY, true, 'tags', 'posts');

    $database->createCollection('authors');
    $database->createAttribute('authors', 'name', Database::VAR_STRING, 100, true);
    $database->createCollection('articles');
    $database->createAttribute('articles', 'title', Database::VAR_STRING, 255, true);
    $database->createRelationship('authors', 'articles', Database::RELATION_ONE_TO_MANY, true, 'articles', 'author');

    $database->createCollection('users');
    $database->createAttribute('users', 'username', Database::VAR_STRING, 100, true);
    $database->createCollection('profiles');
    $database->createAttribute('profiles', 'bio', Database::VAR_STRING, 255, true);
    $database->createRelationship('users', 'profiles', Database::RELATION_ONE_TO_ONE, false, 'profile');

    $database->createCollection('companies');
    $database->createAttribute('companies', 'name', Database::VAR_STRING, 100, true);
    $database->createCollection('employees');
    $database->createAttribute('employees', 'name', Database::VAR_STRING, 100, true);
    $database->createRelationship('employees', 'companies', Database::RELATION_MANY_TO_ONE, false, 'company');

    // Seed referenced docs
    $tagIds = [];
    for ($i = 1; $i <= $config['tags']; $i++) {
        $tagIds[] = $database->createDocument('tags', new Document([
            '$id' => 'tag' . $i,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Tag ' . $i,
        ]))->getId();
    }

    $articleIds = [];
    for ($i = 1; $i <= $config['articles']; $i++) {
        $articleIds[] = $database->createDocument('articles', new Document([
            '$id' => 'article' . $i,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Article ' . $i,
        ]))->getId();
    }

    $profileIds = [];
    for ($i = 1; $i <= $config['users']; $i++) {
        $profileIds[] = $database->createDocument('profiles', new Document([
            '$id' => 'profile' . $i,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'bio' => 'Bio ' . $i,
        ]))->getId();
    }

    $companyIds = [];
    for ($i = 1; $i <= $config['num_companies']; $i++) {
        $companyIds[] = $database->createDocument('companies', new Document([
            '$id' => 'company' . $i,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Company ' . $i,
        ]))->getId();
    }

    // Measure segments
    $result = [];

    // M2M
    $start = microtime(true);
    for ($i = 1; $i <= $config['num_posts']; $i++) {
        $offset = (($i - 1) * 7) % $config['tags'];
        $selected = array_slice($tagIds, $offset, $config['tags_per_post']);
        if ($shape === 'docs') {
            $selected = array_map(fn ($id) => new Document(['$id' => $id, '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]]), $selected);
        }
        $database->createDocument('posts', new Document([
            '$id' => 'post' . $i,
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Post ' . $i,
            'tags' => $selected,
        ]));
    }
    $result['m2m_time_ms'] = (microtime(true) - $start) * 1000;

    // O2M
    $start = microtime(true);
    for ($i = 1; $i <= $config['num_authors']; $i++) {
        $offset = ($i - 1) * $config['articles_per_author'];
        $selected = array_slice($articleIds, $offset, $config['articles_per_author']);
        if ($shape === 'docs') {
            $selected = array_map(fn ($id) => new Document(['$id' => $id, '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]]), $selected);
        }
        $database->createDocument('authors', new Document([
            '$id' => 'author' . $i,
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Author ' . $i,
            'articles' => $selected,
        ]));
    }
    $result['o2m_time_ms'] = (microtime(true) - $start) * 1000;

    // O2O
    $start = microtime(true);
    for ($i = 1; $i <= $config['num_profiles']; $i++) {
        $profile = $profileIds[($i * 3) % count($profileIds)];
        if ($shape === 'docs') {
            $profile = new Document(['$id' => $profile, '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]]);
        }
        $database->createDocument('users', new Document([
            '$id' => 'user' . $i,
            '$permissions' => [Permission::read(Role::any())],
            'username' => 'User ' . $i,
            'profile' => $profile,
        ]));
    }
    $result['o2o_time_ms'] = (microtime(true) - $start) * 1000;

    // M2O
    $start = microtime(true);
    for ($i = 1; $i <= $config['num_companies']; $i++) {
        $company = $companyIds[$i - 1];
        if ($shape === 'docs') {
            $company = new Document(['$id' => $company, '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]]);
        }
        for ($j = 1; $j <= $config['employees_per_company']; $j++) {
            $id = (($i - 1) * $config['employees_per_company']) + $j;
            $database->createDocument('employees', new Document([
                '$id' => 'employee' . $id,
                '$permissions' => [Permission::read(Role::any())],
                'name' => 'Employee ' . $id,
                'company' => $company,
            ]));
        }
    }
    $result['m2o_time_ms'] = (microtime(true) - $start) * 1000;

    $result['total_time_ms'] = $result['m2m_time_ms'] + $result['o2m_time_ms'] + $result['o2o_time_ms'] + $result['m2o_time_ms'];
    $result['queries'] = $queries;

    @unlink($dbFile);
    return $result;
}

$cfg = $levels[$level];

$res = bench($cfg, $shape);

// Simple key=value output mode for scripts
$kv = getenv('BENCH_KV');
if ($kv !== false && $kv !== '') {
    echo "M2M=" . (int) $res['m2m_time_ms'] . "\n";
    echo "O2M=" . (int) $res['o2m_time_ms'] . "\n";
    echo "O2O=" . (int) $res['o2o_time_ms'] . "\n";
    echo "M2O=" . (int) $res['m2o_time_ms'] . "\n";
    echo "TOTAL=" . (int) $res['total_time_ms'] . "\n";
    echo "QUERIES_TOTAL={$res['queries']['total']}\n";
    echo "SELECT={$res['queries']['select']}\n";
    echo "INSERT={$res['queries']['insert']}\n";
    echo "UPDATE={$res['queries']['update']}\n";
    echo "DELETE={$res['queries']['delete']}\n";
    exit(0);
}

Console::info("\nRunning relationship write benchmark at level {$level} (shape={$shape})\n");

Console::log("\nResults (Time)");
Console::log(sprintf("%-10s | %8dms", 'M2M', (int)$res['m2m_time_ms']));
Console::log(sprintf("%-10s | %8dms", 'O2M', (int)$res['o2m_time_ms']));
Console::log(sprintf("%-10s | %8dms", 'O2O', (int)$res['o2o_time_ms']));
Console::log(sprintf("%-10s | %8dms", 'M2O', (int)$res['m2o_time_ms']));
Console::log(str_repeat('-', 24));
Console::success(sprintf("%-10s | %8dms", 'TOTAL', (int)$res['total_time_ms']));

Console::log("\nQueries (total): {$res['queries']['total']}");
Console::log("Select: {$res['queries']['select']} | " .
     "Insert: {$res['queries']['insert']} | " .
     "Update: {$res['queries']['update']} | " .
     "Delete: {$res['queries']['delete']}");
