<?php

/**
 * Comprehensive Operator Performance Benchmark
 *
 * This script benchmarks the performance of ALL operator types against traditional
 * read-modify-write approaches across different database adapters.
 *
 * @example
 * docker compose exec tests bin/operators --adapter=mariadb --iterations=1000
 * docker compose exec tests bin/operators --adapter=postgres --iterations=1000
 * docker compose exec tests bin/operators --adapter=sqlite --iterations=1000
 */

global $cli;

use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\CLI\Console;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\PDO;
use Utopia\Database\Validator\Authorization;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

$cli
    ->task('operators')
    ->desc('Benchmark operator performance vs traditional read-modify-write')
    ->param('adapter', '', new Text(0), 'Database adapter (mariadb, postgres, sqlite)')
    ->param('iterations', 1000, new Integer(true), 'Number of iterations per test', true)
    ->param('name', 'operator_benchmark_' . uniqid(), new Text(0), 'Name of test database', true)
    ->action(function (string $adapter, int $iterations, string $name) {
        $namespace = '_ns';
        $cache = new Cache(new NoCache());

        Console::info("=============================================================");
        Console::info("  OPERATOR PERFORMANCE BENCHMARK");
        Console::info("=============================================================");
        Console::info("Adapter: {$adapter}");
        Console::info("Iterations: {$iterations}");
        Console::info("Database: {$name}");
        Console::info("=============================================================\n");

        // ------------------------------------------------------------------
        // Adapter configuration
        // ------------------------------------------------------------------
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
                'user' => 'root',
                'pass' => 'password',
                'dsn' => static fn (string $host, int $port) => "pgsql:host={$host};port={$port}",
                'adapter' => Postgres::class,
                'attrs' => Postgres::getPDOAttributes(),
            ],
            'sqlite' => [
                'host' => ':memory:',
                'port' => 0,
                'user' => '',
                'pass' => '',
                'dsn' => static fn (string $host, int $port) => "sqlite::memory:",
                'adapter' => SQLite::class,
                'attrs' => [],
            ],
        ];

        if (!isset($dbAdapters[$adapter])) {
            Console::error("Adapter '{$adapter}' not supported. Available: mariadb, postgres, sqlite");
            return;
        }

        $cfg = $dbAdapters[$adapter];
        $dsn = ($cfg['dsn'])($cfg['host'], $cfg['port']);

        try {
            // Initialize database connection
            $pdo = new PDO($dsn, $cfg['user'], $cfg['pass'], $cfg['attrs']);

            $database = (new Database(new ($cfg['adapter'])($pdo), $cache))
                ->setDatabase($name)
                ->setNamespace($namespace);

            // Setup test environment
            setupTestEnvironment($database, $name);

            // Run all benchmarks
            $results = runAllBenchmarks($database, $iterations);

            // Display results
            displayResults($results, $adapter, $iterations);

            // Cleanup
            cleanup($database, $name);

            Console::success("\nBenchmark completed successfully!");

        } catch (\Throwable $e) {
            Console::error("Error: " . $e->getMessage());
            Console::error("Trace: " . $e->getTraceAsString());
            return;
        }
    });

/**
 * Setup test environment with collections and sample data
 */
function setupTestEnvironment(Database $database, string $name): void
{
    Console::info("Setting up test environment...");

    // Delete database if it exists
    if ($database->exists($name)) {
        $database->delete($name);
    }
    $database->create();

    Authorization::setRole(Role::any()->toString());

    // Create test collection
    $database->createCollection('operators_test', permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
        Permission::update(Role::any()),
        Permission::delete(Role::any()),
    ]);

    // Create attributes for all operator types
    // Numeric attributes
    $database->createAttribute('operators_test', 'counter', Database::VAR_INTEGER, 0, false, 0);
    $database->createAttribute('operators_test', 'score', Database::VAR_FLOAT, 0, false, 0.0);
    $database->createAttribute('operators_test', 'multiplier', Database::VAR_FLOAT, 0, false, 1.0);
    $database->createAttribute('operators_test', 'divider', Database::VAR_FLOAT, 0, false, 100.0);
    $database->createAttribute('operators_test', 'modulo_val', Database::VAR_INTEGER, 0, false, 100);
    $database->createAttribute('operators_test', 'power_val', Database::VAR_FLOAT, 0, false, 2.0);

    // String attributes
    $database->createAttribute('operators_test', 'text', Database::VAR_STRING, 500, false, 'initial');
    $database->createAttribute('operators_test', 'description', Database::VAR_STRING, 500, false, 'foo bar baz');

    // Boolean attributes
    $database->createAttribute('operators_test', 'active', Database::VAR_BOOLEAN, 0, false, true);

    // Array attributes
    $database->createAttribute('operators_test', 'tags', Database::VAR_STRING, 50, false, null, true, true);
    $database->createAttribute('operators_test', 'numbers', Database::VAR_INTEGER, 0, false, null, true, true);
    $database->createAttribute('operators_test', 'items', Database::VAR_STRING, 50, false, null, true, true);

    // Date attributes
    $database->createAttribute('operators_test', 'created_at', Database::VAR_DATETIME, 0, false, null, false, false, null, [], ['datetime']);
    $database->createAttribute('operators_test', 'updated_at', Database::VAR_DATETIME, 0, false, null, false, false, null, [], ['datetime']);

    Console::success("Test environment setup complete.\n");
}

/**
 * Run all operator benchmarks
 */
function runAllBenchmarks(Database $database, int $iterations): array
{
    $results = [];
    $failed = [];

    Console::info("Starting benchmarks...\n");

    // Helper function to safely run benchmarks
    $safeBenchmark = function (string $name, callable $benchmark) use (&$results, &$failed) {
        try {
            $results[$name] = $benchmark();
        } catch (\Throwable $e) {
            $failed[$name] = $e->getMessage();
            Console::warning("  ⚠️  {$name} failed: " . $e->getMessage());
        }
    };

    // Numeric operators
    $safeBenchmark('INCREMENT', fn () => benchmarkOperator(
        $database,
        $iterations,
        'INCREMENT',
        'counter',
        Operator::increment(1),
        function ($doc) {
            $doc->setAttribute('counter', $doc->getAttribute('counter', 0) + 1);
            return $doc;
        },
        ['counter' => 0]
    ));

    $safeBenchmark('DECREMENT', fn () => benchmarkOperator(
        $database,
        $iterations,
        'DECREMENT',
        'counter',
        Operator::decrement(1),
        function ($doc) {
            $doc->setAttribute('counter', $doc->getAttribute('counter', 100) - 1);
            return $doc;
        },
        ['counter' => 100]
    ));

    $safeBenchmark('MULTIPLY', fn () => benchmarkOperator(
        $database,
        $iterations,
        'MULTIPLY',
        'multiplier',
        Operator::multiply(1.1),
        function ($doc) {
            $doc->setAttribute('multiplier', $doc->getAttribute('multiplier', 1.0) * 1.1);
            return $doc;
        },
        ['multiplier' => 1.0]
    ));

    $safeBenchmark('DIVIDE', fn () => benchmarkOperator(
        $database,
        $iterations,
        'DIVIDE',
        'divider',
        Operator::divide(1.1),
        function ($doc) {
            $doc->setAttribute('divider', $doc->getAttribute('divider', 100.0) / 1.1);
            return $doc;
        },
        ['divider' => 100.0]
    ));

    $safeBenchmark('MODULO', fn () => benchmarkOperator(
        $database,
        $iterations,
        'MODULO',
        'modulo_val',
        Operator::modulo(7),
        function ($doc) {
            $val = $doc->getAttribute('modulo_val', 100);
            $doc->setAttribute('modulo_val', $val % 7);
            return $doc;
        },
        ['modulo_val' => 100]
    ));

    $safeBenchmark('POWER', fn () => benchmarkOperator(
        $database,
        $iterations,
        'POWER',
        'power_val',
        Operator::power(1.001),
        function ($doc) {
            $doc->setAttribute('power_val', pow($doc->getAttribute('power_val', 2.0), 1.001));
            return $doc;
        },
        ['power_val' => 2.0]
    ));

    // String operators
    $safeBenchmark('CONCAT', fn () => benchmarkOperator(
        $database,
        $iterations,
        'CONCAT',
        'text',
        Operator::concat('x'),
        function ($doc) {
            $doc->setAttribute('text', $doc->getAttribute('text', 'initial') . 'x');
            return $doc;
        },
        ['text' => 'initial']
    ));

    $safeBenchmark('REPLACE', fn () => benchmarkOperator(
        $database,
        $iterations,
        'REPLACE',
        'description',
        Operator::replace('foo', 'bar'),
        function ($doc) {
            $doc->setAttribute('description', str_replace('foo', 'bar', $doc->getAttribute('description', 'foo bar baz')));
            return $doc;
        },
        ['description' => 'foo bar baz']
    ));

    // Boolean operators
    $safeBenchmark('TOGGLE', fn () => benchmarkOperator(
        $database,
        $iterations,
        'TOGGLE',
        'active',
        Operator::toggle(),
        function ($doc) {
            $doc->setAttribute('active', !$doc->getAttribute('active', true));
            return $doc;
        },
        ['active' => true]
    ));

    // Array operators
    $safeBenchmark('ARRAY_APPEND', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_APPEND',
        'tags',
        Operator::arrayAppend(['new']),
        function ($doc) {
            $tags = $doc->getAttribute('tags', ['initial']);
            $tags[] = 'new';
            $doc->setAttribute('tags', $tags);
            return $doc;
        },
        ['tags' => ['initial']]
    ));

    $safeBenchmark('ARRAY_PREPEND', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_PREPEND',
        'tags',
        Operator::arrayPrepend(['first']),
        function ($doc) {
            $tags = $doc->getAttribute('tags', ['initial']);
            array_unshift($tags, 'first');
            $doc->setAttribute('tags', $tags);
            return $doc;
        },
        ['tags' => ['initial']]
    ));

    $safeBenchmark('ARRAY_INSERT', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_INSERT',
        'numbers',
        Operator::arrayInsert(1, 99),
        function ($doc) {
            $numbers = $doc->getAttribute('numbers', [1, 2, 3]);
            array_splice($numbers, 1, 0, [99]);
            $doc->setAttribute('numbers', $numbers);
            return $doc;
        },
        ['numbers' => [1, 2, 3]]
    ));

    $safeBenchmark('ARRAY_REMOVE', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_REMOVE',
        'tags',
        Operator::arrayRemove('unwanted'),
        function ($doc) {
            $tags = $doc->getAttribute('tags', ['keep', 'unwanted', 'also']);
            $tags = array_values(array_filter($tags, fn ($t) => $t !== 'unwanted'));
            $doc->setAttribute('tags', $tags);
            return $doc;
        },
        ['tags' => ['keep', 'unwanted', 'also']]
    ));

    $safeBenchmark('ARRAY_UNIQUE', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_UNIQUE',
        'tags',
        Operator::arrayUnique(),
        function ($doc) {
            $tags = $doc->getAttribute('tags', ['a', 'b', 'a', 'c', 'b']);
            $doc->setAttribute('tags', array_values(array_unique($tags)));
            return $doc;
        },
        ['tags' => ['a', 'b', 'a', 'c', 'b']]
    ));

    $safeBenchmark('ARRAY_INTERSECT', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_INTERSECT',
        'tags',
        Operator::arrayIntersect(['keep', 'this']),
        function ($doc) {
            $tags = $doc->getAttribute('tags', ['keep', 'remove', 'this']);
            $doc->setAttribute('tags', array_values(array_intersect($tags, ['keep', 'this'])));
            return $doc;
        },
        ['tags' => ['keep', 'remove', 'this']]
    ));

    $safeBenchmark('ARRAY_DIFF', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_DIFF',
        'tags',
        Operator::arrayDiff(['remove']),
        function ($doc) {
            $tags = $doc->getAttribute('tags', ['keep', 'remove', 'this']);
            $doc->setAttribute('tags', array_values(array_diff($tags, ['remove'])));
            return $doc;
        },
        ['tags' => ['keep', 'remove', 'this']]
    ));

    $safeBenchmark('ARRAY_FILTER', fn () => benchmarkOperator(
        $database,
        $iterations,
        'ARRAY_FILTER',
        'numbers',
        Operator::arrayFilter('greaterThan', 5),
        function ($doc) {
            $numbers = $doc->getAttribute('numbers', [1, 3, 5, 7, 9]);
            $doc->setAttribute('numbers', array_values(array_filter($numbers, fn ($n) => $n > 5)));
            return $doc;
        },
        ['numbers' => [1, 3, 5, 7, 9]]
    ));

    // Date operators
    $safeBenchmark('DATE_ADD_DAYS', fn () => benchmarkOperator(
        $database,
        $iterations,
        'DATE_ADD_DAYS',
        'created_at',
        Operator::dateAddDays(1),
        function ($doc) {
            $date = new \DateTime($doc->getAttribute('created_at', DateTime::now()));
            $date->modify('+1 day');
            $doc->setAttribute('created_at', DateTime::format($date));
            return $doc;
        },
        ['created_at' => DateTime::now()]
    ));

    $safeBenchmark('DATE_SUB_DAYS', fn () => benchmarkOperator(
        $database,
        $iterations,
        'DATE_SUB_DAYS',
        'updated_at',
        Operator::dateSubDays(1),
        function ($doc) {
            $date = new \DateTime($doc->getAttribute('updated_at', DateTime::now()));
            $date->modify('-1 day');
            $doc->setAttribute('updated_at', DateTime::format($date));
            return $doc;
        },
        ['updated_at' => DateTime::now()]
    ));

    $safeBenchmark('DATE_SET_NOW', fn () => benchmarkOperator(
        $database,
        $iterations,
        'DATE_SET_NOW',
        'updated_at',
        Operator::dateSetNow(),
        function ($doc) {
            $doc->setAttribute('updated_at', DateTime::now());
            return $doc;
        },
        ['updated_at' => DateTime::now()]
    ));

    // Report any failures
    if (!empty($failed)) {
        Console::warning("\n⚠️  Some benchmarks failed:");
        foreach ($failed as $name => $error) {
            Console::warning("  - {$name}: " . substr($error, 0, 100));
        }
    }

    return $results;
}

/**
 * Benchmark a single operator vs traditional approach
 */
function benchmarkOperator(
    Database $database,
    int $iterations,
    string $operatorName,
    string $attribute,
    Operator $operator,
    callable $traditionalModifier,
    array $initialData
): array {
    Console::info("Benchmarking {$operatorName}...");

    // Prepare test documents
    $docIdWith = 'bench_with_' . strtolower($operatorName);
    $docIdWithout = 'bench_without_' . strtolower($operatorName);

    // Create fresh documents for this test
    $baseData = array_merge([
        '$permissions' => [
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ],
    ], $initialData);

    $database->createDocument('operators_test', new Document(array_merge(['$id' => $docIdWith], $baseData)));
    $database->createDocument('operators_test', new Document(array_merge(['$id' => $docIdWithout], $baseData)));

    // Benchmark WITH operator
    $memBefore = memory_get_usage(true);
    $start = microtime(true);

    for ($i = 0; $i < $iterations; $i++) {
        $database->updateDocument('operators_test', $docIdWith, new Document([
            $attribute => $operator
        ]));
    }

    $timeWith = microtime(true) - $start;
    $memWith = memory_get_usage(true) - $memBefore;

    // Benchmark WITHOUT operator (traditional read-modify-write)
    $memBefore = memory_get_usage(true);
    $start = microtime(true);

    for ($i = 0; $i < $iterations; $i++) {
        $doc = $database->getDocument('operators_test', $docIdWithout);
        $doc = $traditionalModifier($doc);
        $database->updateDocument('operators_test', $docIdWithout, $doc);
    }

    $timeWithout = microtime(true) - $start;
    $memWithout = memory_get_usage(true) - $memBefore;

    // Cleanup test documents
    $database->deleteDocument('operators_test', $docIdWith);
    $database->deleteDocument('operators_test', $docIdWithout);

    // Calculate metrics
    $speedup = $timeWithout / $timeWith;
    $improvement = (($timeWithout - $timeWith) / $timeWithout) * 100;

    // Color code the speedup output
    if ($speedup > 1.0) {
        Console::success("  WITH operator: {$timeWith}s | WITHOUT operator: {$timeWithout}s | Speedup: {$speedup}x");
    } elseif ($speedup >= 0.85) {
        Console::warning("  WITH operator: {$timeWith}s | WITHOUT operator: {$timeWithout}s | Speedup: {$speedup}x");
    } else {
        Console::error("  WITH operator: {$timeWith}s | WITHOUT operator: {$timeWithout}s | Speedup: {$speedup}x");
    }

    return [
        'operator' => $operatorName,
        'attribute' => $attribute,
        'time_with' => $timeWith,
        'time_without' => $timeWithout,
        'memory_with' => $memWith,
        'memory_without' => $memWithout,
        'speedup' => $speedup,
        'improvement_percent' => $improvement,
        'iterations' => $iterations,
    ];
}

/**
 * Display formatted results table
 */
function displayResults(array $results, string $adapter, int $iterations): void
{
    Console::info("\n=============================================================");
    Console::info("  BENCHMARK RESULTS");
    Console::info("=============================================================");
    Console::info("Adapter: {$adapter}");
    Console::info("Iterations per test: {$iterations}");
    Console::info("=============================================================\n");

    // Calculate column widths
    $colWidths = [
        'operator' => 20,
        'with' => 12,
        'without' => 12,
        'speedup' => 10,
        'improvement' => 14,
        'mem_diff' => 12,
    ];

    // Header
    $header = sprintf(
        "%-{$colWidths['operator']}s %-{$colWidths['with']}s %-{$colWidths['without']}s %-{$colWidths['speedup']}s %-{$colWidths['improvement']}s %-{$colWidths['mem_diff']}s",
        'OPERATOR',
        'WITH (s)',
        'WITHOUT (s)',
        'SPEEDUP',
        'IMPROVEMENT',
        'MEM DIFF'
    );

    Console::info($header);
    Console::info(str_repeat('-', array_sum($colWidths) + 5));

    // Group results by category
    $categories = [
        'Numeric' => ['INCREMENT', 'DECREMENT', 'MULTIPLY', 'DIVIDE', 'MODULO', 'POWER'],
        'String' => ['CONCAT', 'REPLACE'],
        'Boolean' => ['TOGGLE'],
        'Array' => ['ARRAY_APPEND', 'ARRAY_PREPEND', 'ARRAY_INSERT', 'ARRAY_REMOVE', 'ARRAY_UNIQUE', 'ARRAY_INTERSECT', 'ARRAY_DIFF', 'ARRAY_FILTER'],
        'Date' => ['DATE_ADD_DAYS', 'DATE_SUB_DAYS', 'DATE_SET_NOW'],
    ];

    $totalSpeedup = 0;
    $totalCount = 0;

    foreach ($categories as $categoryName => $operators) {
        Console::info("\n{$categoryName} Operators:");

        foreach ($operators as $operatorName) {
            if (!isset($results[$operatorName])) {
                continue;
            }

            $result = $results[$operatorName];

            $timeWith = number_format($result['time_with'], 4);
            $timeWithout = number_format($result['time_without'], 4);
            $speedup = number_format($result['speedup'], 2);
            $improvement = number_format($result['improvement_percent'], 1);
            $memDiff = formatBytes($result['memory_without'] - $result['memory_with']);

            // Color code based on performance
            // Red: <0.85x (regression), Yellow: 0.85-1.0x (slower), Green: >1.0x (faster)
            $speedupDisplay = $result['speedup'] > 1.0 ? "\033[32m{$speedup}x\033[0m" :
                             ($result['speedup'] >= 0.85 ? "\033[33m{$speedup}x\033[0m" :
                             "\033[31m{$speedup}x\033[0m");

            // Color improvement percent consistently with speedup
            // Green: >0% (faster), Yellow: -15% to 0% (slower but acceptable), Red: <-15% (regression)
            $improvementDisplay = $result['improvement_percent'] > 0 ? "\033[32m+{$improvement}%\033[0m" :
                                 ($result['improvement_percent'] >= -15 ? "\033[33m{$improvement}%\033[0m" :
                                 "\033[31m{$improvement}%\033[0m");

            $row = sprintf(
                "  %-{$colWidths['operator']}s %-{$colWidths['with']}s %-{$colWidths['without']}s %-{$colWidths['speedup']}s %-{$colWidths['improvement']}s %-{$colWidths['mem_diff']}s",
                $operatorName,
                $timeWith,
                $timeWithout,
                $speedupDisplay,
                $improvementDisplay,
                $memDiff
            );

            Console::log($row);

            $totalSpeedup += $result['speedup'];
            $totalCount++;
        }
    }

    // Summary statistics
    $avgSpeedup = $totalCount > 0 ? $totalSpeedup / $totalCount : 0;

    Console::info("\n" . str_repeat('=', array_sum($colWidths) + 5));
    Console::info("SUMMARY:");
    Console::info("  Total operators tested: {$totalCount}");
    Console::info("  Average speedup: " . number_format($avgSpeedup, 2) . "x");

    // Performance insights
    Console::info("\n" . str_repeat('=', array_sum($colWidths) + 5));
    Console::info("PERFORMANCE INSIGHTS:");

    $fastest = array_reduce(
        $results,
        fn ($carry, $item) =>
        $carry === null || $item['speedup'] > $carry['speedup'] ? $item : $carry
    );

    $slowest = array_reduce(
        $results,
        fn ($carry, $item) =>
        $carry === null || $item['speedup'] < $carry['speedup'] ? $item : $carry
    );

    if ($fastest) {
        Console::success("  Fastest operator: {$fastest['operator']} (" . number_format($fastest['speedup'], 2) . "x speedup)");
    }

    if ($slowest) {
        Console::warning("  Slowest operator: {$slowest['operator']} (" . number_format($slowest['speedup'], 2) . "x speedup)");
    }

    Console::info("\n=============================================================\n");
}

/**
 * Format bytes to human-readable format
 */
function formatBytes(int $bytes): string
{
    if ($bytes === 0) {
        return '0 B';
    }

    $sign = $bytes < 0 ? '-' : '+';
    $bytes = abs($bytes);

    $units = ['B', 'KB', 'MB', 'GB'];
    $power = floor(log($bytes, 1024));
    $power = min($power, count($units) - 1);

    return $sign . round($bytes / pow(1024, $power), 2) . ' ' . $units[$power];
}

/**
 * Cleanup test environment
 */
function cleanup(Database $database, string $name): void
{
    Console::info("Cleaning up test environment...");

    try {
        if ($database->exists($name)) {
            $database->delete($name);
        }
        Console::success("Cleanup complete.");
    } catch (\Throwable $e) {
        Console::warning("Cleanup failed: " . $e->getMessage());
    }
}
