<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
use MongoDB\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;

$dbms = $argv[1];
$loadedDB = $argv[2];

// Implemented databases
$supported = [
    'mongodb',
    'mariadb'
];

// Check input
if (!in_array($dbms, $supported)) {
    echo "First argument must be one of: 'mongodb', 'mariadb'";
    return;
}

if (!$loadedDB) {
    echo "Second argument is the name of a filled database";
    return;
}

$database = null;

if ($dbms === 'mongodb') {
    $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
    $client = new Client('mongodb://mongo/',
        [
            'username' => 'root',
            'password' => 'example',
        ],
        $options
    );

    $cache = new Cache(new NoCache());

    $database = new Database(new MongoDB($client), $cache);
}

if ($dbms === 'mariadb') {
    $dbHost = 'mariadb';
    $dbPort = '3306';
    $dbUser = 'root';
    $dbPass = 'password';

    $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, [
        PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
        PDO::ATTR_TIMEOUT => 3, // Seconds
        PDO::ATTR_PERSISTENT => true,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]);

    $cache = new Cache(new NoCache());

    $database = new Database(new MariaDB($pdo), $cache);
}

$database->setNamespace($loadedDB);


// Create index
echo "Creating indexes\n";

echo "For query: text.search('Alice')\n";
$start = microtime(true);
$success = $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
$time = microtime(true) - $start;
echo "Completed in " . $time . "s\n";

echo "For query: [created.greater(1262322000), genre.equal('travel')]\n"; # Jan 1, 2010

$start = microtime(true);
$success = $database->createIndex('articles', 'createdGenre', Database::INDEX_KEY, ['created', 'genre'], [], [Database::ORDER_DESC, Database::ORDER_DESC]);
$time = microtime(true) - $start;
echo "Completed in " . $time . "s\n";

echo "For query: genre.equal('fashion', 'finance', 'sports')\n";

$start = microtime(true);
$success = $database->createIndex('articles', 'genre', Database::INDEX_KEY, ['genre'], [], [Database::ORDER_ASC]);
$time = microtime(true) - $start;
echo "Completed in " . $time . "s\n";

echo "For query: views.greater(100000)\n";

$start = microtime(true);
$success = $database->createIndex('articles', 'views', Database::INDEX_KEY, ['views'], [], [Database::ORDER_DESC]);
$time = microtime(true) - $start;
echo "Completed in " . $time . "s\n";

