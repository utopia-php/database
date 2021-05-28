<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
use MongoDB\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;

// mongodb
$options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
$client = new Client('mongodb://mongo/',
    [
        'username' => 'root',
        'password' => 'example',
    ],
    $options
);

$redis = new Redis();
$redis->connect('redis', 6379);
$redis->flushAll();
$cache = new Cache(new RedisAdapter($redis));

$database = new Database(new MongoDB($client), $cache);
$database->setNamespace('myapp_60afec3d936a0');


// MariaDB
// $dbHost = 'mariadb';
// $dbPort = '3306';
// $dbUser = 'root';
// $dbPass = 'password';

// $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, [
//     PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
//     PDO::ATTR_TIMEOUT => 3, // Seconds
//     PDO::ATTR_PERSISTENT => true,
//     PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
//     PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
// ]);

// $redis = new Redis();
// $redis->connect('redis', 6379);
// $redis->flushAll();
// $cache = new Cache(new RedisAdapter($redis));

// $database = new Database(new MariaDB($pdo), $cache);
// $database->setNamespace('myapp_60afd9a009280');

// Create index
echo "Creating indexes\n";

echo "For query: text.search('Alice')\n";
$start = microtime(true);
$success = $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
$time = microtime(true) - $start;
echo "Completed in " . $time . "s\n";

echo "For query: [created.greater(1262322000), genre.equal('travel')]\n"; # Jan 1, 2010

$start = microtime(true);
$success = $database->createIndex('articles', 'published', Database::INDEX_KEY, ['created', 'genre'], [], [Database::ORDER_DESC, Database::ORDER_DESC]);
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

