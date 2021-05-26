<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
// use Redis;
// use MongoDB\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;
use MongoDB\Client;

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
$database->setNamespace('myapp_60ad4d52a7c01');


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
// $database->setNamespace('myapp_60ae724abe58b');

// Create index
// echo "Creating indexes";

// $start = microtime(true);
// $success = $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
// $time = microtime(true) - $start;

// echo "\nCompleted in " . $time . "s\n";

// $start = microtime(true);
// $success = $database->createIndex('articles', 'published', Database::INDEX_KEY, ['created'], [], [Database::ORDER_DESC]);
// $time = microtime(true) - $start;

// echo "\nCompleted in " . $time . "s\n";

// Query documents


function runQueries($database, $limit = 25) {
    echo "Running query: text.search('Alice')\n";

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('text', Query::TYPE_SEARCH, ['Alice']),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results";
    echo "\nCompleted in " . $time . "s\n";

    echo "Running query: created.greater('1262322000')\n"; # Jan 1, 2010

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('created', Query::TYPE_GREATER, [1262322000]),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results";
    echo "\nCompleted in " . $time . "s\n";
    sleep(1);
}

$faker = Factory::create();

$user = $faker->numerify('user####');
echo "Changing role to '" . $user . "'\n";
Authorization::setRole($user);

runQueries($database);

$count = 100;
echo "Randomly generating " . $count . " roles\n";


for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}
echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

$count = 400;
echo "Randomly generating an additional " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}

echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

$count = 500;
echo "Randomly generating an additional " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}

echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

$count = 1000;
echo "Randomly generating an additional " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}

echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

