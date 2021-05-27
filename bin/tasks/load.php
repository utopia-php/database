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

// Constants
$limit = 250000;

// Mongodb
// $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
// $client = new Client('mongodb://mongo/',
//     [
//         'username' => 'root',
//         'password' => 'example',
//     ],
//     $options
// );

// $redis = new Redis();
// $redis->connect('redis', 6379);
// $redis->flushAll();
// $cache = new Cache(new RedisAdapter($redis));

// $database = new Database(new MongoDB($client), $cache);
// $database->setNamespace('myapp_'.uniqid());

// MariaDB
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

$redis = new Redis();
$redis->connect('redis', 6379);
$redis->flushAll();
$cache = new Cache(new RedisAdapter($redis));

$database = new Database(new MariaDB($pdo), $cache);
$database->setNamespace('myapp_'.uniqid());

// Outline collection schema
$database->create();
$database->createCollection('articles');
$database->createAttribute('articles', 'author', Database::VAR_STRING, 256, true);
$database->createAttribute('articles', 'created', Database::VAR_INTEGER, 0, true);
$database->createAttribute('articles', 'text', Database::VAR_STRING, 5000, true);
$database->createAttribute('articles', 'genre', Database::VAR_STRING, 256, true);
$database->createAttribute('articles', 'views', Database::VAR_INTEGER, 0, true);

// Fill DB
$faker = Factory::create();

$start = microtime(true);
echo 'Filling database with ' . $limit . " documents";
for ($i=0; $i < $limit; $i++) {
    $database->createDocument('articles', new Document([
        // Five random users out of 10,000 get read access
        '$read' => [$faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####')],
        // Three random users out of 10,000 get write access
        '$write' => ['*', $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####')],
        'author' => $faker->name(),
        'created' => $faker->unixTime(),
        'text' => $faker->realTextBetween(1000, 4000),
        'genre' => $faker->randomElement(['fashion', 'food', 'travel', 'music', 'lifestyle', 'fitness', 'diy', 'sports', 'finance']),
        'views' => $faker->randomNumber(6, false)
    ]));
    if ($i % 5000 === 0) {
        echo '.';
    }
}
$time = microtime(true) - $start;
echo "\nCompleted in " . $time . "s\n";

// // Create fulltext index
// echo "Creating index";

// $start = microtime(true);
// $success = $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
// $time = microtime(true) - $start;

// echo "\nCompleted in " . $time . "s\n";

// // Query documents
// echo "Querying\n";

// echo "Changing role to 'user4567'\n";
// Authorization::setRole('user4567');

// echo "Running query: text.search('time')\n";

// $start = microtime(true);
// $documents = $database->find('articles', [
//     new Query('text', Query::TYPE_SEARCH, ['time']),
// ]);
// $time = microtime(true) - $start;

// echo "Found " . count($documents) . " results";
// echo "\nCompleted in " . $time . "s\n";

