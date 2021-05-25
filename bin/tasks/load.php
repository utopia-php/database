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
use Utopia\Database\Validator\Authorization;
use MongoDB\Client;

// Constants
$limit = 35000;

// DB options
$options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
$client = new Client('mongodb://mongo/',
    [
        'username' => 'root',
        'password' => 'example',
    ],
    $options
);

// init database
$redis = new Redis();
$redis->connect('redis', 6379);
$redis->flushAll();
$cache = new Cache(new RedisAdapter($redis));

$database = new Database(new MongoDB($client), $cache);
$database->setNamespace('myapp_'.uniqid());

// Outline collection schema
$database->create();
$database->createCollection('articles');
$database->createAttribute('articles', 'author', Database::VAR_STRING, 256, true);
$database->createAttribute('articles', 'created', Database::VAR_INTEGER, 0, true);
$database->createAttribute('articles', 'text', Database::VAR_STRING, 5000, true);

// Fill DB
$faker = Factory::create();

$start = microtime(true);
echo 'Filling databases';
for ($i=0; $i < $limit; $i++) {
    $database->createDocument('articles', new Document([
        // Five random users out of 10,000 get read access
        '$read' => ['*', $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####')],
        // Three random users out of 10,000 get write access
        '$write' => ['*', $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####')],
        'author' => $faker->name(),
        'created' => $faker->unixTime(),
        'text' => $faker->realTextBetween(1000, 4000),
    ]));
    if ($i % 5000 === 0) {
        echo '.';
    }
}
$time = microtime(true) - $start;
echo "\nCompleted in " . $time . "s\n";

// Create fulltext index
echo "Creating index";

$start = microtime(true);
$success = $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
$time = microtime(true) - $start;

echo "\nCompleted in " . $time . "s\n";

// Query documents
echo "Querying\n";

$start = microtime(true);
$documents = $database->find('articles', [
    new Query('text', Query::TYPE_SEARCH, ['rich and famous']),
]);
$time = microtime(true) - $start;

echo "Found " . count($documents) . " results";
echo "\nCompleted in " . $time . "s\n";

echo "Changing role\n";
Authorization::setRole('user4567');

$start = microtime(true);
$documents = $database->find('articles', [
    new Query('text', Query::TYPE_SEARCH, ['rich and famous']),
]);
$time = microtime(true) - $start;

echo "Found " . count($documents) . " results";
echo "\nCompleted in " . $time . "s\n";

