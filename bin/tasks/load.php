<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
// use Redis;
// use MongoDB\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Adapter\MongoDB;

// Constants
$limit = 50000;

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
$database->createCollection('authors');
$database->createAttribute('authors', 'name', Database::VAR_STRING, 256, true);
$database->createAttribute('authors', 'address', Database::VAR_STRING, 512, true);


// Fill DB
$faker = Factory::create();




$start = microtime(true);
echo 'Filling databases';
for ($i=0; $i < 50000; $i++) {
    $database->createDocument('authors', new Document([
        '$read' => ['*'],
        '$write' => ['*'],
        'name' => $faker->name(),
        'address' => $faker->address()
    ]));
}
$end = microtime(true);
echo 'Completed in ' . ($end - $start) . 's';
