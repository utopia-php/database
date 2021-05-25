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
$limit = 1000000;

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
$database->setNamespace('myapp_60ad50d614c60');

// Query documents
echo "Querying\n";

echo "Changing role to 'user4567'\n";
Authorization::setRole('user4567');

echo "Running query: text.search('time')\n";

$start = microtime(true);
$documents = $database->find('articles', [
    // new Query('text', Query::TYPE_SEARCH, ['time']),
]);
$time = microtime(true) - $start;

echo "Found " . count($documents) . " results";
echo "\nCompleted in " . $time . "s\n";

