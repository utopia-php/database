<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
use MongoDB\Client;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoAdapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;

Swoole\Runtime::enableCoroutine();

$start = null;
Co\run(function() use ($start) {

    $pool = new PDOPool(
        (new PDOConfig())
            ->withHost('mariadb')
            ->withPort(3306)
            // ->withUnixSocket('/tmp/mysql.sock')
            ->withDbName('myapp_60b0e48f63005')
            ->withCharset('utf8mb4')
            ->withUsername('root')
            ->withPassword('password')
    , 128);

    // Constants
    $limit = 80000;

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

    $cache = new Cache(new NoAdapter());

    $uniqid = \uniqid();

    $database = new Database(new MariaDB($pdo), $cache);
    $database->setNamespace('myapp_'.$uniqid);


    // Outline collection schema
    $database->create();
    $database->createCollection('articles');
    $database->createAttribute('articles', 'author', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'created', Database::VAR_INTEGER, 0, true);
    $database->createAttribute('articles', 'text', Database::VAR_STRING, 5000, true);
    $database->createAttribute('articles', 'genre', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'views', Database::VAR_INTEGER, 0, true);

    $database = null;

    // Fill DB
    $faker = Factory::create();

    $start = microtime(true);
    echo 'Filling database with ' . $limit . " documents";

    for ($i=0; $i < $limit; $i++) {
        go(function() use ($pool, $faker, $uniqid, $cache) {
            $pdo = $pool->get();

            $database = new Database(new MariaDB($pdo), $cache);
            $database->setNamespace('myapp_'.$uniqid);

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
            // if ($i % 5000 === 0) {
            //     echo '.';
            // }

            $pool->put($pdo);
            $database = null;
        });
    }

});

$time = microtime(true) - $start;
echo "\nCompleted in " . $time . "s\n";
