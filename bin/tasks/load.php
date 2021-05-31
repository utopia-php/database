<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
use MongoDB\Client;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;

$adapter = $argv[1];
$limit = $argv[2];

// Implemented databases
$supported = [
    'mongodb',
    'mariadb'
];

if (!in_array($adapter, $supported)) {
    echo "First argument must be one of: 'mongodb', 'mariadb'";
    return;
}

function createSchema($database) {
    $database->create();
    $database->createCollection('articles');
    $database->createAttribute('articles', 'author', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'created', Database::VAR_INTEGER, 0, true);
    $database->createAttribute('articles', 'text', Database::VAR_STRING, 5000, true);
    $database->createAttribute('articles', 'genre', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'views', Database::VAR_INTEGER, 0, true);
}

function addArticle($database, $faker) {
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
}

$start = null;

// MariaDB
if ($adapter === 'mariadb') {
    Swoole\Runtime::enableCoroutine();
    Co\run(function() use (&$start, $limit) {

        $pool = new PDOPool(
            (new PDOConfig())
                ->withHost('mariadb')
                ->withPort(3306)
                // ->withUnixSocket('/tmp/mysql.sock')
                ->withDbName('mysql') // db required just to get started
                ->withCharset('utf8mb4')
                ->withUsername('root')
                ->withPassword('password')
        , 128);

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

        $uniqid = \uniqid();

        $database = new Database(new MariaDB($pdo), $cache);
        $database->setNamespace('myapp_'.$uniqid);
        echo 'Database created: myapp_'.$uniqid."\n";

        // Outline collection schema
        createSchema($database);
        $database = null; // Unsetting to reclaim connection

        // Init Faker
        $faker = Factory::create();

        $start = microtime(true);
        echo 'Filling database with ' . $limit . " documents";

        // A coroutine is assigned per 1000 documents
        for ($i=0; $i < $limit/1000; $i++) {
            go(function() use ($pool, $faker, $uniqid, $cache) {
                $pdo = $pool->get();

                $database = new Database(new MariaDB($pdo), $cache);
                $database->setNamespace('myapp_'.$uniqid);

                // Each coroutine loads 1000 documents
                for ($i=0; $i < 1000; $i++) {
                    addArticle($database, $faker);
                }

                // Reclaim resources
                $pool->put($pdo);
                $database = null;
            });
        }

    });
}

// MongoDB
if ($adapter === 'mongodb') {
    $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
    $client = new Client('mongodb://mongo/',
        [
            'username' => 'root',
            'password' => 'example',
        ],
        $options
    );

    $uniqid = \uniqid();

    $cache = new Cache(new NoCache());

    $database = new Database(new MongoDB($client), $cache);
    $database->setNamespace('myapp_'.$uniqid);
    echo 'Database created: myapp_'.$uniqid."\n";

    // Outline collection schema
    createSchema($database);

    // Fill DB
    $faker = Factory::create();

    $start = microtime(true);
    echo 'Filling database with ' . $limit . " documents";
    for ($i=0; $i < $limit; $i++) {
        addArticle($database, $faker);
    }
}

$time = microtime(true) - $start;
echo "\nCompleted in " . $time . "s\n";
