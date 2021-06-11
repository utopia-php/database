<?php

/**
 * @var CLI
 */
global $cli;

use Faker\Factory;
use MongoDB\Client;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\CLI\CLI;
use Utopia\CLI\Console;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;
use Utopia\Validator\Numeric;
use Utopia\Validator\Text;

$cli
    ->task('load')
    ->desc('Load database with mock data for testing')
    ->param('adapter', '', new Text(0), 'Database adapter', false)
    ->param('limit', '', new Numeric(), 'Total number of records to add to database', false)
    ->param('name', 'myapp_'.uniqid(), new Text(0), 'Name of created database.', true)
    ->action(function ($adapter, $limit, $name) {

        $start = null;
        Console::info("Filling {$adapter} with {$limit} records: {$name}");

        Swoole\Runtime::enableCoroutine();
        switch ($adapter) {
            case 'mariadb': 
                Co\run(function() use (&$start, $limit, $name) {
                    // can't use PDO pool to act above the database level e.g. creating schemas
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
                    $database->setNamespace($name);

                    // Outline collection schema
                    createSchema($database);

                    // reclaim resources
                    $database = null;
                    $pdo = null;

                    // Init Faker
                    $faker = Factory::create();

                    $start = microtime(true);

                    // create PDO pool for coroutines
                    $pool = new PDOPool(
                        (new PDOConfig())
                            ->withHost('mariadb')
                            ->withPort(3306)
                            // ->withUnixSocket('/tmp/mysql.sock')
                            ->withDbName($name)
                            ->withCharset('utf8mb4')
                            ->withUsername('root')
                            ->withPassword('password')
                    , 128);

                    // A coroutine is assigned per 1000 documents
                    for ($i=0; $i < $limit/1000; $i++) {
                        go(function() use ($pool, $faker, $name, $cache) {
                            $pdo = $pool->get();

                            $database = new Database(new MariaDB($pdo), $cache);
                            $database->setNamespace($name);

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
                break;

            case 'mysql': 
                Co\run(function() use (&$start, $limit, $name) {
                    // can't use PDO pool to act above the database level e.g. creating schemas
                    $dbHost = 'mysql';
                    $dbPort = '3307';
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
                    $database->setNamespace($name);

                    // Outline collection schema
                    createSchema($database);

                    // reclaim resources
                    $database = null;
                    $pdo = null;

                    // Init Faker
                    $faker = Factory::create();

                    $start = microtime(true);

                    // create PDO pool for coroutines
                    $pool = new PDOPool(
                        (new PDOConfig())
                            ->withHost('mysql')
                            ->withPort(3307)
                            // ->withUnixSocket('/tmp/mysql.sock')
                            ->withDbName($name)
                            ->withCharset('utf8mb4')
                            ->withUsername('root')
                            ->withPassword('password')
                    , 128);

                    // A coroutine is assigned per 1000 documents
                    for ($i=0; $i < $limit/1000; $i++) {
                        go(function() use ($pool, $faker, $name, $cache) {
                            $pdo = $pool->get();

                            $database = new Database(new MariaDB($pdo), $cache);
                            $database->setNamespace($name);

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
                break;

            case 'mongodb':
                Co\run(function() use (&$start, $limit, $name) {
                    $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
                    $client = new Client('mongodb://mongo/',
                        [
                            'username' => 'root',
                            'password' => 'example',
                        ],
                        $options
                    );

                    $database = new Database(new MongoDB($client), new Cache(new NoCache()));
                    $database->setNamespace($name);

                    // Outline collection schema
                    createSchema($database);

                    // Fill DB
                    $faker = Factory::create();

                    $start = microtime(true);

                    for ($i=0; $i < $limit/1000; $i++) {
                        go(function() use ($client, $name, $faker) {
                            $database = new Database(new MongoDB($client), new Cache(new NoCache()));
                            $database->setNamespace($name);

                            // Each coroutine loads 1000 documents
                            for ($i=0; $i < 1000; $i++) {
                                addArticle($database, $faker);
                            }

                            $database = null;
                        });
                    }
                });
                break;

            default:
                echo 'Adapter not supported';
                return;
        }

        $time = microtime(true) - $start;
        Console::success("Completed in {$time} seconds");
    });

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
        '$write' => ['role:all', $faker->numerify('user####'), $faker->numerify('user####'), $faker->numerify('user####')],
        'author' => $faker->name(),
        'created' => $faker->unixTime(),
        'text' => $faker->realTextBetween(1000, 4000),
        'genre' => $faker->randomElement(['fashion', 'food', 'travel', 'music', 'lifestyle', 'fitness', 'diy', 'sports', 'finance']),
        'views' => $faker->randomNumber(6, false)
    ]));
}

