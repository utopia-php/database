<?php

/**
 * @var CLI
 */
global $cli;

use Faker\Factory;
use Faker\Generator;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\CLI\CLI;
use Utopia\CLI\Console;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Validator\Numeric;
use Utopia\Validator\Text;

/**
 * @Example
 * docker compose exec tests bin/load --adapter=mariadb --limit=1000 --name=testing
 */

$cli
    ->task('load')
    ->desc('Load database with mock data for testing')
    ->param('adapter', '', new Text(0), 'Database adapter', false)
    ->param('limit', '', new Numeric(), 'Total number of records to add to database', false)
    ->param('name', 'myapp_'.uniqid(), new Text(0), 'Name of created database.', true)
    ->action(function ($adapter, $limit, $name) {
        $start = null;
        $namespace = '_ns';
        $cache = new Cache(new NoCache());

        Console::info("Filling {$adapter} with {$limit} records: {$name}");

        Swoole\Runtime::enableCoroutine();

        switch ($adapter) {
            case 'mariadb':
                Co\run(function () use (&$start, $limit, $name, $namespace, $cache) {
                    // can't use PDO pool to act above the database level e.g. creating schemas
                    $dbHost = 'mariadb';
                    $dbPort = '3306';
                    $dbUser = 'root';
                    $dbPass = 'password';

                    $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

                    $database = new Database(new MariaDB($pdo), $cache);
                    $database->setDatabase($name);
                    $database->setNamespace($namespace);

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
                            ->withDbName($name)
                            ->withCharset('utf8mb4')
                            ->withUsername('root')
                            ->withPassword('password'),
                        128
                    );

                    // A coroutine is assigned per 1000 documents
                    for ($i = 0; $i < $limit / 1000; $i++) {
                        \go(function () use ($pool, $faker, $name, $cache, $namespace) {
                            $pdo = $pool->get();

                            $database = new Database(new MariaDB($pdo), $cache);
                            $database->setDatabase($name);
                            $database->setNamespace($namespace);

                            // Each coroutine loads 1000 documents
                            for ($i = 0; $i < 1000; $i++) {
                                createDocument($database, $faker);
                            }

                            // Reclaim resources
                            $pool->put($pdo);
                            $database = null;
                        });
                    }
                });
                break;

            case 'mysql':
                Co\run(function () use (&$start, $limit, $name, $namespace, $cache) {
                    // can't use PDO pool to act above the database level e.g. creating schemas
                    $dbHost = 'mysql';
                    $dbPort = '3307';
                    $dbUser = 'root';
                    $dbPass = 'password';

                    $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MySQL::getPDOAttributes());

                    $database = new Database(new MySQL($pdo), $cache);
                    $database->setDatabase($name);
                    $database->setNamespace($namespace);

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
                            ->withPassword('password'),
                        128
                    );

                    // A coroutine is assigned per 1000 documents
                    for ($i = 0; $i < $limit / 1000; $i++) {
                        \go(function () use ($pool, $faker, $name, $cache, $namespace) {
                            $pdo = $pool->get();

                            $database = new Database(new MySQL($pdo), $cache);
                            $database->setDatabase($name);
                            $database->setNamespace($namespace);

                            // Each coroutine loads 1000 documents
                            for ($i = 0; $i < 1000; $i++) {
                                createDocument($database, $faker);
                            }

                            // Reclaim resources
                            $pool->put($pdo);
                            $database = null;
                        });
                    }
                });
                break;

            case 'mongodb':
                Co\run(function () use (&$start, $limit, $name, $namespace, $cache) {
                    $client = new Client(
                        $name,
                        'mongo',
                        27017,
                        'root',
                        'example',
                        false
                    );

                    $database = new Database(new Mongo($client), $cache);
                    $database->setDatabase($name);
                    $database->setNamespace($namespace);

                    // Outline collection schema
                    createSchema($database);

                    // Fill DB
                    $faker = Factory::create();

                    $start = microtime(true);

                    for ($i = 0; $i < $limit / 1000; $i++) {
                        go(function () use ($client, $faker, $name, $namespace, $cache) {
                            $database = new Database(new Mongo($client), $cache);
                            $database->setDatabase($name);
                            $database->setNamespace($namespace);

                            // Each coroutine loads 1000 documents
                            for ($i = 0; $i < 1000; $i++) {
                                createDocument($database, $faker);
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



$cli
    ->error()
    ->inject('error')
    ->action(function (Exception $error) {
        Console::error($error->getMessage());
    });


function createSchema(Database $database): void
{
    if ($database->exists($database->getDatabase())) {
        $database->delete($database->getDatabase());
    }
    $database->create();

    Authorization::setRole(Role::any()->toString());

    $database->createCollection('articles', permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ]);

    $database->createAttribute('articles', 'author', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'created', Database::VAR_DATETIME, 0, true, filters: ['datetime']);
    $database->createAttribute('articles', 'text', Database::VAR_STRING, 5000, true);
    $database->createAttribute('articles', 'genre', Database::VAR_STRING, 256, true);
    $database->createAttribute('articles', 'views', Database::VAR_INTEGER, 0, true);
    $database->createAttribute('articles', 'tags', Database::VAR_STRING, 0, true, array: true);
    $database->createIndex('articles', 'text', Database::INDEX_FULLTEXT, ['text']);
}

function createDocument($database, Generator $faker): void
{
    $database->createDocument('articles', new Document([
        // Five random users out of 10,000 get read access
        // Three random users out of 10,000 get mutate access
        '$permissions' => [
            Permission::read(Role::any()),
            Permission::read(Role::user($faker->randomNumber(9))),
            Permission::read(Role::user($faker->randomNumber(9))),
            Permission::read(Role::user($faker->randomNumber(9))),
            Permission::read(Role::user($faker->randomNumber(9))),
            Permission::create(Role::user($faker->randomNumber(9))),
            Permission::create(Role::user($faker->randomNumber(9))),
            Permission::create(Role::user($faker->randomNumber(9))),
            Permission::update(Role::user($faker->randomNumber(9))),
            Permission::update(Role::user($faker->randomNumber(9))),
            Permission::update(Role::user($faker->randomNumber(9))),
            Permission::delete(Role::user($faker->randomNumber(9))),
            Permission::delete(Role::user($faker->randomNumber(9))),
            Permission::delete(Role::user($faker->randomNumber(9))),
        ],
        'author' => $faker->name(),
        'created' => \Utopia\Database\DateTime::format($faker->dateTime()),
        'text' => $faker->realTextBetween(1000, 4000),
        'genre' => $faker->randomElement(['fashion', 'food', 'travel', 'music', 'lifestyle', 'fitness', 'diy', 'sports', 'finance']),
        'views' => $faker->randomNumber(6),
        'tags' => $faker->randomElements(['short', 'quick', 'easy', 'medium', 'hard'], $faker->numberBetween(1, 5)),
    ]));
}
