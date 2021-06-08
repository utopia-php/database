<?php

/**
 * @var CLI
 */
global $cli;

use Faker\Factory;
use MongoDB\Client;
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
    ->task('index')
    ->desc('Index mock data for testing queries')
    ->param('adapter', '', new Text(0), 'Database adapter', false)
    ->param('name', '', new Text(0), 'Name of created database.', false)
    ->action(function ($adapter, $name) {
        $database = null;

        switch ($adapter) {
            case 'mongodb':
                $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
                $client = new Client('mongodb://mongo/',
                    [
                        'username' => 'root',
                        'password' => 'example',
                    ],
                    $options
                );

                $database = new Database(new MongoDB($client), new Cache(new NoCache()));
                break;

            case 'mariadb':
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

                $database = new Database(new MariaDB($pdo), new Cache(new NoCache()));
                break;

            case 'mysql':
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

                $database = new Database(new MariaDB($pdo), new Cache(new NoCache()));
                break;

            default:
                Console::error('Adapter not supported');
                return;
        }

        $database->setNamespace($name);

        Console::info("For query: [created.greater(1262322000), genre.equal('travel')]");

        $start = microtime(true);
        $success = $database->createIndex('articles', 'createdGenre', Database::INDEX_KEY, ['created', 'genre'], [], [Database::ORDER_DESC, Database::ORDER_DESC]);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");


        Console::info("For query: genre.equal('fashion', 'finance', 'sports')");

        $start = microtime(true);
        $success = $database->createIndex('articles', 'genre', Database::INDEX_KEY, ['genre'], [], [Database::ORDER_ASC]);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");


        Console::info("For query: views.greater(100000)");

        $start = microtime(true);
        $success = $database->createIndex('articles', 'views', Database::INDEX_KEY, ['views'], [], [Database::ORDER_DESC]);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");


        Console::info("For query: text.search('Alice')");
        $start = microtime(true);
        $success = $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");
    });

