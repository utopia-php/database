<?php

/**
 * @var CLI
 */
global $cli;

use Utopia\Database\Adapter\MySQL;
use Utopia\Mongo\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\CLI\CLI;
use Utopia\CLI\Console;
use Utopia\Database\Database;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Validator\Text;

/**
 * @Example
 * docker-compose exec tests bin/index --adapter=mysql --name=testing
 */

$cli
    ->task('index')
    ->desc('Index mock data for testing queries')
    ->param('adapter', '', new Text(0), 'Database adapter', false)
    ->param('name', '', new Text(0), 'Name of created database.', false)
    ->action(function ($adapter, $name) {

        $namespace = '_ns';
        $cache = new Cache(new NoCache());

        switch ($adapter) {
            case 'mongodb':
              $client = new Client(
                  $name,
                  'mongo',
                  27017,
                  'root',
                  'example'
                  , false
               );

                $database = new Database(new Mongo($client), $cache);
                break;

            case 'mariadb':
                $dbHost = 'mariadb';
                $dbPort = '3306';
                $dbUser = 'root';
                $dbPass = 'password';

                $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

                $database = new Database(new MariaDB($pdo), $cache);
                break;

            case 'mysql':
                $dbHost = 'mysql';
                $dbPort = '3307';
                $dbUser = 'root';
                $dbPass = 'password';

                $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MySQL::getPDOAttributes());

                $database = new Database(new MySQL($pdo), $cache);
                break;

            default:
                Console::error('Adapter not supported');
                return;
        }

        $database->setDefaultDatabase($name);
        $database->setNamespace($namespace);

        Console::info("For query: greaterThan(created, 2010-01-01 05:00:00)', 'equal(genre,travel)");

        $start = microtime(true);
        $database->createIndex('articles', 'createdGenre', Database::INDEX_KEY, ['created', 'genre'], [], [Database::ORDER_DESC, Database::ORDER_DESC]);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");

        Console::info("equal('genre', ['fashion', 'finance', 'sports'])");

        $start = microtime(true);
        $database->createIndex('articles', 'genre', Database::INDEX_KEY, ['genre'], [], [Database::ORDER_ASC]);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");


        Console::info("greaterThan('views', 100000)");

        $start = microtime(true);
        $database->createIndex('articles', 'views', Database::INDEX_KEY, ['views'], [], [Database::ORDER_DESC]);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");


        Console::info("search('text', 'Alice')");
        $start = microtime(true);
        $database->createIndex('articles', 'fulltextsearch', Database::INDEX_FULLTEXT, ['text']);
        $time = microtime(true) - $start;
        Console::success("{$time} seconds");
    });

