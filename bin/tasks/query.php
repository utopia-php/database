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
    ->task('query')
    ->desc('Query mock data')
    ->param('adapter', '', new Text(0), 'Database adapter', false)
    ->param('name', '', new Text(0), 'Name of created database.', false)
    ->param('limit', 25, new Numeric(), 'Limit on queried documents', true)
    ->action(function ($adapter, $name, $limit) {
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

            default:
                Console::error('Adapter not supported');
                return;
        }

        $database->setNamespace($name);


        $faker = Factory::create();

        $user = $faker->numerify('user####');
        echo "Changing role to '" . $user . "'\n";
        Authorization::setRole($user);

        runQueries($database, $limit);

        // add another $count roles
        $count = 100;

        for ($i=0; $i < $count; $i++) {
            Authorization::setRole($faker->numerify('user####'));
        }
        echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
        runQueries($database, $limit);

        // add another $count roles
        $count = 400;

        for ($i=0; $i < $count; $i++) {
            Authorization::setRole($faker->numerify('user####'));
        }

        echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
        runQueries($database, $limit);

        // add another $count roles
        $count = 500;

        for ($i=0; $i < $count; $i++) {
            Authorization::setRole($faker->numerify('user####'));
        }

        echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
        runQueries($database, $limit);

        // add another $count roles
        $count = 1000;

        for ($i=0; $i < $count; $i++) {
            Authorization::setRole($faker->numerify('user####'));
        }

        echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
        runQueries($database, $limit);


    });

function runQueries($database, $limit) {
    /**
     * @var Document[]
     */
    $documents = null;


    // Recent travel blogs
    echo "Running query: [created.greater(1262322000), genre.equal('travel')]\n"; # Jan 1, 2010

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('created', Query::TYPE_GREATER, [1262322000]),
        new Query('genre', Query::TYPE_EQUAL, ['travel']),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results\n";
    echo $time." s\n";


    // Favorite genres
    echo "Running query: genre.equal('fashion', 'finance', 'sports')\n";

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('genre', Query::TYPE_EQUAL, ['fashion', 'finance', 'sports']),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results\n";
    echo $time." s\n";


    // Popular posts
    echo "Running query: views.greater(100000)\n";

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('views', Query::TYPE_GREATER, [100000]),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results";
    echo "\nCompleted in " . $time . "s\n";


    // Fulltext Search
    echo "Running query: text.search('Alice')\n";

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('text', Query::TYPE_SEARCH, ['Alice']),
        // new Query('author', Query::TYPE_SEARCH, ['Alice']),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results";
    echo "\nCompleted in " . $time . "s\n";
}

