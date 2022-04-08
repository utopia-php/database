<?php

/**
 * @var CLI
 */ global $cli;
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

        $faker = Factory::create();

        $report = [];

        $count = addRoles($faker, 1);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = addRoles($faker, 100);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = addRoles($faker, 400);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = addRoles($faker, 500);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = addRoles($faker, 1000);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        if (!file_exists('bin/view/results')) {
            mkdir('bin/view/results', 0777, true);
        }

        $time = time();
        $f = fopen("bin/view/results/{$adapter}_{$name}_{$limit}_{$time}.json", 'w');
        fwrite($f, json_encode($report));
        fclose($f);
    });

function runQueries($database, $limit) {
    $results = [];
    // Recent travel blogs
    $query = ["created.greater(1262322000)", "genre.equal('travel')"];
    $results[] = runQuery($query, $database, $limit);

    // Favorite genres
    $query = ["genre.equal('fashion, 'finance', 'sports')"];
    $results[] = runQuery($query, $database, $limit);

    // Popular posts
    $query = ["views.greater(100000)"];
    $results[] = runQuery($query, $database, $limit);

    // Fulltext search
    $query = ["text.search('Alice')"];
    $results[] = runQuery($query, $database, $limit);

    return $results;
}

function addRoles($faker, $count) {
    for ($i=0; $i < $count; $i++) {
        Authorization::setRole($faker->numerify('user####'));
    }
    return count(Authorization::getRoles());
}

function runQuery($query, $database, $limit) {
    Console::log('Running query: ['.implode(', ', $query).']');
    $query = array_map(function($q) {
        return Query::parse($q);
    }, $query);

    $start = microtime(true);
    $documents = $database->find('articles', $query, $limit);
    $time = microtime(true) - $start;
    Console::success("{$time} s");
    return $time;
}
