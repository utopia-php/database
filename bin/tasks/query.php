<?php

/**
 * @var CLI
 */ global $cli;

use Faker\Factory;
use Utopia\Mongo\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\CLI\CLI;
use Utopia\CLI\Console;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Database;
use Utopia\Database\Query;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;
use Utopia\Validator\Numeric;
use Utopia\Validator\Text;

/**
 * @Example
 * docker-compose exec tests bin/query --adapter=mariadb --limit=1000 --name=testing
 */

//  docker-compose exec tests bin/query --adapter=mariadb --limit=10 --name=shmuel

$cli
    ->task('query')
    ->desc('Query mock data')
    ->param('adapter', '', new Text(0), 'Database adapter', false)
    ->param('name', '', new Text(0), 'Name of created database.', false)
    ->param('limit', 25, new Numeric(), 'Limit on queried documents', true)
    ->action(function (string $adapter, string $name, int $limit) {

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
                $database->setDefaultDatabase($name);
                $database->setNamespace($namespace);
                break;

            case 'mariadb':
                $dbHost = 'mariadb';
                $dbPort = '3306';
                $dbUser = 'root';
                $dbPass = 'password';

                $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

                $database = new Database(new MariaDB($pdo), $cache);
                $database->setDefaultDatabase($name);
                $database->setNamespace($namespace);
                break;

            case 'mysql':
                $dbHost = 'mysql';
                $dbPort = '3307';
                $dbUser = 'root';
                $dbPass = 'password';

                $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MySQL::getPDOAttributes());

                $database = new Database(new MySQL($pdo), $cache);
                $database->setDefaultDatabase($name);
                $database->setNamespace($namespace);
                break;

            default:
                Console::error('Adapter not supported');
                return;
        }


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


    $cli
    ->error()
    ->inject('error')
    ->action(function (Exception $error) {
        Console::error($error->getMessage());
    });


function runQueries(Database $database, int $limit)
{
    $results = [];
    // Recent travel blogs

    $results[] = runQuery([
        Query::greaterThan('created', '2010-01-01 05:00:00'),
        Query::equal('genre', ['travel']),
        Query::limit($limit)
    ], $database);

    // Favorite genres

    $results[] = runQuery([
        Query::equal('genre', ['fashion', 'finance', 'sports']),
        Query::limit($limit)
    ], $database);

    // Popular posts

    $results[] = runQuery([
        Query::greaterThan('views', 100000),
        Query::limit($limit)
    ], $database);

    // Fulltext search

    $results[] = runQuery([
        Query::search('text', 'Alice'),
        Query::limit($limit)
    ], $database);

    return $results;
}

function addRoles($faker, $count)
{
    for ($i = 0; $i < $count; $i++) {
        Authorization::setRole($faker->numerify('user####'));
    }
    return count(Authorization::getRoles());
}

function runQuery(array $query, Database $database)
{
    $info = array_map(function ($q){
        /** @var $q Query */
        return $q->getAttribute() . ' : ' . $q->getMethod() . ' : ' . implode(',',$q->getValues());
    } , $query);

    Console::log('Running query: [' . implode(', ', $info) . ']');
    $start = microtime(true);
    $database->find('articles', $query);
    $time = microtime(true) - $start;
    Console::success("{$time} s");
    return $time;
}

