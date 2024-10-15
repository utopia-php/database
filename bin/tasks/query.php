<?php

/**
 * @var CLI
 */ global $cli;

use Faker\Factory;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\CLI\CLI;
use Utopia\CLI\Console;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Database;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Validator\Numeric;
use Utopia\Validator\Text;

/**
 * @Example
 * docker compose exec tests bin/query --adapter=mariadb --limit=1000 --name=testing
 */
$cli
    ->task('query')
    ->desc('Query mock data')
    ->param('adapter', '', new Text(0), 'Database adapter')
    ->param('name', '', new Text(0), 'Name of created database.')
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
                    'example',
                    false
                );

                $database = new Database(new Mongo($client), $cache);
                $database->setDatabase($name);
                $database->setNamespace($namespace);
                break;

            case 'mariadb':
                $dbHost = 'mariadb';
                $dbPort = '3306';
                $dbUser = 'root';
                $dbPass = 'password';

                $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

                $database = new Database(new MariaDB($pdo), $cache);
                $database->setDatabase($name);
                $database->setNamespace($namespace);
                break;

            case 'mysql':
                $dbHost = 'mysql';
                $dbPort = '3307';
                $dbUser = 'root';
                $dbPass = 'password';

                $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MySQL::getPDOAttributes());

                $database = new Database(new MySQL($pdo), $cache);
                $database->setDatabase($name);
                $database->setNamespace($namespace);
                break;

            default:
                Console::error('Adapter not supported');
                return;
        }

        $faker = Factory::create();

        $report = [];

        $count = setRoles($faker, 1);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = setRoles($faker, 100);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = setRoles($faker, 400);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = setRoles($faker, 500);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        $count = setRoles($faker, 1000);
        Console::info("\n{$count} roles:");
        $report[] = [
            'roles' => $count,
            'results' => runQueries($database, $limit)
        ];

        if (!file_exists('bin/view/results')) {
            \mkdir('bin/view/results', 0777, true);
        }

        $time = \time();
        $results = \fopen("bin/view/results/{$adapter}_{$name}_{$limit}_{$time}.json", 'w');
        \fwrite($results, \json_encode($report));
        \fclose($results);
    });

function setRoles($faker, $count): int
{
    for ($i = 0; $i < $count; $i++) {
        Authorization::setRole($faker->numerify('user####'));
    }
    return \count(Authorization::getRoles());
}

function runQueries(Database $database, int $limit): array
{
    $results = [];

    // Recent travel blogs
    $results["Querying greater than, equal[1] and limit"] = runQuery([
        Query::greaterThan('created', '2010-01-01 05:00:00'),
        Query::equal('genre', ['travel']),
        Query::limit($limit)
    ], $database);

    // Favorite genres
    $results["Querying equal[3] and limit"] = runQuery([
        Query::equal('genre', ['fashion', 'finance', 'sports']),
        Query::limit($limit)
    ], $database);

    // Popular posts
    $results["Querying greaterThan, limit({$limit})"] = runQuery([
        Query::greaterThan('views', 100000),
        Query::limit($limit)
    ], $database);

    // Fulltext search
    $results["Query search, limit({$limit})"] = runQuery([
        Query::search('text', 'Alice'),
        Query::limit($limit)
    ], $database);

    // Tags contain query
    $results["Querying contains[1], limit({$limit})"] = runQuery([
        Query::contains('tags', ['tag1']),
        Query::limit($limit)
    ], $database);

    return $results;
}

function runQuery(array $query, Database $database)
{
    $info = array_map(function (Query $q) {
        return $q->getAttribute() . ': ' . $q->getMethod() . ' = ' . implode(',', $q->getValues());
    }, $query);

    Console::log('Running query: [' . implode(', ', $info) . ']');
    $start = microtime(true);
    $database->find('articles', $query);
    $time = microtime(true) - $start;
    Console::success("{$time} s");
    return $time;
}
