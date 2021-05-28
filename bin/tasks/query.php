<?php

require_once '/usr/src/code/vendor/autoload.php';

use Faker\Factory;
use MongoDB\Client;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Adapter\MongoDB;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Validator\Authorization;

$dbms = $argv[1];
$loadedDB = $argv[2];

// Implemented databases
$supported = [
    'mongodb',
    'mariadb'
];

// Check input
if (!in_array($dbms, $supported)) {
    echo "First argument must be one of: 'mongodb', 'mariadb'";
    return;
}

if (!$loadedDB) {
    echo "Second argument is the name of a filled database";
    return;
}

$database = null;

if ($dbms === 'mongodb') {
    $options = ["typeMap" => ['root' => 'array', 'document' => 'array', 'array' => 'array']];
    $client = new Client('mongodb://mongo/',
        [
            'username' => 'root',
            'password' => 'example',
        ],
        $options
    );

    $cache = new Cache(new NoCache());

    $database = new Database(new MongoDB($client), $cache);
}

if ($dbms === 'mariadb') {
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
}

$database->setNamespace($loadedDB);

function runQueries($database, $limit = 25) {
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

    echo "Found " . count($documents) . " results";
    echo "\nCompleted in " . $time . "s\n";


    // Favorite genres
    echo "Running query: genre.equal('fashion', 'finance', 'sports')\n";

    $start = microtime(true);
    $documents = $database->find('articles', [
        new Query('genre', Query::TYPE_EQUAL, ['fashion', 'finance', 'sports']),
    ], $limit);
    $time = microtime(true) - $start;

    echo "Found " . count($documents) . " results";
    echo "\nCompleted in " . $time . "s\n";


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

$faker = Factory::create();

$user = $faker->numerify('user####');
echo "Changing role to '" . $user . "'\n";
Authorization::setRole($user);

runQueries($database);

$count = 100;
echo "Randomly generating " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}
echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

$count = 400;
echo "Randomly generating an additional " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}

echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

$count = 500;
echo "Randomly generating an additional " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}

echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

$count = 1000;
echo "Randomly generating an additional " . $count . " roles\n";

for ($i=0; $i < $count; $i++) {
    Authorization::setRole($faker->numerify('user####'));
}

echo "\nWith " . count(Authorization::getRoles()) . " roles: \n";
runQueries($database);

