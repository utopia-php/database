<?php

require_once '/usr/src/code/vendor/autoload.php';

use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\CLI\CLI;
use Utopia\Console;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\PDO;
use Utopia\DI\Dependency;

ini_set('memory_limit', '-1');

$cli = new CLI();

$database = new Dependency();
$database
    ->setName('database')
    ->setCallback(static fn (): callable => static function (string $adapter, string $name, string $namespace, bool $sharedTables): Database {
        $adapters = [
            'mariadb' => [
                'dsn' => 'mysql:host=mariadb;port=3306;charset=utf8mb4',
                'user' => 'root',
                'password' => 'password',
                'class' => MariaDB::class,
            ],
            'mysql' => [
                'dsn' => 'mysql:host=mysql;port=3307;charset=utf8mb4',
                'user' => 'root',
                'password' => 'password',
                'class' => MySQL::class,
            ],
            'postgres' => [
                'dsn' => 'pgsql:host=postgres;port=5432',
                'user' => 'postgres',
                'password' => 'password',
                'class' => Postgres::class,
            ],
        ];

        $config = $adapters[$adapter] ?? throw new RuntimeException("Adapter '{$adapter}' not supported");
        $class = $config['class'];
        $pdo = new PDO(
            $config['dsn'],
            $config['user'],
            $config['password'],
            $class::getPDOAttributes(),
        );

        return (new Database(new $class($pdo), new Cache(new NoCache())))
            ->setDatabase($name)
            ->setNamespace($namespace)
            ->setSharedTables($sharedTables);
    });
$cli->setResource($database);

include 'tasks/load.php';
include 'tasks/index.php';
include 'tasks/query.php';
include 'tasks/relationships.php';
include 'tasks/operators.php';
include 'tasks/migrate.php';

$cli
    ->error()
    ->inject('error')
    ->action(function ($error) {
        Console::error($error->getMessage());
    });

$cli->run();
