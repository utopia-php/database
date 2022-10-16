<?php

require_once './vendor/autoload.php';

use Utopia\Database\Adapter\ClickHouse;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Database;

$dbHost = 'clickhouse';
$dbPort = '9004';
$dbUser = 'default';
$dbPass = 'password';

$pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, ClickHouse::getPdoAttributes());
$redis = new Redis();
$redis->connect('redis', 6379);
$redis->flushAll();
$cache = new Cache(new RedisAdapter($redis));

$database = new Database(new ClickHouse($pdo), $cache);
$database->setDefaultDatabase('utopiaTests');
$database->setNamespace('myapp_'.uniqid());

// Check if database exists
$res = $database->exists('utopiaTests');
var_dump($res);

$res = $database->exists('notExists');
var_dump($res);

// Create a new Database
$res = $database->create('temporary');
var_dump($res);

$res = $database->exists('temporary', '_metadata');
var_dump($res);

// Create a new collection