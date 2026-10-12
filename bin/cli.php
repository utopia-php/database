<?php

require_once __DIR__.'/../vendor/autoload.php';

use Utopia\CLI\CLI;
use Utopia\Console;

ini_set('memory_limit', '-1');

const PDO_ATTRIBUTES = [
    PDO::ATTR_TIMEOUT => 3,
    PDO::ATTR_PERSISTENT => true,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => true,
    PDO::ATTR_STRINGIFY_FETCHES => true,
];

$cli = new CLI();

include __DIR__.'/tasks/load.php';
include __DIR__.'/tasks/index.php';
include __DIR__.'/tasks/query.php';
include __DIR__.'/tasks/relationships.php';
include __DIR__.'/tasks/operators.php';

$cli
    ->error()
    ->inject('error')
    ->action(function (Throwable $error) {
        Console::error($error->getMessage());
    });

$cli->run();
