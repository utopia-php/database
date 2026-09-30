<?php

require_once __DIR__.'/../vendor/autoload.php';

use Utopia\CLI\CLI;
use Utopia\Console;

ini_set('memory_limit', '-1');

$cli = new CLI();

include __DIR__.'/tasks/load.php';
include __DIR__.'/tasks/index.php';
include __DIR__.'/tasks/query.php';
include __DIR__.'/tasks/relationships.php';
include __DIR__.'/tasks/operators.php';

$cli
    ->error()
    ->inject('error')
    ->action(function ($error) {
        Console::error($error->getMessage());
    });

$cli->run();
