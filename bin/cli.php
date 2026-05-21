<?php

require_once '/usr/src/code/vendor/autoload.php';

use Utopia\CLI\CLI;
use Utopia\Console;

ini_set('memory_limit', '-1');

$cli = new CLI();

include 'tasks/index.php';
include 'tasks/load.php';
include 'tasks/operators.php';
include 'tasks/query.php';
include 'tasks/relationships.php';

$cli
    ->error()
    ->inject('error')
    ->action(function ($error) {
        Console::error($error->getMessage());
    });

$cli->run();
