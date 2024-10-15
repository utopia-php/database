<?php

require_once '/usr/src/code/vendor/autoload.php';

use Utopia\CLI\CLI;
use Utopia\CLI\Console;

ini_set('memory_limit', '-1');

$cli = new CLI();

include 'tasks/load.php';
include 'tasks/index.php';
include 'tasks/query.php';
include 'tasks/coverage.php';

$cli
    ->error()
    ->inject('error')
    ->action(function (Exception $error) {
        Console::error($error->getMessage());
    });

$cli->run();
