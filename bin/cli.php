<?php

// require_once __DIR__.'/init.php';
require_once '/usr/src/code/vendor/autoload.php';

use Utopia\CLI\CLI;
use Utopia\CLI\Console;

$cli = new CLI();

include 'tasks/load.php';
include 'tasks/index.php';
include 'tasks/query.php';

$cli->run();
