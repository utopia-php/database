<?php

require_once '/usr/src/code/vendor/autoload.php';

use Utopia\CLI\CLI;

ini_set('memory_limit', '-1');

$cli = new CLI();

include 'tasks/load.php';
include 'tasks/index.php';
include 'tasks/query.php';

$cli->run();
