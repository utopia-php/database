<?php

\Swoole\Runtime::enableCoroutine();

go(function () {
    global $argc, $argv;
    require __DIR__. '/SwooleMariaDB.php';
});


