<?php

\Swoole\Runtime::enableCoroutine();

Co\run(function () {
    global $argc, $argv;
    require __DIR__. '/SwooleMariaDB.php';
});
