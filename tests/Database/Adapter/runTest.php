<?php

\Swoole\Runtime::enableCoroutine();

Co\run(function () {
    go(function ()  {
        require __DIR__. '/SwooleMariaDB.php';
    });
});
