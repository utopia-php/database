<?php
include __DIR__ . '/../../../vendor/autoload.php';

use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Swoole\Runtime;

Runtime::enableCoroutine();

Co\run(function () {
    $pool = new PDOPool(
        (new PDOConfig())
            ->withHost('mariadb')
            ->withPort(3306)
            ->withDbName('shmuel')
            ->withCharset('utf8mb4')
            ->withUsername('root')
            ->withPassword('password')
            ->withOptions([
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_TIMEOUT => 3, // Seconds
                PDO::ATTR_PERSISTENT => true,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => true,
                PDO::ATTR_STRINGIFY_FETCHES => true,
            ]),
        64
    );

    go(function () use ($pool) {
        $pdo = $pool->get();
        require __DIR__. '/SwooleMariaDB.php';
        $pool->put($pdo);
    });


});
