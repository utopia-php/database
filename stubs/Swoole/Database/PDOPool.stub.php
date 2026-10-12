<?php

namespace Swoole\Database;

class PDOPool
{
    public function __construct(PDOConfig $config, int $size = 64)
    {
    }

    public function get(): PDOProxy
    {
    }

    public function put(?PDOProxy $connection): void
    {
    }
}
