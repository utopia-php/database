<?php

namespace Swoole\Database;

use Throwable;

class DetectsLostConnections
{
    public static function causedByLostConnection(Throwable $error): bool
    {
    }
}
