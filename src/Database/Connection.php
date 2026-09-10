<?php

namespace Utopia\Database;

use Swoole\Database\DetectsLostConnections;
use Throwable;

/**
 * Provides utilities for detecting lost database connections.
 */
class Connection
{
    /**
     * @var array<string>
     */
    protected static array $errors = [
        'Max connect timeout reached',
        'server has gone away',
        'no connection to the server',
        'Lost connection',
        'is dead or not enabled',
        'Error while sending',
        'decryption failed or bad record mac',
        'server closed the connection unexpectedly',
        'SSL connection has been closed unexpectedly',
        'Error writing data to the connection',
        'Resource deadlock avoided',
        'Transaction() on null',
        'child connection forced to terminate due to client_idle_limit',
        'query_wait_timeout',
        'reset by peer',
        'Physical connection is not usable',
        'TCP Provider: Error code 0x68',
        'ORA-03114',
        'Packets out of order. Expected',
        'Adaptive Server connection failed',
        'Communication link failure',
        'connection is no longer usable',
        'Login timeout expired',
        'running with the --read-only option so it cannot execute this statement',
        'SQLSTATE[HY000] [2002] Connection refused',
    ];

    /**
     * Check if the given throwable was caused by a database connection error.
     *
     * @param Throwable $e The exception to inspect
     * @return bool
     */
    public static function hasError(Throwable $e): bool
    {
        if (\class_exists(DetectsLostConnections::class) && DetectsLostConnections::causedByLostConnection($e)) {
            return true;
        }

        $message = $e->getMessage();
        foreach (static::$errors as $needle) {
            if (\mb_strpos($message, $needle) !== false) {
                return true;
            }
        }

        return false;
    }
}
