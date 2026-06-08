<?php

namespace Utopia\Database;

use Swoole\Database\DetectsLostConnections;

class Connection
{
    /**
     * @var array<string>
     */
    protected static array $errors = [
        'Max connect timeout reached'
    ];

    /**
     * @var array<string>
     */
    protected static array $transientConnectErrors = [
        'Access denied',
        'Max connect timeout reached',
        'Connection refused',
        'Too many connections',
    ];

    /**
     * Check if the given throwable was caused by a database connection error.
     *
     * @param \Throwable $e
     * @return bool
     */
    public static function hasError(\Throwable $e): bool
    {
        if (DetectsLostConnections::causedByLostConnection($e)) {
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

    /**
     * Check if the given throwable is a transient connection error that may
     * succeed on retry (e.g. ProxySQL returning "Access denied" transiently).
     *
     * @param \Throwable $e
     * @return bool
     */
    public static function isTransientConnectError(\Throwable $e): bool
    {
        $message = $e->getMessage();
        foreach (static::$transientConnectErrors as $needle) {
            if (\mb_strpos($message, $needle) !== false) {
                return true;
            }
        }

        return false;
    }
}
