<?php

namespace Tests\Unit\Support;

use PDOException;
use Throwable;

/**
 * A PDOException as a driver raises it: the SQLSTATE as its code and the driver's error number in errorInfo.
 */
final class EngineError
{
    public static function create(string $state, int $code, string $message, ?Throwable $previous = null): PDOException
    {
        $error = new class ($message, $state, $previous) extends PDOException {
            public function __construct(string $message, string $state, ?Throwable $previous)
            {
                parent::__construct($message, 0, $previous);
                $this->code = $state;
            }
        };
        $error->errorInfo = [$state, $code, $message];

        return $error;
    }
}
