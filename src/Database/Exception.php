<?php

namespace Utopia\Database;

use Exception as PhpException;
use Throwable;

/**
 * Base exception class for all database-related errors.
 */
class Exception extends PhpException
{
    /**
     * The SQLSTATE a driver reported as the code, which PHP's integer code cannot hold.
     */
    public readonly ?string $state;

    public function __construct(string $message = '', int|string $code = 0, ?Throwable $previous = null)
    {
        $this->state = \is_string($code) ? $code : null;

        parent::__construct($message, self::integerCode($code), $previous);
    }

    private static function integerCode(int|string $code): int
    {
        if (\is_int($code)) {
            return $code;
        }

        return \is_numeric($code) ? (int) $code : 0;
    }
}
