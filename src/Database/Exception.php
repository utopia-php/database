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
     * @param string $message The exception message
     * @param int|string $code The exception code (strings are cast to int)
     * @param Throwable|null $previous The previous throwable for chaining
     */
    public function __construct(string $message, int|string $code = 0, ?Throwable $previous = null)
    {
        if (\is_string($code)) {
            if (\is_numeric($code)) {
                $code = (int) $code;
            } else {
                $code = 0;
            }
        }

        parent::__construct($message, $code, $previous);
    }
}
