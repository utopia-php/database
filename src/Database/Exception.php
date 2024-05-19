<?php

namespace Utopia\Database;

use Throwable;

class Exception extends \Exception
{
    public function __construct(string $message, int|string $code = 0, Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}