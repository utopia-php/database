<?php

namespace Utopia\Database\Exception;

use Throwable;

class Unique extends Duplicate
{
    public function __construct(string $message, int|string $code = 0, ?Throwable $previous = null)
    {
        if ($message === 'Unique index violation') {
            $message = 'Document with the requested unique attributes already exists';
        }

        parent::__construct($message, $code, $previous);
    }
}
