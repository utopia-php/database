<?php

namespace Utopia\Database;

class Exception extends \Exception
{
    public function __construct(string $message = '', int $code = 0, string $file = '', int $line = 0)
    {
        $this->message = $message;
        $this->code = $code;
        $this->file = $file;
        $this->line = $line;
    }
}
