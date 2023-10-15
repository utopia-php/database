<?php

namespace Utopia\Database;

class Exception extends \Exception
{
    public function setFile(string $file): void
    {
        $this->file = $file;
    }
    public function setLine(int $line): void
    {
        $this->line = $line;
    }
}
