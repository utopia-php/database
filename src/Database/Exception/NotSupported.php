<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

class NotSupported extends Exception
{
    public function __construct(string $capability, string $adapter)
    {
        parent::__construct("Capability '{$capability}' is not supported by adapter '{$adapter}'");
    }
}
