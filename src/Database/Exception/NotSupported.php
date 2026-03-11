<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

class NotSupported extends Exception
{
    public function __construct(string $capability, string $adapter = '')
    {
        $message = $adapter !== ''
            ? "Capability '{$capability}' is not supported by adapter '{$adapter}'"
            : "Capability '{$capability}' is not supported";

        parent::__construct($message);
    }
}
