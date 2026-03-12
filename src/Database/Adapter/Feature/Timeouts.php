<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Database;

interface Timeouts
{
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void;
}
