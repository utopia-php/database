<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;

final class InvalidationCache extends Cache
{
    public function __construct()
    {
        parent::__construct(new Memory());
    }
}
