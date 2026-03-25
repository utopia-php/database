<?php

namespace Utopia\Database\Cache;

class CacheRegion
{
    public function __construct(
        public int $ttl = 3600,
        public bool $enabled = true,
    ) {
    }
}
