<?php

namespace Utopia\Database\Cache;

class Region
{
    public function __construct(
        public int $ttl = 3600,
        public bool $enabled = true,
    ) {
    }
}
