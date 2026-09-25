<?php

namespace Utopia\Database\Cache;

final readonly class Scope
{
    public function __construct(
        public string $hostname = '',
        public string $database = '',
        public string $namespace = '',
        public int|string|null $tenant = null,
    ) {
    }
}
