<?php

namespace Utopia\Database\Cache;

final readonly class Registration
{
    public function __construct(
        public string $key,
        public string $field = '',
    ) {
    }
}
