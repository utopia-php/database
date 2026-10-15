<?php

namespace Utopia\Database\Cache;

final readonly class Entry
{
    public function __construct(
        public string $key,
        public string $collection,
        public string $field = '',
        public string $epoch = '',
        public string $slot = '',
    ) {
    }
}
