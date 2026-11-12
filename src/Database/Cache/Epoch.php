<?php

namespace Utopia\Database\Cache;

/**
 * The epoch a collection's documents may be cached under, or none while a write blocks the collection, with the
 * time that write blocked it.
 */
final readonly class Epoch
{
    public function __construct(
        public ?string $value = null,
        public ?int $blockedAt = null,
    ) {
    }
}
